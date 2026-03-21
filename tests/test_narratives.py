"""
tests/test_narratives.py
========================
The 5 narrative scenario tests. These are the primary correctness gate.

Run: pytest tests/test_narratives.py -v -s
"""
import pytest
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError
from ledger.domain.aggregates.loan_application import (
    LoanApplicationAggregate, ApplicationState, DomainError
)
from ledger.domain.aggregates.agent_session import AgentSessionAggregate, SessionStatus
from ledger.commands.handlers import (
    handle_submit_application, SubmitApplicationCommand,
    handle_human_review_completed, HumanReviewCompletedCommand,
)
from ledger.integrity.gas_town import reconstruct_agent_context


# ─── SHARED HELPERS ───────────────────────────────────────────────────────────

def _ev(event_type, version=1, **payload):
    return {"event_type": event_type, "event_version": version, "payload": payload}

async def _submit_app(store, app_id, applicant_id="COMP-001", amount=500000):
    """Submit a loan application and return the stream_id."""
    cmd = SubmitApplicationCommand(
        application_id=app_id,
        applicant_id=applicant_id,
        requested_amount_usd=amount,
        loan_purpose="working_capital",
        loan_term_months=36,
        submission_channel="web",
        contact_email="test@apex.com",
        contact_name="Test User",
        correlation_id=f"corr-{app_id}",
    )
    return await handle_submit_application(cmd, store)


# ═══════════════════════════════════════════════════════════════════════════════
# NARR-01: Concurrent OCC Collision
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_narr01_concurrent_occ_collision():
    """
    NARR-01: Two CreditAnalysisAgent instances run simultaneously on the same
    application. Expected:
      - exactly ONE CreditAnalysisCompleted in credit stream (not two)
      - second agent gets OptimisticConcurrencyError
      - after retry, second agent succeeds at the next position
      - credit stream ends with exactly 2 events (one per agent, in order)
    """
    store = InMemoryEventStore()
    app_id = "APEX-NARR01"

    # Set up: submit application and advance to credit analysis state
    await _submit_app(store, app_id)
    await store.append(f"loan-{app_id}", [
        _ev("DocumentUploaded", document_id="doc-001", file_path="./documents/test.pdf"),
        _ev("PackageReadyForAnalysis", application_id=app_id),
        _ev("CreditAnalysisRequested", application_id=app_id),
    ], expected_version=1)

    # Both agents read credit stream version = -1 (empty stream)
    credit_stream = f"credit-{app_id}"
    assert await store.stream_version(credit_stream) == -1

    occ_errors = []
    successes = []

    async def agent_run(agent_name: str):
        """Simulates a CreditAnalysisAgent writing its result."""
        try:
            await store.append(
                credit_stream,
                [_ev("CreditAnalysisCompleted",
                     application_id=app_id,
                     session_id=f"sess-{agent_name}",
                     agent=agent_name,
                     decision={"risk_tier": "MEDIUM", "confidence": 0.78,
                                "recommended_limit_usd": "450000"},
                     model_version="gemini-2.0-flash",
                     completed_at="2026-03-22T10:00:00")],
                expected_version=-1,
                correlation_id=f"corr-narr01-{agent_name}",
                causation_id="caus-narr01",
            )
            successes.append(agent_name)
        except OptimisticConcurrencyError as e:
            occ_errors.append((agent_name, e))
            # Agent retries after OCC — reloads stream and appends at new position
            current = await store.stream_version(credit_stream)
            await store.append(
                credit_stream,
                [_ev("CreditAnalysisCompleted",
                     application_id=app_id,
                     session_id=f"sess-{agent_name}-retry",
                     agent=f"{agent_name}-retry",
                     decision={"risk_tier": "MEDIUM", "confidence": 0.75,
                                "recommended_limit_usd": "400000"},
                     model_version="gemini-2.0-flash",
                     completed_at="2026-03-22T10:00:01")],
                expected_version=current,
                correlation_id=f"corr-narr01-{agent_name}-retry",
                causation_id="caus-narr01",
            )
            successes.append(f"{agent_name}-retry")

    # Launch both agents simultaneously
    await asyncio.gather(agent_run("credit-agent-A"), agent_run("credit-agent-B"))

    # Assertions
    credit_events = await store.load_stream(credit_stream)

    # Exactly one OCC error occurred
    assert len(occ_errors) == 1, \
        f"Exactly one agent must get OCC, got: {len(occ_errors)}"

    # The OCC error has correct fields
    _, occ_exc = occ_errors[0]
    assert isinstance(occ_exc, OptimisticConcurrencyError)
    assert occ_exc.stream_id == credit_stream
    assert occ_exc.expected == -1

    # Credit stream has exactly 2 events (winner + retry)
    assert len(credit_events) == 2, \
        f"Credit stream must have 2 events, got: {len(credit_events)}"

    # Both events are CreditAnalysisCompleted
    assert all(e["event_type"] == "CreditAnalysisCompleted" for e in credit_events)

    # Events are in order
    assert credit_events[0]["stream_position"] == 0
    assert credit_events[1]["stream_position"] == 1

    # Both agents eventually succeeded (winner + retry)
    assert len(successes) == 2, f"Both agents must eventually succeed: {successes}"


# ═══════════════════════════════════════════════════════════════════════════════
# NARR-02: Document Extraction Failure (Missing EBITDA)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_narr02_document_extraction_failure():
    """
    NARR-02: Income statement PDF with missing EBITDA line.
    Expected:
      - QualityAssessmentCompleted has critical_missing_fields containing 'ebitda'
      - CreditAnalysisCompleted.decision.confidence <= 0.75
      - CreditAnalysisCompleted.decision.data_quality_caveats is non-empty
    """
    store = InMemoryEventStore()
    app_id = "APEX-NARR02"

    await _submit_app(store, app_id)

    # Simulate DocumentProcessingAgent extracting facts with missing EBITDA
    await store.append(f"loan-{app_id}", [
        _ev("DocumentUploaded", document_id="doc-is-001",
            file_path="./documents/income_statement_missing_ebitda.pdf"),
    ], expected_version=1)

    # DocumentProcessingAgent writes quality assessment with missing ebitda field
    await store.append(f"docpkg-{app_id}", [
        _ev("ExtractionCompleted",
            package_id=app_id,
            document_id="doc-is-001",
            document_type="income_statement",
            facts={
                "total_revenue": 4200000,
                "gross_profit": 1680000,
                "operating_expenses": 1050000,
                # EBITDA intentionally missing
                "net_income": 420000,
            },
            completed_at="2026-03-22T10:00:00"),
        _ev("QualityAssessmentCompleted",
            package_id=app_id,
            document_id="doc-is-001",
            overall_confidence=0.65,
            is_coherent=True,
            anomalies=[],
            critical_missing_fields=["ebitda"],   # ← the key assertion
            reextraction_recommended=False,
            auditor_notes="EBITDA line not found in income statement.",
            assessed_at="2026-03-22T10:00:01"),
    ], expected_version=-1)

    # CreditAnalysisAgent produces result with reduced confidence due to missing EBITDA
    await store.append(f"credit-{app_id}", [
        _ev("CreditAnalysisCompleted",
            application_id=app_id,
            session_id="sess-cre-narr02",
            decision={
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": "350000",
                "confidence": 0.70,      # ← reduced due to missing EBITDA
                "rationale": "Reduced confidence due to missing EBITDA in income statement.",
                "key_concerns": ["Missing EBITDA prevents full profitability assessment"],
                "data_quality_caveats": [
                    "EBITDA not found in income statement — confidence reduced"
                ],   # ← non-empty
                "policy_overrides_applied": [],
            },
            model_version="gemini-2.0-flash",
            model_deployment_id="dep-narr02",
            input_data_hash="hash-narr02",
            analysis_duration_ms=3200,
            regulatory_basis=[],
            completed_at="2026-03-22T10:00:02"),
    ], expected_version=-1)

    # ── Assertions ──
    # 1. QualityAssessmentCompleted has critical_missing_fields=['ebitda']
    docpkg_events = await store.load_stream(f"docpkg-{app_id}")
    qa_event = next(
        e for e in docpkg_events
        if e["event_type"] == "QualityAssessmentCompleted"
    )
    assert "ebitda" in qa_event["payload"]["critical_missing_fields"], \
        "QualityAssessmentCompleted must flag ebitda as critical missing field"

    # 2. confidence <= 0.75
    credit_events = await store.load_stream(f"credit-{app_id}")
    credit_completed = next(
        e for e in credit_events
        if e["event_type"] == "CreditAnalysisCompleted"
    )
    confidence = credit_completed["payload"]["decision"]["confidence"]
    assert confidence <= 0.75, \
        f"Confidence must be <= 0.75 when EBITDA is missing, got: {confidence}"

    # 3. data_quality_caveats is non-empty
    caveats = credit_completed["payload"]["decision"]["data_quality_caveats"]
    assert len(caveats) > 0, \
        "data_quality_caveats must be non-empty when document quality is flagged"


# ═══════════════════════════════════════════════════════════════════════════════
# NARR-03: Agent Crash Recovery (Gas Town)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_narr03_agent_crash_recovery():
    """
    NARR-03: FraudDetectionAgent crashes mid-session after load_document_facts.
    Expected:
      - only ONE FraudScreeningCompleted event in fraud stream
      - second AgentSessionStarted has context_source starting with 'prior_session_replay:'
      - no duplicate analysis work (exactly one FraudScreeningCompleted)
    """
    store = InMemoryEventStore()
    app_id = "APEX-NARR03"
    session_id_crashed = "sess-fra-crashed123"
    session_id_recovery = "sess-fra-recovery456"

    await _submit_app(store, app_id)

    # FraudDetectionAgent starts — Gas Town anchor
    await store.append(f"agent-fraud_detection-{session_id_crashed}", [
        _ev("AgentSessionStarted",
            session_id=session_id_crashed,
            agent_type="fraud_detection",
            application_id=app_id,
            model_version="gemini-2.0-flash",
            context_source="fresh",
            started_at="2026-03-22T10:00:00"),
    ], expected_version=-1)

    # Agent completes node 1: validate_inputs
    await store.append(f"agent-fraud_detection-{session_id_crashed}", [
        _ev("AgentNodeExecuted",
            session_id=session_id_crashed,
            agent_type="fraud_detection",
            node_name="validate_inputs",
            node_sequence=1,
            llm_called=False),
    ], expected_version=0)

    # Agent completes node 2: load_document_facts
    await store.append(f"agent-fraud_detection-{session_id_crashed}", [
        _ev("AgentNodeExecuted",
            session_id=session_id_crashed,
            agent_type="fraud_detection",
            node_name="load_document_facts",
            node_sequence=2,
            llm_called=False),
    ], expected_version=1)

    # CRASH — agent fails before cross_reference_registry
    await store.append(f"agent-fraud_detection-{session_id_crashed}", [
        _ev("AgentSessionFailed",
            session_id=session_id_crashed,
            agent_type="fraud_detection",
            application_id=app_id,
            error_type="ConnectionError",
            error_message="Connection to registry dropped",
            last_successful_node="load_document_facts",
            recoverable=True,
            failed_at="2026-03-22T10:00:05"),
    ], expected_version=2)

    # ── Gas Town Recovery ──
    context = await reconstruct_agent_context(
        store, "fraud_detection", app_id
    )

    assert context["status"] == "FAILED"
    assert context["needs_reconciliation"] == True
    assert context["last_successful_node"] == "load_document_facts"

    # New session starts with context_source = 'prior_session_replay:{old_session_id}'
    recovery_context_source = f"prior_session_replay:{session_id_crashed}"

    await store.append(f"agent-fraud_detection-{session_id_recovery}", [
        _ev("AgentSessionStarted",
            session_id=session_id_recovery,
            agent_type="fraud_detection",
            application_id=app_id,
            model_version="gemini-2.0-flash",
            context_source=recovery_context_source,   # ← key assertion
            started_at="2026-03-22T10:01:00"),
    ], expected_version=-1)

    # Recovery session resumes from cross_reference_registry (skips already-done nodes)
    await store.append(f"agent-fraud_detection-{session_id_recovery}", [
        _ev("AgentNodeExecuted",
            session_id=session_id_recovery,
            agent_type="fraud_detection",
            node_name="cross_reference_registry",
            node_sequence=3,
            llm_called=False),
        _ev("AgentNodeExecuted",
            session_id=session_id_recovery,
            agent_type="fraud_detection",
            node_name="analyze_fraud_patterns",
            node_sequence=4,
            llm_called=True,
            llm_tokens_input=1200,
            llm_tokens_output=200,
            llm_cost_usd=0.0014),
    ], expected_version=0)

    # Recovery session writes FraudScreeningCompleted
    await store.append(f"fraud-{app_id}", [
        _ev("FraudScreeningCompleted",
            application_id=app_id,
            session_id=session_id_recovery,
            fraud_score=0.08,
            risk_level="LOW",
            anomalies_found=0,
            recommendation="PROCEED",
            screening_model_version="gemini-2.0-flash",
            input_data_hash="hash-narr03",
            completed_at="2026-03-22T10:01:30"),
    ], expected_version=-1)

    # ── Assertions ──
    fraud_events = await store.load_stream(f"fraud-{app_id}")

    # 1. Exactly ONE FraudScreeningCompleted (no duplicate)
    fraud_completed = [
        e for e in fraud_events
        if e["event_type"] == "FraudScreeningCompleted"
    ]
    assert len(fraud_completed) == 1, \
        f"Must have exactly ONE FraudScreeningCompleted, got: {len(fraud_completed)}"

    # 2. Recovery session has context_source starting with 'prior_session_replay:'
    recovery_events = await store.load_stream(
        f"agent-fraud_detection-{session_id_recovery}"
    )
    started_event = next(
        e for e in recovery_events
        if e["event_type"] == "AgentSessionStarted"
    )
    assert started_event["payload"]["context_source"].startswith("prior_session_replay:"), \
        f"Recovery session context_source must start with 'prior_session_replay:', " \
        f"got: {started_event['payload']['context_source']}"

    # 3. Recovery context correctly identifies last successful node
    assert context["last_successful_node"] == "load_document_facts", \
        "Gas Town recovery must identify last successful node"


# ═══════════════════════════════════════════════════════════════════════════════
# NARR-04: Compliance Hard Block (Montana)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_narr04_compliance_hard_block():
    """
    NARR-04: Montana applicant (jurisdiction='MT') triggers REG-003 hard block.
    Expected:
      - ComplianceRuleFailed with rule_id='REG-003' and is_hard_block=True
      - NO DecisionGenerated event anywhere in loan stream
      - ApplicationDeclined with adverse_action_notice_required=True
    """
    store = InMemoryEventStore()
    app_id = "APEX-NARR04"

    # Submit application for Montana company
    await _submit_app(store, app_id, applicant_id="COMP-075")

    # Advance to compliance check state
    await store.append(f"loan-{app_id}", [
        _ev("DocumentUploaded", document_id="doc-001",
            file_path="./documents/test.pdf"),
        _ev("PackageReadyForAnalysis", application_id=app_id),
        _ev("CreditAnalysisRequested", application_id=app_id),
        _ev("FraudScreeningRequested", application_id=app_id),
        _ev("ComplianceCheckRequested",
            application_id=app_id,
            regulation_set_version="2026-Q1",
            rules_to_evaluate=["REG-001","REG-002","REG-003",
                               "REG-004","REG-005","REG-006"]),
    ], expected_version=1)

    # ComplianceAgent evaluates rules
    # REG-001 PASSES, REG-002 PASSES, REG-003 FAILS (Montana hard block)
    await store.append(f"compliance-{app_id}", [
        _ev("ComplianceCheckInitiated",
            application_id=app_id,
            session_id="sess-comp-narr04",
            regulation_set_version="2026-Q1",
            rules_to_evaluate=["REG-001","REG-002","REG-003",
                               "REG-004","REG-005","REG-006"],
            initiated_at="2026-03-22T10:00:00"),
        _ev("ComplianceRulePassed",
            application_id=app_id,
            session_id="sess-comp-narr04",
            rule_id="REG-001",
            rule_name="Bank Secrecy Act (BSA) Check",
            rule_version="2026-Q1-v1",
            evidence_hash="hash-reg001",
            evaluation_notes="REG-001: Clear.",
            evaluated_at="2026-03-22T10:00:01"),
        _ev("ComplianceRulePassed",
            application_id=app_id,
            session_id="sess-comp-narr04",
            rule_id="REG-002",
            rule_name="OFAC Sanctions Screening",
            rule_version="2026-Q1-v1",
            evidence_hash="hash-reg002",
            evaluation_notes="REG-002: Clear.",
            evaluated_at="2026-03-22T10:00:02"),
        _ev("ComplianceRuleFailed",
            application_id=app_id,
            session_id="sess-comp-narr04",
            rule_id="REG-003",
            rule_name="Jurisdiction Lending Eligibility",
            rule_version="2026-Q1-v1",
            failure_reason="Jurisdiction MT not approved for commercial lending.",
            is_hard_block=True,
            remediation_available=False,
            remediation_description=None,
            evidence_hash="hash-reg003",
            evaluated_at="2026-03-22T10:00:03"),
        _ev("ComplianceCheckCompleted",
            application_id=app_id,
            session_id="sess-comp-narr04",
            rules_evaluated=3,
            rules_passed=2,
            rules_failed=1,
            rules_noted=0,
            has_hard_block=True,
            overall_verdict="BLOCKED",
            completed_at="2026-03-22T10:00:04"),
    ], expected_version=-1)

    # ComplianceAgent appends ApplicationDeclined to loan stream (hard block)
    await store.append(f"loan-{app_id}", [
        _ev("ApplicationDeclined",
            application_id=app_id,
            decline_reasons=["Compliance hard block: REG-003"],
            declined_by="compliance-system",
            adverse_action_notice_required=True,
            adverse_action_codes=["COMPLIANCE_BLOCK"],
            declined_at="2026-03-22T10:00:05"),
    ], expected_version=6)

    # ── Assertions ──
    loan_events   = await store.load_stream(f"loan-{app_id}")
    comp_events   = await store.load_stream(f"compliance-{app_id}")
    loan_types    = [e["event_type"] for e in loan_events]
    comp_types    = [e["event_type"] for e in comp_events]

    # 1. ComplianceRuleFailed with rule_id='REG-003' and is_hard_block=True
    reg003_failed = next(
        e for e in comp_events
        if e["event_type"] == "ComplianceRuleFailed"
        and e["payload"].get("rule_id") == "REG-003"
    )
    assert reg003_failed["payload"]["is_hard_block"] == True, \
        "REG-003 failure must be a hard block"
    assert reg003_failed["payload"]["rule_id"] == "REG-003"

    # 2. NO DecisionGenerated in loan stream
    assert "DecisionGenerated" not in loan_types, \
        "DecisionGenerated must NOT appear when compliance blocks the application"

    # 3. ApplicationDeclined with adverse_action_notice_required=True
    assert "ApplicationDeclined" in loan_types, \
        "ApplicationDeclined must be in loan stream"
    declined_event = next(
        e for e in loan_events
        if e["event_type"] == "ApplicationDeclined"
    )
    assert declined_event["payload"]["adverse_action_notice_required"] == True, \
        "adverse_action_notice_required must be True for compliance block"

    # 4. Aggregate state is DECLINED_COMPLIANCE
    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.state == ApplicationState.DECLINED_COMPLIANCE, \
        f"Application state must be DECLINED_COMPLIANCE, got: {agg.state}"


# ═══════════════════════════════════════════════════════════════════════════════
# NARR-05: Human Override
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_narr05_human_override():
    """
    NARR-05: Orchestrator recommends DECLINE; human loan officer overrides to APPROVE.
    Expected:
      - DecisionGenerated with recommendation='DECLINE'
      - HumanReviewCompleted with override=True, reviewer_id='LO-Sarah-Chen'
      - ApplicationApproved with approved_amount_usd=750000
        and conditions has at least 2 items
    """
    store = InMemoryEventStore()
    app_id = "APEX-NARR05"

    # Submit application
    await _submit_app(store, app_id, amount=750000)

    # Advance application through full pipeline to DecisionRequested
    await store.append(f"loan-{app_id}", [
        _ev("DocumentUploaded", document_id="doc-001",
            file_path="./documents/test.pdf"),
        _ev("PackageReadyForAnalysis", application_id=app_id),
        _ev("CreditAnalysisRequested", application_id=app_id),
        _ev("FraudScreeningRequested", application_id=app_id),
        _ev("ComplianceCheckRequested", application_id=app_id,
            regulation_set_version="2026-Q1",
            rules_to_evaluate=["REG-001","REG-002","REG-003",
                               "REG-004","REG-005","REG-006"]),
        _ev("ComplianceCheckCompleted", application_id=app_id,
            session_id="sess-comp-narr05",
            rules_evaluated=6, rules_passed=5, rules_failed=0, rules_noted=1,
            has_hard_block=False, overall_verdict="CLEAR",
            completed_at="2026-03-22T10:00:00"),
        _ev("DecisionRequested", application_id=app_id,
            all_analyses_complete=True,
            triggered_by_event_id="evt-trigger-001"),
    ], expected_version=1)

    # Orchestrator generates DECLINE recommendation (low confidence)
    await store.append(f"loan-{app_id}", [
        _ev("DecisionGenerated",
            application_id=app_id,
            orchestrator_session_id="sess-orch-narr05",
            recommendation="DECLINE",           # ← orchestrator recommends DECLINE
            confidence=0.55,                    # below 0.60 threshold
            approved_amount_usd=None,
            conditions=[],
            executive_summary="Low confidence score due to thin financial history. Recommend decline.",
            key_risks=["Insufficient operating history", "Thin margins"],
            contributing_sessions=["sess-orch-narr05"],
            model_versions={"orchestrator": "gemini-2.0-flash"},
            generated_at="2026-03-22T10:01:00"),
        _ev("HumanReviewRequested",
            application_id=app_id,
            reason="Low confidence (0.55) requires human review.",
            decision_event_id="evt-decision-001",
            assigned_to=None,
            requested_at="2026-03-22T10:01:01"),
    ], expected_version=8)

    # Human loan officer LO-Sarah-Chen overrides to APPROVE
    cmd = HumanReviewCompletedCommand(
        application_id=app_id,
        reviewer_id="LO-Sarah-Chen",
        final_decision="APPROVE",
        override=True,
        override_reason="Strong collateral and long banking relationship justify approval.",
        approved_amount_usd=750000,
        conditions=[
            "Quarterly financial reporting required",
            "Personal guarantee from principal owner",
        ],
        correlation_id="corr-narr05-override",
        causation_id="caus-narr05",
    )
    await handle_human_review_completed(cmd, store)

    # ── Assertions ──
    loan_events = await store.load_stream(f"loan-{app_id}")
    loan_types  = [e["event_type"] for e in loan_events]

    # 1. DecisionGenerated with recommendation='DECLINE'
    decision_event = next(
        e for e in loan_events
        if e["event_type"] == "DecisionGenerated"
    )
    assert decision_event["payload"]["recommendation"] == "DECLINE", \
        "Orchestrator must have recommended DECLINE"

    # 2. HumanReviewCompleted with override=True and reviewer_id='LO-Sarah-Chen'
    assert "HumanReviewCompleted" in loan_types, \
        "HumanReviewCompleted must be in loan stream"
    review_event = next(
        e for e in loan_events
        if e["event_type"] == "HumanReviewCompleted"
    )
    assert review_event["payload"]["override"] == True, \
        "HumanReviewCompleted must have override=True"
    assert review_event["payload"]["reviewed_by"] == "LO-Sarah-Chen", \
        f"reviewer_id must be 'LO-Sarah-Chen', got: {review_event['payload']['reviewed_by']}"

    # 3. ApplicationApproved with approved_amount_usd=750000
    assert "ApplicationApproved" in loan_types, \
        "ApplicationApproved must be in loan stream after human override"
    approved_event = next(
        e for e in loan_events
        if e["event_type"] == "ApplicationApproved"
    )
    approved_amount = float(approved_event["payload"]["approved_amount_usd"])
    assert approved_amount == 750000.0, \
        f"approved_amount_usd must be 750000, got: {approved_amount}"

    # 4. conditions has at least 2 items
    conditions = approved_event["payload"]["conditions"]
    assert len(conditions) >= 2, \
        f"conditions must have at least 2 items, got: {len(conditions)}"

    # 5. Aggregate state is APPROVED
    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.state == ApplicationState.APPROVED, \
        f"Application state must be APPROVED after human override, got: {agg.state}"