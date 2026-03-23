"""
tests/phase6/test_bonus.py
===========================
Phase 6 tests — Bonus features.

Tests verify the WhatIfProjector concept and audit integrity
features that go beyond the core requirements.

Run: pytest tests/phase6/ -v
"""
import pytest
from ledger.event_store import InMemoryEventStore
from ledger.integrity.audit_chain import AuditChain, compute_event_hash
from ledger.integrity.gas_town import reconstruct_agent_context, find_crashed_sessions
from ledger.projections.compliance_audit import ComplianceAuditProjection


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _ev(event_type, **payload):
    return {"event_type": event_type, "event_version": 1, "payload": payload}


# ─── AUDIT CHAIN — INTEGRITY ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_audit_chain_detects_no_tampering():
    """AuditChain: verify_stream returns is_valid=True for clean stream."""
    store = InMemoryEventStore()
    await store.append("audit-test-001", [
        _ev("ApplicationSubmitted", application_id="BONUS-001"),
        _ev("CreditAnalysisCompleted", application_id="BONUS-001",
            decision={"confidence": 0.82}),
        _ev("ApplicationApproved", application_id="BONUS-001"),
    ], expected_version=-1)

    chain = AuditChain(store)
    result = await chain.verify_stream("audit-test-001")

    assert result.is_valid == True
    assert result.events_checked == 3
    assert result.broken_at is None

@pytest.mark.asyncio
async def test_audit_chain_hash_changes_with_payload():
    """AuditChain: different payloads produce different hashes."""
    event_a = {"event_type": "TestEvent", "stream_id": "s", "stream_position": 0,
               "event_id": "abc", "event_version": 1,
               "payload": {"amount": 100000}, "recorded_at": "2026-01-01"}
    event_b = {"event_type": "TestEvent", "stream_id": "s", "stream_position": 0,
               "event_id": "abc", "event_version": 1,
               "payload": {"amount": 999999},  # different amount
               "recorded_at": "2026-01-01"}

    hash_a = compute_event_hash(event_a, "GENESIS")
    hash_b = compute_event_hash(event_b, "GENESIS")

    assert hash_a != hash_b, "Modifying payload must produce different hash"

@pytest.mark.asyncio
async def test_audit_chain_empty_stream():
    """AuditChain: empty stream is valid with 0 events checked."""
    store = InMemoryEventStore()
    chain = AuditChain(store)
    result = await chain.verify_stream("nonexistent-stream")

    assert result.is_valid == True
    assert result.events_checked == 0

@pytest.mark.asyncio
async def test_audit_chain_stream_hash_consistent():
    """AuditChain: same stream always produces same final hash."""
    store = InMemoryEventStore()
    await store.append("audit-test-002", [
        _ev("ApplicationSubmitted", application_id="BONUS-002"),
        _ev("ApplicationApproved", application_id="BONUS-002"),
    ], expected_version=-1)

    chain = AuditChain(store)
    hash1 = await chain.compute_stream_hash("audit-test-002")
    hash2 = await chain.compute_stream_hash("audit-test-002")

    assert hash1 == hash2
    assert len(hash1) == 64  # SHA-256 produces 64-char hex


# ─── GAS TOWN — CRASH RECOVERY ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_find_crashed_sessions_returns_failed():
    """find_crashed_sessions: returns all sessions in FAILED state."""
    store = InMemoryEventStore()

    for i in range(3):
        sess_id = f"sess-crash-{i:04d}"
        await store.append(f"agent-fraud_detection-{sess_id}", [
            _ev("AgentSessionStarted",
                session_id=sess_id, agent_type="fraud_detection",
                application_id=f"BONUS-00{i}", model_version="gemini-2.0-flash",
                context_source="fresh", started_at="2026-03-22T10:00:00"),
            _ev("AgentSessionFailed",
                session_id=sess_id, agent_type="fraud_detection",
                application_id=f"BONUS-00{i}", error_type="TimeoutError",
                error_message="LLM timeout", last_successful_node="validate_inputs",
                recoverable=True, failed_at="2026-03-22T10:00:10"),
        ], expected_version=-1)

    crashed = await find_crashed_sessions(store)
    assert len(crashed) == 3
    assert all(s["recoverable"] == True for s in crashed)

@pytest.mark.asyncio
async def test_gas_town_recovery_identifies_resume_point():
    """Gas Town: recovery correctly identifies last successful node."""
    store = InMemoryEventStore()
    session_id = "sess-bonus-recovery"

    await store.append(f"agent-credit_analysis-{session_id}", [
        _ev("AgentSessionStarted",
            session_id=session_id, agent_type="credit_analysis",
            application_id="BONUS-010", model_version="gemini-2.0-flash",
            context_source="fresh", started_at="2026-03-22T10:00:00"),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="credit_analysis",
            node_name="validate_inputs", node_sequence=1, llm_called=False),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="credit_analysis",
            node_name="load_applicant_registry", node_sequence=2, llm_called=False),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="credit_analysis",
            node_name="load_extracted_facts", node_sequence=3, llm_called=False),
        _ev("AgentSessionFailed",
            session_id=session_id, agent_type="credit_analysis",
            application_id="BONUS-010", error_type="RateLimitError",
            error_message="Gemini rate limit hit", last_successful_node="load_extracted_facts",
            recoverable=True, failed_at="2026-03-22T10:00:30"),
    ], expected_version=-1)

    context = await reconstruct_agent_context(store, "credit_analysis", "BONUS-010")

    assert context["status"] == "FAILED"
    assert context["needs_reconciliation"] == True
    assert context["last_successful_node"] == "load_extracted_facts"
    assert context["session_id"] == session_id
    assert "prior_session_replay" in context["context_source"]

@pytest.mark.asyncio
async def test_gas_town_completed_session_no_reconciliation():
    """Gas Town: completed session does not need reconciliation."""
    store = InMemoryEventStore()
    session_id = "sess-bonus-complete"

    await store.append(f"agent-compliance-{session_id}", [
        _ev("AgentSessionStarted",
            session_id=session_id, agent_type="compliance",
            application_id="BONUS-011", model_version="gemini-2.0-flash",
            context_source="fresh", started_at="2026-03-22T10:00:00"),
        _ev("AgentSessionCompleted",
            session_id=session_id, agent_type="compliance",
            application_id="BONUS-011", total_llm_calls=0,
            total_tokens_used=0, total_cost_usd=0.0,
            total_duration_ms=1200, completed_at="2026-03-22T10:00:01"),
    ], expected_version=-1)

    context = await reconstruct_agent_context(store, "compliance", "BONUS-011")

    assert context["status"] == "COMPLETED"
    assert context["needs_reconciliation"] == False


# ─── WHATIF PROJECTOR (BONUS) ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_whatif_compliance_different_jurisdiction():
    """
    WhatIf Projector: replay events with modified jurisdiction to see
    how outcome would change.

    This demonstrates the WhatIfProjector concept — replaying a stream
    with one field changed to see a counterfactual outcome.
    """
    store = InMemoryEventStore()
    app_id = "BONUS-WHATIF-001"

    # Original: Montana company gets BLOCKED
    await store.append(f"loan-{app_id}", [
        _ev("ApplicationSubmitted",
            application_id=app_id, applicant_id="COMP-075",
            requested_amount_usd=500000, loan_purpose="working_capital"),
        _ev("ComplianceCheckCompleted",
            application_id=app_id, session_id="sess-whatif",
            rules_evaluated=3, rules_passed=2, rules_failed=1,
            rules_noted=0, has_hard_block=True, overall_verdict="BLOCKED",
            completed_at="2026-03-22T10:00:00"),
        _ev("ApplicationDeclined",
            application_id=app_id,
            decline_reasons=["Compliance hard block: REG-003"],
            declined_by="compliance-system",
            adverse_action_notice_required=True,
            adverse_action_codes=["COMPLIANCE_BLOCK"],
            declined_at="2026-03-22T10:00:01"),
    ], expected_version=-1)

    from ledger.domain.aggregates.loan_application import (
        LoanApplicationAggregate, ApplicationState
    )

    # Verify original outcome
    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.state == ApplicationState.DECLINED_COMPLIANCE

    # WhatIf: replay with jurisdiction='CA' instead of 'MT'
    # Simulate by creating a parallel stream with modified events
    whatif_app_id = f"{app_id}-WHATIF-CA"
    await store.append(f"loan-{whatif_app_id}", [
        _ev("ApplicationSubmitted",
            application_id=whatif_app_id, applicant_id="COMP-075",
            requested_amount_usd=500000, loan_purpose="working_capital"),
        _ev("ComplianceCheckCompleted",
            application_id=whatif_app_id, session_id="sess-whatif-ca",
            rules_evaluated=6, rules_passed=5, rules_failed=0,
            rules_noted=1, has_hard_block=False, overall_verdict="CLEAR",
            completed_at="2026-03-22T10:00:00"),
        _ev("DecisionRequested",
            application_id=whatif_app_id, all_analyses_complete=True,
            triggered_by_event_id="evt-whatif"),
    ], expected_version=-1)

    whatif_agg = await LoanApplicationAggregate.load(store, whatif_app_id)

    # Original: DECLINED_COMPLIANCE (Montana blocked)
    assert agg.state == ApplicationState.DECLINED_COMPLIANCE
    # WhatIf: would proceed to PENDING_DECISION (CA passes REG-003)
    assert whatif_agg.state == ApplicationState.PENDING_DECISION

    # The counterfactual shows the application would have proceeded
    # if the company were in California instead of Montana
    assert agg.state != whatif_agg.state, \
        "WhatIf projection must show different outcome for different jurisdiction"