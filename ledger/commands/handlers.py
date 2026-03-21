"""
ledger/commands/handlers.py
============================
Command handlers following the load → validate → determine → append pattern.

Every handler:
  1. Loads the aggregate by replaying its event stream
  2. Calls aggregate guard methods for ALL business rule validation
     (no validation logic lives in handlers — only in aggregates)
  3. Determines the new events to append
  4. Appends atomically with OCC, passing both correlation_id and causation_id

INTERIM REQUIRED:
  [x] handle_submit_application
  [x] handle_credit_analysis_completed

FINAL REQUIRED (Phase 3):
  [ ] handle_fraud_screening_completed
  [ ] handle_compliance_check_completed
  [ ] handle_generate_decision
  [ ] handle_human_review_completed
  [ ] handle_start_agent_session
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from ledger.domain.aggregates.loan_application import (
    LoanApplicationAggregate, ApplicationState, DomainError
)
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.event_store import (
    ApplicationSubmittedEvent,
    DocumentUploadRequestedEvent,
    CreditAnalysisCompletedEvent,
    FraudScreeningRequestedEvent,
)


# ─── Command dataclasses ──────────────────────────────────────────────────────

@dataclass
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class CreditAnalysisCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    risk_tier: str
    recommended_limit_usd: float
    confidence: float
    rationale: str
    key_concerns: list
    data_quality_caveats: list
    analysis_duration_ms: int
    input_data_hash: str
    model_deployment_id: str | None = None
    regulatory_basis: list | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class FraudScreeningCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    fraud_score: float
    risk_level: str
    anomalies_found: int
    recommendation: str
    screening_model_version: str
    input_data_hash: str
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class ComplianceCheckCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    rules_evaluated: int
    rules_passed: int
    rules_failed: int
    rules_noted: int
    has_hard_block: bool
    overall_verdict: str
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class GenerateDecisionCommand:
    application_id: str
    orchestrator_session_id: str
    recommendation: str
    confidence: float
    approved_amount_usd: float | None
    conditions: list
    executive_summary: str
    key_risks: list
    model_versions: dict
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class HumanReviewCompletedCommand:
    application_id: str
    reviewer_id: str
    final_decision: str           # APPROVE or DECLINE
    override: bool
    override_reason: str | None
    approved_amount_usd: float | None
    conditions: list
    correlation_id: str | None = None
    causation_id: str | None = None


# ─── Command handlers ─────────────────────────────────────────────────────────

async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store,
) -> str:
    """
    Handles a new loan application submission.

    Load → validate → determine → append pattern:
      1. Load LoanApplicationAggregate (NEW state for new applications)
      2. Validate via aggregate guard: application must not already exist
      3. Determine: ApplicationSubmitted + DocumentUploadRequested events
      4. Append atomically with correlation_id and causation_id

    Returns the stream_id for the new application.
    """
    stream_id = f"loan-{cmd.application_id}"

    # 1. Load aggregate
    agg = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. ALL validation lives in aggregate guard methods — not in handler
    agg.assert_not_terminal()
    if agg.state != ApplicationState.NEW:
        raise DomainError(
            f"Application {cmd.application_id} already exists in state {agg.state}."
        )

    # 3. Determine new events using typed event classes
    submitted = ApplicationSubmittedEvent.create(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.requested_amount_usd,
        loan_purpose=cmd.loan_purpose,
        loan_term_months=cmd.loan_term_months,
        submission_channel=cmd.submission_channel,
        contact_email=cmd.contact_email,
        contact_name=cmd.contact_name,
    )

    doc_requested = DocumentUploadRequestedEvent.create(
        application_id=cmd.application_id,
    )

    # 4. Append atomically — expected_version=-1 means new stream
    #    Always pass both correlation_id and causation_id
    await store.append(
        stream_id=stream_id,
        events=[submitted.to_dict(), doc_requested.to_dict()],
        expected_version=-1,
        correlation_id=cmd.correlation_id or str(uuid4()),
        causation_id=cmd.causation_id,
    )

    return stream_id


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store,
) -> None:
    """
    Records a completed credit analysis from the CreditAnalysisAgent.

    Load → validate → determine → append pattern:
      1. Load LoanApplicationAggregate + AgentSessionAggregate
      2. ALL validation via aggregate guard methods:
           - application in CREDIT_ANALYSIS_REQUESTED state
           - agent session has context loaded (Gas Town)
           - model version consistency
      3. Determine: CreditAnalysisCompleted on credit stream
                    FraudScreeningRequested on loan stream
      4. Append atomically with OCC, correlation_id, causation_id
    """
    # 1. Load both aggregates
    app   = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # 2. ALL validation in aggregate guard methods — zero logic in handler
    app.assert_awaiting_credit_analysis()
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)

    # 3. Determine new events using typed event classes
    credit_event = CreditAnalysisCompletedEvent.create(
        application_id=cmd.application_id,
        session_id=cmd.session_id,
        risk_tier=cmd.risk_tier,
        recommended_limit_usd=cmd.recommended_limit_usd,
        confidence=cmd.confidence,
        rationale=cmd.rationale,
        key_concerns=cmd.key_concerns or [],
        data_quality_caveats=cmd.data_quality_caveats or [],
        model_version=cmd.model_version,
        model_deployment_id=cmd.model_deployment_id or f"dep-{uuid4().hex[:8]}",
        input_data_hash=cmd.input_data_hash,
        analysis_duration_ms=cmd.analysis_duration_ms,
        regulatory_basis=cmd.regulatory_basis or [],
    )

    credit_event_id = str(uuid4())
    fraud_event = FraudScreeningRequestedEvent.create(
        application_id=cmd.application_id,
        triggered_by_event_id=credit_event_id,
    )

    corr_id = cmd.correlation_id or str(uuid4())

    # 4. Append to credit stream with OCC
    await store.append(
        stream_id=f"credit-{cmd.application_id}",
        events=[credit_event.to_dict()],
        expected_version=await store.stream_version(
            f"credit-{cmd.application_id}"
        ),
        correlation_id=corr_id,
        causation_id=cmd.causation_id,
    )

    # Append FraudScreeningRequested to loan stream with OCC
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[fraud_event.to_dict()],
        expected_version=app.version - 1,
        correlation_id=corr_id,
        causation_id=cmd.causation_id,
    )


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store,
) -> None:
    """
    Records completed fraud screening and triggers compliance check.
    """
    app   = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # ALL validation in aggregate guards
    app.assert_awaiting_fraud_screening()
    agent.assert_context_loaded()

    now = datetime.utcnow().isoformat()
    corr_id = cmd.correlation_id or str(uuid4())

    fraud_completed = {
        "event_type": "FraudScreeningCompleted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "session_id": cmd.session_id,
            "fraud_score": cmd.fraud_score,
            "risk_level": cmd.risk_level,
            "anomalies_found": cmd.anomalies_found,
            "recommendation": cmd.recommendation,
            "screening_model_version": cmd.screening_model_version,
            "input_data_hash": cmd.input_data_hash,
            "completed_at": now,
        }
    }

    compliance_requested = {
        "event_type": "ComplianceCheckRequested",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "requested_at": now,
            "triggered_by_event_id": str(uuid4()),
            "regulation_set_version": "2026-Q1",
            "rules_to_evaluate": [
                "REG-001", "REG-002", "REG-003",
                "REG-004", "REG-005", "REG-006",
            ],
        }
    }

    await store.append(
        stream_id=f"fraud-{cmd.application_id}",
        events=[fraud_completed],
        expected_version=await store.stream_version(
            f"fraud-{cmd.application_id}"
        ),
        correlation_id=corr_id,
        causation_id=cmd.causation_id,
    )

    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[compliance_requested],
        expected_version=app.version - 1,
        correlation_id=corr_id,
        causation_id=cmd.causation_id,
    )


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand,
    store,
) -> None:
    """
    Records a human loan officer's review decision.
    Supports NARR-05: human override of orchestrator recommendation.
    """
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # ALL validation in aggregate guards
    if app.state not in (
        ApplicationState.PENDING_HUMAN_REVIEW,
        ApplicationState.PENDING_DECISION,
    ):
        raise DomainError(
            f"Cannot complete human review: application is in state {app.state}."
        )

    now = datetime.utcnow().isoformat()
    corr_id = cmd.correlation_id or str(uuid4())

    review_completed = {
        "event_type": "HumanReviewCompleted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "reviewed_by": cmd.reviewer_id,
            "final_decision": cmd.final_decision,
            "override": cmd.override,
            "override_reason": cmd.override_reason,
            "reviewed_at": now,
        }
    }

    if cmd.final_decision == "APPROVE":
        app.assert_can_approve()
        outcome_event = {
            "event_type": "ApplicationApproved",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "approved_amount_usd": str(cmd.approved_amount_usd or 0),
                "interest_rate_pct": 7.5,
                "term_months": 36,
                "conditions": cmd.conditions or [],
                "approved_by": cmd.reviewer_id,
                "effective_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "approved_at": now,
            }
        }
    else:
        outcome_event = {
            "event_type": "ApplicationDeclined",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "decline_reasons": [
                    cmd.override_reason or "Human reviewer decision"
                ],
                "declined_by": cmd.reviewer_id,
                "adverse_action_notice_required": True,
                "adverse_action_codes": ["HUMAN_REVIEW_DECLINED"],
                "declined_at": now,
            }
        }

    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[review_completed, outcome_event],
        expected_version=app.version - 1,
        correlation_id=corr_id,
        causation_id=cmd.causation_id,
    )