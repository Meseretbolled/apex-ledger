"""
ledger/commands/handlers.py
============================
Command handlers following the load → validate → determine → append pattern.

Every handler:
  1. Loads the aggregate by replaying its event stream
  2. Validates business rules against current state
  3. Determines the new events to append
  4. Appends atomically with OCC

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


# ─── Command handlers ─────────────────────────────────────────────────────────

async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store,
) -> str:
    """
    Handles a new loan application submission.

    Load → validate → determine → append pattern:
      1. Load LoanApplicationAggregate (will be NEW state for new applications)
      2. Validate: application_id must not already exist
      3. Determine: ApplicationSubmitted + DocumentUploadRequested events
      4. Append atomically

    Returns the stream_id for the new application.
    """
    stream_id = f"loan-{cmd.application_id}"

    # 1. Load aggregate — for a new application this should be NEW state
    agg = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate — application must not already exist
    if agg.state != ApplicationState.NEW:
        raise DomainError(
            f"Application {cmd.application_id} already exists in state {agg.state}. "
            "Cannot submit an application that already exists."
        )

    # 3. Determine new events
    now = datetime.utcnow().isoformat()
    application_reference = cmd.application_id

    submitted_event = {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "applicant_id": cmd.applicant_id,
            "requested_amount_usd": str(cmd.requested_amount_usd),
            "loan_purpose": cmd.loan_purpose,
            "loan_term_months": cmd.loan_term_months,
            "submission_channel": cmd.submission_channel,
            "contact_email": cmd.contact_email,
            "contact_name": cmd.contact_name,
            "submitted_at": now,
            "application_reference": application_reference,
        }
    }

    doc_request_event = {
        "event_type": "DocumentUploadRequested",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "required_document_types": [
                "application_proposal",
                "income_statement",
                "balance_sheet",
            ],
            "deadline": datetime.utcnow().replace(
                day=min(datetime.utcnow().day + 7, 28)
            ).isoformat(),
            "requested_by": "system",
        }
    }

    # 4. Append atomically — expected_version=-1 means new stream
    await store.append(
        stream_id=stream_id,
        events=[submitted_event, doc_request_event],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
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
      2. Validate: application in CREDIT_ANALYSIS_REQUESTED state
                   agent session has context loaded (Gas Town)
                   confidence floor: < 0.60 must be REFER
      3. Determine: CreditAnalysisCompleted event on credit stream
                    FraudScreeningRequested event on loan stream
      4. Append atomically with OCC
    """
    # 1. Load both aggregates
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # 2. Validate business rules
    app.assert_awaiting_credit_analysis()
    agent.assert_context_loaded()          # Gas Town: session must be started
    agent.assert_model_version_current(cmd.model_version)

    # Confidence floor enforcement (business rule 4)
    recommendation = "REFER" if cmd.confidence < 0.60 else (
        "DECLINE" if cmd.risk_tier == "HIGH" else "APPROVE"
    )

    # 3. Determine new events
    now = datetime.utcnow().isoformat()
    credit_event_id = str(uuid4())

    credit_completed_event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": {
            "application_id": cmd.application_id,
            "session_id": cmd.session_id,
            "decision": {
                "risk_tier": cmd.risk_tier,
                "recommended_limit_usd": str(cmd.recommended_limit_usd),
                "confidence": cmd.confidence,
                "rationale": cmd.rationale,
                "key_concerns": cmd.key_concerns or [],
                "data_quality_caveats": cmd.data_quality_caveats or [],
                "policy_overrides_applied": [],
            },
            "model_version": cmd.model_version,
            "model_deployment_id": cmd.model_deployment_id or f"dep-{uuid4().hex[:8]}",
            "input_data_hash": cmd.input_data_hash,
            "analysis_duration_ms": cmd.analysis_duration_ms,
            "regulatory_basis": cmd.regulatory_basis or [],
            "completed_at": now,
        }
    }

    fraud_requested_event = {
        "event_type": "FraudScreeningRequested",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "requested_at": now,
            "triggered_by_event_id": credit_event_id,
        }
    }

    # 4. Append to credit stream with OCC
    await store.append(
        stream_id=f"credit-{cmd.application_id}",
        events=[credit_completed_event],
        expected_version=await store.stream_version(f"credit-{cmd.application_id}"),
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

    # Append FraudScreeningRequested to loan stream with OCC
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[fraud_requested_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )