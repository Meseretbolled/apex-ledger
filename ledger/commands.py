from __future__ import annotations

from typing import List, Optional

from ledger.aggregates import AuditLedgerAggregate, ComplianceRecordAggregate
from ledger.event_store import EventStore
from ledger.events import BaseEvent


async def handle_submit_application(
    event_store: EventStore,
    stream_id: str,
    applicant_id: str,
    application_data: dict,
) -> List:
    # load
    history = await event_store.load_stream(stream_id)

    # validate
    expected_version = len(history)

    # determine
    new_events = [
        BaseEvent(
            event_type="LoanApplicationSubmitted",
            payload={"applicant_id": applicant_id, "application": application_data},
            metadata={"triggering_event_id": "external-submission"},
        )
    ]

    # append
    return await event_store.append(
        stream_id=stream_id,
        events=new_events,
        expected_version=expected_version,
        stream_type="loan_application",
    )


async def handle_credit_analysis_completed(
    event_store: EventStore,
    stream_id: str,
    confidence: float,
    model_version: str,
    triggering_event_id: str,
) -> List:
    # load
    history = await event_store.load_stream(stream_id)
    compliance = ComplianceRecordAggregate.load(stream_id, history)
    audit = AuditLedgerAggregate.load(stream_id, history)

    # validate
    compliance.validate_decision_allowed()
    audit.validate_context_before_decision()
    audit.validate_model_version_locking(model_version)

    # determine
    credit_event = BaseEvent(
        event_type="CreditAnalysisCompleted",
        event_version=2,
        payload={"confidence": confidence},
        metadata={
            "triggering_event_id": triggering_event_id,
            "model_version": model_version,
        },
    )

    synthetic_credit = await event_store.append(
        stream_id=stream_id,
        events=[credit_event],
        expected_version=len(history),
        stream_type="loan_application",
    )

    updated_history = await event_store.load_stream(stream_id)
    audit_after_credit = AuditLedgerAggregate.load(stream_id, updated_history)
    forced_decision = audit_after_credit.validate_confidence_floor()
    decision = forced_decision if forced_decision else "APPROVE"

    decision_event = BaseEvent(
        event_type="DecisionGenerated",
        event_version=2,
        payload={"decision": decision, "model_versions": {"credit_analysis": model_version}},
        metadata={"triggering_event_id": triggering_event_id},
    )

    decision_result = await event_store.append(
        stream_id=stream_id,
        events=[decision_event],
        expected_version=len(updated_history),
        stream_type="loan_application",
    )

    return synthetic_credit + decision_result