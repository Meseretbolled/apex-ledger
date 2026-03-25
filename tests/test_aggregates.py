import pytest

from ledger.aggregates import AuditLedgerAggregate, ComplianceRecordAggregate
from ledger.events import DomainError, StoredEvent


def make_event(
    event_type: str,
    pos: int,
    payload=None,
    metadata=None,
):
    return StoredEvent(
        stream_id="loan-1",
        stream_position=pos,
        global_position=pos,
        event_type=event_type,
        event_version=1,
        payload=payload or {},
        metadata=metadata or {},
    )


def test_decision_blocked_when_compliance_incomplete():
    history = [
        make_event("KYCCompleted", 1),
        make_event("FraudScreenCompleted", 2),
    ]
    agg = ComplianceRecordAggregate.load("loan-1", history)

    with pytest.raises(DomainError):
        agg.validate_decision_allowed()


def test_confidence_below_floor_forces_refer():
    history = [
        make_event("DocumentFactsExtracted", 1),
        make_event("CreditAnalysisCompleted", 2, payload={"confidence": 0.42}),
    ]
    agg = AuditLedgerAggregate.load("loan-1", history)
    assert agg.validate_confidence_floor() == "REFER"


def test_document_facts_required_before_decision():
    history = [
        make_event(
            "CreditAnalysisCompleted",
            1,
            payload={"confidence": 0.91},
            metadata={"triggering_event_id": "x", "model_version": "m1"},
        )
    ]
    agg = AuditLedgerAggregate.load("loan-1", history)

    with pytest.raises(DomainError):
        agg.validate_context_before_decision()


def test_model_version_locking_enforced():
    history = [
        make_event(
            "CreditAnalysisCompleted",
            1,
            payload={"confidence": 0.9},
            metadata={"triggering_event_id": "x", "model_version": "model-v1"},
        )
    ]
    agg = AuditLedgerAggregate.load("loan-1", history)

    with pytest.raises(DomainError):
        agg.validate_model_version_locking("model-v2")