"""
tests/phase2/test_aggregates.py
================================
Phase 2 tests — LoanApplicationAggregate, AgentSessionAggregate, command handlers.
"""
import pytest
from ledger.domain.aggregates.loan_application import (
    LoanApplicationAggregate, ApplicationState, DomainError
)
from ledger.domain.aggregates.agent_session import (
    AgentSessionAggregate, SessionStatus
)
from ledger.event_store import InMemoryEventStore


# ── LoanApplicationAggregate ──────────────────────────────────────────────────

def test_new_aggregate_is_in_new_state():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    assert agg.state == ApplicationState.NEW
    assert agg.version == 0

def test_apply_application_submitted():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    agg.apply({"event_type": "ApplicationSubmitted", "payload": {
        "applicant_id": "COMP-001",
        "requested_amount_usd": 500000,
        "loan_purpose": "working_capital",
    }})
    assert agg.state == ApplicationState.SUBMITTED
    assert agg.applicant_id == "COMP-001"
    assert agg.requested_amount_usd == 500000
    assert agg.version == 1

def test_full_happy_path_state_machine():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    events = [
        {"event_type": "ApplicationSubmitted", "payload": {"applicant_id": "COMP-001", "requested_amount_usd": 500000, "loan_purpose": "working_capital"}},
        {"event_type": "DocumentUploadRequested", "payload": {}},
        {"event_type": "DocumentUploaded", "payload": {}},
        {"event_type": "PackageReadyForAnalysis", "payload": {}},
        {"event_type": "CreditAnalysisRequested", "payload": {}},
        {"event_type": "FraudScreeningRequested", "payload": {}},
        {"event_type": "ComplianceCheckRequested", "payload": {}},
        {"event_type": "ComplianceCheckCompleted", "payload": {"overall_verdict": "CLEAR", "has_hard_block": False}},
        {"event_type": "DecisionRequested", "payload": {}},
        {"event_type": "ApplicationApproved", "payload": {}},
    ]
    for e in events:
        agg.apply(e)
    assert agg.state == ApplicationState.APPROVED
    assert agg.version == 10

def test_compliance_blocked_leads_to_declined_compliance():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    agg.apply({"event_type": "ApplicationSubmitted", "payload": {"applicant_id": "COMP-075", "requested_amount_usd": 500000, "loan_purpose": "working_capital"}})
    agg.apply({"event_type": "ApplicationDeclined", "payload": {
        "decline_reasons": ["Compliance hard block: REG-003"],
        "adverse_action_notice_required": True,
    }})
    assert agg.state == ApplicationState.DECLINED_COMPLIANCE

def test_assert_valid_transition_raises_on_invalid():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    with pytest.raises((DomainError, ValueError)):
        agg.assert_valid_transition(ApplicationState.APPROVED)

def test_assert_documents_processed_raises_before_docs():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    agg.apply({"event_type": "ApplicationSubmitted", "payload": {"applicant_id": "COMP-001", "requested_amount_usd": 500000, "loan_purpose": "wc"}})
    with pytest.raises(DomainError):
        agg.assert_documents_processed()

def test_compliance_hard_block_prevents_approve():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    agg.compliance_has_hard_block = True
    with pytest.raises(DomainError):
        agg.assert_can_approve()

def test_assert_not_terminal_raises_after_approval():
    agg = LoanApplicationAggregate(application_id="APEX-0001")
    agg.state = ApplicationState.APPROVED
    with pytest.raises(DomainError):
        agg.assert_not_terminal()

@pytest.mark.asyncio
async def test_load_replays_events_from_store():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0001", [
        {"event_type": "ApplicationSubmitted", "event_version": 1, "payload": {
            "applicant_id": "COMP-001", "requested_amount_usd": 500000, "loan_purpose": "wc"
        }},
        {"event_type": "DocumentUploadRequested", "event_version": 1, "payload": {}},
    ], expected_version=-1)
    agg = await LoanApplicationAggregate.load(store, "APEX-0001")
    assert agg.state == ApplicationState.DOCUMENTS_PENDING
    assert agg.applicant_id == "COMP-001"
    assert agg.version == 2


# ── AgentSessionAggregate ─────────────────────────────────────────────────────

def test_new_session_is_not_started():
    sess = AgentSessionAggregate(agent_id="credit-1", session_id="sess-cre-abc123")
    assert sess.status == SessionStatus.NOT_STARTED
    assert sess.context_loaded == False

def test_session_started_sets_active():
    sess = AgentSessionAggregate(agent_id="credit-1", session_id="sess-cre-abc123")
    sess.apply({"event_type": "AgentSessionStarted", "payload": {
        "agent_type": "credit_analysis",
        "application_id": "APEX-0001",
        "model_version": "gemini-2.0-flash",
        "context_source": "fresh",
    }})
    assert sess.status == SessionStatus.ACTIVE
    assert sess.context_loaded == True
    assert sess.agent_type == "credit_analysis"

def test_node_executed_tracks_last_node():
    sess = AgentSessionAggregate(agent_id="credit-1", session_id="sess-cre-abc123")
    sess.apply({"event_type": "AgentSessionStarted", "payload": {
        "agent_type": "credit_analysis", "application_id": "APEX-0001",
        "model_version": "gemini-2.0-flash", "context_source": "fresh",
    }})
    sess.apply({"event_type": "AgentNodeExecuted", "payload": {
        "node_name": "validate_inputs", "llm_called": False,
        "llm_tokens_input": None, "llm_tokens_output": None, "llm_cost_usd": None
    }})
    assert sess.last_successful_node == "validate_inputs"
    assert "validate_inputs" in sess.nodes_executed

def test_session_completed_sets_completed():
    sess = AgentSessionAggregate(agent_id="credit-1", session_id="sess-cre-abc123")
    sess.apply({"event_type": "AgentSessionStarted", "payload": {
        "agent_type": "credit_analysis", "application_id": "APEX-0001",
        "model_version": "gemini-2.0-flash", "context_source": "fresh",
    }})
    sess.apply({"event_type": "AgentSessionCompleted", "payload": {
        "total_llm_calls": 1, "total_tokens_used": 2000, "total_cost_usd": 0.002
    }})
    assert sess.status == SessionStatus.COMPLETED

def test_session_failed_sets_failed():
    sess = AgentSessionAggregate(agent_id="credit-1", session_id="sess-cre-abc123")
    sess.apply({"event_type": "AgentSessionStarted", "payload": {
        "agent_type": "credit_analysis", "application_id": "APEX-0001",
        "model_version": "gemini-2.0-flash", "context_source": "fresh",
    }})
    sess.apply({"event_type": "AgentSessionFailed", "payload": {
        "error_type": "ConnectionError", "last_successful_node": "validate_inputs"
    }})
    assert sess.status == SessionStatus.FAILED

def test_assert_context_loaded_raises_before_start():
    sess = AgentSessionAggregate(agent_id="credit-1", session_id="sess-cre-abc123")
    from ledger.domain.aggregates.agent_session import DomainError as SessionDomainError
    with pytest.raises(SessionDomainError):
        sess.assert_context_loaded()

@pytest.mark.asyncio
async def test_session_load_replays_from_store():
    store = InMemoryEventStore()
    # stream_id matches what load() constructs: agent-{agent_id}-{session_id}
    await store.append("agent-credit-1-sess-cre-abc123", [
        {"event_type": "AgentSessionStarted", "event_version": 1, "payload": {
            "agent_type": "credit_analysis", "application_id": "APEX-0001",
            "model_version": "gemini-2.0-flash", "context_source": "fresh",
        }},
    ], expected_version=-1)
    sess = await AgentSessionAggregate.load(store, "credit-1", "sess-cre-abc123")
    assert sess.status == SessionStatus.ACTIVE
    assert sess.context_loaded == True