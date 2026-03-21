"""
tests/phase4/test_projections_and_integrity.py
===============================================
Phase 4 tests — upcasters, audit chain, gas town recovery.
"""
import pytest
from ledger.upcasters import UpcasterRegistry
from ledger.event_store import InMemoryEventStore
from ledger.integrity.audit_chain import AuditChain, compute_event_hash
from ledger.integrity.gas_town import reconstruct_agent_context, find_crashed_sessions


# ── Upcasters ────────────────────────────────────────────────────────────────

def test_credit_analysis_v1_upcasted_to_v2():
    registry = UpcasterRegistry()
    event = {"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": {}}
    result = registry.upcast(event)
    assert result["event_version"] == 2
    assert "regulatory_basis" in result["payload"]

def test_decision_generated_v1_upcasted_to_v2():
    registry = UpcasterRegistry()
    event = {"event_type": "DecisionGenerated", "event_version": 1, "payload": {}}
    result = registry.upcast(event)
    assert result["event_version"] == 2
    assert "model_versions" in result["payload"]

def test_already_current_version_unchanged():
    registry = UpcasterRegistry()
    event = {"event_type": "CreditAnalysisCompleted", "event_version": 2,
             "payload": {"regulatory_basis": ["2026-Q1"]}}
    result = registry.upcast(event)
    assert result["event_version"] == 2
    assert result["payload"]["regulatory_basis"] == ["2026-Q1"]

def test_unknown_event_type_unchanged():
    registry = UpcasterRegistry()
    event = {"event_type": "SomeOtherEvent", "event_version": 1, "payload": {"data": "x"}}
    result = registry.upcast(event)
    assert result == event

def test_upcaster_does_not_mutate_original():
    registry = UpcasterRegistry()
    original = {"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": {}}
    registry.upcast(original)
    assert original["event_version"] == 1  # original unchanged


# ── Audit Chain ───────────────────────────────────────────────────────────────

def test_hash_is_deterministic():
    event = {"event_type": "Test", "stream_id": "test", "stream_position": 0,
             "event_id": "abc", "event_version": 1, "payload": {}, "recorded_at": "2026-01-01"}
    h1 = compute_event_hash(event, "GENESIS")
    h2 = compute_event_hash(event, "GENESIS")
    assert h1 == h2

def test_different_previous_hash_gives_different_result():
    event = {"event_type": "Test", "stream_id": "test", "stream_position": 0,
             "event_id": "abc", "event_version": 1, "payload": {}, "recorded_at": "2026-01-01"}
    h1 = compute_event_hash(event, "GENESIS")
    h2 = compute_event_hash(event, "DIFFERENT")
    assert h1 != h2

def test_different_payload_gives_different_hash():
    e1 = {"event_type": "Test", "stream_id": "test", "stream_position": 0,
          "event_id": "abc", "event_version": 1, "payload": {"a": 1}, "recorded_at": "2026-01-01"}
    e2 = {"event_type": "Test", "stream_id": "test", "stream_position": 0,
          "event_id": "abc", "event_version": 1, "payload": {"a": 2}, "recorded_at": "2026-01-01"}
    assert compute_event_hash(e1, "GENESIS") != compute_event_hash(e2, "GENESIS")

@pytest.mark.asyncio
async def test_empty_stream_is_valid():
    store = InMemoryEventStore()
    chain = AuditChain(store)
    result = await chain.verify_stream("loan-APEX-EMPTY")
    assert result.is_valid == True
    assert result.events_checked == 0

@pytest.mark.asyncio
async def test_stream_with_events_is_valid():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0001", [
        {"event_type": "ApplicationSubmitted", "event_version": 1,
         "payload": {"application_id": "APEX-0001"}},
        {"event_type": "DocumentUploaded", "event_version": 1,
         "payload": {"application_id": "APEX-0001"}},
    ], expected_version=-1)
    chain = AuditChain(store)
    result = await chain.verify_stream("loan-APEX-0001")
    assert result.is_valid == True
    assert result.events_checked == 2

@pytest.mark.asyncio
async def test_stream_hash_is_consistent():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0001", [
        {"event_type": "ApplicationSubmitted", "event_version": 1, "payload": {}},
    ], expected_version=-1)
    chain = AuditChain(store)
    h1 = await chain.compute_stream_hash("loan-APEX-0001")
    h2 = await chain.compute_stream_hash("loan-APEX-0001")
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex length


# ── Gas Town Recovery ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_no_prior_session_returns_not_started():
    store = InMemoryEventStore()
    context = await reconstruct_agent_context(store, "fraud_detection", "APEX-0099")
    assert context["status"] == "NOT_STARTED"
    assert context["needs_reconciliation"] == False
    assert context["session_id"] is None

@pytest.mark.asyncio
async def test_completed_session_does_not_need_reconciliation():
    store = InMemoryEventStore()
    session_id = "sess-fra-done1234"
    stream = f"agent-fraud_detection-{session_id}"
    await store.append(stream, [
        {"event_type": "AgentSessionStarted", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "fraud_detection",
            "application_id": "APEX-0001", "model_version": "gemini-2.0-flash",
            "context_source": "fresh", "started_at": "2026-03-21T10:00:00"
        }},
        {"event_type": "AgentSessionCompleted", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "fraud_detection",
            "application_id": "APEX-0001", "total_llm_calls": 1,
            "total_tokens_used": 2000, "total_cost_usd": 0.002,
            "total_duration_ms": 5000, "completed_at": "2026-03-21T10:01:00"
        }},
    ], expected_version=-1)
    context = await reconstruct_agent_context(store, "fraud_detection", "APEX-0001")
    assert context["status"] == "COMPLETED"
    assert context["needs_reconciliation"] == False

@pytest.mark.asyncio
async def test_failed_session_needs_reconciliation():
    store = InMemoryEventStore()
    session_id = "sess-fra-fail1234"
    stream = f"agent-fraud_detection-{session_id}"
    await store.append(stream, [
        {"event_type": "AgentSessionStarted", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "fraud_detection",
            "application_id": "APEX-0031", "model_version": "gemini-2.0-flash",
            "context_source": "fresh", "started_at": "2026-03-21T10:00:00"
        }},
        {"event_type": "AgentNodeExecuted", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "fraud_detection",
            "node_name": "load_document_facts", "node_sequence": 2
        }},
        {"event_type": "AgentSessionFailed", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "fraud_detection",
            "application_id": "APEX-0031", "error_type": "ConnectionError",
            "error_message": "Connection dropped", "last_successful_node": "load_document_facts",
            "recoverable": True, "failed_at": "2026-03-21T10:01:00"
        }},
    ], expected_version=-1)
    context = await reconstruct_agent_context(store, "fraud_detection", "APEX-0031")
    assert context["status"] == "FAILED"
    assert context["needs_reconciliation"] == True
    assert context["last_successful_node"] == "load_document_facts"
    assert f"prior_session_replay:{session_id}" == context["context_source"]

@pytest.mark.asyncio
async def test_find_crashed_sessions():
    store = InMemoryEventStore()
    session_id = "sess-fra-crash123"
    stream = f"agent-fraud_detection-{session_id}"
    await store.append(stream, [
        {"event_type": "AgentSessionStarted", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "fraud_detection",
            "application_id": "APEX-0032", "model_version": "gemini-2.0-flash",
            "context_source": "fresh", "started_at": "2026-03-21T10:00:00"
        }},
        {"event_type": "AgentSessionFailed", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "fraud_detection",
            "application_id": "APEX-0032", "error_type": "TimeoutError",
            "error_message": "LLM timeout", "last_successful_node": "validate_inputs",
            "recoverable": True, "failed_at": "2026-03-21T10:01:00"
        }},
    ], expected_version=-1)
    crashed = await find_crashed_sessions(store)
    assert len(crashed) >= 1
    assert any(s["session_id"] == session_id for s in crashed)