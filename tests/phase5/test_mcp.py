"""
tests/phase5/test_mcp.py
=========================
Phase 5 tests — MCP tools and resources.

Tests verify the 8 MCP tools and 6 resources work correctly
against the InMemoryEventStore without requiring a live database.

Run: pytest tests/phase5/ -v
"""
import pytest
from ledger.event_store import InMemoryEventStore
from ledger.mcp.tools import (
    submit_application,
    get_event_stream,
    get_decision_history,
    verify_audit_chain,
    reconstruct_agent_context_tool,
)
from ledger.mcp.resources import (
    get_application_resource,
    get_stream_resource,
)
from ledger.integrity.audit_chain import AuditChain


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _ev(event_type, **payload):
    return {"event_type": event_type, "event_version": 1, "payload": payload}

class MockRegistry:
    async def get_company(self, company_id):
        from dataclasses import dataclass
        @dataclass
        class Company:
            name: str = "Test Co"
            jurisdiction: str = "CA"
            legal_type: str = "LLC"
            founded_year: int = 2015
            trajectory: str = "STABLE"
        return Company()
    async def get_financial_history(self, company_id):
        return []
    async def get_compliance_flags(self, company_id):
        return []
    async def get_loan_relationships(self, company_id):
        return []


# ─── TOOL TESTS ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_tool_submit_application():
    """MCP tool: submit_application creates correct events in event store."""
    store = InMemoryEventStore()
    registry = MockRegistry()

    result = await submit_application(
        store=store,
        registry=registry,
        application_id="MCP-TEST-001",
        applicant_id="COMP-001",
        requested_amount_usd=500000,
        loan_purpose="working_capital",
    )

    assert result["success"] == True
    assert result["application_id"] == "MCP-TEST-001"
    assert "stream_id" in result

    loan_events = await store.load_stream("loan-MCP-TEST-001")
    assert len(loan_events) == 2
    assert loan_events[0]["event_type"] == "ApplicationSubmitted"
    assert loan_events[1]["event_type"] == "DocumentUploadRequested"

@pytest.mark.asyncio
async def test_tool_get_event_stream():
    """MCP tool: get_event_stream returns events for a stream."""
    store = InMemoryEventStore()
    await store.append("loan-MCP-TEST-002", [
        _ev("ApplicationSubmitted", application_id="MCP-TEST-002"),
        _ev("DocumentUploadRequested", application_id="MCP-TEST-002"),
    ], expected_version=-1)

    result = await get_event_stream(store, "loan-MCP-TEST-002")

    assert result["stream_id"] == "loan-MCP-TEST-002"
    assert result["event_count"] == 2
    assert result["events"][0]["event_type"] == "ApplicationSubmitted"
    assert result["events"][1]["event_type"] == "DocumentUploadRequested"

@pytest.mark.asyncio
async def test_tool_get_event_stream_with_pagination():
    """MCP tool: get_event_stream supports from_position pagination."""
    store = InMemoryEventStore()
    await store.append("loan-MCP-TEST-003", [
        _ev("ApplicationSubmitted", application_id="MCP-TEST-003"),
        _ev("DocumentUploadRequested", application_id="MCP-TEST-003"),
        _ev("DocumentUploaded", application_id="MCP-TEST-003"),
    ], expected_version=-1)

    result = await get_event_stream(store, "loan-MCP-TEST-003", from_position=1)

    assert result["event_count"] == 2
    assert result["events"][0]["stream_position"] == 1

@pytest.mark.asyncio
async def test_tool_get_decision_history():
    """MCP tool: get_decision_history returns decision events across streams."""
    store = InMemoryEventStore()
    app_id = "MCP-TEST-004"

    await store.append(f"loan-{app_id}", [
        _ev("ApplicationSubmitted", application_id=app_id),
        _ev("DecisionGenerated", application_id=app_id,
            recommendation="APPROVE", confidence=0.82),
        _ev("ApplicationApproved", application_id=app_id,
            approved_amount_usd="450000"),
    ], expected_version=-1)

    await store.append(f"compliance-{app_id}", [
        _ev("ComplianceCheckCompleted", application_id=app_id,
            overall_verdict="CLEAR", has_hard_block=False),
    ], expected_version=-1)

    result = await get_decision_history(store, app_id)

    assert result["application_id"] == app_id
    assert result["decision_event_count"] >= 2
    decision_types = [e["event_type"] for e in result["decision_history"]]
    assert "DecisionGenerated" in decision_types
    assert "ApplicationApproved" in decision_types

@pytest.mark.asyncio
async def test_tool_verify_audit_chain_valid():
    """MCP tool: verify_audit_chain returns is_valid=True for untampered stream."""
    store = InMemoryEventStore()
    await store.append("loan-MCP-TEST-005", [
        _ev("ApplicationSubmitted", application_id="MCP-TEST-005"),
        _ev("DocumentUploaded", application_id="MCP-TEST-005"),
    ], expected_version=-1)

    result = await verify_audit_chain(store, "loan-MCP-TEST-005")

    assert result["stream_id"] == "loan-MCP-TEST-005"
    assert result["is_valid"] == True
    assert result["events_checked"] == 2
    assert result["broken_at"] is None
    assert len(result["final_hash"]) == 64  # SHA-256 hex

@pytest.mark.asyncio
async def test_tool_verify_audit_chain_empty_stream():
    """MCP tool: verify_audit_chain handles empty stream gracefully."""
    store = InMemoryEventStore()
    result = await verify_audit_chain(store, "loan-MCP-EMPTY")

    assert result["is_valid"] == True
    assert result["events_checked"] == 0

@pytest.mark.asyncio
async def test_tool_reconstruct_agent_context_no_session():
    """MCP tool: reconstruct_agent_context returns NOT_STARTED for missing session."""
    store = InMemoryEventStore()
    result = await reconstruct_agent_context_tool(
        store, "fraud_detection", "MCP-TEST-MISSING"
    )

    assert result["agent_type"] == "fraud_detection"
    assert result["recovery_context"]["status"] == "NOT_STARTED"
    assert result["can_resume"] == False

@pytest.mark.asyncio
async def test_tool_reconstruct_agent_context_crashed():
    """MCP tool: reconstruct_agent_context returns recovery info for crashed session."""
    store = InMemoryEventStore()
    session_id = "sess-fra-mcp-test"

    await store.append(f"agent-fraud_detection-{session_id}", [
        _ev("AgentSessionStarted",
            session_id=session_id, agent_type="fraud_detection",
            application_id="MCP-TEST-006", model_version="gemini-2.0-flash",
            context_source="fresh", started_at="2026-03-22T10:00:00"),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="fraud_detection",
            node_name="load_document_facts", node_sequence=2, llm_called=False),
        _ev("AgentSessionFailed",
            session_id=session_id, agent_type="fraud_detection",
            application_id="MCP-TEST-006", error_type="ConnectionError",
            error_message="Lost connection", last_successful_node="load_document_facts",
            recoverable=True, failed_at="2026-03-22T10:00:05"),
    ], expected_version=-1)

    result = await reconstruct_agent_context_tool(
        store, "fraud_detection", "MCP-TEST-006"
    )

    assert result["can_resume"] == True
    assert result["resume_from"] == "load_document_facts"
    assert result["recovery_context"]["status"] == "FAILED"


# ─── RESOURCE TESTS ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_resource_get_application():
    """MCP resource: get_application_resource returns all stream events."""
    store = InMemoryEventStore()
    app_id = "MCP-RES-001"

    await store.append(f"loan-{app_id}", [
        _ev("ApplicationSubmitted", application_id=app_id),
    ], expected_version=-1)
    await store.append(f"credit-{app_id}", [
        _ev("CreditAnalysisCompleted", application_id=app_id),
    ], expected_version=-1)

    result = await get_application_resource(store, None, app_id)

    assert result["application_id"] == app_id
    assert result["total_events"] == 2
    assert f"loan-{app_id}" in result["streams"]
    assert f"credit-{app_id}" in result["streams"]
    assert len(result["streams"][f"loan-{app_id}"]) == 1
    assert len(result["streams"][f"credit-{app_id}"]) == 1

@pytest.mark.asyncio
async def test_resource_get_stream():
    """MCP resource: get_stream_resource returns raw stream events."""
    store = InMemoryEventStore()
    await store.append("loan-MCP-RES-002", [
        _ev("ApplicationSubmitted", application_id="MCP-RES-002"),
        _ev("DocumentUploadRequested", application_id="MCP-RES-002"),
        _ev("DocumentUploaded", application_id="MCP-RES-002"),
    ], expected_version=-1)

    result = await get_stream_resource(store, "loan-MCP-RES-002")

    assert result["stream_id"] == "loan-MCP-RES-002"
    assert result["event_count"] == 3
    assert result["current_version"] == 2
    assert len(result["events"]) == 3