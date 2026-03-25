import pytest

from ledger.agent_memory import reconstruct_agent_context
from ledger.event_store import EventStore
from ledger.events import BaseEvent


@pytest.mark.asyncio
async def test_reconstruct_agent_context_returns_last_position_and_pending_work():
    store = EventStore()
    await store.append(
        "session-1",
        [
            BaseEvent("AgentSessionStarted", {"step": 1}, {"triggering_event_id": "init"}),
            BaseEvent("PENDING", {"task": "review docs"}, {"triggering_event_id": "init"}),
            BaseEvent("DecisionGenerated", {"decision": "APPROVE"}, {"triggering_event_id": "credit"}),
            BaseEvent("ERROR", {"message": "retry later"}, {"triggering_event_id": "credit"}),
            BaseEvent("AgentNodeExecuted", {"status": "partial"}, {"triggering_event_id": "credit"}),
        ],
        expected_version=0,
    )

    ctx = await reconstruct_agent_context(store, "session-1")
    assert ctx.last_event_position == 5
    assert len(ctx.pending_work) > 0


@pytest.mark.asyncio
async def test_reconstruct_agent_context_flags_needs_reconciliation():
    store = EventStore()
    await store.append(
        "session-2",
        [
            BaseEvent("AgentSessionStarted", {}, {"triggering_event_id": "init"}),
            BaseEvent("DecisionGenerated", {"decision": "REFER"}, {"triggering_event_id": "credit"}),
        ],
        expected_version=0,
    )

    ctx = await reconstruct_agent_context(store, "session-2")
    assert ctx.session_health_status == "NEEDS_RECONCILIATION"