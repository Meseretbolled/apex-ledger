import pytest

from ledger.agent_memory import reconstruct_agent_context
from ledger.event_store import InMemoryEventStore


@pytest.mark.asyncio
async def test_reconstruct_agent_context_returns_last_position_and_pending_work():
    store = InMemoryEventStore()

    await store.append(
        "session-1",
        [
            {
                "event_type": "AgentSessionStarted",
                "event_version": 1,
                "payload": {"step": 1},
                "metadata": {"triggering_event_id": "init"},
            },
            {
                "event_type": "PENDING",
                "event_version": 1,
                "payload": {"task": "review docs"},
                "metadata": {"triggering_event_id": "init"},
            },
            {
                "event_type": "DecisionGenerated",
                "event_version": 1,
                "payload": {"decision": "APPROVE"},
                "metadata": {"triggering_event_id": "credit"},
            },
            {
                "event_type": "ERROR",
                "event_version": 1,
                "payload": {"message": "retry later"},
                "metadata": {"triggering_event_id": "credit"},
            },
            {
                "event_type": "AgentNodeExecuted",
                "event_version": 1,
                "payload": {"status": "partial"},
                "metadata": {"triggering_event_id": "credit"},
            },
        ],
        expected_version=-1,
    )

    ctx = await reconstruct_agent_context(store, "session-1")
    assert ctx.last_event_position == 4
    assert len(ctx.pending_work) > 0


@pytest.mark.asyncio
async def test_reconstruct_agent_context_flags_needs_reconciliation():
    store = InMemoryEventStore()

    await store.append(
        "session-2",
        [
            {
                "event_type": "AgentSessionStarted",
                "event_version": 1,
                "payload": {},
                "metadata": {"triggering_event_id": "init"},
            },
            {
                "event_type": "DecisionGenerated",
                "event_version": 1,
                "payload": {"decision": "REFER"},
                "metadata": {"triggering_event_id": "credit"},
            },
        ],
        expected_version=-1,
    )

    ctx = await reconstruct_agent_context(store, "session-2")
    assert ctx.session_health_status == "NEEDS_RECONCILIATION"