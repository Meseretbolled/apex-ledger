import asyncio
from ledger.event_store import InMemoryEventStore
from ledger.agent_memory import reconstruct_agent_context

async def main():
    store = InMemoryEventStore()

    await store.append(
        "session-1",
        [
            {"event_type": "AgentSessionStarted", "payload": {}},
            {"event_type": "PENDING", "payload": {"task": "review docs"}},
            {"event_type": "ERROR", "payload": {"message": "retry"}},
        ],
        expected_version=-1,
    )

    ctx = await reconstruct_agent_context(store, "session-1")
    print(ctx)

asyncio.run(main())