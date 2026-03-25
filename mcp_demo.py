from ledger.event_store import InMemoryEventStore
from ledger.mcp.tools import submit_application, get_event_stream

import asyncio

from tests.test_event_store import store

async def main():
    store = InMemoryEventStore()

    print("=== STEP 1: Submit Application ===")
    result = await submit_application(
    store,
    "loan-123",
    5000,
    "Test User"
)
    print(result)

    print("\n=== STEP 2: Get Event Stream ===")
    events = await get_event_stream(store, "loan-123")
    for e in events:
        print(e)

asyncio.run(main())

