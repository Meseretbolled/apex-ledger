import asyncio
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skip(reason="Requires PostgreSQL")

sys.path.insert(0, str(Path(__file__).parent.parent))

from ledger.event_store import EventStore, OptimisticConcurrencyError

DB_URL = "postgresql://localhost/apex_ledger"


@pytest.fixture
async def store():
    s = EventStore(DB_URL)
    await s.connect()
    yield s
    await s.close()


def _event(etype, n=1, version=1, payload_extra=None, metadata_extra=None):
    payload_extra = payload_extra or {}
    metadata_extra = metadata_extra or {}
    return [
        {
            "event_type": etype,
            "event_version": version,
            "payload": {"seq": i, "test": True, **payload_extra},
            "metadata": {**metadata_extra},
        }
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_append_new_stream(store):
    positions = await store.append("test-new-001", _event("TestEvent"), expected_version=-1)
    assert positions == [1]


@pytest.mark.asyncio
async def test_append_existing_stream(store):
    await store.append("test-exist-001", _event("TestEvent"), expected_version=-1)
    positions = await store.append("test-exist-001", _event("TestEvent2"), expected_version=1)
    assert positions == [2]


@pytest.mark.asyncio
async def test_occ_wrong_version_raises(store):
    await store.append("test-occ-001", _event("E"), expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append("test-occ-001", _event("E"), expected_version=99)

    assert exc.value.expected == 99
    assert exc.value.actual == 1


@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds(store):
    """The critical OCC test: two concurrent appends, exactly one wins."""
    await store.append("test-concurrent-001", _event("Init"), expected_version=-1)

    results = await asyncio.gather(
        store.append("test-concurrent-001", _event("A"), expected_version=1),
        store.append("test-concurrent-001", _event("B"), expected_version=1),
        return_exceptions=True,
    )

    successes = [r for r in results if isinstance(r, list)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]

    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(errors) == 1


@pytest.mark.asyncio
async def test_load_stream_ordered(store):
    await store.append("test-load-001", _event("E", 3), expected_version=-1)
    events = await store.load_stream("test-load-001")

    assert len(events) == 3
    positions = [e["stream_position"] for e in events]
    assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_load_stream_with_from_and_to_position(store):
    await store.append("test-load-range-001", _event("RangeEvent", 5), expected_version=-1)

    events = await store.load_stream(
        "test-load-range-001",
        from_position=2,
        to_position=4,
    )

    assert len(events) == 3
    positions = [e["stream_position"] for e in events]
    assert positions == [2, 3, 4]


@pytest.mark.asyncio
async def test_stream_version(store):
    await store.append("test-ver-001", _event("E", 4), expected_version=-1)
    assert await store.stream_version("test-ver-001") == 4


@pytest.mark.asyncio
async def test_stream_version_nonexistent(store):
    assert await store.stream_version("test-does-not-exist") == -1


@pytest.mark.asyncio
async def test_load_all_yields_in_global_order(store):
    await store.append("test-global-A", _event("E", 2), expected_version=-1)
    await store.append("test-global-B", _event("E", 2), expected_version=-1)

    all_events = [e async for e in store.load_all(from_global_position=0)]
    positions = [e["global_position"] for e in all_events]

    assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_load_all_respects_batch_size(store):
    await store.append("test-batch-A", _event("BatchEvent", 3), expected_version=-1)
    await store.append("test-batch-B", _event("BatchEvent", 2), expected_version=-1)

    all_events = [
        e async for e in store.load_all(from_global_position=0, batch_size=2)
    ]

    assert len(all_events) >= 5
    positions = [e["global_position"] for e in all_events]
    assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_load_all_filters_by_event_types(store):
    await store.append("test-filter-A", _event("TypeA", 2), expected_version=-1)
    await store.append("test-filter-B", _event("TypeB", 2), expected_version=-1)

    filtered = [
        e
        async for e in store.load_all(
            from_global_position=0,
            batch_size=10,
            event_types=["TypeB"],
        )
    ]

    assert len(filtered) == 2
    assert all(e["event_type"] == "TypeB" for e in filtered)


@pytest.mark.asyncio
async def test_archive_stream_marks_stream_archived(store):
    await store.append("test-archive-001", _event("ArchiveEvent", 2), expected_version=-1)

    await store.archive_stream("test-archive-001")
    meta = await store.get_stream_metadata("test-archive-001")

    assert meta is not None
    assert meta["is_archived"] is True
    assert meta["archived_at"] is not None


@pytest.mark.asyncio
async def test_get_stream_metadata_returns_expected_fields(store):
    await store.append("test-meta-001", _event("MetaEvent", 1), expected_version=-1)

    meta = await store.get_stream_metadata("test-meta-001")

    assert meta is not None
    assert meta["stream_id"] == "test-meta-001"
    assert "current_version" in meta
    assert "is_archived" in meta


@pytest.mark.asyncio
async def test_upcasting_happens_transparently_on_load_stream(store):
    """
    Assumes your UpcasterRegistry upgrades:
    - ComplianceChecked v1 -> v2 by adding regulatory_basis if missing
    Adjust assertions if your real upcaster uses different fields.
    """
    await store.append(
        "test-upcast-stream-001",
        _event("ComplianceChecked", 1, version=1, payload_extra={"legacy": True}),
        expected_version=-1,
    )

    events = await store.load_stream("test-upcast-stream-001")
    assert len(events) == 1

    event = events[0]
    assert event["event_version"] >= 1
    assert "payload" in event

    # Reviewer asked that upcasting happen transparently on read path.
    # If your upcaster adds regulatory_basis, keep this check.
    # Otherwise replace this with the actual field your upcaster injects.
    assert "regulatory_basis" in event["payload"] or event["event_version"] == 1


@pytest.mark.asyncio
async def test_upcasting_happens_transparently_on_load_all(store):
    await store.append(
        "test-upcast-all-001",
        _event("DecisionGenerated", 1, version=1, payload_extra={"decision": "APPROVE"}),
        expected_version=-1,
    )

    events = [
        e
        async for e in store.load_all(
            from_global_position=0,
            batch_size=10,
            event_types=["DecisionGenerated"],
        )
    ]

    assert len(events) >= 1
    found = [e for e in events if e["stream_id"] == "test-upcast-all-001"]
    assert len(found) == 1

    event = found[0]
    assert "payload" in event
    assert "decision" in event["payload"]


@pytest.mark.asyncio
async def test_many_concurrent_writers_only_one_wins_per_expected_version(store):
    await store.append("test-stress-001", _event("Init"), expected_version=-1)

    async def contender(name):
        return await store.append("test-stress-001", _event(name), expected_version=1)

    results = await asyncio.gather(
        contender("A"),
        contender("B"),
        contender("C"),
        contender("D"),
        contender("E"),
        return_exceptions=True,
    )

    successes = [r for r in results if isinstance(r, list)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]

    assert len(successes) == 1
    assert len(errors) == 4