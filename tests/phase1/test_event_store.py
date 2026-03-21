
import asyncio
import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


# ─── SCHEMA TESTS ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_new_stream_version_is_minus_one():
    store = InMemoryEventStore()
    v = await store.stream_version("loan-APEX-0001")
    assert v == -1, "Non-existent stream must return -1"

@pytest.mark.asyncio
async def test_append_new_stream_succeeds():
    store = InMemoryEventStore()
    positions = await store.append(
        "loan-APEX-0001",
        [_ev("ApplicationSubmitted", application_id="APEX-0001")],
        expected_version=-1,
    )
    assert positions == [0]
    assert await store.stream_version("loan-APEX-0001") == 0

@pytest.mark.asyncio
async def test_append_increments_version():
    store = InMemoryEventStore()
    await store.append("s", [_ev("E1")], expected_version=-1)
    await store.append("s", [_ev("E2")], expected_version=0)
    await store.append("s", [_ev("E3")], expected_version=1)
    assert await store.stream_version("s") == 2

@pytest.mark.asyncio
async def test_append_wrong_version_raises():
    store = InMemoryEventStore()
    await store.append("s", [_ev("E1")], expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await store.append("s", [_ev("E2")], expected_version=-1)  # should be 0
    assert exc_info.value.stream_id == "s"
    assert exc_info.value.expected == -1
    assert exc_info.value.actual == 0

@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds():
    """
    The critical OCC test: two concurrent tasks race to append to the same
    stream at the same expected_version.

    Asserts:
      - exactly ONE task succeeds
      - the LOSER specifically raises OptimisticConcurrencyError
      - the final stream length is exactly 4 events (3 base + 1 winner)
      - the winning event is at position 3 (0-indexed)
      - the stream version is 3 after the race
    """
    store = InMemoryEventStore()

    # Seed the stream with 3 events so expected_version=2
    await store.append("loan-APEX-TEST", [_ev("ApplicationSubmitted")], expected_version=-1)
    await store.append("loan-APEX-TEST", [_ev("DocumentUploadRequested")], expected_version=0)
    await store.append("loan-APEX-TEST", [_ev("DocumentUploaded")], expected_version=1)

    assert await store.stream_version("loan-APEX-TEST") == 2

    # Two agents race simultaneously — both believe expected_version=2
    successes = []
    occ_errors = []
    winning_positions = []

    async def attempt(agent_name: str):
        try:
            positions = await store.append(
                "loan-APEX-TEST",
                [_ev("CreditAnalysisCompleted", agent=agent_name)],
                expected_version=2,
            )
            successes.append(agent_name)
            winning_positions.extend(positions)
        except OptimisticConcurrencyError as e:
            occ_errors.append((agent_name, e))

    await asyncio.gather(
        attempt("credit-agent-A"),
        attempt("credit-agent-B"),
    )

    # Exactly one wins, one raises OptimisticConcurrencyError
    assert len(successes) == 1, \
        f"Exactly one agent must succeed, got: {successes}"
    assert len(occ_errors) == 1, \
        f"Exactly one agent must raise OCC, got: {occ_errors}"

    # The loser raised OptimisticConcurrencyError specifically (not any other error)
    losing_agent, occ_exc = occ_errors[0]
    assert isinstance(occ_exc, OptimisticConcurrencyError), \
        "Loser must raise OptimisticConcurrencyError"
    assert occ_exc.stream_id == "loan-APEX-TEST"
    assert occ_exc.expected == 2

    # The winning event is at position 3 (the 4th event, 0-indexed)
    assert winning_positions == [3], \
        f"Winning event must be at position 3, got: {winning_positions}"

    # Final stream has exactly 4 events (3 base + 1 winner)
    final_events = await store.load_stream("loan-APEX-TEST")
    assert len(final_events) == 4, \
        f"Stream must have exactly 4 events, got: {len(final_events)}"

    # Stream version is now 3
    assert await store.stream_version("loan-APEX-TEST") == 3, \
        "Stream version must be 3 after race"

    # The winning event's payload identifies the winner
    winning_event = final_events[3]
    assert winning_event["stream_position"] == 3
    assert winning_event["event_type"] == "CreditAnalysisCompleted"
    assert winning_event["payload"]["agent"] == successes[0]

@pytest.mark.asyncio
async def test_load_stream_returns_events_in_order():
    store = InMemoryEventStore()
    for i in range(5):
        ver = await store.stream_version("s")
        await store.append("s", [_ev(f"Event{i}", seq=i)], expected_version=ver)

    events = await store.load_stream("s")
    assert len(events) == 5
    for i, ev in enumerate(events):
        assert ev["stream_position"] == i
        assert ev["event_type"] == f"Event{i}"

@pytest.mark.asyncio
async def test_load_stream_with_from_position():
    store = InMemoryEventStore()
    for i in range(5):
        ver = await store.stream_version("s")
        await store.append("s", [_ev(f"E{i}")], expected_version=ver)

    events = await store.load_stream("s", from_position=2)
    assert len(events) == 3
    assert events[0]["stream_position"] == 2

@pytest.mark.asyncio
async def test_load_all_yields_all_events_globally():
    store = InMemoryEventStore()
    await store.append("s1", [_ev("E1"), _ev("E2")], expected_version=-1)
    await store.append("s2", [_ev("E3")], expected_version=-1)

    all_events = [e async for e in store.load_all(from_position=0)]
    assert len(all_events) == 3

@pytest.mark.asyncio
async def test_checkpoints_persist():
    store = InMemoryEventStore()
    assert await store.load_checkpoint("proj_a") == 0  # default
    await store.save_checkpoint("proj_a", 42)
    assert await store.load_checkpoint("proj_a") == 42

@pytest.mark.asyncio
async def test_append_multiple_events_in_one_call():
    store = InMemoryEventStore()
    events = [_ev(f"E{i}") for i in range(3)]
    positions = await store.append("s", events, expected_version=-1)
    assert positions == [0, 1, 2]
    assert await store.stream_version("s") == 2

@pytest.mark.asyncio
async def test_correlation_and_causation_ids_stored_in_metadata():
    """
    correlation_id and causation_id must be stored in event metadata.
    This is required for causation chain audits.
    """
    store = InMemoryEventStore()
    await store.append(
        "loan-APEX-0001",
        [_ev("ApplicationSubmitted", application_id="APEX-0001")],
        expected_version=-1,
        correlation_id="corr-abc123",
        causation_id="caus-xyz789",
    )
    events = await store.load_stream("loan-APEX-0001")
    assert len(events) == 1
    meta = events[0]["metadata"]
    assert meta.get("correlation_id") == "corr-abc123"
    assert meta.get("causation_id") == "caus-xyz789"

@pytest.mark.asyncio
async def test_occ_error_has_correct_fields():
    """
    OptimisticConcurrencyError must expose stream_id, expected, and actual
    for callers to inspect and retry.
    """
    store = InMemoryEventStore()
    await store.append("s", [_ev("E1")], expected_version=-1)
    try:
        await store.append("s", [_ev("E2")], expected_version=5)
        assert False, "Should have raised"
    except OptimisticConcurrencyError as e:
        assert e.stream_id == "s"
        assert e.expected == 5
        assert e.actual == 0
        assert "s" in str(e)
        assert "5" in str(e)
        assert "0" in str(e)


# ─── EVENT SCHEMA CONFORMANCE ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_all_seed_event_types_validate():
    """
    Loads seed_events.jsonl (produced by datagen/generate_all.py --validate-only)
    and verifies every event validates against EVENT_REGISTRY.
    """
    import json
    from pathlib import Path
    from ledger.schema.events import EVENT_REGISTRY

    seed_file = Path("data/seed_events.jsonl")
    if not seed_file.exists():
        pytest.skip("data/seed_events.jsonl not found — run datagen first")

    errors = []
    validated = 0
    with open(seed_file) as f:
        for line in f:
            rec = json.loads(line)
            event_type = rec["event_type"]
            payload = rec["payload"]
            cls = EVENT_REGISTRY.get(event_type)
            if cls is None:
                errors.append(f"Unknown type: {event_type}")
                continue
            try:
                cls(event_type=event_type, **payload)
                validated += 1
            except Exception as e:
                errors.append(f"{event_type}: {e}")

    assert not errors, f"{len(errors)} schema errors:\n" + "\n".join(errors[:10])
    assert validated > 0, "No events found in seed file"
    print(f"\nValidated {validated} seed events against EVENT_REGISTRY")