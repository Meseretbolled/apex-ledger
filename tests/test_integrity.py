import pytest

from ledger.event_store import EventStore
from ledger.events import BaseEvent
from ledger.integrity import run_integrity_check


@pytest.mark.asyncio
async def test_integrity_check_passes_for_clean_stream():
    store = EventStore()
    await store.append(
        "loan-1",
        [
            BaseEvent("LoanApplicationSubmitted", {"x": 1}, {"chain_hash": None}),
            BaseEvent("DocumentFactsExtracted", {"y": 2}, {"chain_hash": None}),
        ],
        expected_version=0,
    )

    result = await run_integrity_check(store, "loan-1")
    assert result.checked_events >= 2
    assert result.tamper_detected is False


@pytest.mark.asyncio
async def test_integrity_check_detects_tampered_payload():
    store = EventStore()
    await store.append(
        "loan-1",
        [BaseEvent("LoanApplicationSubmitted", {"x": 1}, {"chain_hash": "fake"})],
        expected_version=0,
    )

    result = await run_integrity_check(store, "loan-1")
    assert result.tamper_detected is True