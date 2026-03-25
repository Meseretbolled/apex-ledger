import pytest

from ledger.event_store import InMemoryEventStore
from ledger.integrity import run_integrity_check


@pytest.mark.asyncio
async def test_integrity_check_passes_for_clean_stream():
    store = InMemoryEventStore()

    await store.append(
        "loan-1",
        [
            {
                "event_type": "LoanApplicationSubmitted",
                "event_version": 1,
                "payload": {"x": 1},
                "metadata": {"chain_hash": None},
            },
            {
                "event_type": "DocumentFactsExtracted",
                "event_version": 1,
                "payload": {"y": 2},
                "metadata": {"chain_hash": None},
            },
        ],
        expected_version=-1,
    )

    result = await run_integrity_check(store, "loan-1")
    assert result.checked_events >= 2
    assert result.tamper_detected is False


@pytest.mark.asyncio
async def test_integrity_check_detects_tampered_payload():
    store = InMemoryEventStore()

    await store.append(
        "loan-1",
        [
            {
                "event_type": "LoanApplicationSubmitted",
                "event_version": 1,
                "payload": {"x": 1},
                "metadata": {"chain_hash": "fake"},
            }
        ],
        expected_version=-1,
    )

    result = await run_integrity_check(store, "loan-1")
    assert result.tamper_detected is True