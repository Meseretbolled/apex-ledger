from __future__ import annotations

import hashlib
import json

from ledger.event_store import EventStore
from ledger.events import BaseEvent, IntegrityCheckResult


def _canonical_event_hash(event_dict: dict, prev_hash: str) -> str:
    canonical = json.dumps(
        {
            "event_id": event_dict["event_id"],
            "stream_id": event_dict["stream_id"],
            "stream_position": event_dict["stream_position"],
            "global_position": event_dict["global_position"],
            "event_type": event_dict["event_type"],
            "event_version": event_dict["event_version"],
            "payload": event_dict["payload"],
            "metadata": {k: v for k, v in event_dict["metadata"].items() if k != "chain_hash"},
            "prev_hash": prev_hash,
        },
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


async def run_integrity_check(event_store: EventStore, stream_id: str) -> IntegrityCheckResult:
    events = await event_store.load_stream(stream_id)
    prev_hash = ""
    tamper_detected = False
    checked = 0

    for event in events:
        event_dict = event.model_dump(mode="json")
        computed = _canonical_event_hash(event_dict, prev_hash)
        stored_hash = event.metadata.get("chain_hash")

        if stored_hash is not None and stored_hash != computed:
            tamper_detected = True

        prev_hash = computed
        checked += 1

    result = IntegrityCheckResult(
        chain_valid=not tamper_detected,
        tamper_detected=tamper_detected,
        checked_events=checked,
    )

    current_version = await event_store.stream_version(stream_id)
    await event_store.append(
        stream_id=stream_id,
        expected_version=current_version,
        events=[
            BaseEvent(
                event_type="AuditIntegrityCheckRun",
                payload=result.model_dump(),
                metadata={"triggering_event_id": "system-integrity-check"},
            )
        ],
        stream_type="loan_application",
    )
    return result