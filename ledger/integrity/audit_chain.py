"""
ledger/integrity/audit_chain.py
================================
SHA-256 hash chain for tamper detection.

Every event is linked to the previous event's hash.
If any event is modified, the chain breaks at that point.

Usage:
    chain = AuditChain(store)
    result = await chain.verify_stream("loan-APEX-0001")
    if result.is_valid:
        print("Stream is tamper-free")
    else:
        print(f"Tamper detected at position {result.broken_at}")
"""
from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass


@dataclass
class ChainVerificationResult:
    stream_id: str
    is_valid: bool
    events_checked: int
    broken_at: int | None = None
    error_message: str | None = None


def compute_event_hash(event: dict, previous_hash: str = "GENESIS") -> str:
    """
    Compute SHA-256 hash of an event chained to the previous hash.
    Deterministic — same inputs always produce same hash.
    """
    chain_data = {
        "previous_hash": previous_hash,
        "event_id": str(event.get("event_id", "")),
        "stream_id": event.get("stream_id", ""),
        "stream_position": event.get("stream_position", 0),
        "event_type": event.get("event_type", ""),
        "event_version": event.get("event_version", 1),
        "payload": event.get("payload", {}),
        "recorded_at": str(event.get("recorded_at", "")),
    }
    return hashlib.sha256(
        json.dumps(chain_data, sort_keys=True, default=str).encode()
    ).hexdigest()


class AuditChain:
    """
    Verifies the integrity of an event stream using SHA-256 hash chaining.
    Used by the MCP audit tool and AuditIntegrityCheckRun events.
    """

    def __init__(self, store):
        self.store = store

    async def verify_stream(self, stream_id: str) -> ChainVerificationResult:
        """
        Verify the hash chain of a stream.
        Returns ChainVerificationResult with is_valid=True if untampered.
        """
        events = await self.store.load_stream(stream_id)
        if not events:
            return ChainVerificationResult(
                stream_id=stream_id,
                is_valid=True,
                events_checked=0,
            )
        previous_hash = "GENESIS"
        for i, event in enumerate(events):
            stored_hash = event.get("metadata", {}).get("chain_hash")
            computed_hash = compute_event_hash(event, previous_hash)
            if stored_hash and stored_hash != computed_hash:
                return ChainVerificationResult(
                    stream_id=stream_id,
                    is_valid=False,
                    events_checked=i + 1,
                    broken_at=event.get("stream_position"),
                    error_message=f"Hash mismatch at position {event.get('stream_position')}: "
                                  f"stored={stored_hash[:8]}... computed={computed_hash[:8]}...",
                )
            previous_hash = computed_hash

        return ChainVerificationResult(
            stream_id=stream_id,
            is_valid=True,
            events_checked=len(events),
        )

    async def compute_stream_hash(self, stream_id: str) -> str:
        """
        Compute the final hash of an entire stream.
        Used as a tamper-evident fingerprint.
        """
        events = await self.store.load_stream(stream_id)
        previous_hash = "GENESIS"
        for event in events:
            previous_hash = compute_event_hash(event, previous_hash)
        return previous_hash

    def compute_event_hash(self, event: dict, previous_hash: str = "GENESIS") -> str:
        return compute_event_hash(event, previous_hash)