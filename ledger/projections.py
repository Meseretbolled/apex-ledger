from __future__ import annotations

import asyncio
from typing import Dict, Optional

from ledger.event_store import EventStore
from ledger.events import StoredEvent


class ComplianceAuditView:
    """
    Simplified in-memory CQRS projection.
    Rebuilds into shadow storage and swaps it in to avoid disrupting live reads.
    """

    def __init__(self, event_store: EventStore) -> None:
        self.event_store = event_store
        self._rows: Dict[str, dict] = {}
        self._shadow_rows: Optional[Dict[str, dict]] = None
        self._last_global_position: int = 0
        self._retry_count: int = 3

    def read(self, stream_id: str) -> Optional[dict]:
        return self._rows.get(stream_id)

    async def apply_event(self, event: StoredEvent, target: Optional[Dict[str, dict]] = None) -> None:
        store = target if target is not None else self._rows
        row = store.setdefault(
            event.stream_id,
            {
                "stream_id": event.stream_id,
                "last_decision": None,
                "compliance_completed": [],
                "last_global_position": 0,
            },
        )

        if event.event_type.endswith("Completed"):
            row["compliance_completed"].append(event.event_type)

        if event.event_type == "DecisionGenerated":
            row["last_decision"] = event.payload.get("decision")

        row["last_global_position"] = event.global_position

    async def run_once(self) -> None:
        async for event in self.event_store.load_all(from_global_position=self._last_global_position):
            await self.apply_event(event)
            self._last_global_position = event.global_position

    async def rebuild_from_scratch(self) -> None:
        shadow: Dict[str, dict] = {}
        async for event in self.event_store.load_all(from_global_position=0, batch_size=500):
            await self.apply_event(event, target=shadow)

        # atomic swap style
        self._shadow_rows = shadow
        self._rows = self._shadow_rows
        self._shadow_rows = None

    async def run_daemon(self, interval_seconds: float = 0.1, stop_after: float = 1.0) -> None:
        elapsed = 0.0
        while elapsed < stop_after:
            await self.run_once()
            await asyncio.sleep(interval_seconds)
            elapsed += interval_seconds