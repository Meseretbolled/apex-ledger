"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST (implement in order):
  [x] Phase 1, Day 1: append() + stream_version()
  [x] Phase 1, Day 1: load_stream()
  [x] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [x] Phase 1, Day 2: get_event() (needed for causation chain)
  [x] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations
import json
from datetime import datetime
from typing import AsyncGenerator
from uuid import UUID
import asyncpg


class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id; self.expected = expected; self.actual = actual
        super().__init__(f"OCC on '{stream_id}': expected v{expected}, actual v{actual}")


class EventStore:
    """
    Append-only PostgreSQL event store. All agents and projections use this class.
    """

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def _init_connection(self, conn):
        """Register JSON/JSONB codecs so asyncpg returns dicts instead of strings."""
        await conn.set_type_codec(
            'jsonb',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        await conn.set_type_codec(
            'json',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self.db_url,
            min_size=2,
            max_size=10,
            init=self._init_connection
        )

    async def close(self) -> None:
        if self._pool: await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """Returns current version, or -1 if stream doesn't exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id)
            return row["current_version"] if row else -1

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,    # -1=new stream, 0+=expected current
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Atomically appends events to stream_id with OCC.
        Returns list of stream positions assigned.
        Raises OptimisticConcurrencyError if stream version != expected_version.
        Writes to outbox in same transaction.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row (prevents concurrent appends)
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)

                # 2. OCC check
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                # 3. Create stream if new
                if row is None:
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id, stream_id.split("-")[0])

                # 4. Insert each event + write to outbox in same transaction
                positions = []
                meta = {**(metadata or {})}
                if correlation_id: meta["correlation_id"] = correlation_id
                if causation_id: meta["causation_id"] = causation_id
                for i, event in enumerate(events):
                    pos = expected_version + 1 + i
                    row_id = await conn.fetchrow(
                        "INSERT INTO events(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at)"
                        " VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7)"
                        " RETURNING event_id",
                        stream_id, pos,
                        event["event_type"], event.get("event_version", 1),
                        json.dumps(event.get("payload", {})),
                        json.dumps(meta),
                        datetime.utcnow())
                    await conn.execute(
                        "INSERT INTO outbox(event_id, destination, payload)"
                        " VALUES($1, $2, $3::jsonb)",
                        row_id["event_id"], "default",
                        json.dumps(event.get("payload", {})))
                    positions.append(pos)

                # 5. Update stream version
                await conn.execute(
                    "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                    expected_version + len(events), stream_id)
                return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        """
        Loads events from a stream in stream_position order.
        Applies upcasters if self.upcasters is set.
        """
        async with self._pool.acquire() as conn:
            q = ("SELECT event_id, stream_id, stream_position, event_type,"
                 " event_version, payload, metadata, recorded_at"
                 " FROM events WHERE stream_id=$1 AND stream_position>=$2")
            params = [stream_id, from_position]
            if to_position is not None:
                q += " AND stream_position<=$3"; params.append(to_position)
            q += " ORDER BY stream_position ASC"
            rows = await conn.fetch(q, *params)
            events = []
            for row in rows:
                payload = row["payload"]
                metadata = row["metadata"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                e = {**dict(row), "payload": payload, "metadata": metadata}
                if self.upcasters: e = self.upcasters.upcast(e)
                events.append(e)
            return events

    async def load_all(
        self,
        from_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncGenerator[dict, None]:
        """
        Async generator yielding all events by global_position.
        Used by the ProjectionDaemon.
        Optionally filter by event_types list.
        """
        async with self._pool.acquire() as conn:
            pos = from_position
            while True:
                if event_types:
                    rows = await conn.fetch(
                        "SELECT global_position, stream_id, stream_position,"
                        " event_type, event_version, payload, metadata, recorded_at"
                        " FROM events WHERE global_position > $1"
                        " AND event_type = ANY($2)"
                        " ORDER BY global_position ASC LIMIT $3",
                        pos, event_types, batch_size)
                else:
                    rows = await conn.fetch(
                        "SELECT global_position, stream_id, stream_position,"
                        " event_type, event_version, payload, metadata, recorded_at"
                        " FROM events WHERE global_position > $1"
                        " ORDER BY global_position ASC LIMIT $2",
                        pos, batch_size)
                if not rows: break
                for row in rows:
                    payload = row["payload"]
                    metadata = row["metadata"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if isinstance(metadata, str):
                        metadata = json.loads(metadata)
                    e = {**dict(row), "payload": payload, "metadata": metadata}
                    if self.upcasters: e = self.upcasters.upcast(e)
                    yield e
                pos = rows[-1]["global_position"]
                if len(rows) < batch_size: break

    async def get_event(self, event_id: UUID) -> dict | None:
        """Loads one event by UUID. Used for causation chain lookups."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM events WHERE event_id=$1", event_id)
            if not row: return None
            payload = row["payload"]
            metadata = row["metadata"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            return {**dict(row), "payload": payload, "metadata": metadata}

    async def archive_stream(self, stream_id: str) -> None:
        """Marks a stream as archived."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = NOW() WHERE stream_id = $1",
                stream_id)

    async def get_stream_metadata(self, stream_id: str) -> dict | None:
        """Returns stream metadata."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM event_streams WHERE stream_id = $1", stream_id)
            return dict(row) if row else None


# ─────────────────────────────────────────────────────────────────────────────
# UPCASTER REGISTRY — Phase 4
# ─────────────────────────────────────────────────────────────────────────────

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.

    REGISTER AN UPCASTER:
        registry = UpcasterRegistry()

        @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
        def upcast_credit_v1_v2(payload: dict) -> dict:
            payload.setdefault("model_versions", {})
            return payload

    REQUIRED FOR PHASE 4:
        - CreditAnalysisCompleted  v1 → v2  (adds model_versions: dict)
        - DecisionGenerated        v1 → v2  (adds model_versions: dict)
    """

    def __init__(self):
        self._upcasters: dict[str, dict[int, callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        """Apply chain of upcasters until latest version reached."""
        et = event["event_type"]
        v = event.get("event_version", 1)
        chain = self._upcasters.get(et, {})
        while v in chain:
            event["payload"] = chain[v](dict(event["payload"]))
            v += 1
            event["event_version"] = v
        return event


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for tests only
# ─────────────────────────────────────────────────────────────────────────────

class InMemoryEventStore:
    """
    In-memory event store for unit tests. No database required.
    Identical interface to EventStore — swap transparently in conftest.py.
    Your Phase 1 tests use this.
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry
        self._streams: dict[str, list[dict]] = {}
        self._global: list[dict] = []

    async def stream_version(self, stream_id: str) -> int:
        events = self._streams.get(stream_id, [])
        return len(events) - 1  # -1 if empty, 0-based index otherwise

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        current = await self.stream_version(stream_id)
        if current != expected_version:
            raise OptimisticConcurrencyError(stream_id, expected_version, current)
        self._streams.setdefault(stream_id, [])
        positions = []
        for i, event in enumerate(events):
            pos = expected_version + 1 + i
            stored = {
                "event_id": str(__import__("uuid").uuid4()),
                "stream_id": stream_id,
                "stream_position": pos,
                "global_position": len(self._global),
                "event_type": event["event_type"],
                "event_version": event.get("event_version", 1),
                "payload": dict(event.get("payload", {})),
                "metadata": {
                    **(metadata or {}),
                    **({"correlation_id": correlation_id} if correlation_id else {}),
                    **({"causation_id": causation_id} if causation_id else {}),
                },
                "recorded_at": __import__("datetime").datetime.utcnow(),
            }
            self._streams[stream_id].append(stored)
            self._global.append(stored)
            positions.append(pos)
        return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        events = self._streams.get(stream_id, [])
        result = [e for e in events if e["stream_position"] >= from_position]
        if to_position is not None:
            result = [e for e in result if e["stream_position"] <= to_position]
        if self.upcasters:
            result = [self.upcasters.upcast(dict(e)) for e in result]
        return result

    async def load_all(
        self,
        from_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ):
        for event in self._global:
            if event["global_position"] >= from_position:
                if event_types is None or event["event_type"] in event_types:
                    yield dict(event)

    async def get_event(self, event_id) -> dict | None:
        for event in self._global:
            if event["event_id"] == str(event_id):
                return dict(event)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for Phase 1 tests only
# Identical interface to EventStore. Drop-in for tests; never use in production.
# ─────────────────────────────────────────────────────────────────────────────

import asyncio as _asyncio
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime
from uuid import uuid4 as _uuid4


class InMemoryEventStore:
    """
    Thread-safe (asyncio-safe) in-memory event store.
    Used exclusively in Phase 1 tests and conftest fixtures.
    Same interface as EventStore — swap one for the other with no code changes.
    """

    def __init__(self):
        self._streams: dict[str, list[dict]] = _defaultdict(list)
        self._versions: dict[str, int] = {}
        self._global: list[dict] = []
        self._checkpoints: dict[str, int] = {}
        self._locks: dict[str, _asyncio.Lock] = _defaultdict(_asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            positions = []
            meta = {**(metadata or {})}
            if correlation_id: meta["correlation_id"] = correlation_id
            if causation_id: meta["causation_id"] = causation_id

            for i, event in enumerate(events):
                pos = current + 1 + i
                stored = {
                    "event_id": str(_uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global),
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                    "metadata": meta,
                    "recorded_at": _datetime.utcnow().isoformat(),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = current + len(events)
            return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        events = [
            e for e in self._streams.get(stream_id, [])
            if e["stream_position"] >= from_position
            and (to_position is None or e["stream_position"] <= to_position)
        ]
        return sorted(events, key=lambda e: e["stream_position"])

    async def load_all(
        self,
        from_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ):
        for e in self._global:
            if e["global_position"] >= from_position:
                if event_types is None or e["event_type"] in event_types:
                    yield e

    async def get_event(self, event_id: str) -> dict | None:
        for e in self._global:
            if e["event_id"] == event_id:
                return e
        return None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)