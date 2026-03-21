"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST:
  [x] Phase 1, Day 1: append() + stream_version()
  [x] Phase 1, Day 1: load_stream()
  [x] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [x] Phase 1, Day 2: get_event() (needed for causation chain)
  [x] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncGenerator, Any
from uuid import UUID, uuid4
import asyncpg


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────

class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"OCC on '{stream_id}': expected v{expected}, actual v{actual}"
        )


class StreamNotFoundError(Exception):
    """Raised when a stream is expected to exist but doesn't."""
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream '{stream_id}' not found.")


# ─────────────────────────────────────────────────────────────────────────────
# TYPED EVENT MODELS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BaseEvent:
    """
    Base class for all domain events.
    Every event has a type, version, and payload.
    """
    event_type: str
    event_version: int = 1
    payload: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "payload": self.payload,
        }


@dataclass
class StoredEvent:
    """
    A domain event as stored in and retrieved from the event store.
    Immutable — never modify a stored event.
    """
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: dict
    metadata: dict
    recorded_at: datetime

    @classmethod
    def from_row(cls, row: dict) -> "StoredEvent":
        payload = row["payload"]
        metadata = row["metadata"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        return cls(
            event_id=row["event_id"],
            stream_id=row["stream_id"],
            stream_position=row["stream_position"],
            global_position=row.get("global_position", 0),
            event_type=row["event_type"],
            event_version=row["event_version"],
            payload=payload,
            metadata=metadata,
            recorded_at=row["recorded_at"],
        )

    def to_dict(self) -> dict:
        """Return a plain dict for backward-compatibility with existing agent code."""
        return {
            "event_id": str(self.event_id),
            "stream_id": self.stream_id,
            "stream_position": self.stream_position,
            "global_position": self.global_position,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "payload": self.payload,
            "metadata": self.metadata,
            "recorded_at": self.recorded_at,
        }


@dataclass
class StreamMetadata:
    """Metadata about an aggregate stream."""
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None = None
    metadata: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# TYPED DOMAIN EVENT CLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ApplicationSubmittedEvent(BaseEvent):
    event_type: str = "ApplicationSubmitted"
    event_version: int = 1

    @classmethod
    def create(cls, application_id: str, applicant_id: str,
               requested_amount_usd: float, loan_purpose: str,
               loan_term_months: int, submission_channel: str,
               contact_email: str, contact_name: str) -> "ApplicationSubmittedEvent":
        return cls(payload={
            "application_id": application_id,
            "applicant_id": applicant_id,
            "requested_amount_usd": str(requested_amount_usd),
            "loan_purpose": loan_purpose,
            "loan_term_months": loan_term_months,
            "submission_channel": submission_channel,
            "contact_email": contact_email,
            "contact_name": contact_name,
            "submitted_at": datetime.utcnow().isoformat(),
            "application_reference": application_id,
        })


@dataclass
class DocumentUploadRequestedEvent(BaseEvent):
    event_type: str = "DocumentUploadRequested"
    event_version: int = 1

    @classmethod
    def create(cls, application_id: str) -> "DocumentUploadRequestedEvent":
        return cls(payload={
            "application_id": application_id,
            "required_document_types": [
                "application_proposal",
                "income_statement",
                "balance_sheet",
            ],
            "deadline": datetime.utcnow().isoformat(),
            "requested_by": "system",
        })


@dataclass
class CreditAnalysisCompletedEvent(BaseEvent):
    event_type: str = "CreditAnalysisCompleted"
    event_version: int = 2

    @classmethod
    def create(cls, application_id: str, session_id: str,
               risk_tier: str, recommended_limit_usd: float,
               confidence: float, rationale: str, key_concerns: list,
               data_quality_caveats: list, model_version: str,
               model_deployment_id: str, input_data_hash: str,
               analysis_duration_ms: int,
               regulatory_basis: list | None = None) -> "CreditAnalysisCompletedEvent":
        return cls(payload={
            "application_id": application_id,
            "session_id": session_id,
            "decision": {
                "risk_tier": risk_tier,
                "recommended_limit_usd": str(recommended_limit_usd),
                "confidence": confidence,
                "rationale": rationale,
                "key_concerns": key_concerns or [],
                "data_quality_caveats": data_quality_caveats or [],
                "policy_overrides_applied": [],
            },
            "model_version": model_version,
            "model_deployment_id": model_deployment_id,
            "input_data_hash": input_data_hash,
            "analysis_duration_ms": analysis_duration_ms,
            "regulatory_basis": regulatory_basis or [],
            "completed_at": datetime.utcnow().isoformat(),
        })


@dataclass
class FraudScreeningRequestedEvent(BaseEvent):
    event_type: str = "FraudScreeningRequested"
    event_version: int = 1

    @classmethod
    def create(cls, application_id: str,
               triggered_by_event_id: str) -> "FraudScreeningRequestedEvent":
        return cls(payload={
            "application_id": application_id,
            "requested_at": datetime.utcnow().isoformat(),
            "triggered_by_event_id": triggered_by_event_id,
        })


@dataclass
class ApplicationApprovedEvent(BaseEvent):
    event_type: str = "ApplicationApproved"
    event_version: int = 1

    @classmethod
    def create(cls, application_id: str, approved_amount_usd: float,
               conditions: list, approved_by: str = "auto") -> "ApplicationApprovedEvent":
        return cls(payload={
            "application_id": application_id,
            "approved_amount_usd": str(approved_amount_usd),
            "interest_rate_pct": 7.5,
            "term_months": 36,
            "conditions": conditions,
            "approved_by": approved_by,
            "effective_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "approved_at": datetime.utcnow().isoformat(),
        })


@dataclass
class ApplicationDeclinedEvent(BaseEvent):
    event_type: str = "ApplicationDeclined"
    event_version: int = 1

    @classmethod
    def create(cls, application_id: str, decline_reasons: list,
               declined_by: str = "auto") -> "ApplicationDeclinedEvent":
        return cls(payload={
            "application_id": application_id,
            "decline_reasons": decline_reasons,
            "declined_by": declined_by,
            "adverse_action_notice_required": True,
            "adverse_action_codes": ["HIGH_RISK"],
            "declined_at": datetime.utcnow().isoformat(),
        })


@dataclass
class AgentSessionStartedEvent(BaseEvent):
    event_type: str = "AgentSessionStarted"
    event_version: int = 1

    @classmethod
    def create(cls, session_id: str, agent_type: str, application_id: str,
               model_version: str, context_source: str) -> "AgentSessionStartedEvent":
        return cls(payload={
            "session_id": session_id,
            "agent_type": agent_type,
            "application_id": application_id,
            "model_version": model_version,
            "context_source": context_source,
            "started_at": datetime.utcnow().isoformat(),
        })


@dataclass
class AgentNodeExecutedEvent(BaseEvent):
    event_type: str = "AgentNodeExecuted"
    event_version: int = 1

    @classmethod
    def create(cls, session_id: str, agent_type: str, node_name: str,
               node_sequence: int, llm_called: bool = False,
               llm_tokens_input: int | None = None,
               llm_tokens_output: int | None = None,
               llm_cost_usd: float | None = None) -> "AgentNodeExecutedEvent":
        return cls(payload={
            "session_id": session_id,
            "agent_type": agent_type,
            "node_name": node_name,
            "node_sequence": node_sequence,
            "llm_called": llm_called,
            "llm_tokens_input": llm_tokens_input,
            "llm_tokens_output": llm_tokens_output,
            "llm_cost_usd": llm_cost_usd,
            "executed_at": datetime.utcnow().isoformat(),
        })


# ─────────────────────────────────────────────────────────────────────────────
# UPCASTER REGISTRY — Phase 4
# ─────────────────────────────────────────────────────────────────────────────

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.
    """

    def __init__(self):
        self._upcasters: dict[str, dict[int, callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        et = event["event_type"]
        v = event.get("event_version", 1)
        chain = self._upcasters.get(et, {})
        while v in chain:
            event["payload"] = chain[v](dict(event["payload"]))
            v += 1
            event["event_version"] = v
        return event


# ─────────────────────────────────────────────────────────────────────────────
# EVENT STORE — PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────

class EventStore:
    """
    Append-only PostgreSQL event store. All agents and projections use this class.

    Schema highlights:
      events.global_position  — BIGINT GENERATED ALWAYS AS IDENTITY (no gaps)
      events.(stream_id, stream_position) — UNIQUE constraint for OCC
      outbox.event_id          — FK to events(event_id), same transaction
    """

    def __init__(self, db_url: str, upcaster_registry: UpcasterRegistry | None = None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def _init_connection(self, conn):
        """Register JSON/JSONB codecs so asyncpg returns dicts instead of strings."""
        await conn.set_type_codec(
            'jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')
        await conn.set_type_codec(
            'json', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self.db_url, min_size=2, max_size=10, init=self._init_connection)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """Returns current version, or -1 if stream doesn't exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id)
            return row["current_version"] if row else -1

    async def stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        """Returns full stream metadata, or None if stream doesn't exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM event_streams WHERE stream_id = $1", stream_id)
            if not row:
                return None
            return StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row.get("archived_at"),
                metadata=row["metadata"] if isinstance(row["metadata"], dict)
                         else json.loads(row["metadata"]),
            )

    async def append(
        self,
        stream_id: str,
        events: list[dict | BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Atomically appends events to stream_id with OCC.

        Uses SELECT ... FOR UPDATE on event_streams to serialise concurrent
        appends. The UNIQUE(stream_id, stream_position) constraint is the
        second safety net. The outbox write is in the same transaction.

        Returns list of stream positions assigned.
        Raises OptimisticConcurrencyError if stream version != expected_version.
        """
        # Normalise BaseEvent instances to dicts
        event_dicts = [
            e.to_dict() if isinstance(e, BaseEvent) else e
            for e in events
        ]

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row — prevents concurrent appends to same stream
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)

                # 2. OCC check
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(
                        stream_id, expected_version, current)

                # 3. Create stream if new
                if row is None:
                    await conn.execute(
                        "INSERT INTO event_streams"
                        "(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id, stream_id.split("-")[0])

                # 4. Insert events + outbox in same transaction
                positions = []
                meta = {**(metadata or {})}
                if correlation_id:
                    meta["correlation_id"] = correlation_id
                if causation_id:
                    meta["causation_id"] = causation_id

                for i, event in enumerate(event_dicts):
                    pos = expected_version + 1 + i
                    row_id = await conn.fetchrow(
                        "INSERT INTO events"
                        "(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at)"
                        " VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7)"
                        " RETURNING event_id",
                        stream_id, pos,
                        event["event_type"],
                        event.get("event_version", 1),
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
                    "UPDATE event_streams SET current_version=$1"
                    " WHERE stream_id=$2",
                    expected_version + len(event_dicts), stream_id)
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
        Returns plain dicts for backward-compatibility.
        """
        async with self._pool.acquire() as conn:
            q = ("SELECT event_id, stream_id, stream_position, event_type,"
                 " event_version, payload, metadata, recorded_at"
                 " FROM events WHERE stream_id=$1 AND stream_position>=$2")
            params = [stream_id, from_position]
            if to_position is not None:
                q += " AND stream_position<=$3"
                params.append(to_position)
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
                if self.upcasters:
                    e = self.upcasters.upcast(e)
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
        Used by the ProjectionDaemon. Optionally filter by event_types.
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
                if not rows:
                    break
                for row in rows:
                    payload = row["payload"]
                    metadata = row["metadata"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if isinstance(metadata, str):
                        metadata = json.loads(metadata)
                    e = {**dict(row), "payload": payload, "metadata": metadata}
                    if self.upcasters:
                        e = self.upcasters.upcast(e)
                    yield e
                pos = rows[-1]["global_position"]
                if len(rows) < batch_size:
                    break

    async def get_event(self, event_id: UUID) -> dict | None:
        """Loads one event by UUID. Used for causation chain lookups."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM events WHERE event_id=$1", event_id)
            if not row:
                return None
            payload = row["payload"]
            metadata = row["metadata"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            return {**dict(row), "payload": payload, "metadata": metadata}

    async def archive_stream(self, stream_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = NOW()"
                " WHERE stream_id = $1", stream_id)


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for tests only
# ─────────────────────────────────────────────────────────────────────────────

import asyncio as _asyncio
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime
from uuid import uuid4 as _uuid4


class InMemoryEventStore:
    """
    Thread-safe asyncio in-memory event store for unit tests.
    Identical interface to EventStore — swap one for the other with no changes.
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
        events: list[dict | BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        event_dicts = [
            e.to_dict() if isinstance(e, BaseEvent) else e
            for e in events
        ]
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)
            positions = []
            meta = {**(metadata or {})}
            if correlation_id:
                meta["correlation_id"] = correlation_id
            if causation_id:
                meta["causation_id"] = causation_id
            for i, event in enumerate(event_dicts):
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
            self._versions[stream_id] = current + len(event_dicts)
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

    async def stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        if stream_id not in self._versions:
            return None
        return StreamMetadata(
            stream_id=stream_id,
            aggregate_type=stream_id.split("-")[0],
            current_version=self._versions[stream_id],
            created_at=_datetime.utcnow(),
        )

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)