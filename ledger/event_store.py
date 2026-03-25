from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncGenerator
from uuid import UUID, uuid4

try:
    import asyncpg
except ModuleNotFoundError:
    asyncpg = None

from ledger.upcasters import UpcasterRegistry


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────

class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""

    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(f"OCC on '{stream_id}': expected v{expected}, actual v{actual}")


class StreamNotFoundError(Exception):
    """Raised when a stream is expected to exist but doesn't."""

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream '{stream_id}' not found.")


class DomainError(Exception):
    """Raised when domain invariants are violated."""

    def __init__(self, message: str):
        super().__init__(message)


# ─────────────────────────────────────────────────────────────────────────────
# TYPED EVENT MODELS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BaseEvent:
    event_type: str
    event_version: int = 1
    payload: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "payload": self.payload,
            "metadata": self.metadata,
        }


@dataclass
class StoredEvent:
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
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    updated_at: datetime | None = None
    is_archived: bool = False
    archived_at: datetime | None = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "aggregate_type": self.aggregate_type,
            "current_version": self.current_version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "is_archived": self.is_archived,
            "archived_at": self.archived_at,
            "metadata": self.metadata,
        }


# ─────────────────────────────────────────────────────────────────────────────
# TYPED DOMAIN EVENT CLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ApplicationSubmittedEvent(BaseEvent):
    event_type: str = "ApplicationSubmitted"
    event_version: int = 2

    @classmethod
    def create(
        cls,
        application_id: str,
        applicant_id: str,
        requested_amount_usd: float,
        loan_purpose: str,
        loan_term_months: int,
        submission_channel: str,
        contact_email: str,
        contact_name: str,
    ) -> "ApplicationSubmittedEvent":
        return cls(
            payload={
                "application_id": application_id,
                "applicant_id": applicant_id,
                "requested_amount_usd": str(requested_amount_usd),
                "loan_purpose": loan_purpose,
                "loan_term_months": loan_term_months,
                "submission_channel": submission_channel,
                "contact_email": contact_email,
                "contact_name": contact_name,
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "application_reference": application_id,
            }
        )


@dataclass
class DocumentUploadRequestedEvent(BaseEvent):
    event_type: str = "DocumentUploadRequested"
    event_version: int = 1

    @classmethod
    def create(cls, application_id: str) -> "DocumentUploadRequestedEvent":
        return cls(
            payload={
                "application_id": application_id,
                "required_document_types": [
                    "application_proposal",
                    "income_statement",
                    "balance_sheet",
                ],
                "deadline": datetime.now(timezone.utc).isoformat(),
                "requested_by": "system",
            }
        )


@dataclass
class CreditAnalysisCompletedEvent(BaseEvent):
    event_type: str = "CreditAnalysisCompleted"
    event_version: int = 2

    @classmethod
    def create(
        cls,
        application_id: str,
        session_id: str,
        risk_tier: str,
        recommended_limit_usd: float,
        confidence: float,
        rationale: str,
        key_concerns: list,
        data_quality_caveats: list,
        model_version: str,
        model_deployment_id: str,
        input_data_hash: str,
        analysis_duration_ms: int,
        regulatory_basis: list | None = None,
    ) -> "CreditAnalysisCompletedEvent":
        return cls(
            payload={
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
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
        )


@dataclass
class FraudScreeningRequestedEvent(BaseEvent):
    event_type: str = "FraudScreeningRequested"
    event_version: int = 1

    @classmethod
    def create(cls, application_id: str, triggered_by_event_id: str) -> "FraudScreeningRequestedEvent":
        return cls(
            payload={
                "application_id": application_id,
                "requested_at": datetime.now(timezone.utc).isoformat(),
                "triggered_by_event_id": triggered_by_event_id,
            }
        )


@dataclass
class ApplicationApprovedEvent(BaseEvent):
    event_type: str = "ApplicationApproved"
    event_version: int = 1

    @classmethod
    def create(
        cls,
        application_id: str,
        approved_amount_usd: float,
        conditions: list,
        approved_by: str = "auto",
    ) -> "ApplicationApprovedEvent":
        return cls(
            payload={
                "application_id": application_id,
                "approved_amount_usd": str(approved_amount_usd),
                "interest_rate_pct": 7.5,
                "term_months": 36,
                "conditions": conditions,
                "approved_by": approved_by,
                "effective_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "approved_at": datetime.now(timezone.utc).isoformat(),
            }
        )


@dataclass
class ApplicationDeclinedEvent(BaseEvent):
    event_type: str = "ApplicationDeclined"
    event_version: int = 1

    @classmethod
    def create(
        cls,
        application_id: str,
        decline_reasons: list,
        declined_by: str = "auto",
    ) -> "ApplicationDeclinedEvent":
        return cls(
            payload={
                "application_id": application_id,
                "decline_reasons": decline_reasons,
                "declined_by": declined_by,
                "adverse_action_notice_required": True,
                "adverse_action_codes": ["HIGH_RISK"],
                "declined_at": datetime.now(timezone.utc).isoformat(),
            }
        )


@dataclass
class AgentSessionStartedEvent(BaseEvent):
    event_type: str = "AgentSessionStarted"
    event_version: int = 1

    @classmethod
    def create(
        cls,
        session_id: str,
        agent_type: str,
        application_id: str,
        model_version: str,
        context_source: str,
    ) -> "AgentSessionStartedEvent":
        return cls(
            payload={
                "session_id": session_id,
                "agent_type": agent_type,
                "application_id": application_id,
                "model_version": model_version,
                "context_source": context_source,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
        )


@dataclass
class AgentNodeExecutedEvent(BaseEvent):
    event_type: str = "AgentNodeExecuted"
    event_version: int = 1

    @classmethod
    def create(
        cls,
        session_id: str,
        agent_type: str,
        node_name: str,
        node_sequence: int,
        llm_called: bool = False,
        llm_tokens_input: int | None = None,
        llm_tokens_output: int | None = None,
        llm_cost_usd: float | None = None,
        status: str = "completed",
    ) -> "AgentNodeExecutedEvent":
        return cls(
            payload={
                "session_id": session_id,
                "agent_type": agent_type,
                "node_name": node_name,
                "node_sequence": node_sequence,
                "llm_called": llm_called,
                "llm_tokens_input": llm_tokens_input,
                "llm_tokens_output": llm_tokens_output,
                "llm_cost_usd": llm_cost_usd,
                "status": status,
                "executed_at": datetime.now(timezone.utc).isoformat(),
            }
        )


# ─────────────────────────────────────────────────────────────────────────────
# EVENT STORE — PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────

class EventStore:
    """
    Append-only PostgreSQL event store with optimistic concurrency control.
    """

    def __init__(self, db_url: str, upcaster_registry: UpcasterRegistry | None = None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool = None

    async def _init_connection(self, conn):
        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
        await conn.set_type_codec(
            "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    async def connect(self) -> None:
        if asyncpg is None:
            raise RuntimeError(
                "asyncpg is not installed. Install requirements.txt to use PostgreSQL EventStore."
            )
        self._pool = await asyncpg.create_pool(
            self.db_url,
            min_size=1,
            max_size=10,
            init=self._init_connection,
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns current version, or -1 if stream doesn't exist.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return row["current_version"] if row else -1

    async def get_stream_metadata(self, stream_id: str) -> dict | None:
        """
        Returns stream metadata as a dict, or None if stream doesn't exist.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, aggregate_type, current_version,
                       created_at, updated_at, is_archived, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
                """,
                stream_id,
            )
            if not row:
                return None

            metadata = row["metadata"]
            if isinstance(metadata, str):
                metadata = json.loads(metadata)

            return {
                "stream_id": row["stream_id"],
                "aggregate_type": row["aggregate_type"],
                "current_version": row["current_version"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "is_archived": row["is_archived"],
                "archived_at": row["archived_at"],
                "metadata": metadata,
            }

    async def stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        meta = await self.get_stream_metadata(stream_id)
        if not meta:
            return None
        return StreamMetadata(
            stream_id=meta["stream_id"],
            aggregate_type=meta["aggregate_type"],
            current_version=meta["current_version"],
            created_at=meta["created_at"],
            updated_at=meta["updated_at"],
            is_archived=meta["is_archived"],
            archived_at=meta["archived_at"],
            metadata=meta["metadata"],
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
        Atomically appends events with OCC.
        Returns stream positions.
        """
        event_dicts = [
            e.to_dict() if hasattr(e, "to_dict") else e
            for e in events
        ]

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT current_version, is_archived
                    FROM event_streams
                    WHERE stream_id = $1
                    FOR UPDATE
                    """,
                    stream_id,
                )

                current = row["current_version"] if row else -1
                is_archived = row["is_archived"] if row else False

                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                if is_archived:
                    raise DomainError(f"Cannot append to archived stream '{stream_id}'")

                if row is None:
                    await conn.execute(
                        """
                        INSERT INTO event_streams
                            (stream_id, aggregate_type, current_version, is_archived, metadata)
                        VALUES ($1, $2, 0, FALSE, $3::jsonb)
                        """,
                        stream_id,
                        stream_id.split("-")[0],
                        json.dumps({}),
                    )

                positions: list[int] = []
                base_meta = {**(metadata or {})}
                if correlation_id:
                    base_meta["correlation_id"] = correlation_id
                if causation_id:
                    base_meta["causation_id"] = causation_id

                for i, event in enumerate(event_dicts):
                    pos = expected_version + 1 + i
                    event_meta = {**base_meta, **event.get("metadata", {})}

                    row_id = await conn.fetchrow(
                        """
                        INSERT INTO events
                            (stream_id, stream_position, event_type, event_version,
                             payload, metadata, recorded_at)
                        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)
                        RETURNING event_id
                        """,
                        stream_id,
                        pos,
                        event["event_type"],
                        event.get("event_version", 1),
                        json.dumps(event.get("payload", {})),
                        json.dumps(event_meta),
                        datetime.now(timezone.utc),
                    )

                    await conn.execute(
                        """
                        INSERT INTO outbox(event_id, destination, payload)
                        VALUES($1, $2, $3::jsonb)
                        """,
                        row_id["event_id"],
                        "default",
                        json.dumps(event.get("payload", {})),
                    )
                    positions.append(pos)

                await conn.execute(
                    """
                    UPDATE event_streams
                    SET current_version = $1,
                        updated_at = NOW()
                    WHERE stream_id = $2
                    """,
                    expected_version + len(event_dicts),
                    stream_id,
                )

                return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        """
        Loads a stream in stream_position order.
        Applies upcasting transparently.
        """
        async with self._pool.acquire() as conn:
            query = """
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events
                WHERE stream_id = $1
                  AND stream_position >= $2
            """
            params: list[Any] = [stream_id, from_position]

            if to_position is not None:
                query += " AND stream_position <= $3"
                params.append(to_position)

            query += " ORDER BY stream_position ASC"

            rows = await conn.fetch(query, *params)
            events: list[dict] = []

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
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
        from_position: int | None = None,
    ) -> AsyncGenerator[dict, None]:
        """
        Async generator yielding all events in global_position order.

        Supports both:
        - from_global_position (preferred)
        - from_position (legacy compatibility)
        """
        async with self._pool.acquire() as conn:
            pos = from_position if from_position is not None else from_global_position

            while True:
                if event_types:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, global_position, stream_id, stream_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1
                          AND event_type = ANY($2)
                        ORDER BY global_position ASC
                        LIMIT $3
                        """,
                        pos,
                        event_types,
                        batch_size,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, global_position, stream_id, stream_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1
                        ORDER BY global_position ASC
                        LIMIT $2
                        """,
                        pos,
                        batch_size,
                    )

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
        """
        Loads one event by UUID.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT event_id, global_position, stream_id, stream_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events
                WHERE event_id = $1
                """,
                event_id,
            )
            if not row:
                return None

            payload = row["payload"]
            metadata = row["metadata"]

            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(metadata, str):
                metadata = json.loads(metadata)

            e = {**dict(row), "payload": payload, "metadata": metadata}
            if self.upcasters:
                e = self.upcasters.upcast(e)
            return e

    async def archive_stream(self, stream_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE event_streams
                SET is_archived = TRUE,
                    archived_at = NOW(),
                    updated_at = NOW()
                WHERE stream_id = $1
                """,
                stream_id,
            )


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for tests only
# ─────────────────────────────────────────────────────────────────────────────

import asyncio as _asyncio
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime


class InMemoryEventStore:
    """
    asyncio-safe in-memory event store with same interface as EventStore.
    """

    def __init__(self, upcaster_registry: UpcasterRegistry | None = None):
        self._streams: dict[str, list[dict]] = _defaultdict(list)
        self._versions: dict[str, int] = {}
        self._stream_meta: dict[str, dict] = {}
        self._global: list[dict] = []
        self._checkpoints: dict[str, int] = {}
        self._locks: dict[str, _asyncio.Lock] = _defaultdict(_asyncio.Lock)
        self.upcasters = upcaster_registry

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def get_stream_metadata(self, stream_id: str) -> dict | None:
        return self._stream_meta.get(stream_id)

    async def stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        meta = self._stream_meta.get(stream_id)
        if not meta:
            return None
        return StreamMetadata(
            stream_id=meta["stream_id"],
            aggregate_type=meta["aggregate_type"],
            current_version=meta["current_version"],
            created_at=meta["created_at"],
            updated_at=meta.get("updated_at"),
            is_archived=meta.get("is_archived", False),
            archived_at=meta.get("archived_at"),
            metadata=meta.get("metadata", {}),
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
        event_dicts = [
            e.to_dict() if hasattr(e, "to_dict") else e
            for e in events
        ]

        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            meta = self._stream_meta.get(stream_id)
            if meta and meta.get("is_archived"):
                raise DomainError(f"Cannot append to archived stream '{stream_id}'")

            if stream_id not in self._stream_meta:
                now = _datetime.now(timezone.utc)
                self._stream_meta[stream_id] = {
                    "stream_id": stream_id,
                    "aggregate_type": stream_id.split("-")[0],
                    "current_version": current,
                    "created_at": now,
                    "updated_at": now,
                    "is_archived": False,
                    "archived_at": None,
                    "metadata": {},
                }

            positions = []
            base_meta = {**(metadata or {})}
            if correlation_id:
                base_meta["correlation_id"] = correlation_id
            if causation_id:
                base_meta["causation_id"] = causation_id

            for i, event in enumerate(event_dicts):
                pos = current + 1 + i
                stored = {
                    "event_id": str(uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global) + 1,
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                    "metadata": {**base_meta, **event.get("metadata", {})},
                    "recorded_at": _datetime.now(timezone.utc),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = current + len(event_dicts)
            self._stream_meta[stream_id]["current_version"] = self._versions[stream_id]
            self._stream_meta[stream_id]["updated_at"] = _datetime.now(timezone.utc)

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
        events = sorted(events, key=lambda e: e["stream_position"])

        out = []
        for e in events:
            item = dict(e)
            if self.upcasters:
                item = self.upcasters.upcast(item)
            out.append(item)
        return out

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
        from_position: int | None = None,
    ):
        """
        Backward-compatible async generator yielding all events in global order.

        Supports both:
        - from_global_position (new)
        - from_position (legacy tests)
        """
        if from_position is not None:
            from_global_position = from_position

        count = 0
        for e in self._global:
            if e["global_position"] > from_global_position:
                if event_types is None or e["event_type"] in event_types:
                    item = dict(e)
                    if self.upcasters:
                        item = self.upcasters.upcast(item)
                    yield item
                    count += 1
                    if count % batch_size == 0:
                        await _asyncio.sleep(0)

    async def get_event(self, event_id: str) -> dict | None:
        for e in self._global:
            if e["event_id"] == event_id:
                item = dict(e)
                if self.upcasters:
                    item = self.upcasters.upcast(item)
                return item
        return None

    async def archive_stream(self, stream_id: str) -> None:
        if stream_id in self._stream_meta:
            self._stream_meta[stream_id]["is_archived"] = True
            self._stream_meta[stream_id]["archived_at"] = _datetime.now(timezone.utc)
            self._stream_meta[stream_id]["updated_at"] = _datetime.now(timezone.utc)

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)