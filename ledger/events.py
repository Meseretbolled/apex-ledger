from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ConfigDict


class OptimisticConcurrencyError(Exception):
    """Raised when an append attempts to write at an unexpected version."""


class DomainError(Exception):
    """Raised when a domain rule or state transition is violated."""


class BaseEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str
    event_version: int = 1
    payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StoredEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    recorded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class StreamMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stream_id: str
    stream_type: str
    current_version: int = 0
    is_archived: bool = False
    archived_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AgentContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    context_text: str
    last_event_position: int
    pending_work: List[str] = Field(default_factory=list)
    session_health_status: str


class IntegrityCheckResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chain_valid: bool
    tamper_detected: bool
    checked_events: int


class MCPToolError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error_type: str
    message: str
    context: Dict[str, Any] = Field(default_factory=dict)
    suggested_action: Optional[str] = None