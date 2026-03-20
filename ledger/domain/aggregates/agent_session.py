"""
ledger/domain/aggregates/agent_session.py
==========================================
AgentSession aggregate — tracks one AI agent's work session.

Gas Town pattern: every agent appends AgentSessionStarted as its
very first event before doing any work. On crash recovery, the agent
replays this stream to reconstruct exactly where it left off.

BUSINESS RULES ENFORCED:
  1. AgentSessionStarted must be the first event (Gas Town anchor)
  2. No decision events allowed before AgentSessionStarted
  3. Model version must be recorded on session start
  4. Session cannot receive new events after AgentSessionCompleted or AgentSessionFailed
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum


class SessionStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RECOVERED = "RECOVERED"


class DomainError(Exception):
    """Raised when a business rule is violated."""
    pass


@dataclass
class AgentSessionAggregate:
    agent_id: str
    session_id: str

    status: SessionStatus = SessionStatus.NOT_STARTED
    agent_type: str | None = None
    application_id: str | None = None
    model_version: str | None = None
    context_source: str | None = None

    # Gas Town tracking
    context_loaded: bool = False
    last_successful_node: str | None = None
    nodes_executed: list[str] = field(default_factory=list)
    total_llm_calls: int = 0
    total_tokens: int = 0
    total_cost_usd: float = 0.0

    # Recovery tracking
    recovered_from_session_id: str | None = None
    recovery_point: str | None = None

    version: int = 0

    @classmethod
    async def load(cls, store, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(agent_id=agent_id, session_id=session_id)
        stream_events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        for event in stream_events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one event to update aggregate state."""
        et = event.get("event_type")
        p = event.get("payload", {})
        self.version += 1

        if et == "AgentSessionStarted":
            self.status = SessionStatus.ACTIVE
            self.agent_type = p.get("agent_type")
            self.application_id = p.get("application_id")
            self.model_version = p.get("model_version")
            self.context_source = p.get("context_source", "fresh")
            self.context_loaded = True

        elif et == "AgentInputValidated":
            # Inputs have been validated — session is properly initialised
            pass

        elif et == "AgentInputValidationFailed":
            # Inputs failed validation — session cannot proceed
            self.status = SessionStatus.FAILED

        elif et == "AgentNodeExecuted":
            node_name = p.get("node_name")
            if node_name:
                self.nodes_executed.append(node_name)
                self.last_successful_node = node_name
            # Accumulate LLM costs
            if p.get("llm_called"):
                self.total_llm_calls += 1
                self.total_tokens += (p.get("llm_tokens_input") or 0) + (p.get("llm_tokens_output") or 0)
                self.total_cost_usd += p.get("llm_cost_usd") or 0.0

        elif et == "AgentToolCalled":
            # Tool calls are informational — no state change needed
            pass

        elif et == "AgentOutputWritten":
            # Agent has written its output events to domain streams
            pass

        elif et == "AgentSessionCompleted":
            self.status = SessionStatus.COMPLETED
            self.total_llm_calls = p.get("total_llm_calls", self.total_llm_calls)
            self.total_tokens = p.get("total_tokens_used", self.total_tokens)
            self.total_cost_usd = p.get("total_cost_usd", self.total_cost_usd)

        elif et == "AgentSessionFailed":
            self.status = SessionStatus.FAILED
            self.last_successful_node = p.get("last_successful_node")

        elif et == "AgentSessionRecovered":
            self.status = SessionStatus.RECOVERED
            self.recovered_from_session_id = p.get("recovered_from_session_id")
            self.recovery_point = p.get("recovery_point")

    # ── Business rule assertions ──────────────────────────────────────────────

    def assert_context_loaded(self) -> None:
        """
        Gas Town rule — agent must have AgentSessionStarted before any decision.
        This is the core Gas Town pattern enforcement.
        """
        if not self.context_loaded:
            raise DomainError(
                f"Agent session {self.session_id} has no AgentSessionStarted event. "
                "Gas Town pattern requires session to be started before any decision."
            )

    def assert_active(self) -> None:
        """Assert the session is active and can receive new events."""
        if self.status == SessionStatus.NOT_STARTED:
            raise DomainError(
                f"Session {self.session_id} has not been started yet."
            )
        if self.status == SessionStatus.COMPLETED:
            raise DomainError(
                f"Session {self.session_id} is already completed. Cannot append more events."
            )
        if self.status == SessionStatus.FAILED:
            raise DomainError(
                f"Session {self.session_id} has failed. Cannot append more events."
            )

    def assert_model_version_current(self, model_version: str) -> None:
        """Assert the model version matches what was declared at session start."""
        if self.model_version and self.model_version != model_version:
            raise DomainError(
                f"Model version mismatch: session started with {self.model_version}, "
                f"but current call uses {model_version}."
            )

    def assert_not_completed(self) -> None:
        """Assert the session has not been completed or failed."""
        if self.status in (SessionStatus.COMPLETED, SessionStatus.FAILED):
            raise DomainError(
                f"Session {self.session_id} is in terminal state {self.status}."
            )

    def get_recovery_context(self) -> dict:
        """
        Returns context needed for crash recovery.
        Used by reconstruct_agent_context() in Gas Town recovery.
        """
        return {
            "session_id": self.session_id,
            "agent_type": self.agent_type,
            "application_id": self.application_id,
            "model_version": self.model_version,
            "context_source": self.context_source,
            "status": self.status,
            "last_successful_node": self.last_successful_node,
            "nodes_executed": self.nodes_executed,
            "total_nodes_executed": len(self.nodes_executed),
            "recovered_from_session_id": self.recovered_from_session_id,
            "recovery_point": self.recovery_point,
            "needs_reconciliation": self.status == SessionStatus.FAILED,
        }