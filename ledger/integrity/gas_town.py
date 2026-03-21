"""
ledger/integrity/gas_town.py
=============================
Gas Town crash recovery — reconstruct_agent_context().

When an agent crashes, this function:
  1. Finds the crashed session stream
  2. Replays all events to rebuild context
  3. Returns the context the agent needs to resume

Named after the Gas Town pattern: every agent must record its session
start BEFORE loading any context, so recovery is always possible.

Usage:
    context = await reconstruct_agent_context(
        store=store,
        agent_type="fraud_detection",
        application_id="APEX-0031",
    )
    if context["needs_reconciliation"]:
        # Resume from context["last_successful_node"]
        agent.resume(context)
"""
from __future__ import annotations
from datetime import datetime


async def reconstruct_agent_context(
    store,
    agent_type: str,
    application_id: str,
) -> dict:
    """
    Finds the most recent agent session for the given agent_type and
    application_id, replays its events, and returns the recovery context.

    Returns dict with:
        session_id            — the crashed session ID
        agent_type            — agent type
        application_id        — application being processed
        model_version         — model used in the session
        context_source        — "prior_session_replay:{session_id}"
        status                — ACTIVE / FAILED / COMPLETED
        last_successful_node  — last node that completed successfully
        nodes_executed        — list of all nodes that ran
        needs_reconciliation  — True if session failed and needs recovery
        started_at            — when session started
        failed_at             — when session failed (if applicable)
    """
    # Find all agent streams for this agent type
    all_events = []
    async for event in store.load_all(event_types=["AgentSessionStarted"]):
        p = event.get("payload", {})
        if (p.get("agent_type") == agent_type and
                p.get("application_id") == application_id):
            all_events.append(event)

    if not all_events:
        return {
            "session_id": None,
            "agent_type": agent_type,
            "application_id": application_id,
            "model_version": None,
            "context_source": "no_prior_session",
            "status": "NOT_STARTED",
            "last_successful_node": None,
            "nodes_executed": [],
            "needs_reconciliation": False,
            "started_at": None,
            "failed_at": None,
        }

    # Get the most recent session
    latest = all_events[-1]
    session_id = latest["payload"]["session_id"]
    session_stream = f"agent-{agent_type}-{session_id}"

    # Replay the session stream
    session_events = await store.load_stream(session_stream)

    context = {
        "session_id": session_id,
        "agent_type": agent_type,
        "application_id": application_id,
        "model_version": latest["payload"].get("model_version"),
        "context_source": f"prior_session_replay:{session_id}",
        "status": "ACTIVE",
        "last_successful_node": None,
        "nodes_executed": [],
        "needs_reconciliation": False,
        "started_at": latest["payload"].get("started_at"),
        "failed_at": None,
    }

    for event in session_events:
        et = event.get("event_type")
        p = event.get("payload", {})

        if et == "AgentNodeExecuted":
            node_name = p.get("node_name")
            if node_name:
                context["nodes_executed"].append(node_name)
                context["last_successful_node"] = node_name

        elif et == "AgentSessionCompleted":
            context["status"] = "COMPLETED"
            context["needs_reconciliation"] = False

        elif et == "AgentSessionFailed":
            context["status"] = "FAILED"
            context["needs_reconciliation"] = True
            context["failed_at"] = p.get("failed_at")
            context["last_successful_node"] = p.get("last_successful_node")

    return context


async def find_crashed_sessions(
    store,
    agent_type: str | None = None,
) -> list[dict]:
    """
    Finds all agent sessions that are in FAILED state.
    Used by the recovery daemon to find sessions needing reconciliation.
    """
    crashed = []
    seen_sessions = set()

    async for event in store.load_all(event_types=["AgentSessionFailed"]):
        p = event.get("payload", {})
        session_id = p.get("session_id")
        if session_id in seen_sessions:
            continue
        if agent_type and p.get("agent_type") != agent_type:
            continue
        seen_sessions.add(session_id)
        crashed.append({
            "session_id": session_id,
            "agent_type": p.get("agent_type"),
            "application_id": p.get("application_id"),
            "error_type": p.get("error_type"),
            "error_message": p.get("error_message"),
            "last_successful_node": p.get("last_successful_node"),
            "recoverable": p.get("recoverable", False),
            "failed_at": p.get("failed_at"),
        })

    return crashed