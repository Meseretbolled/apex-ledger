from __future__ import annotations

from typing import List

from ledger.events import AgentContext


async def reconstruct_agent_context(event_store, session_stream_id: str) -> AgentContext:
    events = await event_store.load_stream(session_stream_id)

    if not events:
        return AgentContext(
            context_text="No session history found.",
            last_event_position=0,
            pending_work=[],
            session_health_status="EMPTY",
        )

    last_position = events[-1]["stream_position"]
    pending_work: List[str] = []
    older_summary: List[str] = []
    preserved_lines: List[str] = []

    older = events[:-3]
    recent = events[-3:]

    for event in older:
        if event["event_type"] in {"PENDING", "ERROR"}:
            preserved_lines.append(
                f'{event["stream_position"]}: {event["event_type"]} -> {event["payload"]}'
            )
        else:
            older_summary.append(event["event_type"])

    for event in recent:
        preserved_lines.append(
            f'{event["stream_position"]}: {event["event_type"]} -> {event["payload"]}'
        )

    for event in events:
        if event["event_type"] in {"PENDING", "ERROR"}:
            pending_work.append(
                f'{event["event_type"]} at position {event["stream_position"]}'
            )

    decision_exists = any(e["event_type"] == "DecisionGenerated" for e in events)
    completion_exists = any(
        e["event_type"] == "AgentNodeExecuted"
        and e["payload"].get("status") == "completed"
        for e in events
    )

    health = "HEALTHY"
    if decision_exists and not completion_exists:
        health = "NEEDS_RECONCILIATION"
        pending_work.append("Decision exists without completion event.")

    parts: List[str] = []
    if older_summary:
        parts.append("Older summary: " + ", ".join(older_summary))
    if preserved_lines:
        parts.append("Preserved recent/PENDING/ERROR events:\n" + "\n".join(preserved_lines))

    return AgentContext(
        context_text="\n\n".join(parts),
        last_event_position=last_position,
        pending_work=pending_work,
        session_health_status=health,
    )