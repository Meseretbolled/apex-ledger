"""
ledger/mcp/resources.py
========================
6 MCP resources for The Ledger.

Resources are read-only data sources that external systems can query.
Unlike tools, resources don't trigger side effects.

RESOURCES:
  1. ledger://applications/{id}           — full application event history
  2. ledger://applications/{id}/summary   — projection snapshot
  3. ledger://applications/{id}/compliance — compliance audit at time T
  4. ledger://agents/performance           — all agent performance metrics
  5. ledger://streams/{stream_id}          — raw event stream
  6. ledger://ledger/health               — projection lag and system health
"""
from __future__ import annotations
from datetime import datetime


async def get_application_resource(
    store,
    pool,
    application_id: str,
) -> dict:
    """
    ledger://applications/{id}
    Full application event history across all streams.
    """
    loan_events = await store.load_stream(f"loan-{application_id}")
    credit_events = await store.load_stream(f"credit-{application_id}")
    fraud_events = await store.load_stream(f"fraud-{application_id}")
    compliance_events = await store.load_stream(f"compliance-{application_id}")

    # Get agent sessions
    all_agent_events = []
    async for event in store.load_all(
        event_types=["AgentSessionStarted"],
    ):
        if event["payload"].get("application_id") == application_id:
            session_id = event["payload"]["session_id"]
            agent_type = event["payload"]["agent_type"]
            session_events = await store.load_stream(f"agent-{agent_type}-{session_id}")
            all_agent_events.extend(session_events)

    return {
        "application_id": application_id,
        "retrieved_at": datetime.utcnow().isoformat(),
        "streams": {
            f"loan-{application_id}": [_serialize_event(e) for e in loan_events],
            f"credit-{application_id}": [_serialize_event(e) for e in credit_events],
            f"fraud-{application_id}": [_serialize_event(e) for e in fraud_events],
            f"compliance-{application_id}": [_serialize_event(e) for e in compliance_events],
            "agent_sessions": [_serialize_event(e) for e in all_agent_events],
        },
        "total_events": len(loan_events) + len(credit_events) + len(fraud_events) + len(compliance_events) + len(all_agent_events),
    }


async def get_application_summary_resource(
    pool,
    application_id: str,
) -> dict:
    """
    ledger://applications/{id}/summary
    Current state from the ApplicationSummary projection.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id
        )
    if not row:
        return {"found": False, "application_id": application_id}
    return {
        "found": True,
        "application_id": row["application_id"],
        "state": row["state"],
        "applicant_id": row["applicant_id"],
        "requested_amount_usd": float(row["requested_amount_usd"] or 0),
        "risk_tier": row["risk_tier"],
        "credit_confidence": float(row["credit_confidence"] or 0),
        "fraud_score": float(row["fraud_score"] or 0),
        "compliance_status": row["compliance_status"],
        "decision": row["decision"],
        "approved_amount_usd": float(row["approved_amount_usd"] or 0),
        "last_event_type": row["last_event_type"],
        "last_event_at": row["last_event_at"].isoformat() if row["last_event_at"] else None,
        "final_decision_at": row["final_decision_at"].isoformat() if row["final_decision_at"] else None,
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
    }


async def get_compliance_resource(
    pool,
    application_id: str,
    at_time: str | None = None,
) -> dict:
    """
    ledger://applications/{id}/compliance
    Compliance audit snapshot — temporal query supported via at_time parameter.
    """
    from ledger.projections.compliance_audit import ComplianceAuditProjection
    proj = ComplianceAuditProjection(pool)

    if at_time:
        try:
            ts = datetime.fromisoformat(at_time)
        except Exception:
            ts = datetime.utcnow()
    else:
        ts = datetime.utcnow()

    result = await proj.get_compliance_at(application_id, ts)
    if not result:
        return {
            "found": False,
            "application_id": application_id,
            "queried_at": ts.isoformat(),
        }
    return {
        "found": True,
        "queried_at": ts.isoformat(),
        **result,
    }


async def get_agent_performance_resource(
    pool,
) -> dict:
    """
    ledger://agents/performance
    All agent performance metrics from the AgentPerformance projection.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger ORDER BY analyses_completed DESC"
        )
    agents = [
        {
            "agent_id": row["agent_id"],
            "model_version": row["model_version"],
            "analyses_completed": row["analyses_completed"],
            "avg_confidence": float(row["avg_confidence"] or 0),
            "total_cost_usd": float(row["total_cost_usd"] or 0),
            "avg_duration_ms": float(row["avg_duration_ms"] or 0),
            "approve_count": row["approve_count"],
            "decline_count": row["decline_count"],
            "refer_count": row["refer_count"],
        }
        for row in rows
    ]
    return {
        "retrieved_at": datetime.utcnow().isoformat(),
        "total_agents": len(agents),
        "agents": agents,
    }


async def get_stream_resource(
    store,
    stream_id: str,
    from_position: int = 0,
) -> dict:
    """
    ledger://streams/{stream_id}
    Raw event stream — any aggregate stream by ID.
    """
    events = await store.load_stream(stream_id, from_position)
    version = await store.stream_version(stream_id)
    return {
        "stream_id": stream_id,
        "current_version": version,
        "event_count": len(events),
        "from_position": from_position,
        "retrieved_at": datetime.utcnow().isoformat(),
        "events": [_serialize_event(e) for e in events],
    }


async def get_health_resource(
    store,
    pool,
) -> dict:
    """
    ledger://ledger/health
    Projection lag and system health.
    SLO: application_summary < 500ms, others < 2s.
    """
    async with pool.acquire() as conn:
        checkpoints = await conn.fetch(
            "SELECT projection_name, last_position, updated_at "
            "FROM projection_checkpoints"
        )
        event_count = await conn.fetchval("SELECT COUNT(*) FROM events")
        stream_count = await conn.fetchval("SELECT COUNT(*) FROM event_streams")

    projections = {}
    for row in checkpoints:
        lag_ms = int(
            (datetime.utcnow() - row["updated_at"].replace(tzinfo=None)).total_seconds() * 1000
        )
        slo_ms = 500 if row["projection_name"] == "application_summary" else 2000
        projections[row["projection_name"]] = {
            "last_position": row["last_position"],
            "updated_at": row["updated_at"].isoformat(),
            "lag_ms": lag_ms,
            "slo_ms": slo_ms,
            "within_slo": lag_ms <= slo_ms,
        }

    all_within_slo = all(p["within_slo"] for p in projections.values())
    return {
        "status": "healthy" if all_within_slo else "degraded",
        "checked_at": datetime.utcnow().isoformat(),
        "event_store": {
            "total_events": event_count,
            "total_streams": stream_count,
        },
        "projections": projections,
    }


def _serialize_event(e: dict) -> dict:
    return {
        "stream_id": e.get("stream_id"),
        "stream_position": e.get("stream_position"),
        "global_position": e.get("global_position"),
        "event_type": e.get("event_type"),
        "event_version": e.get("event_version"),
        "payload": e.get("payload", {}),
        "recorded_at": str(e.get("recorded_at", "")),
    }