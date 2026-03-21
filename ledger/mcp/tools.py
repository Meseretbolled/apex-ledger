"""
ledger/mcp/tools.py
====================
8 MCP tools for The Ledger.

Tools allow external systems (Claude Desktop, other AI agents) to
interact with the event store and trigger pipeline operations.

TOOLS:
  1. submit_application       — submit a new loan application
  2. get_application_status   — get current state from projection
  3. get_event_stream         — load full event history for a stream
  4. get_decision_history     — get all decision events for an application
  5. trigger_compliance_check — manually trigger compliance re-evaluation
  6. get_agent_performance    — get agent metrics from projection
  7. verify_audit_chain       — verify stream integrity
  8. reconstruct_agent_context — Gas Town recovery for crashed agent
"""
from __future__ import annotations
import json
from datetime import datetime


async def submit_application(
    store,
    registry,
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    loan_term_months: int = 36,
    submission_channel: str = "web",
    contact_email: str = "",
    contact_name: str = "",
) -> dict:
    """
    Submit a new loan application to the event store.
    Returns the stream_id and application_id.
    """
    from ledger.commands.handlers import handle_submit_application, SubmitApplicationCommand
    cmd = SubmitApplicationCommand(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=requested_amount_usd,
        loan_purpose=loan_purpose,
        loan_term_months=loan_term_months,
        submission_channel=submission_channel,
        contact_email=contact_email,
        contact_name=contact_name,
    )
    stream_id = await handle_submit_application(cmd, store)
    return {
        "success": True,
        "application_id": application_id,
        "stream_id": stream_id,
        "message": f"Application {application_id} submitted successfully.",
    }


async def get_application_status(
    pool,
    application_id: str,
) -> dict:
    """
    Get current application state from the ApplicationSummary projection.
    SLO: < 500ms. Returns stale-but-consistent read model data.
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


async def get_event_stream(
    store,
    stream_id: str,
    from_position: int = 0,
    to_position: int | None = None,
) -> dict:
    """
    Load the full event history for any aggregate stream.
    Supports pagination via from_position and to_position.
    """
    events = await store.load_stream(stream_id, from_position, to_position)
    return {
        "stream_id": stream_id,
        "event_count": len(events),
        "from_position": from_position,
        "to_position": to_position,
        "events": [
            {
                "stream_position": e["stream_position"],
                "global_position": e.get("global_position"),
                "event_type": e["event_type"],
                "event_version": e["event_version"],
                "payload": e["payload"],
                "recorded_at": str(e.get("recorded_at", "")),
            }
            for e in events
        ],
    }


async def get_decision_history(
    store,
    application_id: str,
) -> dict:
    """
    Get all decision-related events for an application.
    Shows the complete causal chain: credit → fraud → compliance → decision.
    """
    decision_event_types = {
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "ComplianceCheckCompleted",
        "DecisionGenerated",
        "ApplicationApproved",
        "ApplicationDeclined",
        "HumanReviewRequested",
        "HumanReviewCompleted",
    }
    loan_events = await store.load_stream(f"loan-{application_id}")
    credit_events = await store.load_stream(f"credit-{application_id}")
    fraud_events = await store.load_stream(f"fraud-{application_id}")
    compliance_events = await store.load_stream(f"compliance-{application_id}")

    all_events = loan_events + credit_events + fraud_events + compliance_events
    decision_events = [
        {
            "stream_id": e["stream_id"],
            "stream_position": e["stream_position"],
            "event_type": e["event_type"],
            "payload": e["payload"],
            "recorded_at": str(e.get("recorded_at", "")),
        }
        for e in all_events
        if e["event_type"] in decision_event_types
    ]
    decision_events.sort(key=lambda e: str(e["recorded_at"]))

    return {
        "application_id": application_id,
        "decision_event_count": len(decision_events),
        "decision_history": decision_events,
    }


async def trigger_compliance_check(
    store,
    registry,
    application_id: str,
) -> dict:
    """
    Manually trigger a compliance re-evaluation for an application.
    Used when regulation set is updated or flags change.
    """
    from ledger.agents.stub_agents import ComplianceAgent
    agent = ComplianceAgent(
        agent_id=f"compliance-mcp-{application_id}",
        agent_type="compliance",
        store=store,
        registry=registry,
    )
    try:
        await agent.process_application(application_id)
        return {
            "success": True,
            "application_id": application_id,
            "message": "Compliance re-evaluation triggered successfully.",
        }
    except Exception as e:
        return {
            "success": False,
            "application_id": application_id,
            "error": str(e),
        }


async def get_agent_performance(
    pool,
    agent_id: str | None = None,
) -> dict:
    """
    Get agent performance metrics from the AgentPerformance projection.
    Returns all agents if agent_id is None.
    """
    async with pool.acquire() as conn:
        if agent_id:
            rows = await conn.fetch(
                "SELECT * FROM agent_performance_ledger WHERE agent_id = $1",
                agent_id
            )
        else:
            rows = await conn.fetch("SELECT * FROM agent_performance_ledger")

    results = [
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
            "override_count": row["override_count"],
            "last_updated": row["last_updated"].isoformat() if row["last_updated"] else None,
        }
        for row in rows
    ]
    return {
        "agent_count": len(results),
        "agents": results,
    }


async def verify_audit_chain(
    store,
    stream_id: str,
) -> dict:
    """
    Verify the SHA-256 hash chain of an event stream.
    Returns is_valid=True if the stream has not been tampered with.
    """
    from ledger.integrity.audit_chain import AuditChain
    chain = AuditChain(store)
    result = await chain.verify_stream(stream_id)
    final_hash = await chain.compute_stream_hash(stream_id)
    return {
        "stream_id": stream_id,
        "is_valid": result.is_valid,
        "events_checked": result.events_checked,
        "broken_at": result.broken_at,
        "error_message": result.error_message,
        "final_hash": final_hash,
        "verified_at": datetime.utcnow().isoformat(),
    }


async def reconstruct_agent_context_tool(
    store,
    agent_type: str,
    application_id: str,
) -> dict:
    """
    Reconstruct context for a crashed agent session (Gas Town recovery).
    Returns the context needed to resume from the last successful node.
    """
    from ledger.integrity.gas_town import reconstruct_agent_context
    context = await reconstruct_agent_context(store, agent_type, application_id)
    return {
        "application_id": application_id,
        "agent_type": agent_type,
        "recovery_context": context,
        "can_resume": context["needs_reconciliation"],
        "resume_from": context["last_successful_node"],
    }