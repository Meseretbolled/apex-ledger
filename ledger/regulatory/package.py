
"""
ledger/regulatory/package.py
=============================
generate_regulatory_package(application_id, examination_date)

Produces a self-contained JSON examination package that a regulator can:
  1. Verify against the database independently
  2. Inspect the full event stream in order
  3. See compliance state at examination_date (temporal query)
  4. Verify cryptographic integrity of the audit chain
  5. Read a human-readable narrative of the application lifecycle
  6. Check model versions, confidence scores, and input data hashes
     for every AI agent that participated

The package NEVER requires trusting the system to validate accuracy —
every field is independently verifiable from the raw events table.

Usage:
    package = await generate_regulatory_package(
        store=store,
        application_id="APEX-0007",
        examination_date=datetime(2026, 3, 24),
    )
    with open(f"APEX-0007-regulatory.json", "w") as f:
        json.dump(package, f, indent=2, default=str)
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from ledger.integrity.audit_chain import AuditChain, compute_event_hash


# ─── Narrative templates ──────────────────────────────────────────────────────

_NARRATIVE_TEMPLATES: dict[str, str] = {
    "ApplicationSubmitted":
        "Application {application_id} was submitted by applicant {applicant_id} "
        "requesting ${requested_amount_usd:,.0f} for {loan_purpose}.",

    "DocumentUploadRequested":
        "Document upload was requested for application {application_id}.",

    "DocumentUploaded":
        "Document '{filename}' was uploaded for application {application_id}.",

    "PackageReadyForAnalysis":
        "Document package for {application_id} was validated and is ready for analysis.",

    "CreditAnalysisRequested":
        "Credit analysis was requested for application {application_id}.",

    "CreditAnalysisCompleted":
        "Credit analysis completed: risk tier {risk_tier}, "
        "confidence {confidence_score:.0%}, "
        "recommended limit ${recommended_limit_usd:,.0f}.",

    "CreditAnalysisDeferred":
        "Credit analysis was deferred: {deferral_reason}.",

    "FraudScreeningRequested":
        "Fraud screening was triggered for application {application_id}.",

    "FraudScreeningCompleted":
        "Fraud screening completed: fraud score {fraud_score:.2f}. "
        "Anomalies detected: {anomaly_count}.",

    "ComplianceCheckRequested":
        "Compliance check was initiated for application {application_id}.",

    "ComplianceRulePassed":
        "Compliance rule {rule_id} passed.",

    "ComplianceRuleFailed":
        "Compliance rule {rule_id} FAILED: {failure_reason}.",

    "ComplianceRuleNoted":
        "Compliance rule {rule_id} noted (advisory, not blocking).",

    "ComplianceCheckCompleted":
        "Compliance check completed: verdict {overall_verdict}. "
        "{rules_passed} rules passed, {rules_failed} rules failed.",

    "DecisionRequested":
        "All analyses complete. Decision synthesis was requested.",

    "DecisionGenerated":
        "Decision generated: recommendation {recommendation}, "
        "confidence {confidence_score:.0%}. "
        "Contributing agents: {agent_count}.",

    "HumanReviewRequested":
        "Human review was requested for application {application_id}.",

    "HumanReviewCompleted":
        "Human review completed by reviewer {reviewer_id}. "
        "Final decision: {final_decision}."
        "{override_note}",

    "ApplicationApproved":
        "Application APPROVED: ${approved_amount_usd:,.0f} at {interest_rate:.2%} interest. "
        "Approved by: {approved_by}.",

    "ApplicationDeclined":
        "Application DECLINED. Reasons: {decline_reasons}. "
        "Adverse action notice required: {adverse_action_notice_required}.",

    "AgentSessionStarted":
        "Agent {agent_type} started session {session_id} "
        "using model {model_version}.",

    "AgentSessionCompleted":
        "Agent {agent_type} completed session {session_id} in {total_duration_ms}ms. "
        "LLM calls: {total_llm_calls}, tokens: {total_tokens_used}, "
        "cost: ${total_cost_usd:.6f}.",

    "AgentSessionFailed":
        "Agent {agent_type} session {session_id} FAILED: {error_type} — {error_message}. "
        "Recoverable: {recoverable}.",
}


def _narrative_for_event(event: dict) -> str:
    """Generate one plain-English sentence for an event."""
    et = event.get("event_type", "")
    p  = event.get("payload", {})

    template = _NARRATIVE_TEMPLATES.get(et)
    if not template:
        return f"Event {et} occurred."

    try:
        # Enrich payload with computed fields
        enriched = dict(p)

        # fraud anomaly count
        if et == "FraudScreeningCompleted":
            enriched["anomaly_count"] = len(p.get("anomaly_flags", []))

        # decision agent count
        if et == "DecisionGenerated":
            enriched["agent_count"] = len(p.get("contributing_agent_sessions", []))

        # human review override note
        if et == "HumanReviewCompleted":
            if p.get("override"):
                enriched["override_note"] = f" (Override: {p.get('override_reason', 'no reason given')})"
            else:
                enriched["override_note"] = ""

        # numeric safety
        for key in ["requested_amount_usd", "recommended_limit_usd",
                    "approved_amount_usd", "fraud_score", "confidence_score",
                    "interest_rate", "total_cost_usd"]:
            if key in enriched and enriched[key] is not None:
                enriched[key] = float(enriched[key])

        return template.format_map(enriched)

    except (KeyError, ValueError, TypeError):
        return f"Event {et} occurred (payload: {json.dumps(p, default=str)[:120]})."


def _agent_info_from_events(events: list[dict]) -> list[dict]:
    """
    Extract model versions, confidence scores, and input data hashes
    for every AI agent that participated in the decision.
    """
    agents: dict[str, dict] = {}

    for event in events:
        et = event.get("event_type", "")
        p  = event.get("payload", {})

        if et == "AgentSessionStarted":
            sid = p.get("session_id", "")
            agents[sid] = {
                "session_id":    sid,
                "agent_type":    p.get("agent_type"),
                "agent_id":      p.get("agent_id"),
                "model_version": p.get("model_version"),
                "started_at":    str(event.get("recorded_at", "")),
                "status":        "ACTIVE",
                "confidence_score":   None,
                "input_data_hash":    None,
                "duration_ms":        None,
                "llm_calls":          None,
                "total_tokens":       None,
                "total_cost_usd":     None,
                "decision_summary":   None,
            }

        if et == "CreditAnalysisCompleted":
            sid = p.get("session_id", "")
            if sid in agents:
                agents[sid]["confidence_score"] = p.get("confidence_score") or p.get("confidence")
                agents[sid]["input_data_hash"]  = p.get("input_data_hash")
                agents[sid]["decision_summary"] = (
                    f"risk_tier={p.get('risk_tier')} "
                    f"limit=${p.get('recommended_limit_usd', 0):,.0f}"
                )

        if et == "FraudScreeningCompleted":
            sid = p.get("session_id", "")
            if sid in agents:
                agents[sid]["decision_summary"] = (
                    f"fraud_score={p.get('fraud_score', 0):.3f} "
                    f"anomalies={len(p.get('anomaly_flags', []))}"
                )

        if et == "DecisionGenerated":
            sid = p.get("session_id", "")
            if sid in agents:
                agents[sid]["confidence_score"] = p.get("confidence_score")
                agents[sid]["decision_summary"] = (
                    f"recommendation={p.get('recommendation')}"
                )

        if et in ("AgentSessionCompleted", "AgentSessionFailed"):
            sid = p.get("session_id", "")
            if sid in agents:
                agents[sid]["status"]       = "COMPLETED" if et == "AgentSessionCompleted" else "FAILED"
                agents[sid]["duration_ms"]  = p.get("total_duration_ms")
                agents[sid]["llm_calls"]    = p.get("total_llm_calls")
                agents[sid]["total_tokens"] = p.get("total_tokens_used")
                agents[sid]["total_cost_usd"] = p.get("total_cost_usd")

    return list(agents.values())


def _compliance_state_at(
    compliance_events: list[dict],
    examination_date: datetime,
) -> dict:
    """
    Reconstruct compliance state as it existed at examination_date.
    This is the temporal query — state at a specific moment in the past.
    """
    rules_passed: list[str] = []
    rules_failed: list[str] = []
    rules_noted:  list[str] = []
    has_hard_block = False
    block_rule_id  = None
    overall_verdict = None

    for event in compliance_events:
        recorded_at = event.get("recorded_at", "")
        if isinstance(recorded_at, str):
            try:
                recorded_dt = datetime.fromisoformat(
                    recorded_at.replace("Z", "+00:00")
                )
            except ValueError:
                continue
        elif isinstance(recorded_at, datetime):
            recorded_dt = recorded_at
        else:
            continue

        # Make both timezone-aware for comparison
        if recorded_dt.tzinfo is None:
            recorded_dt = recorded_dt.replace(tzinfo=timezone.utc)
        exam_dt = examination_date
        if exam_dt.tzinfo is None:
            exam_dt = exam_dt.replace(tzinfo=timezone.utc)

        if recorded_dt > exam_dt:
            continue  # event happened AFTER examination date — skip

        et = event.get("event_type", "")
        p  = event.get("payload", {})

        if et == "ComplianceRulePassed":
            rules_passed.append(p.get("rule_id", "unknown"))
        elif et == "ComplianceRuleFailed":
            rid = p.get("rule_id", "unknown")
            rules_failed.append(rid)
            if p.get("is_hard_block") or p.get("hard_block"):
                has_hard_block = True
                block_rule_id  = rid
        elif et == "ComplianceRuleNoted":
            rules_noted.append(p.get("rule_id", "unknown"))
        elif et == "ComplianceCheckCompleted":
            overall_verdict = p.get("overall_verdict")
            has_hard_block  = p.get("has_hard_block", has_hard_block)

    return {
        "rules_passed":    rules_passed,
        "rules_failed":    rules_failed,
        "rules_noted":     rules_noted,
        "has_hard_block":  has_hard_block,
        "block_rule_id":   block_rule_id,
        "overall_verdict": overall_verdict,
        "as_of":           str(examination_date),
    }


def _build_verification_fingerprint(events: list[dict]) -> str:
    """
    Independent verification fingerprint.
    A regulator can reproduce this by hashing the raw events table rows.
    """
    canonical = json.dumps(
        [
            {
                "event_id":       str(e.get("event_id", "")),
                "stream_id":      e.get("stream_id", ""),
                "stream_position":e.get("stream_position", 0),
                "event_type":     e.get("event_type", ""),
                "event_version":  e.get("event_version", 1),
                "payload":        e.get("payload", {}),
                "recorded_at":    str(e.get("recorded_at", "")),
            }
            for e in events
        ],
        sort_keys=True,
        default=str,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


async def generate_regulatory_package(
    store,
    application_id: str,
    examination_date: datetime | None = None,
) -> dict[str, Any]:
    """
    Generate a complete, self-contained regulatory examination package.

    Parameters
    ----------
    store            : EventStore or InMemoryEventStore
    application_id   : e.g. "APEX-0007"
    examination_date : temporal query point; defaults to now

    Returns
    -------
    dict — JSON-serialisable package with these sections:
        package_metadata      — who, when, version
        event_stream          — full ordered event stream with payloads
        projection_states     — application summary + compliance at examination_date
        integrity_verification— SHA-256 hash chain result
        agent_participation   — model versions, confidence, hashes per agent
        narrative             — human-readable lifecycle summary
        verification_instructions — how a regulator can independently verify
    """
    if examination_date is None:
        examination_date = datetime.now(timezone.utc)

    # ── 1. Load all relevant event streams ──────────────────────────────────
    loan_stream       = await store.load_stream(f"loan-{application_id}")
    docpkg_stream     = await store.load_stream(f"docpkg-{application_id}")
    credit_stream     = await store.load_stream(f"credit-{application_id}")
    fraud_stream      = await store.load_stream(f"fraud-{application_id}")
    compliance_stream = await store.load_stream(f"compliance-{application_id}")

    # Collect all agent session streams referenced in any stream
    agent_stream_ids: set[str] = set()
    for event in loan_stream + credit_stream + fraud_stream + compliance_stream:
        p = event.get("payload", {})
        for key in ("session_id", "agent_session_id"):
            sid = p.get(key)
            if sid:
                for agent_type in [
                    "credit_analysis", "fraud_detection",
                    "compliance", "decision_orchestrator", "document_processing",
                ]:
                    agent_stream_ids.add(f"agent-{agent_type}-{sid}")

    agent_events: list[dict] = []
    for stream_id in agent_stream_ids:
        evts = await store.load_stream(stream_id)
        agent_events.extend(evts)

    # All events combined and sorted by recorded_at
    all_events = (
        loan_stream + docpkg_stream + credit_stream +
        fraud_stream + compliance_stream + agent_events
    )
    all_events.sort(key=lambda e: str(e.get("recorded_at", "")))

    # ── 2. Integrity verification ────────────────────────────────────────────
    chain = AuditChain(store)
    loan_integrity = await chain.verify_stream(f"loan-{application_id}")

    integrity_result = {
        "loan_stream": {
            "stream_id":     f"loan-{application_id}",
            "is_valid":      loan_integrity.is_valid,
            "events_checked":loan_integrity.events_checked,
            "broken_at":     loan_integrity.broken_at,
            "error_message": loan_integrity.error_message,
        },
        "package_fingerprint": _build_verification_fingerprint(all_events),
        "verification_note": (
            "The package_fingerprint is a SHA-256 hash of all event payloads "
            "in chronological order. Reproduce by running: "
            "SELECT event_id, stream_id, stream_position, event_type, "
            "event_version, payload, recorded_at FROM events "
            f"WHERE stream_id LIKE '%-{application_id}' "
            "ORDER BY recorded_at ASC; "
            "then hash the result using the same algorithm."
        ),
    }

    # ── 3. Application summary (current state) ───────────────────────────────
    app_state = "UNKNOWN"
    applicant_id = None
    requested_amount = None
    final_decision = None
    risk_tier = None
    fraud_score = None

    for event in loan_stream:
        et = event.get("event_type", "")
        p  = event.get("payload", {})
        if et == "ApplicationSubmitted":
            app_state = "SUBMITTED"
            applicant_id = p.get("applicant_id")
            requested_amount = p.get("requested_amount_usd")
        elif et == "CreditAnalysisCompleted":
            risk_tier = p.get("risk_tier")
            app_state = "CREDIT_COMPLETE"
        elif et == "FraudScreeningCompleted":
            fraud_score = p.get("fraud_score")
            app_state = "FRAUD_COMPLETE"
        elif et == "ComplianceCheckCompleted":
            app_state = "COMPLIANCE_COMPLETE"
        elif et == "DecisionGenerated":
            final_decision = p.get("recommendation")
            app_state = "DECISION_GENERATED"
        elif et == "ApplicationApproved":
            app_state = "APPROVED"
            final_decision = "APPROVE"
        elif et == "ApplicationDeclined":
            app_state = "DECLINED"
            final_decision = "DECLINE"

    projection_states = {
        "application_summary": {
            "application_id":      application_id,
            "state":               app_state,
            "applicant_id":        applicant_id,
            "requested_amount_usd":requested_amount,
            "risk_tier":           risk_tier,
            "fraud_score":         fraud_score,
            "final_decision":      final_decision,
            "total_events":        len(loan_stream),
            "as_of":               str(examination_date),
        },
        "compliance_at_examination_date": _compliance_state_at(
            compliance_stream, examination_date
        ),
    }

    # ── 4. Agent participation ───────────────────────────────────────────────
    agent_participation = _agent_info_from_events(
        loan_stream + credit_stream + fraud_stream +
        compliance_stream + agent_events
    )

    # ── 5. Narrative ─────────────────────────────────────────────────────────
    # Only narrate significant loan lifecycle events (not internal agent plumbing)
    significant_types = {
        "ApplicationSubmitted", "DocumentUploaded", "PackageReadyForAnalysis",
        "CreditAnalysisCompleted", "CreditAnalysisDeferred",
        "FraudScreeningCompleted", "ComplianceRulePassed", "ComplianceRuleFailed",
        "ComplianceCheckCompleted", "DecisionGenerated",
        "HumanReviewCompleted", "ApplicationApproved", "ApplicationDeclined",
        "AgentSessionStarted", "AgentSessionCompleted", "AgentSessionFailed",
    }

    narrative_events = [
        e for e in all_events
        if e.get("event_type") in significant_types
    ]

    narrative_lines: list[str] = []
    for i, event in enumerate(narrative_events, 1):
        ts = str(event.get("recorded_at", ""))[:19]
        sentence = _narrative_for_event(event)
        narrative_lines.append(f"{i:02d}. [{ts}] {sentence}")

    # ── 6. Full event stream for package ────────────────────────────────────
    serialised_events = []
    for event in all_events:
        serialised_events.append({
            "event_id":       str(event.get("event_id", "")),
            "stream_id":      event.get("stream_id", ""),
            "stream_position":event.get("stream_position"),
            "event_type":     event.get("event_type", ""),
            "event_version":  event.get("event_version", 1),
            "payload":        event.get("payload", {}),
            "metadata":       event.get("metadata", {}),
            "recorded_at":    str(event.get("recorded_at", "")),
        })

    # ── 7. Assemble package ──────────────────────────────────────────────────
    package = {
        "package_metadata": {
            "application_id":   application_id,
            "generated_at":     str(datetime.now(timezone.utc)),
            "examination_date": str(examination_date),
            "package_version":  "1.0",
            "generator":        "apex-ledger/regulatory/package.py",
            "total_events":     len(serialised_events),
            "streams_included": list({e["stream_id"] for e in serialised_events}),
        },

        "event_stream": serialised_events,

        "projection_states": projection_states,

        "integrity_verification": integrity_result,

        "agent_participation": agent_participation,

        "narrative": {
            "summary": (
                f"Application {application_id} submitted by {applicant_id or 'unknown'} "
                f"for ${float(requested_amount or 0):,.0f}. "
                f"Final outcome: {final_decision or 'pending'}."
            ),
            "lifecycle": narrative_lines,
            "event_count": len(narrative_lines),
        },

        "verification_instructions": {
            "step_1": (
                "Query the events table in PostgreSQL: "
                f"SELECT * FROM events WHERE stream_id LIKE '%-{application_id}' "
                "ORDER BY recorded_at ASC;"
            ),
            "step_2": (
                "Verify event count matches package_metadata.total_events."
            ),
            "step_3": (
                "Recompute the package_fingerprint using SHA-256 over the ordered "
                "event payloads (see integrity_verification.verification_note) "
                "and compare to integrity_verification.package_fingerprint."
            ),
            "step_4": (
                "For temporal compliance query: compare projection_states."
                "compliance_at_examination_date against the raw compliance stream "
                f"events with recorded_at <= '{examination_date}'."
            ),
            "step_5": (
                "For agent integrity: cross-reference agent_participation entries "
                "against the agent session streams. Each session_id corresponds to "
                "stream agent-{agent_type}-{session_id} in the events table."
            ),
        },
    }

    return package