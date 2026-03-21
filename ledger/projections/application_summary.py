"""
ledger/projections/application_summary.py
==========================================
ApplicationSummary projection — current state of each loan application.

SLO: < 500ms lag from event commit to projection update.

TABLE: application_summary
  application_id       TEXT PRIMARY KEY
  state                TEXT
  applicant_id         TEXT
  requested_amount_usd NUMERIC
  risk_tier            TEXT
  credit_confidence    NUMERIC
  fraud_score          NUMERIC
  compliance_status    TEXT
  decision             TEXT
  approved_amount_usd  NUMERIC
  last_event_type      TEXT
  last_event_at        TIMESTAMPTZ
  human_reviewer_id    TEXT
  final_decision_at    TIMESTAMPTZ
  updated_at           TIMESTAMPTZ
"""
from __future__ import annotations
from datetime import datetime


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS application_summary (
    application_id       TEXT PRIMARY KEY,
    state                TEXT NOT NULL DEFAULT 'NEW',
    applicant_id         TEXT,
    requested_amount_usd NUMERIC,
    risk_tier            TEXT,
    credit_confidence    NUMERIC,
    fraud_score          NUMERIC,
    compliance_status    TEXT,
    decision             TEXT,
    approved_amount_usd  NUMERIC,
    last_event_type      TEXT,
    last_event_at        TIMESTAMPTZ,
    human_reviewer_id    TEXT,
    final_decision_at    TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Events this projection cares about
HANDLED_EVENTS = {
    "ApplicationSubmitted",
    "DocumentUploadRequested",
    "DocumentUploaded",
    "CreditAnalysisRequested",
    "CreditAnalysisCompleted",
    "FraudScreeningRequested",
    "FraudScreeningCompleted",
    "ComplianceCheckRequested",
    "ComplianceCheckCompleted",
    "DecisionRequested",
    "DecisionGenerated",
    "ApplicationApproved",
    "ApplicationDeclined",
    "HumanReviewRequested",
    "HumanReviewCompleted",
    "PackageReadyForAnalysis",
}


class ApplicationSummaryProjection:
    name = "application_summary"

    def __init__(self, pool):
        self.pool = pool

    async def setup(self):
        """Create the projection table if it doesn't exist."""
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)

    async def handle(self, event: dict):
        """Update projection based on event type."""
        et = event.get("event_type")
        if et not in HANDLED_EVENTS:
            return
        p = event.get("payload", {})
        app_id = p.get("application_id")
        if not app_id:
            return
        recorded_at = event.get("recorded_at")
        if isinstance(recorded_at, str):
            try:
                recorded_at = datetime.fromisoformat(recorded_at)
            except Exception:
                recorded_at = datetime.utcnow()

        async with self.pool.acquire() as conn:
            # Ensure row exists
            await conn.execute(
                "INSERT INTO application_summary(application_id) VALUES($1) "
                "ON CONFLICT DO NOTHING",
                app_id
            )

            if et == "ApplicationSubmitted":
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state='SUBMITTED', applicant_id=$2, requested_amount_usd=$3, "
                    "last_event_type=$4, last_event_at=$5, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id, p.get("applicant_id"),
                    float(str(p.get("requested_amount_usd", 0)).replace(",","")),
                    et, recorded_at
                )

            elif et == "DocumentUploaded":
                await conn.execute(
                    "UPDATE application_summary SET state='DOCUMENTS_UPLOADED', "
                    "last_event_type=$2, last_event_at=$3, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id, et, recorded_at
                )

            elif et == "PackageReadyForAnalysis":
                await conn.execute(
                    "UPDATE application_summary SET state='DOCUMENTS_PROCESSED', "
                    "last_event_type=$2, last_event_at=$3, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id, et, recorded_at
                )

            elif et == "CreditAnalysisCompleted":
                decision = p.get("decision") or {}
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state='CREDIT_ANALYSIS_COMPLETE', risk_tier=$2, "
                    "credit_confidence=$3, last_event_type=$4, last_event_at=$5, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id,
                    decision.get("risk_tier"),
                    float(decision.get("confidence", 0)),
                    et, recorded_at
                )

            elif et == "FraudScreeningCompleted":
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state='FRAUD_SCREENING_COMPLETE', fraud_score=$2, "
                    "last_event_type=$3, last_event_at=$4, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id,
                    float(p.get("fraud_score", 0)),
                    et, recorded_at
                )

            elif et == "ComplianceCheckCompleted":
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state='COMPLIANCE_CHECK_COMPLETE', compliance_status=$2, "
                    "last_event_type=$3, last_event_at=$4, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id,
                    p.get("overall_verdict"),
                    et, recorded_at
                )

            elif et == "DecisionGenerated":
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state='PENDING_DECISION', decision=$2, "
                    "last_event_type=$3, last_event_at=$4, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id,
                    p.get("recommendation"),
                    et, recorded_at
                )

            elif et == "ApplicationApproved":
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state='APPROVED', decision='APPROVE', "
                    "approved_amount_usd=$2, final_decision_at=$3, "
                    "last_event_type=$4, last_event_at=$5, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id,
                    float(str(p.get("approved_amount_usd", 0)).replace(",","")),
                    recorded_at, et, recorded_at
                )

            elif et == "ApplicationDeclined":
                reasons = p.get("decline_reasons", [])
                is_compliance = any("REG-" in str(r) or "compliance" in str(r).lower() for r in reasons)
                state = "DECLINED_COMPLIANCE" if is_compliance else "DECLINED"
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state=$2, decision='DECLINE', final_decision_at=$3, "
                    "last_event_type=$4, last_event_at=$5, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id, state, recorded_at, et, recorded_at
                )

            elif et == "HumanReviewRequested":
                await conn.execute(
                    "UPDATE application_summary SET "
                    "state='PENDING_HUMAN_REVIEW', "
                    "last_event_type=$2, last_event_at=$3, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id, et, recorded_at
                )

            elif et == "HumanReviewCompleted":
                await conn.execute(
                    "UPDATE application_summary SET "
                    "human_reviewer_id=$2, "
                    "last_event_type=$3, last_event_at=$4, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id,
                    p.get("reviewed_by"),
                    et, recorded_at
                )

            else:
                await conn.execute(
                    "UPDATE application_summary SET "
                    "last_event_type=$2, last_event_at=$3, updated_at=NOW() "
                    "WHERE application_id=$1",
                    app_id, et, recorded_at
                )