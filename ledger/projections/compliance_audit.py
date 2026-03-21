"""
ledger/projections/compliance_audit.py
=======================================
ComplianceAudit projection — temporal snapshots.

SLO: < 2s lag.

Answers: get_compliance_at(application_id, timestamp)
  → what was the compliance status at that point in time?

TABLE: compliance_audit_view
  application_id   TEXT
  snapshot_at      TIMESTAMPTZ
  overall_verdict  TEXT
  rules_evaluated  INT
  rules_passed     INT
  rules_failed     INT
  rules_noted      INT
  has_hard_block   BOOLEAN
  block_rule_id    TEXT
  rule_results     JSONB
  PRIMARY KEY (application_id, snapshot_at)
"""
from __future__ import annotations
from datetime import datetime
import json


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS compliance_audit_view (
    application_id  TEXT NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL,
    overall_verdict TEXT,
    rules_evaluated INT NOT NULL DEFAULT 0,
    rules_passed    INT NOT NULL DEFAULT 0,
    rules_failed    INT NOT NULL DEFAULT 0,
    rules_noted     INT NOT NULL DEFAULT 0,
    has_hard_block  BOOLEAN NOT NULL DEFAULT FALSE,
    block_rule_id   TEXT,
    rule_results    JSONB NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (application_id, snapshot_at)
);
CREATE INDEX IF NOT EXISTS idx_compliance_audit_app
    ON compliance_audit_view (application_id, snapshot_at DESC);
"""

HANDLED_EVENTS = {
    "ComplianceCheckCompleted",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
    "ComplianceRuleNoted",
}


class ComplianceAuditProjection:
    name = "compliance_audit"

    def __init__(self, pool):
        self.pool = pool
        self._pending: dict[str, dict] = {}  # app_id -> partial snapshot

    async def setup(self):
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)

    async def handle(self, event: dict):
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

        if app_id not in self._pending:
            self._pending[app_id] = {"rule_results": [], "has_hard_block": False, "block_rule_id": None}

        pending = self._pending[app_id]

        if et in ("ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"):
            result = {
                "rule_id": p.get("rule_id"),
                "rule_name": p.get("rule_name"),
                "result": "PASSED" if et == "ComplianceRulePassed" else ("FAILED" if et == "ComplianceRuleFailed" else "NOTED"),
                "is_hard_block": p.get("is_hard_block", False),
                "failure_reason": p.get("failure_reason"),
                "evaluated_at": p.get("evaluated_at"),
            }
            pending["rule_results"].append(result)
            if p.get("is_hard_block") and et == "ComplianceRuleFailed":
                pending["has_hard_block"] = True
                pending["block_rule_id"] = p.get("rule_id")

        elif et == "ComplianceCheckCompleted":
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO compliance_audit_view("
                    "application_id, snapshot_at, overall_verdict, "
                    "rules_evaluated, rules_passed, rules_failed, rules_noted, "
                    "has_hard_block, block_rule_id, rule_results) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb) "
                    "ON CONFLICT DO NOTHING",
                    app_id, recorded_at,
                    p.get("overall_verdict"),
                    p.get("rules_evaluated", 0),
                    p.get("rules_passed", 0),
                    p.get("rules_failed", 0),
                    p.get("rules_noted", 0),
                    pending["has_hard_block"],
                    pending["block_rule_id"],
                    json.dumps(pending["rule_results"])
                )
            # Clear pending state
            self._pending.pop(app_id, None)

    async def get_compliance_at(self, application_id: str, timestamp) -> dict | None:
        """
        Temporal query — returns compliance state at a specific point in time.
        Used for regulatory audits and NARR-05 demo.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM compliance_audit_view "
                "WHERE application_id = $1 AND snapshot_at <= $2 "
                "ORDER BY snapshot_at DESC LIMIT 1",
                application_id, timestamp
            )
            if not row:
                return None
            return {
                "application_id": row["application_id"],
                "snapshot_at": row["snapshot_at"].isoformat(),
                "overall_verdict": row["overall_verdict"],
                "rules_evaluated": row["rules_evaluated"],
                "rules_passed": row["rules_passed"],
                "rules_failed": row["rules_failed"],
                "rules_noted": row["rules_noted"],
                "has_hard_block": row["has_hard_block"],
                "block_rule_id": row["block_rule_id"],
                "rule_results": list(row["rule_results"]),
            }