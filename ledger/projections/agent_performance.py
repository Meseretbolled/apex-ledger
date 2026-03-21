"""
ledger/projections/agent_performance.py
========================================
AgentPerformance projection — per-agent metrics.

SLO: < 2s lag.

TABLE: agent_performance_ledger
  agent_id              TEXT
  model_version         TEXT
  analyses_completed    INT
  avg_confidence        NUMERIC
  total_cost_usd        NUMERIC
  avg_duration_ms       NUMERIC
  approve_count         INT
  decline_count         INT
  refer_count           INT
  override_count        INT
  last_updated          TIMESTAMPTZ
  PRIMARY KEY (agent_id, model_version)
"""
from __future__ import annotations
from datetime import datetime


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id           TEXT NOT NULL,
    model_version      TEXT NOT NULL,
    analyses_completed INT NOT NULL DEFAULT 0,
    avg_confidence     NUMERIC,
    total_cost_usd     NUMERIC NOT NULL DEFAULT 0,
    avg_duration_ms    NUMERIC,
    approve_count      INT NOT NULL DEFAULT 0,
    decline_count      INT NOT NULL DEFAULT 0,
    refer_count        INT NOT NULL DEFAULT 0,
    override_count     INT NOT NULL DEFAULT 0,
    last_updated       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_id, model_version)
);
"""

HANDLED_EVENTS = {
    "CreditAnalysisCompleted",
    "DecisionGenerated",
    "AgentSessionCompleted",
}


class AgentPerformanceProjection:
    name = "agent_performance"

    def __init__(self, pool):
        self.pool = pool

    async def setup(self):
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)

    async def handle(self, event: dict):
        et = event.get("event_type")
        if et not in HANDLED_EVENTS:
            return
        p = event.get("payload", {})

        async with self.pool.acquire() as conn:
            if et == "CreditAnalysisCompleted":
                agent_id = p.get("session_id", "unknown")
                model_version = p.get("model_version", "unknown")
                decision = p.get("decision") or {}
                confidence = float(decision.get("confidence", 0))
                duration_ms = float(p.get("analysis_duration_ms", 0))
                await conn.execute(
                    "INSERT INTO agent_performance_ledger"
                    "(agent_id, model_version, analyses_completed, avg_confidence, avg_duration_ms) "
                    "VALUES($1, $2, 1, $3, $4) "
                    "ON CONFLICT(agent_id, model_version) DO UPDATE SET "
                    "analyses_completed = agent_performance_ledger.analyses_completed + 1, "
                    "avg_confidence = (agent_performance_ledger.avg_confidence * agent_performance_ledger.analyses_completed + $3) "
                    "  / (agent_performance_ledger.analyses_completed + 1), "
                    "avg_duration_ms = (agent_performance_ledger.avg_duration_ms * agent_performance_ledger.analyses_completed + $4) "
                    "  / (agent_performance_ledger.analyses_completed + 1), "
                    "last_updated = NOW()",
                    agent_id, model_version, confidence, duration_ms
                )

            elif et == "AgentSessionCompleted":
                agent_id = p.get("agent_id", p.get("session_id", "unknown"))
                model_version = "gemini-2.0-flash"
                cost = float(p.get("total_cost_usd", 0))
                duration_ms = float(p.get("total_duration_ms", 0))
                await conn.execute(
                    "INSERT INTO agent_performance_ledger"
                    "(agent_id, model_version, analyses_completed, total_cost_usd, avg_duration_ms) "
                    "VALUES($1, $2, 1, $3, $4) "
                    "ON CONFLICT(agent_id, model_version) DO UPDATE SET "
                    "analyses_completed = agent_performance_ledger.analyses_completed + 1, "
                    "total_cost_usd = agent_performance_ledger.total_cost_usd + $3, "
                    "avg_duration_ms = (agent_performance_ledger.avg_duration_ms * agent_performance_ledger.analyses_completed + $4) "
                    "  / (agent_performance_ledger.analyses_completed + 1), "
                    "last_updated = NOW()",
                    agent_id, model_version, cost, duration_ms
                )

            elif et == "DecisionGenerated":
                agent_id = p.get("orchestrator_session_id", "unknown")
                model_version = (p.get("model_versions") or {}).get("orchestrator", "unknown")
                rec = p.get("recommendation", "REFER")
                col = {"APPROVE": "approve_count", "DECLINE": "decline_count"}.get(rec, "refer_count")
                await conn.execute(
                    f"INSERT INTO agent_performance_ledger(agent_id, model_version, {col}) "
                    f"VALUES($1, $2, 1) "
                    f"ON CONFLICT(agent_id, model_version) DO UPDATE SET "
                    f"{col} = agent_performance_ledger.{col} + 1, "
                    f"last_updated = NOW()",
                    agent_id, model_version
                )