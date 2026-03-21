"""
ledger/upcasters.py — UpcasterRegistry
=======================================
COMPLETION STATUS: COMPLETE

Upcasters transform old event versions to the current version ON READ.
They NEVER write to the events table. Immutability is non-negotiable.

IMPLEMENTED:
  CreditAnalysisCompleted v1 → v2: add regulatory_basis=[] if absent
  DecisionGenerated v1 → v2: add model_versions={} if absent
"""
from __future__ import annotations


class UpcasterRegistry:
    """Apply on load_stream() — never on append()."""

    def upcast(self, event: dict) -> dict:
        et = event.get("event_type")
        ver = event.get("event_version", 1)

        if et == "CreditAnalysisCompleted" and ver < 2:
            event = dict(event)
            event["event_version"] = 2
            p = dict(event.get("payload", {}))
            p.setdefault("regulatory_basis", [])
            p.setdefault("model_deployment_id", "legacy-pre-2026")
            event["payload"] = p

        if et == "DecisionGenerated" and ver < 2:
            event = dict(event)
            event["event_version"] = 2
            p = dict(event.get("payload", {}))
            p.setdefault("model_versions", {})
            p.setdefault("contributing_sessions", [])
            event["payload"] = p

        return event