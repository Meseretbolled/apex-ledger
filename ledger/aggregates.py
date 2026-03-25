from __future__ import annotations

from typing import Dict, List, Optional, Set

from pydantic import BaseModel, Field

from ledger.events import DomainError, StoredEvent


REQUIRED_COMPLIANCE_RULES = {
    "KYCCompleted",
    "FraudScreenCompleted",
    "AffordabilityCheckCompleted",
    "PolicyEligibilityChecked",
    "RegulatoryComplianceChecked",
    "DocumentFactsExtracted",
}


class ComplianceRecordAggregate(BaseModel):
    stream_id: str
    completed_rules: Set[str] = Field(default_factory=set)
    blocked: bool = False
    last_decision: Optional[str] = None

    def apply(self, event: StoredEvent) -> None:
        if event.event_type in REQUIRED_COMPLIANCE_RULES:
            self.completed_rules.add(event.event_type)

        if event.event_type == "ComplianceBlocked":
            self.blocked = True

        if event.event_type == "DecisionGenerated":
            self.last_decision = event.payload.get("decision")

    @classmethod
    def load(cls, stream_id: str, events: List[StoredEvent]) -> "ComplianceRecordAggregate":
        agg = cls(stream_id=stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def validate_decision_allowed(self) -> None:
        if self.blocked:
            raise DomainError("Decision cannot proceed because compliance is BLOCKED.")

        missing = REQUIRED_COMPLIANCE_RULES - self.completed_rules
        if missing:
            raise DomainError(
                f"Decision cannot proceed; missing compliance requirements: {sorted(missing)}"
            )


class AuditLedgerAggregate(BaseModel):
    stream_id: str
    has_document_facts: bool = False
    confidence_score: Optional[float] = None
    model_versions: Dict[str, str] = Field(default_factory=dict)
    triggering_event_ids: List[str] = Field(default_factory=list)
    decision_generated: bool = False

    def apply(self, event: StoredEvent) -> None:
        if event.event_type == "DocumentFactsExtracted":
            self.has_document_facts = True

        if event.event_type == "CreditAnalysisCompleted":
            self.confidence_score = event.payload.get("confidence")
            model_version = event.metadata.get("model_version")
            if model_version:
                self.model_versions["credit_analysis"] = model_version

        if event.event_type == "DecisionGenerated":
            self.decision_generated = True

        trigger = event.metadata.get("triggering_event_id")
        if trigger:
            self.triggering_event_ids.append(trigger)

    @classmethod
    def load(cls, stream_id: str, events: List[StoredEvent]) -> "AuditLedgerAggregate":
        agg = cls(stream_id=stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def validate_context_before_decision(self) -> None:
        if not self.has_document_facts:
            raise DomainError("DocumentFactsExtracted must occur before decision generation.")

    def validate_confidence_floor(self) -> Optional[str]:
        if self.confidence_score is not None and self.confidence_score < 0.60:
            return "REFER"
        return None

    def validate_model_version_locking(self, incoming_model_version: Optional[str]) -> None:
        existing = self.model_versions.get("credit_analysis")
        if existing and incoming_model_version and existing != incoming_model_version:
            raise DomainError(
                f"Model version mismatch: existing={existing}, incoming={incoming_model_version}"
            )

    def validate_causal_chain(self, event: StoredEvent) -> None:
        if event.event_type != "LoanApplicationSubmitted":
            if not event.metadata.get("triggering_event_id"):
                raise DomainError(
                    f"{event.event_type} must include metadata.triggering_event_id"
                )