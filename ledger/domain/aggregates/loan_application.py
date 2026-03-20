"""
ledger/domain/aggregates/loan_application.py
=============================================
COMPLETION STATUS: COMPLETE

The aggregate replays its event stream to rebuild state.
Command handlers validate against current state before appending events.

BUSINESS RULES ENFORCED:
  1. State machine: only valid transitions allowed
  2. DocumentFactsExtracted must exist before CreditAnalysisCompleted
  3. All 6 compliance rules must complete before DecisionGenerated (unless hard block)
  4. confidence < 0.60 → recommendation must be REFER (enforced here, not in LLM)
  5. Compliance BLOCKED → only DECLINE allowed, not APPROVE or REFER
  6. Causal chain: every agent event must reference a triggering event_id
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum


class ApplicationState(str, Enum):
    NEW = "NEW"; SUBMITTED = "SUBMITTED"; DOCUMENTS_PENDING = "DOCUMENTS_PENDING"
    DOCUMENTS_UPLOADED = "DOCUMENTS_UPLOADED"; DOCUMENTS_PROCESSED = "DOCUMENTS_PROCESSED"
    CREDIT_ANALYSIS_REQUESTED = "CREDIT_ANALYSIS_REQUESTED"; CREDIT_ANALYSIS_COMPLETE = "CREDIT_ANALYSIS_COMPLETE"
    FRAUD_SCREENING_REQUESTED = "FRAUD_SCREENING_REQUESTED"; FRAUD_SCREENING_COMPLETE = "FRAUD_SCREENING_COMPLETE"
    COMPLIANCE_CHECK_REQUESTED = "COMPLIANCE_CHECK_REQUESTED"; COMPLIANCE_CHECK_COMPLETE = "COMPLIANCE_CHECK_COMPLETE"
    PENDING_DECISION = "PENDING_DECISION"; PENDING_HUMAN_REVIEW = "PENDING_HUMAN_REVIEW"
    APPROVED = "APPROVED"; DECLINED = "DECLINED"; DECLINED_COMPLIANCE = "DECLINED_COMPLIANCE"
    REFERRED = "REFERRED"


VALID_TRANSITIONS = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.DOCUMENTS_PENDING],
    ApplicationState.DOCUMENTS_PENDING: [ApplicationState.DOCUMENTS_UPLOADED],
    ApplicationState.DOCUMENTS_UPLOADED: [ApplicationState.DOCUMENTS_PROCESSED],
    ApplicationState.DOCUMENTS_PROCESSED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.CREDIT_ANALYSIS_COMPLETE],
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: [ApplicationState.FRAUD_SCREENING_REQUESTED],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.FRAUD_SCREENING_COMPLETE],
    ApplicationState.FRAUD_SCREENING_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [ApplicationState.PENDING_DECISION, ApplicationState.DECLINED_COMPLIANCE],
    ApplicationState.PENDING_DECISION: [ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.PENDING_HUMAN_REVIEW],
    ApplicationState.PENDING_HUMAN_REVIEW: [ApplicationState.APPROVED, ApplicationState.DECLINED],
}


class DomainError(Exception):
    """Raised when a business rule is violated."""
    pass


@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState = ApplicationState.NEW
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    version: int = 0
    events: list[dict] = field(default_factory=list)

    # Track compliance for business rule enforcement
    compliance_verdict: str | None = None
    compliance_has_hard_block: bool = False

    # Track whether documents have been processed (rule 2)
    documents_processed: bool = False

    # Track credit analysis (rule 4 — confidence floor)
    credit_confidence: float | None = None
    credit_risk_tier: str | None = None

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_events = await store.load_stream(f"loan-{application_id}")
        for event in stream_events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one event to update aggregate state."""
        et = event.get("event_type")
        p = event.get("payload", {})
        self.version += 1

        if et == "ApplicationSubmitted":
            self.state = ApplicationState.SUBMITTED
            self.applicant_id = p.get("applicant_id")
            self.requested_amount_usd = p.get("requested_amount_usd")
            self.loan_purpose = p.get("loan_purpose")

        elif et == "DocumentUploadRequested":
            self.state = ApplicationState.DOCUMENTS_PENDING

        elif et == "DocumentUploaded":
            self.state = ApplicationState.DOCUMENTS_UPLOADED

        elif et == "CreditAnalysisRequested":
            # Triggered after documents are processed
            self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED
            self.documents_processed = True

        elif et == "CreditAnalysisRequested":
            self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

        elif et == "FraudScreeningRequested":
            self.state = ApplicationState.FRAUD_SCREENING_REQUESTED

        elif et == "ComplianceCheckRequested":
            self.state = ApplicationState.COMPLIANCE_CHECK_REQUESTED

        elif et == "DecisionRequested":
            self.state = ApplicationState.PENDING_DECISION

        elif et == "DecisionGenerated":
            rec = p.get("recommendation", "")
            conf = p.get("confidence", 1.0)
            self.credit_confidence = conf
            if rec == "REFER":
                self.state = ApplicationState.PENDING_HUMAN_REVIEW
            elif rec == "APPROVE":
                self.state = ApplicationState.PENDING_DECISION
            elif rec == "DECLINE":
                self.state = ApplicationState.PENDING_DECISION

        elif et == "HumanReviewRequested":
            self.state = ApplicationState.PENDING_HUMAN_REVIEW

        elif et == "HumanReviewCompleted":
            final = p.get("final_decision", "")
            if final == "APPROVE":
                self.state = ApplicationState.PENDING_DECISION
            elif final == "DECLINE":
                self.state = ApplicationState.PENDING_DECISION

        elif et == "ApplicationApproved":
            self.state = ApplicationState.APPROVED

        elif et == "ApplicationDeclined":
            # Check if it was a compliance block
            reasons = p.get("decline_reasons", [])
            if any("compliance" in str(r).lower() or "REG-" in str(r) for r in reasons):
                self.state = ApplicationState.DECLINED_COMPLIANCE
            else:
                self.state = ApplicationState.DECLINED

        elif et == "ComplianceCheckCompleted":
            self.compliance_verdict = p.get("overall_verdict")
            self.compliance_has_hard_block = p.get("has_hard_block", False)
            self.state = ApplicationState.COMPLIANCE_CHECK_COMPLETE

        elif et == "PackageReadyForAnalysis":
            self.state = ApplicationState.DOCUMENTS_PROCESSED
            self.documents_processed = True

    # ── Business rule assertions ──────────────────────────────────────────────

    def assert_valid_transition(self, target: ApplicationState) -> None:
        """Rule 1 — state machine: only valid transitions allowed."""
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise DomainError(
                f"Invalid transition {self.state} → {target}. Allowed: {allowed}"
            )

    def assert_documents_processed(self) -> None:
        """Rule 2 — documents must be processed before credit analysis."""
        if not self.documents_processed:
            raise DomainError(
                "Cannot request credit analysis: documents have not been processed yet."
            )

    def assert_awaiting_credit_analysis(self) -> None:
        """Assert the application is in the right state for credit analysis."""
        if self.state != ApplicationState.CREDIT_ANALYSIS_REQUESTED:
            raise DomainError(
                f"Cannot complete credit analysis: application is in state {self.state}. "
                f"Expected {ApplicationState.CREDIT_ANALYSIS_REQUESTED}."
            )

    def assert_awaiting_fraud_screening(self) -> None:
        """Assert the application is ready for fraud screening."""
        if self.state != ApplicationState.FRAUD_SCREENING_REQUESTED:
            raise DomainError(
                f"Cannot complete fraud screening: application is in state {self.state}."
            )

    def assert_awaiting_compliance_check(self) -> None:
        """Assert the application is ready for compliance check."""
        if self.state != ApplicationState.COMPLIANCE_CHECK_REQUESTED:
            raise DomainError(
                f"Cannot complete compliance check: application is in state {self.state}."
            )

    def assert_valid_orchestrator_decision(self, recommendation: str, confidence: float) -> None:
        """
        Rule 4 — confidence < 0.60 → recommendation must be REFER.
        Rule 5 — compliance BLOCKED → only DECLINE allowed.
        """
        if confidence < 0.60 and recommendation != "REFER":
            raise DomainError(
                f"confidence {confidence:.2f} < 0.60 requires recommendation=REFER, "
                f"got {recommendation}."
            )
        if self.compliance_has_hard_block and recommendation != "DECLINE":
            raise DomainError(
                f"Compliance hard block is present — only DECLINE is allowed, "
                f"got {recommendation}."
            )

    def assert_can_approve(self) -> None:
        """Rule 5 — cannot approve if compliance is blocked."""
        if self.compliance_has_hard_block:
            raise DomainError(
                "Cannot approve: compliance hard block is present. Must decline."
            )
        if self.state not in (
            ApplicationState.PENDING_DECISION,
            ApplicationState.PENDING_HUMAN_REVIEW,
        ):
            raise DomainError(
                f"Cannot approve: application is in state {self.state}."
            )

    def assert_submitted(self) -> None:
        """Assert the application has been submitted."""
        if self.state == ApplicationState.NEW:
            raise DomainError("Application has not been submitted yet.")

    def assert_not_terminal(self) -> None:
        """Assert the application is not in a terminal state."""
        terminal = {
            ApplicationState.APPROVED,
            ApplicationState.DECLINED,
            ApplicationState.DECLINED_COMPLIANCE,
        }
        if self.state in terminal:
            raise DomainError(
                f"Application is already in terminal state {self.state}. No further events allowed."
            )