"""
scripts/demo_narr05.py
=======================
NARR-05 Demo — Human Override of Orchestrator Recommendation.

Demonstrates:
  1. Orchestrator recommends DECLINE (low confidence)
  2. Human loan officer LO-Sarah-Chen overrides to APPROVE
  3. ApplicationApproved with $750,000 and 2 conditions

Run:
    python scripts/demo_narr05.py
"""
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()

import asyncpg
from ledger.event_store import EventStore, InMemoryEventStore
from ledger.upcasters import UpcasterRegistry
from ledger.commands.handlers import (
    SubmitApplicationCommand,
    HumanReviewCompletedCommand,
    handle_submit_application,
    handle_human_review_completed,
)
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate


async def demo_narr05():
    print("\n" + "="*60)
    print("NARR-05 DEMO — Human Override")
    print("="*60 + "\n")

    # Use InMemoryEventStore for demo (no DB needed)
    store = InMemoryEventStore()
    app_id = "APEX-NARR05-DEMO"

    # Step 1: Submit application
    print("Step 1: Submitting application...")
    await handle_submit_application(SubmitApplicationCommand(
        application_id=app_id,
        applicant_id="COMP-042",
        requested_amount_usd=750000,
        loan_purpose="equipment_purchase",
        loan_term_months=60,
        submission_channel="web",
        contact_email="cfo@company.com",
        contact_name="Jane Smith",
        correlation_id="corr-narr05-demo",
    ), store)
    print(f"  ApplicationSubmitted + DocumentUploadRequested written")

    # Step 2: Simulate pipeline advancing to DecisionGenerated (DECLINE)
    print("\nStep 2: Simulating pipeline — orchestrator recommends DECLINE...")
    await store.append(f"loan-{app_id}", [
        {"event_type": "DocumentUploaded",         "event_version": 1, "payload": {"document_id": "doc-001", "file_path": "./documents/test.pdf"}},
        {"event_type": "PackageReadyForAnalysis",  "event_version": 1, "payload": {"application_id": app_id}},
        {"event_type": "CreditAnalysisRequested",  "event_version": 1, "payload": {"application_id": app_id}},
        {"event_type": "FraudScreeningRequested",  "event_version": 1, "payload": {"application_id": app_id}},
        {"event_type": "ComplianceCheckRequested", "event_version": 1, "payload": {"application_id": app_id, "regulation_set_version": "2026-Q1", "rules_to_evaluate": ["REG-001","REG-002","REG-003","REG-004","REG-005","REG-006"]}},
        {"event_type": "ComplianceCheckCompleted", "event_version": 1, "payload": {"application_id": app_id, "session_id": "sess-comp", "rules_evaluated": 6, "rules_passed": 5, "rules_failed": 0, "rules_noted": 1, "has_hard_block": False, "overall_verdict": "CLEAR", "completed_at": "2026-03-22T10:00:00"}},
        {"event_type": "DecisionRequested",        "event_version": 1, "payload": {"application_id": app_id, "all_analyses_complete": True, "triggered_by_event_id": "evt-001"}},
        {"event_type": "DecisionGenerated",        "event_version": 2, "payload": {"application_id": app_id, "orchestrator_session_id": "sess-orch", "recommendation": "DECLINE", "confidence": 0.52, "executive_summary": "Low confidence due to thin financial history.", "key_risks": ["Thin margins", "Short history"], "contributing_sessions": ["sess-orch"], "model_versions": {"orchestrator": "gemini-2.0-flash"}, "generated_at": "2026-03-22T10:01:00"}},
        {"event_type": "HumanReviewRequested",     "event_version": 1, "payload": {"application_id": app_id, "reason": "Low confidence (0.52) requires human review.", "decision_event_id": "evt-002", "assigned_to": None, "requested_at": "2026-03-22T10:01:01"}},
    ], expected_version=1)
    print(f"  Orchestrator recommended: DECLINE (confidence: 0.52)")

    # Step 3: Human loan officer overrides to APPROVE
    print("\nStep 3: LO-Sarah-Chen overrides to APPROVE...")
    await handle_human_review_completed(HumanReviewCompletedCommand(
        application_id=app_id,
        reviewer_id="LO-Sarah-Chen",
        final_decision="APPROVE",
        override=True,
        override_reason="Strong collateral and 10-year banking relationship justify approval.",
        approved_amount_usd=750000,
        conditions=[
            "Quarterly financial reporting required",
            "Personal guarantee from principal owner",
        ],
        correlation_id="corr-narr05-override",
        causation_id="caus-narr05",
    ), store)

    # Step 4: Verify outcome
    print("\nStep 4: Verifying outcome...")
    loan_events = await store.load_stream(f"loan-{app_id}")
    loan_types = [e["event_type"] for e in loan_events]

    approved = next(e for e in loan_events if e["event_type"] == "ApplicationApproved")
    review   = next(e for e in loan_events if e["event_type"] == "HumanReviewCompleted")
    decision = next(e for e in loan_events if e["event_type"] == "DecisionGenerated")

    print(f"\n  Loan stream events:")
    for e in loan_events:
        print(f"    [{e['stream_position']}] {e['event_type']}")

    print(f"\n  DecisionGenerated.recommendation : {decision['payload']['recommendation']}")
    print(f"  HumanReviewCompleted.override    : {review['payload']['override']}")
    print(f"  HumanReviewCompleted.reviewed_by : {review['payload']['reviewed_by']}")
    print(f"  ApplicationApproved.amount       : ${approved['payload']['approved_amount_usd']}")
    print(f"  ApplicationApproved.conditions   : {len(approved['payload']['conditions'])} items")

    agg = await LoanApplicationAggregate.load(store, app_id)
    print(f"  Final aggregate state            : {agg.state}")

    print("\n" + "="*60)
    print("NARR-05 DEMO COMPLETE")
    print(f"  Orchestrator said DECLINE — Human overrode to APPROVE")
    print(f"  Amount: $750,000 | Reviewer: LO-Sarah-Chen")
    print("="*60 + "\n")

    assert decision["payload"]["recommendation"] == "DECLINE"
    assert review["payload"]["override"] == True
    assert review["payload"]["reviewed_by"] == "LO-Sarah-Chen"
    assert float(approved["payload"]["approved_amount_usd"]) == 750000.0
    assert len(approved["payload"]["conditions"]) >= 2
    print("All assertions passed!")


if __name__ == "__main__":
    asyncio.run(demo_narr05())