"""
tests/test_regulatory_package.py
==================================
Tests for generate_regulatory_package().

Run: pytest tests/test_regulatory_package.py -v
"""
import json
import pytest
from datetime import datetime, timezone

from ledger.event_store import InMemoryEventStore
from ledger.regulatory.package import generate_regulatory_package


def _ev(event_type, **payload):
    return {"event_type": event_type, "event_version": 1, "payload": payload}


@pytest.fixture
async def store_with_full_lifecycle():
    """Full loan application lifecycle in an InMemoryEventStore."""
    store = InMemoryEventStore()
    app_id = "TEST-REG-001"

    # Loan stream
    await store.append(f"loan-{app_id}", [
        _ev("ApplicationSubmitted",
            application_id=app_id, applicant_id="COMP-042",
            requested_amount_usd=500000, loan_purpose="working_capital"),
        _ev("CreditAnalysisRequested",
            application_id=app_id, assigned_agent_id="credit-agent-01"),
        _ev("CreditAnalysisCompleted",
            application_id=app_id, session_id="sess-cred-001",
            risk_tier="MEDIUM", confidence_score=0.82,
            recommended_limit_usd=400000, analysis_duration_ms=3200,
            input_data_hash="abc123def456"),
        _ev("FraudScreeningRequested",  application_id=app_id),
        _ev("FraudScreeningCompleted",
            application_id=app_id, session_id="sess-frau-001",
            fraud_score=0.12, anomaly_flags=[],
            screening_model_version="fraud-v2"),
        _ev("ComplianceCheckRequested", application_id=app_id),
        _ev("ComplianceCheckCompleted",
            application_id=app_id, session_id="sess-comp-001",
            rules_evaluated=6, rules_passed=6, rules_failed=0,
            rules_noted=1, has_hard_block=False,
            overall_verdict="CLEAR"),
        _ev("DecisionRequested",        application_id=app_id,
            all_analyses_complete=True, triggered_by_event_id="evt-001"),
        _ev("DecisionGenerated",
            application_id=app_id, session_id="sess-orch-001",
            recommendation="APPROVE", confidence_score=0.87,
            contributing_agent_sessions=["sess-cred-001","sess-frau-001"],
            decision_basis_summary="Solid financials, low fraud risk."),
        _ev("ApplicationApproved",
            application_id=app_id, approved_amount_usd=400000,
            interest_rate=0.065, conditions=[], approved_by="auto",
            effective_date="2026-03-24"),
    ], expected_version=-1)

    # Compliance stream
    await store.append(f"compliance-{app_id}", [
        _ev("ComplianceRulePassed",
            application_id=app_id, rule_id="REG-001", rule_version="2026-Q1"),
        _ev("ComplianceRulePassed",
            application_id=app_id, rule_id="REG-002", rule_version="2026-Q1"),
        _ev("ComplianceRulePassed",
            application_id=app_id, rule_id="REG-003", rule_version="2026-Q1"),
        _ev("ComplianceRulePassed",
            application_id=app_id, rule_id="REG-004", rule_version="2026-Q1"),
        _ev("ComplianceRulePassed",
            application_id=app_id, rule_id="REG-005", rule_version="2026-Q1"),
        _ev("ComplianceRuleNoted",
            application_id=app_id, rule_id="REG-006", rule_version="2026-Q1"),
        _ev("ComplianceCheckCompleted",
            application_id=app_id, rules_evaluated=6, rules_passed=5,
            rules_failed=0, rules_noted=1,
            has_hard_block=False, overall_verdict="CLEAR"),
    ], expected_version=-1)

    # Agent session stream
    await store.append("agent-credit_analysis-sess-cred-001", [
        _ev("AgentSessionStarted",
            session_id="sess-cred-001", agent_type="credit_analysis",
            agent_id="credit-agent-01", application_id=app_id,
            model_version="google/gemini-2.0-flash-001",
            context_source="fresh", started_at="2026-03-24T10:00:00"),
        _ev("AgentSessionCompleted",
            session_id="sess-cred-001", agent_type="credit_analysis",
            application_id=app_id,
            total_nodes_executed=7, total_llm_calls=1,
            total_tokens_used=1842, total_cost_usd=0.000921,
            total_duration_ms=3200, completed_at="2026-03-24T10:00:03"),
    ], expected_version=-1)

    return store, app_id


@pytest.mark.asyncio
async def test_package_has_all_required_sections(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    assert "package_metadata"          in package
    assert "event_stream"              in package
    assert "projection_states"         in package
    assert "integrity_verification"    in package
    assert "agent_participation"       in package
    assert "narrative"                 in package
    assert "verification_instructions" in package


@pytest.mark.asyncio
async def test_package_metadata_correct(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    meta = package["package_metadata"]
    assert meta["application_id"]  == app_id
    assert meta["package_version"] == "1.0"
    assert meta["total_events"]    > 0
    assert len(meta["streams_included"]) > 0


@pytest.mark.asyncio
async def test_event_stream_is_ordered(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    events = package["event_stream"]
    assert len(events) > 0

    # Every event must have required fields
    for e in events:
        assert "event_type"     in e
        assert "stream_id"      in e
        assert "event_version"  in e
        assert "payload"        in e
        assert "recorded_at"    in e


@pytest.mark.asyncio
async def test_application_summary_correct(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    summary = package["projection_states"]["application_summary"]
    assert summary["application_id"]       == app_id
    assert summary["applicant_id"]         == "COMP-042"
    assert float(summary["requested_amount_usd"]) == 500000
    assert summary["state"]                == "APPROVED"
    assert summary["final_decision"]       == "APPROVE"
    assert summary["risk_tier"]            == "MEDIUM"


@pytest.mark.asyncio
async def test_compliance_temporal_query(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle

    # Query at a past date — before any events
    past = datetime(2020, 1, 1, tzinfo=timezone.utc)
    package = await generate_regulatory_package(store, app_id, examination_date=past)

    compliance = package["projection_states"]["compliance_at_examination_date"]
    # No events before 2020 — should be empty
    assert compliance["rules_passed"]   == []
    assert compliance["rules_failed"]   == []
    assert compliance["overall_verdict"] is None

    # Query at now — should see all rules
    package_now = await generate_regulatory_package(store, app_id)
    compliance_now = package_now["projection_states"]["compliance_at_examination_date"]
    assert len(compliance_now["rules_passed"]) >= 5
    assert compliance_now["has_hard_block"]    == False
    assert compliance_now["overall_verdict"]   == "CLEAR"


@pytest.mark.asyncio
async def test_integrity_verification_present(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    iv = package["integrity_verification"]
    assert "loan_stream"          in iv
    assert "package_fingerprint"  in iv
    assert iv["loan_stream"]["is_valid"] == True
    assert len(iv["package_fingerprint"]) == 64  # SHA-256 hex


@pytest.mark.asyncio
async def test_agent_participation_has_model_versions(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    agents = package["agent_participation"]
    assert len(agents) >= 1

    credit_agent = next(
        (a for a in agents if a["agent_type"] == "credit_analysis"), None
    )
    assert credit_agent is not None
    assert credit_agent["model_version"] == "google/gemini-2.0-flash-001"
    assert credit_agent["session_id"]    == "sess-cred-001"


@pytest.mark.asyncio
async def test_narrative_has_lifecycle_steps(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    narrative = package["narrative"]
    assert len(narrative["lifecycle"]) > 0
    assert "COMP-042"  in narrative["summary"]
    assert "500,000"   in narrative["summary"]

    # Narrative lines should be numbered
    first = narrative["lifecycle"][0]
    assert first.startswith("01.")


@pytest.mark.asyncio
async def test_package_is_json_serialisable(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    # Must not raise
    serialised = json.dumps(package, default=str)
    assert len(serialised) > 100


@pytest.mark.asyncio
async def test_package_fingerprint_is_deterministic(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle

    package1 = await generate_regulatory_package(store, app_id)
    package2 = await generate_regulatory_package(store, app_id)

    fp1 = package1["integrity_verification"]["package_fingerprint"]
    fp2 = package2["integrity_verification"]["package_fingerprint"]
    assert fp1 == fp2, "Same events must always produce same fingerprint"


@pytest.mark.asyncio
async def test_verification_instructions_present(store_with_full_lifecycle):
    store, app_id = store_with_full_lifecycle
    package = await generate_regulatory_package(store, app_id)

    vi = package["verification_instructions"]
    assert "step_1" in vi
    assert "step_2" in vi
    assert "step_3" in vi
    assert "step_4" in vi
    assert "step_5" in vi
    assert app_id in vi["step_1"]  # SQL query should include app_id


@pytest.mark.asyncio
async def test_declined_application_shows_correct_state():
    """Declined application shows DECLINED state and DECLINE decision."""
    store  = InMemoryEventStore()
    app_id = "TEST-REG-DECLINED"

    await store.append(f"loan-{app_id}", [
        _ev("ApplicationSubmitted",
            application_id=app_id, applicant_id="COMP-099",
            requested_amount_usd=1000000, loan_purpose="acquisition"),
        _ev("ComplianceCheckCompleted",
            application_id=app_id, session_id="sess-comp-dec",
            rules_evaluated=3, rules_passed=2, rules_failed=1,
            rules_noted=0, has_hard_block=True, overall_verdict="BLOCKED"),
        _ev("ApplicationDeclined",
            application_id=app_id,
            decline_reasons=["Compliance hard block: REG-002"],
            declined_by="compliance-system",
            adverse_action_notice_required=True),
    ], expected_version=-1)

    package = await generate_regulatory_package(store, app_id)
    summary = package["projection_states"]["application_summary"]

    assert summary["state"]         == "DECLINED"
    assert summary["final_decision"]== "DECLINE"
    assert "500,000" not in package["narrative"]["summary"]