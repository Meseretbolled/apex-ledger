"""
tests/phase3/test_agents.py
============================
Phase 3 tests — agent graph construction and REGULATIONS dict.
"""
import pytest
from ledger.agents.stub_agents import (
    FraudDetectionAgent,
    ComplianceAgent,
    DecisionOrchestratorAgent,
    REGULATIONS,
)


def test_fraud_agent_builds_graph():
    agent = FraudDetectionAgent("fraud-1", "fraud_detection", None, None)
    graph = agent.build_graph()
    assert graph is not None

def test_compliance_agent_builds_graph():
    agent = ComplianceAgent("comp-1", "compliance", None, None)
    graph = agent.build_graph()
    assert graph is not None

def test_orchestrator_agent_builds_graph():
    agent = DecisionOrchestratorAgent("orch-1", "decision_orchestrator", None, None)
    graph = agent.build_graph()
    assert graph is not None

def test_regulations_has_all_6_rules():
    assert set(REGULATIONS.keys()) == {"REG-001","REG-002","REG-003","REG-004","REG-005","REG-006"}

def test_reg002_is_hard_block():
    assert REGULATIONS["REG-002"]["is_hard_block"] == True

def test_reg003_is_hard_block():
    assert REGULATIONS["REG-003"]["is_hard_block"] == True

def test_reg005_is_hard_block():
    assert REGULATIONS["REG-005"]["is_hard_block"] == True

def test_reg001_is_not_hard_block():
    assert REGULATIONS["REG-001"]["is_hard_block"] == False

def test_reg006_always_passes():
    co = {"jurisdiction": "MT", "legal_type": "LLC", "founded_year": 2010,
          "compliance_flags": [], "requested_amount_usd": 500000}
    assert REGULATIONS["REG-006"]["check"](co) == True

def test_reg003_blocks_montana():
    co = {"jurisdiction": "MT", "compliance_flags": [], "founded_year": 2010,
          "legal_type": "LLC", "requested_amount_usd": 100000}
    assert REGULATIONS["REG-003"]["check"](co) == False

def test_reg003_passes_non_montana():
    co = {"jurisdiction": "CA", "compliance_flags": [], "founded_year": 2010,
          "legal_type": "LLC", "requested_amount_usd": 100000}
    assert REGULATIONS["REG-003"]["check"](co) == True

def test_reg005_blocks_new_company():
    co = {"founded_year": 2024, "compliance_flags": [], "jurisdiction": "CA",
          "legal_type": "LLC", "requested_amount_usd": 100000}
    assert REGULATIONS["REG-005"]["check"](co) == False

def test_reg005_passes_established_company():
    co = {"founded_year": 2015, "compliance_flags": [], "jurisdiction": "CA",
          "legal_type": "LLC", "requested_amount_usd": 100000}
    assert REGULATIONS["REG-005"]["check"](co) == True

def test_reg004_blocks_sole_prop_over_250k():
    co = {"legal_type": "Sole Proprietor", "requested_amount_usd": 300000,
          "compliance_flags": [], "jurisdiction": "CA", "founded_year": 2015}
    assert REGULATIONS["REG-004"]["check"](co) == False

def test_reg004_passes_sole_prop_under_250k():
    co = {"legal_type": "Sole Proprietor", "requested_amount_usd": 100000,
          "compliance_flags": [], "jurisdiction": "CA", "founded_year": 2015}
    assert REGULATIONS["REG-004"]["check"](co) == True

def test_reg001_blocks_active_aml_flag():
    co = {"compliance_flags": [{"flag_type": "AML_WATCH", "is_active": True}],
          "jurisdiction": "CA", "founded_year": 2015,
          "legal_type": "LLC", "requested_amount_usd": 100000}
    assert REGULATIONS["REG-001"]["check"](co) == False

def test_reg001_passes_inactive_aml_flag():
    co = {"compliance_flags": [{"flag_type": "AML_WATCH", "is_active": False}],
          "jurisdiction": "CA", "founded_year": 2015,
          "legal_type": "LLC", "requested_amount_usd": 100000}
    assert REGULATIONS["REG-001"]["check"](co) == True