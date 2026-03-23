"""
scripts/run_pipeline.py
========================
End-to-end pipeline runner — processes one application through all 5 agents.

Usage:
    python scripts/run_pipeline.py --application APEX-0007
    python scripts/run_pipeline.py --application APEX-0031 --phase document
    python scripts/run_pipeline.py --application APEX-0075 --phase all

Phases: all | document | credit | fraud | compliance | decision
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()

import asyncpg
from ledger.event_store import EventStore
from ledger.upcasters import UpcasterRegistry
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.stub_agents import (
    DocumentProcessingAgent,
    FraudDetectionAgent,
    ComplianceAgent,
    DecisionOrchestratorAgent,
)
from ledger.agents.base_agent import CreditAnalysisAgent


async def run_pipeline(application_id: str, phase: str, db_url: str):
    pool  = await asyncpg.create_pool(db_url)
    store = EventStore(db_url, upcaster_registry=UpcasterRegistry())
    await store.connect()
    registry = ApplicantRegistryClient(pool)

    print(f"\n{'='*60}")
    print(f"Processing {application_id} — phase: {phase}")
    print(f"{'='*60}\n")

    agents = []
    if phase in ("all", "document"):
        agents.append(("DocumentProcessing", DocumentProcessingAgent(
            f"doc-{application_id}", "document_processing", store, registry)))
    if phase in ("all", "credit"):
        agents.append(("CreditAnalysis", CreditAnalysisAgent(
            f"cre-{application_id}", "credit_analysis", store, registry)))
    if phase in ("all", "fraud"):
        agents.append(("FraudDetection", FraudDetectionAgent(
            f"fra-{application_id}", "fraud_detection", store, registry)))
    if phase in ("all", "compliance"):
        agents.append(("Compliance", ComplianceAgent(
            f"com-{application_id}", "compliance", store, registry)))
    if phase in ("all", "decision"):
        agents.append(("DecisionOrchestrator", DecisionOrchestratorAgent(
            f"orch-{application_id}", "decision_orchestrator", store, registry)))

    for name, agent in agents:
        print(f"Running {name}Agent for {application_id}...")
        try:
            await agent.process_application(application_id)
            print(f"  {name}Agent: COMPLETED")
        except Exception as e:
            print(f"  {name}Agent: FAILED — {e}")
            break

    # Print final loan stream
    loan_events = await store.load_stream(f"loan-{application_id}")
    print(f"\nLoan stream ({len(loan_events)} events):")
    for e in loan_events:
        print(f"  [{e['stream_position']}] {e['event_type']}")

    await store.close()
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--application", required=True, help="Application ID e.g. APEX-0007")
    parser.add_argument("--phase", default="all",
                        choices=["all","document","credit","fraud","compliance","decision"])
    parser.add_argument("--db-url", default=os.environ.get(
        "DATABASE_URL", "postgresql://ledger_user:ledger123@127.0.0.1:5433/apex_ledger"))
    args = parser.parse_args()
    asyncio.run(run_pipeline(args.application, args.phase, args.db_url))