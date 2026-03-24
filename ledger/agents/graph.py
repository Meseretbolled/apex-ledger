import os
from dotenv import load_dotenv
load_dotenv()

from ledger.event_store import EventStore
from ledger.agents.base_agent import CreditAnalysisAgent

DB_URL = os.getenv("DATABASE_URL", "postgresql://ledger_user:ledger123@127.0.0.1:5433/apex_ledger")

store = EventStore(db_url=DB_URL)

agent = CreditAnalysisAgent(
    agent_id="credit-agent-01",
    agent_type="credit_analysis",
    store=store,
    registry=None,
)

graph = agent.build_graph()
