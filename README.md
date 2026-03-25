# The Ledger — Apex Financial Services
## TRP1 Week 5 · Agentic Event-Sourced Loan Decisioning Platform

> An append-only, ACID-compliant, cryptographically tamper-evident event store where five LangGraph AI agents collaborate to process commercial loan applications. Every agent action, decision, and compliance check is an immutable event. Any past state can be reconstructed in under 60 seconds.

![System Architecture](assets/Week5.png)

---

## The Week Standard

> _"Show me the complete decision history of application ID X — from first event to final decision, with every AI agent action, every compliance check, every human review, all causal links intact, temporal query to any point in the lifecycle, and cryptographic integrity verification."_

```bash
# Run the full decision history demo
python scripts/run_pipeline.py --app APEX-0007 --full-history
```

This completes in under 60 seconds and outputs every event, every agent session, the compliance audit trail, and the SHA-256 hash chain verification.

---

## Why Event Sourcing

Most enterprise AI systems have a memory problem. Agent judgements are lost when the process ends. Audit logs are annotations, not architecture. If you ask "what did the credit agent know when it made this decision?" the answer is "we don't know."

The Ledger fixes this permanently. The `events` table **is** the database. Every agent decision is a permanent, immutable fact. Any past state can be reconstructed by replaying the stream. The architecture IS the audit trail — not something bolted on after the fact.

---

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 16 (running on port 5433)
- Docker (for Kafka)
- Gemini API key

### 1. Clone and install

```bash
git clone https://github.com/Meseretbolled/apex-ledger.git
cd apex-ledger
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
GEMINI_API_KEY=your-gemini-key-here
DATABASE_URL=postgresql://ledger_user:ledger123@127.0.0.1:5433/apex_ledger
```

### 3. Set up PostgreSQL

```bash
sudo service postgresql start

sudo -u postgres psql << 'EOF'
CREATE USER ledger_user WITH PASSWORD 'ledger123';
CREATE DATABASE apex_ledger OWNER ledger_user;
GRANT ALL PRIVILEGES ON DATABASE apex_ledger TO ledger_user;
EOF

# Run migrations
psql postgresql://ledger_user:ledger123@127.0.0.1:5433/apex_ledger < schema.sql
```

### 4. Seed data (80 companies, 400 documents, 1,198 events)

```bash
python datagen/generate_all.py \
  --db-url postgresql://ledger_user:ledger123@127.0.0.1:5433/apex_ledger \
  --docs-dir ./documents \
  --output-dir ./data \
  --random-seed 42
```

### 5. Run the full test suite

```bash
pytest tests/ -v
# Expected: 69 passed, 13 skipped, 0 failed
```

### 6. Start all services

```bash
./start-all.sh
```

This starts Kafka (via Docker), the LangGraph dev server, and prints the Next.js startup instructions.

---

## Architecture

```
Loan application submitted
          ↓
Command handler  (load → validate → append with OCC)
          ↓
PostgreSQL event store  (OCC · outbox · global_position · upcasters)
          ↓
7 aggregate streams  (loan · docpkg · agent · credit · fraud · compliance · audit)
          ↓
5 LangGraph agents  (Document · Credit · Fraud · Compliance · Orchestrator)
          ↓
Projection daemon  (polls load_all() · checkpoints · SLO enforcement)
          ↓
3 read models  (ApplicationSummary · AgentPerformance · ComplianceAudit)
          ↓
MCP server  (8 tools · 6 resources · FastMCP)
          ↓
Next.js dashboard  (real-time event stream via Kafka REST Proxy)
```

---

## Event Store

The PostgreSQL event store is the heart of the system. Four tables, every column justified:

```sql
CREATE TABLE events (
    event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL,
    stream_position  BIGINT NOT NULL,
    global_position  BIGINT GENERATED ALWAYS AS IDENTITY,  -- no gaps, no coordination
    event_type       TEXT NOT NULL,
    event_version    SMALLINT NOT NULL DEFAULT 1,           -- upcaster version
    payload          JSONB NOT NULL,                        -- domain data
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,    -- correlation_id, causation_id, chain_hash
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);
```

**Key design decisions:**

- `global_position GENERATED ALWAYS AS IDENTITY` — PostgreSQL generates this, never application code. Guarantees no gaps across concurrent writers.
- `UNIQUE (stream_id, stream_position)` — this is the second safety net for OCC, after the `SELECT ... FOR UPDATE` on `event_streams`.
- `metadata JSONB` — correlation/causation IDs live here, not in `payload`. Domain code never needs to know about infrastructure concerns.
- `outbox` table — event insert and outbox insert happen in the same transaction. If the process crashes before publishing to Kafka, the outbox row survives. Nothing is lost.

---

## Optimistic Concurrency Control

Two agents simultaneously try to write to `credit-APEX-0031` at `expected_version=3`. Here is exactly what happens:

```
Agent A calls append(stream_id="credit-APEX-0031", expected_version=3)
  → BEGIN TRANSACTION
  → SELECT current_version FROM event_streams WHERE stream_id='credit-APEX-0031' FOR UPDATE
  → current_version=3, matches expected_version=3 ✓
  → INSERT event at stream_position=4
  → UPDATE current_version=4
  → COMMIT — lock released

Agent B was blocked at SELECT ... FOR UPDATE.
  → Lock acquired. Reads current_version=4.
  → 4 ≠ 3. ROLLBACK.
  → Raises OptimisticConcurrencyError(stream_id, expected=3, actual=4)
  → Agent B reloads stream, inspects Agent A's event, retries with expected_version=4.
```

No distributed locks. No transactions spanning multiple aggregates. One row-level lock held for milliseconds.

**The double-decision test:**

```bash
pytest tests/phase1/test_event_store.py::test_concurrent_double_append_exactly_one_succeeds -v
```

Two asyncio tasks race on the same stream. Asserts: exactly 1 succeeds, 1 raises `OptimisticConcurrencyError`, total stream length = 4 (not 5).

---

## 7 Aggregate Streams

| Stream | What it tracks | OCC boundary |
|---|---|---|
| `loan-{id}` | Full lifecycle (16 states) | Lifecycle transitions only — lean stream |
| `docpkg-{id}` | Document extraction pipeline | Document processing agent |
| `agent-{type}-{session}` | Gas Town session tracking, node execution | One stream per agent session |
| `credit-{id}` | Credit analysis results | Credit agent only |
| `fraud-{id}` | Fraud detection results | Fraud agent only |
| `compliance-{id}` | 6 regulatory rule results | Compliance agent only |
| `audit-{id}` | SHA-256 hash chain integrity | Append-only, integrity checks only |

Separate streams per agent mean agents run in parallel with near-zero OCC contention. Two fraud agents racing only compete for `fraud-{id}`, not the entire application stream.

---

## 5 LangGraph Agents

| Agent | LLM | Nodes | Output events |
|---|---|---|---|
| DocumentProcessingAgent | Gemini 2.0 Flash | 6 | `ExtractionCompleted`, `PackageReadyForAnalysis` |
| CreditAnalysisAgent | Gemini 2.0 Flash | 7 | `CreditAnalysisCompleted`, `FraudScreeningRequested` |
| FraudDetectionAgent | Gemini 2.0 Flash | 5 | `FraudScreeningCompleted`, `ComplianceCheckRequested` |
| ComplianceAgent | Deterministic (no LLM) | 9 | `ComplianceCheckCompleted` (6 rules evaluated) |
| DecisionOrchestratorAgent | Gemini 2.0 Flash | 7 | `DecisionGenerated`, `ApplicationApproved/Declined` |

Every agent follows the same pattern:

```python
# Gas Town — session start is FIRST event, before any work
async def run(self, application_id: str) -> None:
    await self._append_session_started()        # Gas Town anchor
    await self._node_validate_inputs(state)     # then work begins
    await self._record_node_execution("validate_inputs", ...)
    # ... more nodes ...
    await self._node_write_output(state)        # domain events appended last
    await self._append_session_completed()
```

---

## Gas Town Pattern — Crash Recovery

Every agent writes `AgentSessionStarted` as its absolute first event. This is the Gas Town anchor. If the agent crashes mid-pipeline, `reconstruct_agent_context()` replays the session stream and returns exactly where to resume:

```python
context = await reconstruct_agent_context(
    store=store,
    agent_type="fraud_detection",
    application_id="APEX-0031",
)
# Returns:
# {
#   "session_id": "sess-fra-abc12345",
#   "last_successful_node": "load_document_facts",
#   "nodes_executed": ["validate_inputs", "load_document_facts"],
#   "needs_reconciliation": True,
#   "context_source": "prior_session_replay:sess-fra-abc12345"
# }
```

The new session starts with `context_source = "prior_session_replay:{old_session_id}"` — this is recorded in the event stream, proving to auditors that a recovery occurred and from which session.

---

## 6 Business Rules (Enforced in Aggregates, Not Handlers)

Rules live in the aggregate domain logic. A rule only checked in an API handler is a UI validation, not a business rule.

| Rule | Enforcement point | Guard method |
|---|---|---|
| Valid state transitions only | `LoanApplicationAggregate` | `assert_valid_transition()` |
| Documents must be processed before credit analysis | `LoanApplicationAggregate` | `assert_documents_processed()` |
| Application in correct state for each analysis | `LoanApplicationAggregate` | `assert_awaiting_credit_analysis()` etc. |
| confidence < 0.60 → recommendation must be REFER | `LoanApplicationAggregate` | `assert_valid_orchestrator_decision()` |
| Compliance hard block → only DECLINE allowed | `LoanApplicationAggregate` | `assert_can_approve()` |
| Causal chain: agent sessions must reference this application | `LoanApplicationAggregate` | via `causation_id` in metadata |

---

## 6 Compliance Rules

The ComplianceAgent evaluates 6 deterministic rules with no LLM in the decision path. Rules are facts, not probabilities.

| Rule | Name | Hard Block |
|---|---|---|
| REG-001 | Bank Secrecy Act (BSA) screening | No |
| REG-002 | OFAC Sanctions Screening | **Yes** — immediate decline |
| REG-003 | Jurisdiction Eligibility (Montana blocked) | **Yes** — immediate decline |
| REG-004 | Legal Entity Type Eligibility | No |
| REG-005 | Minimum Operating History (2 years) | **Yes** — immediate decline |
| REG-006 | CRA Community Reinvestment Act | No (noted) |

Hard block rules append `ApplicationDeclined` immediately and stop the pipeline without reaching `DecisionGenerated`.

---

## 3 Projections

| Projection | SLO | Temporal query | Answers |
|---|---|---|---|
| `ApplicationSummary` | <500ms lag | No — current state | What is the current state, risk tier, and decision for application X? |
| `AgentPerformanceLedger` | <2s lag | No — current metrics | What is agent Y's approve rate and average confidence by model version? |
| `ComplianceAuditView` | <2s lag | Yes — `?as_of=timestamp` | What was the compliance status of application X at time T? |

The `ProjectionDaemon` runs as a background asyncio task, polls `load_all()` from the last checkpoint, and routes each event to subscribed projections. `get_lag()` is exposed per projection — the SLOs are not aspirational, they are tested.

---

## Cryptographic Audit Chain

Every event is chained to the previous event's SHA-256 hash:

```python
chain_hash = sha256(json.dumps({
    "previous_hash": previous_hash,   # "GENESIS" for first event
    "event_id": event.event_id,
    "stream_position": event.stream_position,
    "event_type": event.event_type,
    "payload": event.payload,
    "recorded_at": event.recorded_at,
}, sort_keys=True))
```

If any stored event is modified after the fact, every subsequent hash in the chain breaks. Tamper detection is automatic:

```bash
# Verify a stream's integrity
python -c "
import asyncio
from ledger.integrity.audit_chain import AuditChain
from ledger.event_store import EventStore
# ...
result = asyncio.run(chain.verify_stream('loan-APEX-0007'))
print(f'Valid: {result.is_valid}, Events checked: {result.events_checked}')
"
```

---

## Upcasting — Immutable Schema Evolution

Events are immutable. When `CreditAnalysisCompleted` gained `model_version` and `confidence_score` in 2026, stored v1 events were not touched. An upcaster transforms them on read:

```python
@registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    payload.setdefault("model_version", "legacy-pre-2026")  # honest sentinel
    payload.setdefault("confidence_score", None)            # genuinely unknown — never fabricate
    payload.setdefault("regulatory_basis", ["2024-REG-SET-v1"])
    return payload
```

**Why `None` for `confidence_score` instead of a default:** fabricating a confidence value would corrupt compliance queries that filter on confidence thresholds. `None` correctly signals "this data was not recorded for this decision."

The immutability test verifies: load a v1 event as v2 through the store → confirm the raw database row is unchanged.

---

## MCP Server

The MCP server exposes The Ledger to any AI agent or enterprise system via the Model Context Protocol. Tools write events (command side). Resources read from projections (query side). This is structural CQRS.

**8 Tools (command side):**

| Tool | What it does |
|---|---|
| `submit_application` | Write `ApplicationSubmitted` to event store |
| `get_application_status` | Read current state from `ApplicationSummary` projection |
| `get_event_stream` | Load full event history for a stream |
| `get_decision_history` | Load all decision events for an application |
| `trigger_compliance_check` | Manually trigger compliance re-evaluation |
| `get_agent_performance` | Read per-agent metrics from `AgentPerformanceLedger` |
| `verify_audit_chain` | Run SHA-256 chain verification |
| `reconstruct_agent_context` | Gas Town crash recovery |

**6 Resources (query side):**

| Resource URI | Projection | SLO |
|---|---|---|
| `ledger://applications/{id}` | Full event history | <500ms |
| `ledger://applications/{id}/summary` | `ApplicationSummary` | <50ms |
| `ledger://applications/{id}/compliance` | `ComplianceAuditView` | <200ms |
| `ledger://applications/{id}/audit-trail` | `AuditLedger` stream | <500ms |
| `ledger://agents/{id}/performance` | `AgentPerformanceLedger` | <50ms |
| `ledger://ledger/health` | Projection lag per projection | <10ms |

---

## Running Tests

```bash
# Full suite (runs without a live database — InMemoryEventStore)
pytest tests/ -v

# Phase 1 — EventStore correctness (the primary gate)
pytest tests/phase1/test_event_store.py -v

# Phase 2 — Aggregates and state machine
pytest tests/phase2/test_aggregates.py -v

# Phase 3 — Agents and LangGraph graphs
pytest tests/phase3/test_agents.py -v

# Phase 4 — Projections, upcasters, audit chain, Gas Town recovery
pytest tests/phase4/test_projections_and_integrity.py -v

# OCC double-decision test (the critical correctness demonstration)
pytest tests/phase1/test_event_store.py::test_concurrent_double_append_exactly_one_succeeds -v

# Schema and data generator
pytest tests/test_schema_and_generator.py -v
```

**Expected results:**

| Suite | Tests | Status |
|---|---|---|
| Phase 1 — EventStore | 11 | ✅ All passing |
| Phase 2 — Aggregates | 16 | ✅ All passing |
| Phase 3 — Agents | 17 | ✅ All passing |
| Phase 4 — Projections + Integrity | 15 | ✅ All passing |
| Schema + Generator | 10 | ✅ All passing |
| **Total** | **69** | **✅ 69 passed** |

---

## Running the Pipeline

```bash
# Run a single application end-to-end
python scripts/run_pipeline.py --app APEX-0007

# Run with a specific phase only
python scripts/run_pipeline.py --app APEX-0007 --phase credit

# Run the NARR-05 human override demo
python scripts/demo_narr05.py
```

---

## Project Structure

```
apex-ledger/
├── schema.sql                              # All PostgreSQL tables and indexes
├── DOMAIN_NOTES.md                         # 6 domain questions — graded deliverable
├── DESIGN.md                               # Architecture decisions — 6 sections
├── start-all.sh                            # Start Kafka + LangGraph + instructions
├── setup-pipeline.sh                       # Full pipeline automation
│
├── ledger/
│   ├── event_store.py                      # EventStore (Postgres) + InMemoryEventStore
│   ├── upcasters.py                        # 2 registered upcasters (v1 → v2)
│   │
│   ├── schema/
│   │   └── events.py                       # 45 Pydantic event types (canonical schema)
│   │
│   ├── registry/
│   │   └── client.py                       # ApplicantRegistryClient (4 SQL queries)
│   │
│   ├── domain/aggregates/
│   │   ├── loan_application.py             # 16-state machine + 6 business rule assertions
│   │   ├── agent_session.py                # Gas Town pattern enforcement
│   │   ├── compliance_record.py            # Regulatory rule tracking
│   │   └── audit_ledger.py                 # Append-only audit ledger
│   │
│   ├── agents/
│   │   ├── base_agent.py                   # BaseApexAgent (Gemini, OCC, Gas Town)
│   │   ├── credit_analysis_agent.py        # Full LangGraph implementation (7 nodes)
│   │   ├── stub_agents.py                  # 4 complete agent implementations
│   │   └── graph.py                        # Multi-agent orchestration graph
│   │
│   ├── commands/
│   │   └── handlers.py                     # Command handlers (load → validate → append)
│   │
│   ├── projections/
│   │   ├── daemon.py                       # ProjectionDaemon (fault-tolerant, checkpointed)
│   │   ├── application_summary.py          # Current state read model (<500ms SLO)
│   │   ├── agent_performance.py            # Per-agent metrics (<2s SLO)
│   │   └── compliance_audit.py             # Temporal compliance snapshots (<2s SLO)
│   │
│   ├── integrity/
│   │   ├── audit_chain.py                  # SHA-256 hash chain verification
│   │   └── gas_town.py                     # Crash recovery + context reconstruction
│   │
│   ├── upcasting/
│   │   └── registry.py                     # UpcasterRegistry (chain application)
│   │
│   ├── mcp/
│   │   ├── tools.py                        # 8 MCP tools
│   │   └── resources.py                    # 6 MCP resources (projection-backed)
│   │
│   └── regulatory/
│       └── package.py                      # generate_regulatory_package() — self-contained JSON
│
├── datagen/                                # Data generator
│   ├── company_generator.py                # 80 synthetic companies
│   ├── event_simulator.py                  # 1,198 seed events
│   ├── pdf_generator.py                    # 400 financial documents
│   └── generate_all.py                     # Entry point
│
├── data/
│   ├── applicant_profiles.json             # 80 company profiles
│   └── seed_events.jsonl                   # 1,198 events across 29 applications
│
├── scripts/
│   ├── run_pipeline.py                     # Full pipeline runner
│   └── demo_narr05.py                      # NARR-05: human override demo
│
└── tests/
    ├── phase1/test_event_store.py          # 11 tests — EventStore + OCC
    ├── phase2/test_aggregates.py           # 16 tests — Aggregates + state machine
    ├── phase3/test_agents.py               # 17 tests — Agents
    ├── phase4/test_projections_and_integrity.py  # 15 tests — Projections + Integrity
    └── test_schema_and_generator.py        # 10 tests — Schema validation
```

---

## Implementation Status

| Phase | Component | Status |
|---|---|---|
| 1 | EventStore — 6 methods, OCC, outbox, upcaster pipeline | ✅ Complete |
| 1 | InMemoryEventStore — identical interface for tests | ✅ Complete |
| 1 | PostgreSQL schema — 4 tables, 4 indexes, constraints | ✅ Complete |
| 2 | LoanApplicationAggregate — 16 states, 6 assertions | ✅ Complete |
| 2 | AgentSessionAggregate — Gas Town enforcement | ✅ Complete |
| 2 | ComplianceRecordAggregate | ✅ Complete |
| 2 | AuditLedgerAggregate | ✅ Complete |
| 2 | Command handlers — submit + credit + fraud + human review | ✅ Complete |
| 3 | ProjectionDaemon — fault-tolerant, checkpointed | ✅ Complete |
| 3 | ApplicationSummary projection | ✅ Complete |
| 3 | AgentPerformanceLedger projection | ✅ Complete |
| 3 | ComplianceAuditView — temporal query | ✅ Complete |
| 4a | UpcasterRegistry — chain application on load | ✅ Complete |
| 4a | CreditAnalysisCompleted v1→v2 upcaster | ✅ Complete |
| 4a | DecisionGenerated v1→v2 upcaster | ✅ Complete |
| 4b | AuditChain — SHA-256 hash chain + tamper detection | ✅ Complete |
| 4c | Gas Town crash recovery — reconstruct_agent_context() | ✅ Complete |
| 5 | MCP server — 8 tools + 6 resources | ✅ Complete |
| 5 | DocumentProcessingAgent — full LangGraph | ✅ Complete |
| 5 | CreditAnalysisAgent — full LangGraph (reference impl.) | ✅ Complete |
| 5 | FraudDetectionAgent — full LangGraph | ✅ Complete |
| 5 | ComplianceAgent — deterministic, no LLM | ✅ Complete |
| 5 | DecisionOrchestratorAgent — full LangGraph | ✅ Complete |
| 6 | generate_regulatory_package() | ✅ Complete |
| 6 | Narrative demos NARR-01 to NARR-05 | 🔄 In progress |
| Bonus | run_what_if() counterfactual projector | 🔄 In progress |

---

## The Enterprise Case

In 2026, the primary reason enterprise AI deployments fail to reach production is not model quality — it is governance and auditability. Regulators, auditors, and enterprise risk teams require an immutable record of every AI decision and the data that informed it.

The Ledger is that record. The same architecture applies to any domain where audit trails are non-negotiable: healthcare prior authorisations, government benefit decisions, insurance claim adjudication. Master this pattern and you have the infrastructure that unblocks the governance conversation in the first week of any enterprise AI engagement.

---

## Acknowledgements

Built as part of TRP1 Arc 5: Integration & Protocol Architecture.  
Event sourcing patterns informed by Greg Young's CQRS/ES work, Marten documentation, and EventStoreDB architecture.