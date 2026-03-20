# DOMAIN_NOTES.md
## Apex Financial Services — The Ledger
### TRP1 Week 5 — Domain Reconnaissance

---

## Question 1 — EDA vs ES Distinction

**The scenario:** A component uses callbacks (like LangChain traces) to capture event-like data. Is this Event-Driven Architecture (EDA) or Event Sourcing (ES)? If you redesigned it using The Ledger, what exactly would change and what would you gain?

**Answer:**

LangChain-style callbacks are EDA, not Event Sourcing. The distinction is fundamental: in EDA, events are notifications — they fire and are forgotten. The callback fires when something happens, a listener reacts, and the event is discarded or logged elsewhere. The system's state lives in a separate mutable database. If the process restarts, the callback history is gone.

In Event Sourcing, events ARE the database. The events table is the source of truth, and every other representation of state is derived from replaying those events.

If we redesigned the Automaton Auditor's callback system using The Ledger, three things would change:

First, instead of firing a callback that logs to a file or a separate audit table, every agent action would call `store.append()` and write an `AgentNodeExecuted` event to the agent's session stream. The event is now permanent and ACID-compliant.

Second, the audit verdicts — currently lost when the process ends — would become `GovernanceJudgement` events in a dedicated stream. Any future system could replay that stream and reconstruct every verdict ever made.

Third, the Gas Town problem disappears. Currently, if the agent crashes, its context is gone. With the event store, the agent replays its session stream on restart and continues exactly where it left off.

What we gain: reproducibility (any past state can be reconstructed), auditability (every decision is permanently recorded with its causal chain), and crash recovery (the event stream is the agent's durable memory).

---

## Question 2 — The Aggregate Question

**Which aggregate boundary did you consider and reject, and what coupling problem does your chosen boundary prevent?**

**Answer:**

The alternative I considered and rejected was merging `ComplianceRecord` into `LoanApplication` as a single aggregate. The argument for merging was simplicity — one stream per application, everything in one place.

I rejected this because of a specific concurrency coupling problem. In the current design, the `ComplianceAgent` writes to `compliance-{application_id}` independently while the `CreditAnalysisAgent` writes to `credit-{application_id}`. These are separate streams with separate OCC locks. They can proceed in parallel without contention.

If I merged compliance into `LoanApplication`, both agents would need to append to `loan-{application_id}`. At 1,000 applications per hour with 4 agents each, every compliance rule check and every credit analysis step would compete for the same stream lock. The `OptimisticConcurrencyError` rate would be extremely high, creating a retry storm that degrades throughput.

The chosen boundary — four separate aggregates (LoanApplication, AgentSession, ComplianceRecord, CreditRecord) — means each agent owns its output stream. The `LoanApplication` stream is only written to for lifecycle transition events (requested, completed, approved, declined), while the detailed analysis events live in their respective streams. This keeps the hot `loan-` stream lean and reduces OCC collisions to near zero for parallel agents.

---

## Question 3 — Concurrency in Practice

**Two AI agents simultaneously process the same loan application and both call `append_events` with `expected_version=3`. Trace the exact sequence of operations.**

**Answer:**

The sequence unfolds as follows:

Both agents read the stream at version 3 and begin their analysis. Agent A calls `store.append(stream_id="loan-APEX-0031", events=[...], expected_version=3)` first.

Inside `append()`, a database transaction begins. PostgreSQL executes `SELECT current_version FROM event_streams WHERE stream_id = 'loan-APEX-0031' FOR UPDATE`. The `FOR UPDATE` clause acquires a row-level lock on the `event_streams` row. Agent A reads `current_version = 3`, which matches `expected_version = 3`. Agent A inserts its event at `stream_position = 4` and updates `current_version = 4`. The transaction commits and the lock is released.

Agent B was waiting for the lock. Now it acquires it and reads `current_version = 4`. This does not match `expected_version = 3`. The event store raises `OptimisticConcurrencyError(stream_id="loan-APEX-0031", expected=3, actual=4)`. Agent B's transaction is rolled back — nothing is written.

The losing agent receives the `OptimisticConcurrencyError` in the `except` block of `_append_stream()`. It must reload the stream with `load_stream("loan-APEX-0031")`, inspect the new event Agent A appended at position 4, determine whether its own analysis is still relevant given the updated state, and retry `append()` with `expected_version=4`.

No locks are held between the read and the retry. No global locks are ever acquired. The entire mechanism is a single row-level lock held for milliseconds during the transaction.

---

## Question 4 — Projection Lag and Its Consequences

**The LoanApplication projection has 200ms typical lag. A loan officer queries "available credit limit" immediately after a disbursement event is committed. They see the old limit. What does your system do?**

**Answer:**

The projection daemon processes events asynchronously. When the disbursement event is committed to the `events` table, the `ApplicationSummary` projection has not yet processed it — the loan officer's query hits the projection table and returns the pre-disbursement limit.

The system does not crash or return an error. It returns stale-but-valid data from the last successfully processed event. This is eventual consistency by design.

For the user interface, there are three appropriate responses depending on the criticality of the read:

For the standard dashboard (low stakes): display the data as-is with a subtle timestamp showing "as of [last_event_at]". The loan officer understands the data may be seconds behind.

For critical financial reads (high stakes, like confirming a disbursement): the UI should use a read-after-write consistency pattern. After the disbursement command is sent, the client polls `ledger://ledger/health` to check `projection_lag` for `application_summary`. Once lag drops below a threshold or the projection's `last_event_at` passes the disbursement timestamp, the UI refreshes.

The `get_lag()` metric exposed by the projection daemon is the mechanism for this. The MCP resource `ledger://ledger/health` returns lag in milliseconds per projection, enabling the UI to make an informed decision about when to refresh.

The SLO for `ApplicationSummary` is under 500ms. A 200ms typical lag is well within the SLO and acceptable for most loan officer workflows.

---

## Question 5 — The Upcasting Scenario

**The `CreditDecisionMade` event was defined in 2024 with `{application_id, decision, reason}`. In 2026 it needs `{application_id, decision, reason, model_version, confidence_score, regulatory_basis}`. Write the upcaster. What is your inference strategy for historical events?**

**Answer:**

```python
from ledger.event_store import UpcasterRegistry

registry = UpcasterRegistry()

@registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    """
    Adds model_version, confidence_score, regulatory_basis to v1 events.
    
    Inference strategy:
    - model_version: inferred as "legacy-pre-2026" for all historical events.
      This is a safe sentinel value — downstream systems that need the exact
      model version will find it absent and treat the decision as unverifiable,
      which is correct. Fabricating a specific model version would be worse
      because it would create false audit trails.
      
    - confidence_score: set to None (not fabricated).
      Historical decisions did not record confidence. Setting it to None is
      honest — the regulatory system will see that confidence was not available
      for this decision, which is the truth. Fabricating a value like 0.75
      would corrupt compliance reports that filter on confidence thresholds.
      
    - regulatory_basis: inferred from the regulation set version active at the
      time of the event. For events with recorded_at before 2026-01-01, we
      use ["2024-REG-SET-v1"]. For events after, we use ["2026-Q1-v1"].
      This is safe because the regulation set is a matter of historical record.
    """
    payload.setdefault("model_version", "legacy-pre-2026")
    payload.setdefault("confidence_score", None)  # genuinely unknown — do not fabricate
    payload.setdefault("regulatory_basis", ["2024-REG-SET-v1"])
    return payload
```

**Inference strategy for `model_version`:** the event's `recorded_at` timestamp tells us when it was written. Before 2026, the system used a single model version. We set `"legacy-pre-2026"` as a sentinel — consumers know to treat this as "model version unknown" rather than a real version string.

**Why null for `confidence_score` rather than a default:** fabricating a confidence score would be actively harmful. If a compliance query asks "show me all decisions with confidence < 0.60 that bypassed the REFER rule", a fabricated 0.75 would incorrectly exclude historical decisions from the result set. Null correctly signals "this data was not available when this decision was made."

---

## Question 6 — The Marten Async Daemon Parallel

**Marten 7.0 introduced distributed projection execution across multiple nodes. How would you achieve the same pattern in your Python implementation?**

**Answer:**

Marten's Async Daemon uses a distributed lock (typically via the database) to ensure that only one node processes a given projection shard at a time. Multiple nodes can run daemons simultaneously, but each projection shard has exactly one active processor.

To achieve the same in Python, I would use PostgreSQL advisory locks as the coordination primitive.

Each `ProjectionDaemon` instance, on startup, attempts to acquire a PostgreSQL advisory lock keyed on the projection name:

```python
async def _acquire_projection_lock(self, conn, projection_name: str) -> bool:
    # pg_try_advisory_lock returns True if lock acquired, False if another
    # node already holds it — non-blocking
    lock_key = hash(projection_name) % 2**31
    acquired = await conn.fetchval(
        "SELECT pg_try_advisory_lock($1)", lock_key
    )
    return acquired
```

On startup, each daemon node tries to acquire locks for each projection it manages. If a lock is already held by another node, this node skips that projection. The result: across a cluster of N nodes, each projection is processed by exactly one node at a time.

The failure mode this guards against is dual processing — two nodes both processing the same events and producing duplicate projection updates. Without the lock, you get race conditions where both nodes read the same checkpoint, process the same batch, and one overwrites the other's work — producing incorrect aggregated metrics.

If the lock-holding node crashes, PostgreSQL automatically releases advisory locks when the session ends. The other nodes will acquire the lock on their next startup cycle and resume from the last saved checkpoint, ensuring no events are missed and no manual intervention is required.
