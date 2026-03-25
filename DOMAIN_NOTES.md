# DOMAIN_NOTES.md
## Apex Financial Services — The Ledger
### TRP1 Week 5 · Domain Reconnaissance — Final Submission

---

## Question 1 — EDA vs ES Distinction

**The scenario:** A component uses callbacks (like LangChain traces) to capture event-like data. Is this Event-Driven Architecture (EDA) or Event Sourcing (ES)? If you redesigned it using The Ledger, what exactly would change in the architecture and what would you gain?

**Answer:**

LangChain-style callbacks are EDA, not Event Sourcing. The distinction is fundamental and consequential, not semantic.

In EDA, events are notifications — the sender fires and forgets. A callback fires when something happens, a listener reacts, and the event is discarded or logged to a side table. The system's source of truth is a separate mutable database. If the process restarts, the callback history is gone. The Automaton Auditor's verdict stream is EDA — it produces events as output but the audit verdicts live nowhere permanent.

In Event Sourcing, events ARE the database. The `events` table is the source of truth. Everything else — projections, aggregates, dashboards — is a derived view built by replaying those events. There is no separate "state" to get out of sync.

**If we redesigned the Automaton Auditor using The Ledger, three things change:**

First, instead of firing a callback that logs to a file, every agent action calls `store.append()` and writes an `AgentNodeExecuted` event to the agent's session stream. The event is now ACID-committed to PostgreSQL. It cannot be lost.

Second, audit verdicts — currently ephemeral — become `GovernanceJudgement` events in a dedicated stream. Any future system can replay that stream and reconstruct every verdict ever issued, with full causation chains intact.

Third, the Gas Town failure mode disappears. Currently, if the Automaton Auditor crashes mid-session, its context is gone and the work must restart from scratch. With the event store, it replays its session stream on restart and continues from `last_successful_node`. No work is lost and no work is duplicated.

**What we gain:** reproducibility (any past state can be reconstructed), auditability (every decision is permanently recorded with what data informed it), temporal queries (what did the system know at any point in time?), and crash recovery (the event stream is the agent's durable memory). All of these are non-negotiable requirements for regulatory-grade AI systems.

---

## Question 2 — The Aggregate Question

**Which aggregate boundary did you consider and reject, and what coupling problem does your chosen boundary prevent?**

**Answer:**

The alternative I considered and rejected was merging `ComplianceRecord` into `LoanApplication` as a single aggregate. The argument for merging is simplicity — one stream per application, everything in one place, no cross-aggregate reads.

I rejected it because of a specific concurrency coupling problem that materialises at scale.

In the current design, the `ComplianceAgent` writes to `compliance-{application_id}` while the `CreditAnalysisAgent` writes to `credit-{application_id}`. These are separate aggregate streams with separate OCC locks. They proceed in parallel with zero contention.

If I merged compliance into `LoanApplication`, both agents would compete for the same `loan-{id}` stream lock. At 1,000 applications per hour with 4 agents each writing multiple events per application, every compliance rule check and every credit analysis step would fight for the same row-level lock in `event_streams`. The `OptimisticConcurrencyError` rate would be high, and each retry requires a full stream reload — turning a lock contention problem into both a latency and a cost problem.

The failure mode is precise: a slow `ComplianceAgent` (9 nodes, evaluating 6 regulatory rules) would hold the `loan-{id}` lock long enough that a `CreditAnalysisAgent` attempting to write its completion event would fail its OCC check repeatedly. Under load, this compounds into a retry storm.

The chosen boundary — four separate aggregate streams (`loan`, `credit`, `fraud`, `compliance`) plus per-session `agent` streams — means the `loan-{id}` stream is only written to at well-defined lifecycle transition points (ApplicationSubmitted, CreditAnalysisRequested, ApplicationApproved, etc.), not during the analysis work. The hot lock is lean by design.

**The coupling prevented:** concurrent agent writes from producing OCC retry storms on the application lifecycle stream.

---

## Question 3 — Concurrency in Practice

**Two AI agents simultaneously process the same loan application and both call `append_events` with `expected_version=3`. Trace the exact sequence of operations in your event store. What does the losing agent receive, and what must it do next?**

**Answer:**

Both agents read `stream_version("loan-APEX-0031") = 3` and begin their analysis concurrently. Both call `store.append(stream_id="loan-APEX-0031", events=[...], expected_version=3)`.

**Agent A reaches the database first:**

```
BEGIN TRANSACTION
SELECT current_version FROM event_streams
  WHERE stream_id='loan-APEX-0031'
  FOR UPDATE                        -- acquires row-level exclusive lock
→ current_version=3                 -- matches expected_version=3 ✓
INSERT INTO events (stream_id='loan-APEX-0031', stream_position=4, ...)
UPDATE event_streams SET current_version=4 WHERE stream_id='loan-APEX-0031'
INSERT INTO outbox (event_id=..., destination='default', ...)
COMMIT                              -- lock released
```

**Agent B was blocked at `SELECT ... FOR UPDATE`.** PostgreSQL queued its lock request. The moment Agent A's transaction commits:

```
→ Agent B acquires the row lock
→ Reads current_version=4
→ 4 ≠ expected_version=3
ROLLBACK
→ Raises OptimisticConcurrencyError(
      stream_id="loan-APEX-0031",
      expected=3,
      actual=4
  )
```

**What Agent B must do next:** catch `OptimisticConcurrencyError` in its `_append_stream()` error handler, call `load_stream("loan-APEX-0031")` to reload the stream (now including Agent A's event at position 4), inspect Agent A's decision to determine whether its own analysis is still relevant (did Agent A already complete what Agent B was about to do?), and if still relevant, retry `append()` with `expected_version=4`.

**The UNIQUE constraint on `(stream_id, stream_position)` is the second safety net.** If two transactions somehow both pass the version check (a race condition the `FOR UPDATE` should prevent), the constraint will cause one INSERT to fail with a unique violation, which is caught and converted to `OptimisticConcurrencyError` by the same error handler.

No distributed locks are held. No global state is modified outside the transaction. The entire mechanism is a row-level lock held for the duration of one INSERT statement.

---

## Question 4 — Projection Lag and Its Consequences

**Your LoanApplication projection has 200ms typical lag. A loan officer queries "available credit limit" immediately after an agent commits a disbursement event. They see the old limit. What does your system do, and how do you communicate this to the user interface?**

**Answer:**

The projection daemon processes events asynchronously. When the disbursement event is committed to the `events` table, the `ApplicationSummary` projection has not yet processed it — the loan officer's query hits the `application_summary` table and returns the pre-disbursement limit. The system does not crash or return an error. It returns stale-but-valid data from the last successfully processed event. This is eventual consistency by design, and it is the correct behaviour for this read model.

**What the system does:** nothing abnormal. The query succeeds. The SLO for `ApplicationSummary` is <500ms lag, and 200ms typical lag is well within the SLO. The system is operating correctly.

**How we communicate this to the UI:** three strategies depending on the criticality of the read.

For the standard loan officer dashboard (most reads): display data as-is with the `last_event_at` timestamp visible, formatted as "updated 200ms ago." The loan officer understands the dashboard is near-real-time, not instantaneous. This matches how every financial dashboard works.

For critical financial reads (confirming a disbursement has been recorded): the UI uses a read-after-write consistency pattern. After the disbursement command returns success, the client polls `ledger://ledger/health` and checks the `application_summary` projection's lag metric. Once the projection's `last_event_at` timestamp advances past the disbursement timestamp (or lag drops below a threshold), the UI refreshes. The MCP resource `ledger://ledger/health` exposes lag per projection for exactly this purpose.

For regulatory audit reads (a compliance officer confirming the final state): the system directs them to `ledger://applications/{id}/audit-trail`, which loads from the `AuditLedger` stream directly and is always current. Projections are for operational reads. The audit trail is for authoritative reads.

**The anti-pattern to avoid:** showing a loading spinner until the projection catches up. Under normal operation (200ms lag), this creates an unnecessary 200ms delay on every read. The correct pattern is to show the data immediately and let the timestamp communicate freshness.

---

## Question 5 — The Upcasting Scenario

**The `CreditDecisionMade` event was defined in 2024 with `{application_id, decision, reason}`. In 2026 it needs `{application_id, decision, reason, model_version, confidence_score, regulatory_basis}`. Write the upcaster. What is your inference strategy for historical events that predate `model_version`?**

**Answer:**

```python
from ledger.event_store import UpcasterRegistry

registry = UpcasterRegistry()

@registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    """
    Adds model_version, confidence_score, and regulatory_basis to v1 events.

    Called transparently by EventStore.load_stream() and load_all() whenever
    a v1 event is loaded. The stored event in the database is NEVER modified.
    """
    # model_version: sentinel value — honest about what we don't know
    payload.setdefault("model_version", "legacy-pre-2026")

    # confidence_score: genuinely unknown — do not fabricate
    payload.setdefault("confidence_score", None)

    # regulatory_basis: safe to infer from historical record
    payload.setdefault("regulatory_basis", ["2024-REG-SET-v1"])

    return payload
```

**Inference strategy per field:**

`model_version` — set to `"legacy-pre-2026"` as a sentinel. This is the safest choice because: (a) before 2026, the system ran exactly one credit model version, so any credit event before a certain date was produced by that model; (b) the sentinel is human-readable and immediately understood by auditors as "model version not recorded for this event"; (c) it allows downstream consumers to filter on `model_version = "legacy-pre-2026"` to find all pre-2026 decisions, which is exactly the regulatory use case.

`confidence_score` — set to `None`. This is the only honest choice. Historical credit analyses did not record a numeric confidence score. Fabricating a value like `0.75` would: (a) corrupt compliance reports that filter decisions by `confidence < 0.60` (the REFER threshold); (b) create false audit trails that claim a regulatory floor was checked when it was not; (c) potentially trigger or suppress human review incorrectly when the audit trail is replayed for counterfactual analysis. Null correctly signals "this data was not available when this decision was made." The regulatory consequence of a fabricated inference is worse than the consequence of a null.

`regulatory_basis` — set to `["2024-REG-SET-v1"]`. This is safe to infer because the regulation set active before 2026 is a matter of historical record, not dependent on individual event data. All v1 events were produced under that regulation set, so the inference has near-zero error rate.

**The immutability guarantee:** upcasters are pure functions. They receive a payload dict and return a new payload dict. They never call `store.append()` or execute any database write. The `test_upcaster_does_not_mutate_stored_event` test verifies: load a v1 event → confirm it arrives as v2 → directly query the `events` table → confirm raw `event_version = 1` and original payload are unchanged.

---

## Question 6 — The Marten Async Daemon Parallel

**Marten 7.0 introduced distributed projection execution across multiple nodes. Describe how you would achieve the same pattern in your Python implementation. What coordination primitive do you use, and what failure mode does it guard against?**

**Answer:**

Marten's Async Daemon uses distributed locking at the PostgreSQL level to ensure that across a cluster of N application nodes, each projection shard is processed by exactly one node at a time. Multiple nodes can run daemons, but a given projection cannot be processed by two nodes simultaneously.

**To achieve the same in Python, I use PostgreSQL advisory locks as the coordination primitive.**

On startup, each `ProjectionDaemon` instance attempts to acquire a non-blocking advisory lock keyed on the projection name:

```python
async def _try_acquire_projection_lock(
    self, conn, projection_name: str
) -> bool:
    # pg_try_advisory_lock returns True immediately if lock acquired,
    # False if another session already holds it — never blocks.
    lock_key = abs(hash(projection_name)) % (2**31)
    return await conn.fetchval(
        "SELECT pg_try_advisory_lock($1)", lock_key
    )

async def start(self) -> None:
    self._running = True
    async with self._pool.acquire() as lock_conn:
        # Hold this connection open for the duration of the daemon —
        # advisory lock is released automatically when connection closes.
        for projection in self._projections:
            acquired = await self._try_acquire_projection_lock(
                lock_conn, projection.name
            )
            if acquired:
                self._active_projections.append(projection)
                log.info(f"Acquired lock for projection: {projection.name}")
            else:
                log.info(f"Lock held by another node: {projection.name} — skipping")

        while self._running:
            await self._process_batch()
            await asyncio.sleep(self.poll_interval)
```

**The failure mode this guards against: dual processing.** Without the lock, two nodes both reading from the same checkpoint position would process identical event batches and produce duplicate projection updates. For `ApplicationSummary`, this means a single `CreditAnalysisCompleted` event could be processed twice — updating the risk tier to the same value twice is idempotent and harmless, but updating an approval count or cost metric twice would corrupt the `AgentPerformanceLedger`.

**On node failure:** PostgreSQL automatically releases all advisory locks when a session ends — whether through graceful shutdown, network partition, or process crash. Other daemon nodes will acquire the lock on their next startup cycle (or next poll iteration if they check periodically) and resume from the last saved checkpoint. No manual intervention is required. No events are missed. This is the same guarantee Marten provides through its database-level locking.

**What EventStoreDB gives that PostgreSQL requires more work to achieve:** EventStoreDB's persistent subscriptions handle consumer group management natively — each subscriber gets the next unprocessed event, and the server tracks per-subscriber positions. In our PostgreSQL implementation, we achieve the same result by combining advisory locks (one processor per projection) with the `projection_checkpoints` table (per-projection position). EventStoreDB collapses both into a single subscription primitive. The operational overhead is higher on the PostgreSQL side, but the behaviour is equivalent.