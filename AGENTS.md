# Codex branch and release workflow

For every new Codex conversation that changes this project:

1. Fetch the latest production `main` branch before editing.
2. Create a new, conversation-specific `codex/*` branch from that exact `main` revision.
3. Never make code changes directly on `main`, and never reuse a branch from another conversation.
4. Implement and test all changes on the conversation branch.
5. Commit and push the conversation branch, merge it into `main`, then deploy only the merged `main` revision.
6. Keep unrelated user changes out of the branch; use a separate worktree when the current tree is dirty.

Read-only investigation does not require a branch until a file change is needed.

# Deployment scope policy

For every task, scan the planned and actual change scope before implementation
and again before deployment. Classify the affected runtime boundary as Windows/QMT,
Linux/server, or cross-end.

1. Default to a single-end release limited to the affected runtime boundary.
2. Do not deploy or restart an unaffected endpoint.
3. Changes involving shared data, database schemas, QMT protocols, scheduler task
   ownership, or cross-end configuration require a coordinated Windows and Linux
   release/cutover.
4. If the affected boundary cannot be determined safely, treat the change as
   cross-end and require coordinated release.
5. Before deployment, state the classification and the files or contracts that
   justify it.

# No transitional designs

1. Implement the final target architecture directly. Transitional designs are
   not allowed.
2. Do not introduce temporary compatibility layers, dual old/new execution paths,
   temporary feature flags, placeholder implementations, disposable adapters,
   migration shims, or workarounds intended to be removed later.
3. Do not accept plans framed as "ship this now and redesign/refactor it later."
4. Remove superseded paths as part of the same change once the final path is
   active, while preserving data and rollback safety.
5. If an external dependency or missing decision makes the final design impossible,
   stop and report the blocker instead of implementing a transitional version.

# Production incident and recovery workflow

Use this workflow for production incidents, data gaps, stuck jobs, failed releases,
external-client failures, and any task whose first attempted fix did not work.

1. Before changing anything, state the user-visible objective in one sentence and
   define the machine-verifiable evidence that will prove completion.
2. Map the critical path from source to durable outcome. For data work, inspect at
   least source/client, adapter or bridge, scheduler/owner, transport, database
   write, completion receipt, and coverage/readback.
3. Probe those boundaries in source-to-sink order with read-only checks. Mark facts
   as observed or inferred; do not turn an inference into a root cause.
4. Identify the first failing boundary and change only that boundary. Do not edit
   downstream consumers while an upstream prerequisite is still unproven.
5. Freeze the incident scope. Record unrelated defects for later work; do not put
   maintenance, refactoring, verifier improvements, or unrelated releases on the
   recovery critical path unless direct causal evidence makes them necessary.
6. Prefer an already-proven final-architecture path over repairing a broken
   secondary control path during the incident.
7. After the same failure occurs twice, or after 30 minutes without measurable
   forward progress, stop retrying. Re-establish the boundary facts, preserve the
   evidence, and choose a materially different approach before another mutation.
8. Never run duplicate deployers, collectors, backfills, migrations, or recovery
   jobs for the same scope. Prove that the prior run is terminal before starting
   another one.

# Completion and status semantics

Use precise states: `NOT_STARTED`, `RUNNING`, `BLOCKED`, `FAILED`, and `COMPLETE`.

1. A healthy process, fresh heartbeat, accepted request, populated queue, or
   successful source response proves only its own stage. None proves the requested
   business outcome.
2. Do not say that a backfill or acquisition has started until the business job
   itself has a run identity and has begun processing its target partitions.
3. Data acquisition is `COMPLETE` only when durable database writes, the canonical
   completion receipt, expected partition/date/security coverage, and a readback
   check all agree. Row activity without coverage proof is not completion.
4. Deployment is `COMPLETE` only when the merged revision is active on every
   affected endpoint and the post-deploy acceptance check passes. A built artifact,
   accepted release request, or healthy old revision is not completion.
5. If only part of a request completed, report the exact completed and remaining
   partitions. Never convert partial progress into a global success statement.

# Bounded operations and progress

1. Every external call and every operational stage must have a timeout, a total
   deadline, bounded retries, and a terminal error that names the failed stage.
2. Long-running work must expose total units, completed units, current unit, last
   progress time, and a durable checkpoint. Retries must resume from the checkpoint
   rather than restart completed work.
3. If a bulk API exists, do not serialize it into one request per item without a
   measured safety reason. Use bounded batches with response-size and latency caps.
4. A watchdog must distinguish slow progress from no progress. CPU use, process
   existence, or an open socket alone is not progress.
5. Process and TCP liveness are insufficient for remote dependencies. Use a bounded
   application-level probe, such as an authenticated `SELECT 1`, and verify the
   complete end-to-end path.
6. Scheduler `running` state must be backed by a renewable owner lease. Reclaim a
   run only after the lease expires and the owning process or instance is proven
   absent; never leave phantom runs requiring manual database cleanup.

# External UI and runtime control

1. Identify the authoritative runtime control plane before automating an external
   application. An editor, configuration row, or source file is not necessarily the
   running instance.
2. Bind mutations to semantic identity: process, window, strategy/job name, row,
   instance ID, and expected current state. Hard-coded screen coordinates or DPI
   offsets must not be the sole selector for a production action.
3. Every start, stop, reload, or switch must have an independent postcondition from
   the runtime itself. For QMT this includes the target running-instance list,
   heartbeat state, model instance ID, build/source identity, and a bounded no-order
   capability request.
4. Once production evidence proves that a control path does not affect the runtime,
   record that fact as an invariant and add a regression test. Do not reintroduce
   the rejected path without new live evidence that invalidates the invariant.
5. Mocked or source-text tests are not sufficient for external UI control. Before
   declaring the fix complete, run one controlled live acceptance test with exact
   before/after evidence and rollback safety.

# Release and architecture decoupling

1. Version and compare deployable artifacts by their own content and contract
   identity. An unrelated repository commit must not force a QMT model reload,
   service restart, cache rebuild, or data recapture when that artifact is unchanged.
2. Expensive reference-data refreshes, catalog rebuilds, cache warming, and historical
   scans must be resumable business jobs. Do not make them a global release-readiness
   prerequisite unless every released consumer strictly requires fresh data.
3. Gate each workload only on the datasets and contracts it actually consumes. A
   failed sector catalog refresh, for example, must not block stock daily/minute
   ingestion that is valid against an existing verified catalog snapshot.
4. Reuse immutable verified snapshots by content hash and validity period. Do not
   reproduce identical reference data merely to bind it to a new repository SHA.
5. A runtime-only implementation change remains single-end unless a shared protocol,
   schema, ownership contract, or cross-end configuration actually changes. Path
   heuristics alone are not sufficient evidence for a coordinated release.

# Testing and incident communication

1. Test in layers: deterministic unit tests, contract tests at the changed boundary,
   a live canary for the external dependency, then end-to-end business acceptance.
2. A regression test must reproduce the observed production failure, not merely
   assert the new implementation text or mocked call sequence.
3. During an incident, every update must state the current exact state, the next
   acceptance gate, measurable progress since the previous update, and the timeout
   or stop condition. Do not repeat "still running" without new evidence.
4. Correct an inaccurate status statement immediately. In particular, keep
   "client healthy", "request sent", "data written", and "dataset complete" as
   separate claims.
