# Stable QMT history backfill

## Scope and release boundary

This is a coordinated Windows/QMT and Linux/server release. The Windows
publisher changes its batch persistence and resource-pressure outcome; the
shared scheduler changes history-task serialization and its registered repair
window. No database schema or native QMT strategy source change is required.
Deploy only the merged main revision through the production release broker and
the registered Windows updater. Keep the older full-year/local-gap tasks disabled.

## Acquisition behavior

- Stock minute requests and durable checkpoints are capped at five symbols,
  including oversized inherited configuration. Newly fetched batches are paced
  by at least 0.25 seconds, preserving the previous 40-symbol/two-second pacing.
- Closed-session, full-session captures retain batches on durable disk,
  including the current trading date after 15:05. Same-day source finality still
  remains a separate publication check. The next historical day gets a distinct
  capture scope and never rewrites a same-day capture's original timestamp.
- Closed single-day minute reads inspect QMT's native local cache first. Only
  symbols without the complete valid 241-point native grid are downloaded;
  refreshed data is read again for the entire batch under one real response.
  Daily evidence continues its native refresh because one cached daily row does
  not establish that it was captured after the close.
  Resume preserves capture identity and revalidates persisted evidence before
  publication. Unfinished or corrupt evidence never certifies a complete date.
- A missing stock or minute no longer stops acquisition of the remaining
  batches. An internally valid but incomplete native response is retained as
  pending verification, with its original rows, timestamps, source receipts and
  coverage reasons. The collector visits the remaining batches before full-day
  acceptance. A possible suspension is recorded as a gap, not assumed to be a
  verified no-trade day.
- Pending responses remain separate from EXACT batches. A restart during the
  first acquisition sweep replays both retained raw pending responses and exact
  batches, so unvisited stocks are acquired first. Once the sweep has visited
  all batches, a later attempt re-fetches only incomplete batches. Source
  identity, original timestamps and evidence hashes are revalidated on replay;
  pending data never becomes publication authority. No partial day is published
  into canonical tables. The date owner
  records the failed date and continues other dates, while global resource or
  provenance failures remain blocking.
- Native history admission stops at 3.2 GiB private memory and has a 3.5 GiB
  hard no-call ceiling. Resource pressure exits the external writer with code
  75; its owner preserves completed checkpoints, rotates the exact QMT process,
  proves automatic login plus a fresh strategy heartbeat, and resumes the same
  immutable partition. The 20,000-handle boundary uses the same rotation path.
- A native fatal exit of the external Python writer does not authorize logging
  into QMT again. Already saved batches remain available to the next run.
- QMT stock, index, ETF and local historical publishers use one scheduler lane.
  Live quote collection retains its independent scheduling path.
- The existing gap-repair owner checks the last 22 exchange sessions, attempts
  at most 30 missing partitions per run with recent dates first, and checks the overnight window before
  every new partition. It keeps completed partitions and stops opening new work
  at 08:00. Ordinary missing data remains distinct from global resource pressure.
- Failed runs retain the scheduler's 15-minute completion-to-retry backoff.
  The existing registration upsert updates the same repair task; no duplicate
  collector or new scheduler is introduced.

## Acceptance and limitations

Regression coverage must prove restart resumes completed batches without native
refetch, validates frozen source identities and content digests, rejects altered
evidence, and never publishes an incomplete date. Scheduler coverage must prove
single-worker contention, ordinary error isolation, global-pressure yielding and
08:00 boundaries. Production acceptance also needs advancing checkpoints under
the merged revision and fresh native heartbeat/resource samples.

Acquisition-before-acceptance regression coverage also needs a missing middle
batch followed by successful later batches, durable pending raw evidence,
retrying only the incomplete batch, exact-only publication and cleanup, and
failure-closed behavior for invalid identity, storage errors and memory pressure.
Checkpoint scopes remain bound to the release build. A new release preserves
prior captures on disk but does not relabel them as captures of the new build.
The September 19 acquisition-before-acceptance update passed 176 targeted tests,
including incomplete native responses, durable pending evidence, later-batch
progress, exact-only publication, date isolation and resource-pressure handling.

Validation before merge: 678 selected tests and 87 subtests passed, covering
checkpoint recovery/corruption/disk failures, exact publication, resource error
propagation, daily resume, scheduler contention/retry and overnight boundaries.
The pre-existing September 17 capture completed under the prior release at
23:20:21 on September 18, with an exact readback of 1,337,550 native minute rows;
this is a baseline observation, not acceptance evidence for the new checkpoint.

The prior Python 3.14.3 writer fatal (`Executing a cache`, weakref frame) has not
been attributed to a confirmed interpreter or extension defect. Bounded capture
and durable resume reduce lost work; they are not proof that every native crash
is eliminated. QMT must remain running and authenticated. A sleeping computer or
permanent source-capacity refusal prevents progress, and missing minute-flow
capability is not repaired by slowing OHLCV acquisition.
