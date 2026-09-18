# Stable QMT history backfill

## Scope and release boundary

This is a coordinated Windows/QMT and Linux/server release. The Windows
publisher changes its batch persistence and resource-pressure outcome; the
shared scheduler changes history-task serialization and its registered repair
window. No database schema or native QMT strategy source change is required.
Deploy only the merged main revision through the production release broker and
the registered Windows updater. Keep the older full-year/local-gap tasks disabled.

## Acquisition behavior

- Stock minute requests are capped at 40 symbols, including oversized inherited
  configuration. Newly fetched batches are paced by at least two seconds.
- Closed-date, full-session captures retain validated batches on durable disk.
  Resume preserves capture identity and revalidates persisted evidence before
  publication. Unfinished or corrupt evidence never certifies a complete date.
- The native 4 GiB private-memory guard and system-memory reserve are unchanged.
  Resource pressure exits the external writer with code 75; its owner preserves
  the typed capacity failure, stops that repair run and waits for normal retry.
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
