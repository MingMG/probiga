# Recover Windows task claims after an OS restart

Windows Update rebooted the QMT host at 2026-09-18 02:01:01. The recent-history
task started at 01:28:16 retained its `running` database claim. The new scheduler
correctly refused to infer process death merely from its empty process registry,
but the existing older-build recovery also rejected the unchanged build.

Recovery now permits a same-build Windows claim only when CIM boot time agrees
with kernel uptime within 120 seconds, the boot predates this scheduler, every
matching history owner belongs to this host, every run predates boot by more than
one minute, and each exact old owner PID is absent. Failed sampling, clock
disagreement, reused/live PIDs, other hosts, or post-boot task starts retain the
claim. All owners are validated before any write. Existing transaction locks and
claim timestamp/cardinality checks remain in force.

Recovered history is recorded as `failed` with `previous_windows_boot` evidence;
it never asserts successful collection. The ordinary scheduler decides whether
and when to retry. Existing older-build recovery remains supported by the same
owner-validation path. Linux same-build recovery is unchanged.

Release scope is cross-end: `server/api/scheduler_runtime.py` governs shared task
ownership and writes shared history/claim rows. Deploy the merged main revision
through the coordinated release broker. There is no database schema change or
change to native QMT strategy code, data coverage thresholds, or trading behavior.

Validation: 304 related tests plus 87 subtests passed. The actual Windows reader
returned `2026-09-18 02:01:01.500000`, matching OS boot evidence. Tests cover
same-build preboot recovery, current-boot rejection, unavailable evidence,
foreign hosts, live/reused PIDs, CIM/uptime disagreement and read failure.
