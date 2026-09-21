# QMT recovery-chain investigation, 2026-09-21

## Proven failures

The production checkout and scheduler were already at `9600fef`. The native
terminal fault at 17:33:20 was followed by bridge failure, terminal absence and
a relaunch to the login form. The latest two native dumps both report a write
access violation at address zero in `python36.dll+0x1aa886`. This identifies the
crash location, not the operation that originally corrupted interpreter state.
The 20:38:25 terminal exit has no corresponding new native fault dump; its cause
is not established.

At 21:07 the native terminal and minute acquisition were active again, while
`consumer_status.json` still dated from 20:38:25. The consumer and local live
supervisor processes were absent. The updater nevertheless returned exact-ready
without ensuring the supervisor. A read-only production probe reproduced
`release activation trigger seal is unavailable` with the consumer launcher's
existing SHA-only environment. Binding production mode and the Windows edge
role made the same activation check return READY, without changing the database.

## Corrections

- The snapshot consumer uses the existing hash-locked Python 3.13 QMT runtime.
  Its launcher binds production mode, Windows role, code root, QMT interpreter
  and exact checkout build together, and restores its parent environment even
  when process creation fails. Existing release validation remains mandatory.
- The existing updater ensures the existing local live supervisor after release
  activation, including equal-build retries and logged-out data states. No new
  scheduler task or polling service is introduced. The launcher uses the
  supervisor's lifetime mutex rather than potentially unreadable process command
  lines, serializes launch attempts and confirms child startup.
- An authenticated observation retires a previous ambiguous login attempt before
  model recovery or terminal rotation. A subsequent model failure cannot poison
  the next authentication attempt. Rejected, unconfirmed and identity-mismatched
  logins remain blocked; foreground, modal and credential checks are unchanged.

## Boundary and verification

Runtime changes are in the Windows login/recovery and supervisor/updater scripts.
The release controller binds the Windows build to the shared activation and
component contract, so this release requires the existing coordinated cutover.
There is no data-schema, source-response or trading-order change.

Isolated tests exercise exact credential-attempt retirement, model failure after
successful authentication, rejected credentials, parent-environment restoration,
consumer build rollover, unreadable process command lines, singleton startup and
activated updater ordering. Live acceptance must separately establish fresh
native and consumer heartbeats, successful activation checks and actual history
checkpoint/partition progress.

These corrections address demonstrated recovery defects. They do not establish
the cause or elimination of native interpreter corruption. A short healthy
interval or successful unit tests must not be reported as proof that all QMT
crashes are fixed. Native diagnosis still needs a reproducible fault or vendor
symbolized analysis of the existing dumps; restarting repeatedly is not a cure.
