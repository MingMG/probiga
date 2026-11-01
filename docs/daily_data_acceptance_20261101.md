# Daily Data Acceptance

## Objective

Separate deployment health from completed daily data. A running API or
scheduler is not evidence that the current trading session was acquired,
validated, and published.

## Schedule

- `21:50` `daily_data_closeout_repair` performs the one bounded repair pass. It
  records its result but does not send a preliminary alert.
- `23:20` the existing `daily_data_closeout_verify` task delegates to
  `tools/daily_data_acceptance.py`.

The final acceptance task is read-only against business data. It writes an
atomic JSON receipt to
`/var/lib/probiga/data-acceptance/<trade-date>.json`.

## Seven Checks

1. A current standard daily receipt exists and is no older than 24 hours.
2. Target-day stock-code coverage is at least 90% of the previous session.
3. The QMT daily import receipt is complete, available before acceptance, and
   no older than 24 hours.
4. Membership is not `DATA_BLOCKED` and is current within two sessions.
5. The enabled ETF daily task has a successful target-day run.
6. The Eastmoney concept snapshot is current within two sessions.
7. Every enabled V3 task has a successful target-day run.

## Receipts and Alerts

The JSON status is one of:

- `DATA_ACCEPTANCE_OK`
- `DATA_ACCEPTANCE_FAILED`
- `DATA_ACCEPTANCE_SKIPPED`

All checks pass: no WeCom message is sent.

Any check fails: the task exits non-zero and sends one idempotent WeCom message
containing each failed check and its concrete remediation. The same failure
does not produce duplicate messages.

The acceptance task never mutates source data, scheduler state, service state,
or release state. Repair remains isolated to the earlier bounded repair task.
