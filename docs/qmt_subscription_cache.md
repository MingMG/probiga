# Full QMT quote acquisition

The managed market now uses one native `subscribe_whole_quote` subscription.
Full-market publication, tracked Level-1 publication, queued current requests,
and direct acquisition read the same in-process quote book. Outside-universe
ad-hoc queries still explicitly query QMT; they are not reported as missing
merely because they were not on the managed watchlist.

The former repeated full-market `get_full_tick` sweep is removed. On each
subscription start, a cold-start cache read fills at most 40 securities per
timer invocation. Outside weekday 09:15–15:10 there is no native quote
subscription. A closing cache is read once per closing acquisition slot;
crossing midnight or a weekend does not repeat it. Entering the next live
session or closing slot rebuilds the cache. This slot is not a trading-calendar
assertion; holiday quotes retain their actual native times. Initial values never become Level-1 callback
evidence, and a racing real callback takes precedence over initial cache data.
Every full snapshot retains original native event times. Publication time
does not prove data freshness.

Callbacks only normalize and replace in-memory rows. File publication stays
on the existing timer. Generation fencing ignores callbacks from superseded
subscriptions; older native times cannot overwrite newer quotes. Unchanged
watchlists do not resubscribe. During weekday continuous trading sessions,
120 seconds without accepted market callbacks triggers subscription renewal,
with at least 30 seconds between attempts. Lunch and overnight silence do not
trigger renewal. The weekday rule is conservative and may renew on holidays.

Heartbeat fields report acquisition mode, cached symbol count, pending seed
count and last accepted market callback. Missing quotes, stale event times and
existing source/release identity checks remain visible to consumers.

Windows strategy recovery reads its JSON artifacts explicitly as UTF-8,
matching the writer. Windows PowerShell 5.1 otherwise interprets BOM-less
UTF-8 as the system code page and cannot reload backups containing the
Chinese Guojin installation path. Regression coverage round-trips and restores
the actual backup contract using both ASCII and Chinese directory names.

Ordinary scheduler audit writes that fail during a database disconnect retain
the worker's exact terminal outcome. Once that worker releases ownership, the
scheduler retries those writes in bounded batches during polling or shutdown.
The shutdown receipt still requires the committed audit rows. This prevents a
transient tunnel outage from leaving a stopped collector waiting forever;
delivery/activation transactions are not replayed by this audit retry queue.

## Scope and limits

Runtime implementation: Windows/QMT strategy only. Deployment boundary:
cross-end, because Linux consumers validate this strategy's source/blob hash.
No database schema, data-source identity or task ownership is changed.

This changes quote acquisition, not historical downloading. A blocked native
history call still cannot be interrupted safely by an external timeout.
Neither passing tests nor reducing repeated native calls proves that the
client's Python access violations are fixed. Acceptance requires a coordinated
merged-main deployment, real post-start coverage/identity checks, and observed
market-session callbacks and consumer progress without repeated crashes.
