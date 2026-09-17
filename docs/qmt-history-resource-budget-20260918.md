# QMT history memory admission and recent-only collection

The September 18 observation found QMT private memory above 5 GB, working set
near 4.9 GB and roughly 31,000 handles (mostly native semaphores). Host available
physical memory fell to about 1.2 GiB while the yearly and recent repair workers
both sent history requests. The process remained alive; the available evidence
does not establish the cause of earlier native access violations. After stopping
the workers, handles and working set declined, while private allocation remained
high. Reducing working set is not proof that private allocations were released.

The pipeline already batches source requests (minute: at most 50 symbols for a
day; daily: at most 20 symbols). The canonical minute writer appends each outer
batch to a database stage and publishes only after whole-partition validation.
Local history also upserts each validated batch. It does not load a whole year
before writing. Whole-day validation and per-batch copies still have memory
costs in the separate Python workers.

Native history downloads and reads now require a fresh Windows resource sample.
The admission budget reserves at least 1 GiB or 10% of host physical memory,
whichever is larger, in both available physical and commit capacity. QMT private
allocation must be below the smaller of 4 GiB or one quarter of host physical
memory. These conservative limits bound new admissions; a single native call can
still allocate more memory than predicted. Sampling failure also denies admission.
The Win32 bindings and ctypes types are cached, avoiding per-call type retention.

The guard covers spool downloads, context history readers and the independently
hashed acquisition model's injected native download functions. Cached quote
reads, heartbeats, cancellation and control requests remain available. A denied
request returns an explicit resource error; the canonical CLI maps it to existing
exit code 3 so the publisher does not treat it as a login/restart opportunity.
No timer resets, forced working-set trims or automatic QMT restarts are used.
The latest sample and decision appear in the heartbeat for diagnosis. This is an
allocation safety boundary, not a claim that the native crash root cause is proven.

At the user's request, yearly local history and old local-gap execution tasks
are disabled in the registered contract. Installation preserves these per-task
defaults instead of enabling every operations task. Existing data and the recent
canonical repair task are retained. Runtime disable operations were applied as
an immediate stop of future dispatch; stopping active children uses the existing
identity-bound scheduler shutdown request, not forged task completion statuses.

Release scope is cross-end: embedded Windows QMT code, the resource response
classification consumed by acquisition workers, and shared scheduler defaults.
Deploy only merged main through the existing coordinated release controller.
Historical run-evidence TEXT capacity and source-empty old partitions remain
separate outstanding issues outside the user's narrowed collection scope.
