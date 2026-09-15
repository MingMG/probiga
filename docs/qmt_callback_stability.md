# QMT callback stability

## Failure evidence (2026-09-15)

The latest 15 QMT minidumps all report access violation `0xc0000005`;
14 exception addresses are in the bundled Python 3.6.8 runtime. Repeated
offsets include `python36.dll+0x1aa697` and `+0x1aa886`. Several failures
leave a partially written full-market snapshot. These facts locate activity
around the failures, but do not establish that the interpreter itself is the
origin of the memory corruption.

Independent reproduction of the producer's locking shows a definite liveness
defect: when a native request waits for a callback on another thread, the
callback cannot acquire the lock held by `bridge_tick`. The old callback also
serializes, flushes and retries file replacement on the native quote thread.

## Execution model

- The quote callback normalizes and replaces in-memory rows under a short
  ingress lock. It never publishes files or waits for file replacement.
- A single timer owns requests, subscription changes, publication and direct
  acquisition. Overlapping or reentrant timer calls return immediately.
- Native QMT calls and disk operations never hold the ingress lock.
- Publication copies the row mapping under the ingress lock, then releases
  it. Normalized rows are replaced, never mutated by later callbacks, so
  a published snapshot remains consistent while new quotes arrive.
- JSON is encoded once, then written in one operation. Atomic replacement,
  flush/fsync, schema versions, native callback receipt times, and release
  identity validation are preserved.
- Direct acquisition still gets polled if quote publication fails. The
  superseded second timer entry point is removed.

## Verification

`tests/test_bigqmt_callback_liveness.py` exercises a callback while a native
request waits, callback delivery during slow publication, immutable published
rows, reentrant timer calls, error recovery, and callbacks during unsubscribe.
The original implementation fails the first three regression scenarios.

The bundled QMT Python 3.6.8 interpreter also completed 100 atomic publications
of the current 5,563-symbol snapshot with equal decoded contents. Five encoding
iterations averaged 247 ms with `json.dump` and 49 ms with `json.dumps` on this
machine. This is an isolated runtime test, not proof of stability inside the
QMT host or during live market traffic.

## Release and acceptance

The implementation runs on Windows/QMT. Deployment requires coordinated
release because `validate_strategy_release_payload` on consumers verifies the
strategy Git blob/source hash against their application revision; installing
only the changed producer would fail that existing identity contract. No
identity checks may be bypassed.

Acceptance requires the merged-main strategy identity, a logged-in QMT
instance, fresh bridge/full snapshots and consumer receipts, and subsequent
market-hours observation. Do not report all native crashes fixed solely from
unit tests or an isolated Python process.
