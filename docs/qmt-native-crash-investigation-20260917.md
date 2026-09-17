# QMT native crash investigation (2026-09-17)

Release boundary: Windows/QMT only. The changed runtime file is
`integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py`. Request/response
formats, data provenance, database schemas, and Windows/Linux scheduler ownership
are unchanged. No Linux deployment is required.

## Evidence

The working directory was on an older development branch; the registered Windows
production checkout was at `60a6149`. The running strategy source was blob
`ecad415e77aae5498b577f1ece48ece843b7afe3`, introduced by `a9841aa`.
The initial bridge in `dec494a` and the actual production source were compared.

Read-only parsing of the client's existing minidumps found access violations
(`0xc0000005`), including:

| Local time | Dump | Module and relative address |
| --- | --- | --- |
| Sep 17 09:46:10 | adc89290-a985-4f5c-9551-23c4b7909dde | python36.dll + 0x1aa886 |
| Sep 17 00:24:50 | 55b0274f-b33b-4b4f-8f98-71a10930add3 | python36.dll + 0x1b0aaa |
| Sep 16 11:24:23 | eba32766-3519-41e7-9171-ea0b66f5869a | python36.dll + 0x1b092a |
| Sep 16 03:57:39 | 399cf7ca-72d9-4258-92eb-669821dec0b0 | python36.dll + 0x118283 |

The first address is inside `_PyEval_EvalFrameDefault`; its failing instruction
decrements an object reference through a null pointer. These are native runtime
faults, not catchable Python exceptions. Failures also occurred outside the live
quote subscription window. Stack memory includes embedded Python calls from
`se_base.dll`; scanning stack values is not a symbolized stack unwind and does
not prove which application operation originally corrupted the object.

The existing logs also show strategy termination interrupting `_download_history`
inside a `bridge_tick` invocation. The bridge had an uncancellable periodic
`run_time` timer, a second acquisition entry in `after_init`, a blocking shutdown
lock, and no stopped-state fence against subsequent timer/quote callbacks.

## Final lifecycle

Use QMT's supported `schedule_run`/`cancel_schedule_run` APIs, verified in the
installed `_PyContextInfo.py` and the vendor's
[system function documentation](https://dict.thinktrader.net/innerApi/system_function.html).
One cancellable, one-shot task is armed one second after the previous pass
finishes. No periodic timer queues work during a long native call. `after_init`
does not perform acquisition. Generation checks reject duplicate or late timer
deliveries. Stop fences callbacks first, cancels outstanding work, and never
waits on an active execution lock. An active pass completes cleanup on exit and
does not launch another acquisition phase.

`native_fault.log` retains Python fatal-error stacks in the bridge directory.
The descriptor lives as long as the embedded interpreter, including after stop,
so an interpreter teardown fault can still be recorded.

## Validation limits

Regression tests cover slow work, duplicate/late callbacks, cancellation,
reentrant shutdown, publication exceptions, and existing bridge contracts.
They establish the lifecycle guarantees; they cannot reproduce or prove the
absence of corruption inside the closed-source QMT runtime. Timer/lifecycle
changes remove concrete unsafe execution paths. Attribution of all historical
native faults to those paths remains a hypothesis until native reproduction or
longer live operation supports it. A short post-release observation outside
market hours cannot establish full trading-session stability.
