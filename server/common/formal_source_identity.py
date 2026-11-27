"""Content identities shared by frozen research inputs and durable checkpoints."""

from hashlib import sha256
from pathlib import Path


# Exact transport-only revisions proved equivalent to the existing frozen input
# contract. Any other bytes receive a new identity and cannot resume that input.
_EQUIVALENT = {
    "historical_announcement_read.py": (
        "c9332bfd88a3b9b312a70e37d4fb1bc0beb18a7dfdf8ecd327a9c85c21c37288",
        "6da2e215759ddbd06a66727b138eeeda6a200454fa29b8136ab88321e07c6941",
    ),
    "daily_features.py": (
        "7f79d5b2773f6e6e2bf5da8d051c7b5e5c9f438fadf2a9a2d2c86637d15280bf",
        "2778aecde7730763b9e6decf07ff3785e43b515865f7e1f7e67633902863e3a2",
    ),
    "pit_facts.py": (
        "aeaf9ae3d31145a4e19fb8b86202b1f16d492ca0fe6512b52a0300a847493256",
        "9cc2a928536be3f6ed84e9bd2f16e947129fc5a4d78d85b9c5a06b417c88a4b8",
    ),
    "pit_sealed_event_read.py": (
        "89e28342a5a96e61117de5a8822c91e22904c79ed7e930d4681b81e9f8ad0727",
        "e5f167192421518bfd7b9d97f4a0a83ee431fb9dc01c53c6f319dd65bfdaca2b",
    ),
    "strategy_native_fact_snapshot.py": (
        "18e4073cb6bebd005fa52ee9627c55177fd41053f63d09b1d3530e6afd927745",
        "716a80cdfb892a48512f180cae70df8486fae6a8c72e23a8096105f3b404f5c3",
    ),
    "strategy_price_backtest.py": (
        "a05997f078b78edd49e3b02d5905ec955a0116beee5bb3d27bb6979c5267c07b",
        "9980a368af8efb32c762a05a025c9a82477b9b6997cbaff2956fd014b55a403d",
    ),
}
_BOUNDED_READ_SHA256 = "6cedb9abadc5550d72a1e56e1c9c8dd6314f1024094e31be91f942a1abfcc067"


def _source_sha256(path: Path) -> str:
    return sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def frozen_source_sha256(path: Path) -> str:
    observed = _source_sha256(path)
    new, frozen = _EQUIVALENT.get(path.name, ("", ""))
    if path.name in ("pit_facts.py", "daily_features.py"):
        transport = _source_sha256(Path(__file__).with_name("bounded_read.py"))
        if observed == new and transport == _BOUNDED_READ_SHA256:
            return frozen
        return sha256((observed + transport).encode("ascii")).hexdigest()
    if path.name == "strategy_price_backtest.py" and observed == new:
        transport = _source_sha256(Path(__file__).with_name("bounded_read.py"))
        return frozen if transport == _BOUNDED_READ_SHA256 else sha256((observed + transport).encode("ascii")).hexdigest()
    return frozen if observed == new else observed
