"""Content identities shared by frozen research inputs and durable checkpoints."""

from hashlib import sha256
from pathlib import Path


# Exact transport-only revisions proved equivalent to the existing frozen input
# contract. Any other bytes receive a new identity and cannot resume that input.
_EQUIVALENT = {
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
}
_BOUNDED_READ_SHA256 = "8c6deed0e7e1ae758588f938b72a712c884e8ee3983259851046297e81a1617c"


def _source_sha256(path: Path) -> str:
    return sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def frozen_source_sha256(path: Path) -> str:
    observed = _source_sha256(path)
    new, frozen = _EQUIVALENT.get(path.name, ("", ""))
    if path.name == "pit_facts.py":
        transport = _source_sha256(Path(__file__).with_name("bounded_read.py"))
        if observed == new and transport == _BOUNDED_READ_SHA256:
            return frozen
        return sha256((observed + transport).encode("ascii")).hexdigest()
    return frozen if observed == new else observed
