import logging
from pathlib import Path
from time import monotonic

import pytest

from server.common.bounded_read import read_id_batches
from server.common.formal_source_identity import frozen_source_sha256
from server.common.qmt_announcement_pit import (
    LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE,
    QMTAnnouncementBlocked,
)


@pytest.mark.parametrize("failed_sizes", [{160}, {160, 80}])
def test_reply_timeout_splits_and_reads_both_halves(failed_sizes, caplog):
    ids = list(range(321))

    def fetch(batch):
        if len(batch) in failed_sizes:
            raise QMTAnnouncementBlocked(LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE, "revision")
        return list(batch)

    with caplog.at_level(logging.INFO):
        result = read_id_batches(ids, fetch, size=160, deadline=monotonic() + 5,
                                 retryable=lambda exc: isinstance(exc, QMTAnnouncementBlocked),
                                 logger=logging.getLogger(__name__), stage="revision")
    assert result == ids
    assert "outcome=split" in caplog.text
    assert "rows=1" in caplog.text


def test_timeout_exhaustion_never_returns_incomplete_results():
    def fetch(batch):
        if 162 in batch:
            raise TimeoutError("reply")
        return list(batch)

    with pytest.raises(TimeoutError, match="reply"):
        read_id_batches(list(range(200)), fetch, size=160, deadline=monotonic() + 5,
                        retryable=lambda exc: isinstance(exc, TimeoutError),
                        logger=logging.getLogger(__name__), stage="revision")


def test_frozen_checkpoint_identity_requires_exact_transport_source(tmp_path):
    root = Path(__file__).resolve().parents[1]
    path = root / "server/common/pit_facts.py"
    prior = "9cc2a928536be3f6ed84e9bd2f16e947129fc5a4d78d85b9c5a06b417c88a4b8"
    assert frozen_source_sha256(path) == prior
    edited = tmp_path / path.name
    edited.write_bytes(path.read_bytes() + b"\n# unrelated change\n")
    assert frozen_source_sha256(edited) != prior
