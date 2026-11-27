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


def test_single_id_timeout_logs_the_exact_failed_id(caplog):
    def fetch(batch):
        assert batch == ["revision-17"]
        raise TimeoutError("server reply timeout")

    with pytest.raises(TimeoutError, match="server reply timeout"):
        read_id_batches(["revision-17"], fetch, deadline=monotonic() + 5,
                        retryable=lambda exc: isinstance(exc, TimeoutError),
                        logger=logging.getLogger(__name__), stage="revision")
    assert "revision batch=1" in caplog.text and "failed_ids=['revision-17']" in caplog.text
    assert "outcome=split" not in caplog.text


def test_frozen_checkpoint_identity_requires_exact_transport_source(tmp_path):
    root = Path(__file__).resolve().parents[1]
    path = root / "server/common/pit_facts.py"
    prior = "9cc2a928536be3f6ed84e9bd2f16e947129fc5a4d78d85b9c5a06b417c88a4b8"
    assert frozen_source_sha256(path) == prior
    edited = tmp_path / path.name
    edited.write_bytes(path.read_bytes() + b"\n# unrelated change\n")
    assert frozen_source_sha256(edited) != prior


def test_concept_transport_preserves_frozen_input_identity_only_for_reviewed_bytes(tmp_path):
    from server.common.formal_source_identity import _source_sha256
    path = Path(__file__).resolve().parents[1] / 'server/trading_v3/daily_features.py'
    frozen = '2778aecde7730763b9e6decf07ff3785e43b515865f7e1f7e67633902863e3a2'
    assert frozen_source_sha256(path) == frozen
    edited = tmp_path / path.name
    edited.write_bytes(path.read_bytes() + b'\n# unrelated edit\n')
    assert frozen_source_sha256(edited) != frozen
