"""A segment's closing print must stay publishable through its quiet interval.

Production 2026-10-13: from 15:02:16 to 15:10:59 every poll produced a
``data_quality_block`` event with ``full_transport_received: 5576`` but
``full_received: 0``, ``full_invalid_count: 5576`` and ``full_coverage: 0.0``.
The transport delivered the whole universe and every row was dropped: the
envelope was fresh, but the exchange's last tick is frozen at the 15:00:00
close, and native-event age was measured against the wall clock. Measured that
way a closing print is stale exactly ``max_event_age_seconds`` after it prints,
so the interval rejects the only print it has.

That morning's closing print survived by luck, not by design: the bridge polled
at 15:00:40 and 15:01:52, inside the 120s limit, and published the 15:00:00 rows
under batch ``bigqmt_full_20261013150150``. Any day whose polls miss that window
loses the close, which is why the limit -- not the polling cadence -- is the
defect. The same gate collides with every quiet interval of the session, so the
midday break (receipts stop 11:31:38, resume 13:00:08, on 5 of 5 sampled trading
days) and the post-auction lull (09:26:56 to 09:30:08) fail the same way.
"""

import time as _real_time
from datetime import datetime, timedelta

import pytest

from integrations.bigqmt import bridge
from integrations.bigqmt.bridge import (
    SNAPSHOT_ACQUISITION_METHOD,
    SNAPSHOT_ACQUISITION_MODE,
    SNAPSHOT_ACQUISITION_PROTOCOL,
    session_freshness_reference,
)
from integrations.bigqmt.spool import PROVIDER_ID
from tools import run_big_qmt_bridge as consumer

TRADE_DAY = "2026-10-13"
# Each segment's closing print, and production's first blocked poll after it.
# The gate expires at close+120s, so a poll one cadence later blocks.
AFTERNOON_CLOSE_AT = datetime(2026, 10, 13, 15, 0, 0)
MORNING_CLOSE_AT = datetime(2026, 10, 13, 11, 30, 0)
AUCTION_MATCH_AT = datetime(2026, 10, 13, 9, 25, 0)
TAIL_BLOCKED_AT = datetime(2026, 10, 13, 15, 2, 16)
LUNCH_BLOCKED_AT = datetime(2026, 10, 13, 11, 32, 16)
LUNCH_RESUMED_AT = datetime(2026, 10, 13, 13, 0, 8)
PREOPEN_BLOCKED_AT = datetime(2026, 10, 13, 9, 27, 16)
AUCTION_AT = datetime(2026, 10, 13, 9, 20, 0)


class _FrozenClock:
    """Freeze the wall clock at one instant; leave monotonic live."""

    def __init__(self, moment: datetime):
        self._epoch = moment.timestamp()

    def time(self) -> float:
        return self._epoch

    def monotonic(self) -> float:
        return _real_time.monotonic()


def _payload(count: int, print_at: datetime, published_at: datetime) -> dict:
    """A full envelope republishing one frozen print at the poll instant.

    ``_probiga_observed_at`` stays at the print: the strategy retains a quote's
    first observation for as long as its native tick is unchanged, so only the
    envelope's publication time advances across a quiet interval.
    """

    quote = {
        "lastPrice": 10.5,
        "lastClose": 10.0,
        "volume": 100,
        "amount": 105000,
        "time": int(print_at.timestamp() * 1000),
        "_probiga_observed_at": print_at.isoformat(),
        "_probiga_acquisition_method": SNAPSHOT_ACQUISITION_METHOD,
    }
    return {
        "source": PROVIDER_ID,
        "quote_acquisition_protocol": SNAPSHOT_ACQUISITION_PROTOCOL,
        "quote_acquisition_mode": SNAPSHOT_ACQUISITION_MODE,
        "generated_ts": published_at.timestamp(),
        "quotes": {f"{index:06d}.SZ": dict(quote) for index in range(1, count + 1)},
    }


def _prepare_ingest(monkeypatch, payload, now: datetime):
    monkeypatch.setattr(consumer, "_snapshot_freshness_required", lambda _engine: True)
    monkeypatch.setattr(consumer, "read_json", lambda _path: {})
    monkeypatch.setattr(consumer, "_read_snapshot_if_changed", lambda kind, **kw:
                        (payload, "full-file") if kind == "full" else ({}, ""))
    monkeypatch.setattr(consumer, "_write_status", lambda *args, **kw: None)
    monkeypatch.setattr(consumer, "_table_exists", lambda *args: False)
    monkeypatch.setattr(bridge, "time", _FrozenClock(now))
    # Freeze the wall clock the consumer reads through datetime.now() too, so the
    # real session anchor is exercised rather than stubbed out.
    monkeypatch.setattr(consumer, "datetime", _frozen_datetime_class(now))
    monkeypatch.setenv("BIG_QMT_MIN_FULL_COVERAGE", "0.95")
    monkeypatch.setenv("BIG_QMT_MAX_UNPRICED_RATIO", "0.10")


def _frozen_datetime_class(now: datetime):
    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    return _FrozenDatetime


def _ingest(monkeypatch, tmp_path, payload, now: datetime, *, publishes: bool):
    _prepare_ingest(monkeypatch, payload, now)
    if publishes:
        monkeypatch.setattr(consumer, "_replace_full_snapshot", lambda _e, frame: len(frame))
        receipts: list[dict] = []
        monkeypatch.setattr(consumer, "_record_realtime_sync_receipt",
                            lambda _engine, **kw: receipts.append(kw) or {"quality_status": "PASS"})
    else:
        receipts = []
        for writer in ("_replace_full_snapshot", "_replace_tracked_subset",
                       "_record_realtime_sync_receipt"):
            monkeypatch.setattr(consumer, writer,
                                lambda *args, **kw: pytest.fail("collapsed snapshot reached a writer"))
    return consumer.ingest_once(
        object(), qmt_home=tmp_path,
        universe=[f"{index:06d}" for index in range(1, 11)],
        tracked=[], short_name_map={},
    ), receipts


@pytest.mark.parametrize("moment, expected", [
    # While the exchange can print, staleness is still measured against the clock.
    (datetime(2026, 10, 13, 10, 30, 0), datetime(2026, 10, 13, 10, 30, 0)),
    (datetime(2026, 10, 13, 14, 0, 0), datetime(2026, 10, 13, 14, 0, 0)),
    # The opening call auction is an active printing phase: it is not anchored,
    # so a frozen auction value stays a real staleness signal.
    (AUCTION_AT, AUCTION_AT),
    (datetime(2026, 10, 13, 9, 16, 58), datetime(2026, 10, 13, 9, 16, 58)),
    # The opening auction has matched and continuous trading has not opened.
    (AUCTION_MATCH_AT, AUCTION_MATCH_AT),
    (PREOPEN_BLOCKED_AT, AUCTION_MATCH_AT),
    (datetime(2026, 10, 13, 9, 29, 59), AUCTION_MATCH_AT),
    (datetime(2026, 10, 13, 9, 30, 0), datetime(2026, 10, 13, 9, 30, 0)),
    # The midday break holds the morning closing print.
    (MORNING_CLOSE_AT, MORNING_CLOSE_AT),
    (datetime(2026, 10, 13, 11, 31, 38), MORNING_CLOSE_AT),
    (LUNCH_BLOCKED_AT, MORNING_CLOSE_AT),
    (datetime(2026, 10, 13, 12, 0, 0), MORNING_CLOSE_AT),
    (datetime(2026, 10, 13, 12, 59, 59), MORNING_CLOSE_AT),
    (LUNCH_RESUMED_AT, datetime(2026, 10, 13, 13, 0, 8)),
    # The post-close tail holds the afternoon closing print. Against the wall
    # clock the close is still accepted at 15:02:00 and stale from 15:02:01;
    # production's first blocked poll was 15:02:16.
    (AFTERNOON_CLOSE_AT, AFTERNOON_CLOSE_AT),
    (datetime(2026, 10, 13, 15, 2, 0), AFTERNOON_CLOSE_AT),
    (datetime(2026, 10, 13, 15, 2, 1), AFTERNOON_CLOSE_AT),
    (TAIL_BLOCKED_AT, AFTERNOON_CLOSE_AT),
    (datetime(2026, 10, 13, 15, 10, 59), AFTERNOON_CLOSE_AT),
    # Past the freshness window the caller requires no freshness at all.
    (datetime(2026, 10, 13, 15, 11, 0), datetime(2026, 10, 13, 15, 11, 0)),
    (datetime(2026, 10, 13, 22, 0, 0), datetime(2026, 10, 13, 22, 0, 0)),
])
def test_session_freshness_anchors_to_the_last_segment_close(moment, expected):
    assert session_freshness_reference(moment) == expected


@pytest.mark.parametrize("print_at, now, label", [
    (AFTERNOON_CLOSE_AT, TAIL_BLOCKED_AT, "post-close tail"),
    (MORNING_CLOSE_AT, LUNCH_BLOCKED_AT, "midday break"),
    (AUCTION_MATCH_AT, PREOPEN_BLOCKED_AT, "post-auction lull"),
])
def test_quiet_interval_closing_print_publishes(monkeypatch, tmp_path, print_at, now, label):
    """The regression: each quiet interval must carry its segment's closing print."""

    payload = _payload(10, print_at, now - timedelta(seconds=1))
    result, receipts = _ingest(monkeypatch, tmp_path, payload, now, publishes=True)

    assert result["full_rows"] == 10, label
    assert result["full_coverage"] == 1.0, label
    assert result["full_invalid_count"] == 0, label
    assert receipts[0]["expected_count"] == receipts[0]["observed_count"] == 10, label


@pytest.mark.parametrize("print_at, now, label", [
    (AFTERNOON_CLOSE_AT, TAIL_BLOCKED_AT, "post-close tail"),
    (MORNING_CLOSE_AT, LUNCH_BLOCKED_AT, "midday break"),
    (AUCTION_MATCH_AT, PREOPEN_BLOCKED_AT, "post-auction lull"),
])
def test_quiet_interval_closing_print_collapses_against_the_wall_clock(
    monkeypatch, tmp_path, print_at, now, label
):
    """The production failure signature: a fresh envelope that delivers nothing."""

    payload = _payload(10, print_at, now - timedelta(seconds=1))
    # The pre-fix reference: whatever instant the wall clock reads.
    _prepare_ingest(monkeypatch, payload, now)
    monkeypatch.setattr(consumer, "_snapshot_freshness_reference", lambda **kw: now)
    for writer in ("_replace_full_snapshot", "_replace_tracked_subset",
                   "_record_realtime_sync_receipt"):
        monkeypatch.setattr(consumer, writer,
                            lambda *args, **kw: pytest.fail("collapsed snapshot reached a writer"))

    with pytest.raises(consumer.BigQmtDataQualityError) as caught:
        consumer.ingest_once(
            object(), qmt_home=tmp_path,
            universe=[f"{index:06d}" for index in range(1, 11)],
            tracked=[], short_name_map={},
        )

    details = caught.value.details
    assert details["full_transport_received"] == 10, label
    assert details["full_received"] == 0, label
    assert details["full_invalid_count"] == 10, label
    assert details["full_coverage"] == 0.0, label


def test_opening_auction_frozen_value_is_still_rejected(monkeypatch, tmp_path):
    """The auction is a live printing phase, so its freeze stays a real signal.

    Anchoring every quiet-looking stretch would silence this; the exchange can
    print during the call auction, so a value frozen from the auction's start is
    genuinely stale and must not become publishable.
    """

    payload = _payload(10, datetime(2026, 10, 13, 9, 15, 0), AUCTION_AT - timedelta(seconds=1))
    with pytest.raises(consumer.BigQmtDataQualityError) as caught:
        _ingest(monkeypatch, tmp_path, payload, AUCTION_AT, publishes=False)

    assert caught.value.details["full_received"] == 0


def test_quiet_interval_still_rejects_a_genuinely_stale_print(monkeypatch, tmp_path):
    """Anchoring to the close must not turn a quiet interval into accept-anything."""

    payload = _payload(10, AFTERNOON_CLOSE_AT - timedelta(hours=1), TAIL_BLOCKED_AT - timedelta(seconds=1))
    with pytest.raises(consumer.BigQmtDataQualityError) as caught:
        _ingest(monkeypatch, tmp_path, payload, TAIL_BLOCKED_AT, publishes=False)

    assert caught.value.details["full_received"] == 0


def test_quiet_interval_rejects_an_event_from_the_future(monkeypatch, tmp_path):
    """Anchoring to the close must not admit an event the clock has not reached."""

    payload = _payload(10, TAIL_BLOCKED_AT + timedelta(minutes=5), TAIL_BLOCKED_AT - timedelta(seconds=1))
    with pytest.raises(consumer.BigQmtDataQualityError) as caught:
        _ingest(monkeypatch, tmp_path, payload, TAIL_BLOCKED_AT, publishes=False)

    assert caught.value.details["full_received"] == 0


def test_quiet_interval_accepts_a_print_after_the_segment_close(monkeypatch, tmp_path):
    """A print after the anchor is newer than it, not stale against it."""

    payload = _payload(10, AFTERNOON_CLOSE_AT + timedelta(seconds=3), TAIL_BLOCKED_AT - timedelta(seconds=1))
    result, _receipts = _ingest(monkeypatch, tmp_path, payload, TAIL_BLOCKED_AT, publishes=True)

    assert result["full_rows"] == 10
