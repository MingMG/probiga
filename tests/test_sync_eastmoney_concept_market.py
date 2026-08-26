from __future__ import annotations

from datetime import datetime
import hashlib
import json

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from tools import sync_eastmoney_concept_market as market


TARGET = "2026-08-26"
SHANGHAI_CLOSE = datetime(2026, 8, 26, 15, 30, tzinfo=market.SHANGHAI)
RUN_TIME = datetime(2026, 8, 26, 19, 0, tzinfo=market.SHANGHAI)


def _item(code: str, *, source_time: datetime = SHANGHAI_CLOSE) -> dict:
    return {
        "f2": 101.2,
        "f3": 1.1,
        "f4": 1.2,
        "f5": 1000,
        "f6": 2000,
        "f12": code,
        "f14": code,
        "f15": 103.0,
        "f16": 99.0,
        "f17": 100.0,
        "f124": int(source_time.timestamp()),
    }


def _daily_line(day: str) -> str:
    return f"{day},100,101,103,99,1000,2000,3,1,1,0"


def _minute_lines(day: str) -> list[str]:
    return [
        f"{moment:%Y-%m-%d %H:%M},100,101,103,99,10,20,3,1,1,0"
        for moment in market._minute_grid(day)
    ]


class _Provider:
    def __init__(
        self,
        items: list[dict],
        *,
        missing_daily: set[str] | None = None,
        mixed_minute_code: str = "",
    ) -> None:
        self.items = list(items)
        self.missing_daily = set(missing_daily or ())
        self.mixed_minute_code = mixed_minute_code

    def fetch_directory_page(self, page: int, page_size: int) -> dict:
        start = (page - 1) * page_size
        return {
            "data": {
                "total": len(self.items),
                "diff": self.items[start : start + page_size],
            }
        }

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> dict:
        del start_date
        return {
            "data": {
                "code": code,
                "klines": [] if code in self.missing_daily else [_daily_line(end_date)],
            }
        }

    def fetch_minute(self, code: str) -> dict:
        day = "2026-08-25" if code == self.mixed_minute_code else TARGET
        return {"data": {"code": code, "klines": _minute_lines(day)}}


@pytest.fixture
def calendar_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date TEXT PRIMARY KEY, trade_status INTEGER NOT NULL)")
        )
        connection.execute(
            text("INSERT INTO si_trade_calendar(trade_date,trade_status) VALUES (:d,1)"),
            {"d": TARGET},
        )
    return engine


def test_empty_directory_is_blocked_before_any_publish(monkeypatch, calendar_engine):
    publish = lambda *_args, **_kwargs: pytest.fail("empty directory must not publish")
    monkeypatch.setattr(market, "publish_frames_atomically", publish)

    with pytest.raises(market.DataBlocked, match="implausibly small"):
        market.run_publisher(
            calendar_engine,
            _Provider([]),
            datasets=["current"],
            trade_date=TARGET,
            now=RUN_TIME,
        )


def test_partial_daily_code_coverage_is_blocked_without_dml(monkeypatch, calendar_engine):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    publish_called = False

    def publish(*_args, **_kwargs):
        nonlocal publish_called
        publish_called = True

    monkeypatch.setattr(market, "publish_frames_atomically", publish)
    provider = _Provider(
        [_item("BK0001"), _item("BK0002")],
        missing_daily={"BK0002"},
    )

    with pytest.raises(market.DataBlocked, match="daily code coverage is partial"):
        market.run_publisher(
            calendar_engine,
            provider,
            datasets=["kline"],
            trade_date=TARGET,
            now=RUN_TIME,
            workers=2,
        )

    assert publish_called is False


def test_mixed_provider_dates_are_blocked_without_dml(monkeypatch, calendar_engine):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    previous = datetime(2026, 8, 25, 15, 30, tzinfo=market.SHANGHAI)
    provider = _Provider([_item("BK0001"), _item("BK0002", source_time=previous)])
    publish_called = False

    def publish(*_args, **_kwargs):
        nonlocal publish_called
        publish_called = True

    monkeypatch.setattr(market, "publish_frames_atomically", publish)

    with pytest.raises(market.DataBlocked, match="not an exact target-date inventory"):
        market.run_publisher(
            calendar_engine,
            provider,
            datasets=["current"],
            trade_date=TARGET,
            now=RUN_TIME,
        )

    assert publish_called is False


def test_complete_current_daily_and_minute_have_exact_matrices(monkeypatch, calendar_engine):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    result = market.run_publisher(
        calendar_engine,
        _Provider([_item("BK0001"), _item("BK0002")]),
        datasets=["all"],
        trade_date=TARGET,
        now=RUN_TIME,
        workers=2,
        dry_run=True,
    )

    assert result["published"] is False
    assert result["directory"]["observed_count"] == 2
    assert result["dataset_results"]["current"]["row_count"] == 2
    assert result["dataset_results"]["kline"]["row_count"] == 2
    assert result["dataset_results"]["minute"]["row_count"] == 480
    assert result["dataset_results"]["minute"]["date_count"] == 1


def _atomic_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE sm_concept_east_current ("
                "index_code TEXT PRIMARY KEY, trade_time DATETIME NOT NULL, trade_date TEXT NOT NULL, "
                "open REAL NOT NULL, price REAL NOT NULL CHECK(price>0), high REAL NOT NULL, low REAL NOT NULL, "
                "volume REAL NOT NULL, amount REAL NOT NULL, change REAL NOT NULL, change_pct REAL NOT NULL, "
                "snapshot_at DATETIME NOT NULL, etl_sync_at DATETIME NOT NULL)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE sm_concept_east_kline ("
                "index_code TEXT NOT NULL, trade_time DATETIME NOT NULL, trade_date TEXT NOT NULL, "
                "k_type INTEGER NOT NULL, open REAL NOT NULL, close REAL NOT NULL CHECK(close>0), "
                "high REAL NOT NULL, low REAL NOT NULL, volume REAL NOT NULL, amount REAL NOT NULL, "
                "change REAL NOT NULL, change_pct REAL NOT NULL, etl_sync_at DATETIME NOT NULL, "
                "PRIMARY KEY(index_code,trade_date,k_type))"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sm_concept_east_current VALUES "
                "('BKOLD','2026-08-26 15:00:00',:d,1,1,1,1,1,1,0,0,"
                "'2026-08-26 15:00:00','2026-08-26 15:01:00')"
            ),
            {"d": TARGET},
        )
        connection.execute(
            text(
                "INSERT INTO sm_concept_east_kline VALUES "
                "('BKOLD','2026-08-26 00:00:00',:d,1,1,1,1,1,1,1,0,0,"
                "'2026-08-26 15:01:00')"
            ),
            {"d": TARGET},
        )
    return engine


def test_second_dataset_insert_failure_rolls_back_every_scope():
    engine = _atomic_engine()
    timestamp = datetime(2026, 8, 26, 15, 30)
    current = pd.DataFrame(
        [
            {
                "index_code": "BKNEW",
                "trade_time": timestamp,
                "trade_date": TARGET,
                "open": 1,
                "price": 1,
                "high": 1,
                "low": 1,
                "volume": 1,
                "amount": 1,
                "change": 0,
                "change_pct": 0,
                "snapshot_at": timestamp,
                "etl_sync_at": timestamp,
            }
        ]
    )
    invalid_kline = pd.DataFrame(
        [
            {
                "index_code": "BKNEW",
                "trade_time": datetime(2026, 8, 26),
                "trade_date": TARGET,
                "k_type": 1,
                "open": 1,
                "close": -1,
                "high": 1,
                "low": 1,
                "volume": 1,
                "amount": 1,
                "change": 0,
                "change_pct": 0,
                "etl_sync_at": timestamp,
            }
        ]
    )

    with pytest.raises(Exception):
        market.publish_frames_atomically(
            engine,
            {"current": current, "kline": invalid_kline},
            start_date=TARGET,
            end_date=TARGET,
            use_mysql_lock=False,
        )

    with engine.connect() as connection:
        current_codes = connection.execute(
            text("SELECT index_code FROM sm_concept_east_current")
        ).scalars().all()
        kline_codes = connection.execute(
            text("SELECT index_code FROM sm_concept_east_kline")
        ).scalars().all()
    assert current_codes == ["BKOLD"]
    assert kline_codes == ["BKOLD"]


def test_cli_emits_one_recomputable_receipt(monkeypatch, capsys, calendar_engine):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    monkeypatch.setattr(
        market,
        "authoritative_closed_trade_date",
        lambda _engine, now=None: TARGET,
    )
    provider = _Provider([_item("BK0001"), _item("BK0002")])

    exit_code = market.main(
        ["--dataset", "current", "--trade-date", TARGET, "--dry-run", "--json"],
        engine_factory=lambda: calendar_engine,
        provider_factory=lambda: provider,
    )

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert exit_code == 0
    assert len(lines) == 1
    receipt = json.loads(lines[0])
    assert receipt["schema"] == market.RECEIPT_SCHEMA
    assert receipt["status"] == "PASS"
    assert receipt["requested_trade_date"] == TARGET
    assert receipt["target_trade_date"] == TARGET
    assert receipt["directory_count"] == 2
    assert receipt["dataset_results"]["current"]["row_count"] == 2
    unsigned = dict(receipt)
    supplied = unsigned.pop("result_sha256")
    assert supplied == hashlib.sha256(
        json.dumps(
            unsigned,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def test_blocked_cli_receipt_keeps_target_and_directory_counts(
    monkeypatch, capsys, calendar_engine
):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    monkeypatch.setattr(
        market,
        "authoritative_closed_trade_date",
        lambda _engine, now=None: TARGET,
    )
    provider = _Provider(
        [_item("BK0001"), _item("BK0002")],
        missing_daily={"BK0002"},
    )

    exit_code = market.main(
        ["--dataset", "kline", "--trade-date", TARGET, "--json"],
        engine_factory=lambda: calendar_engine,
        provider_factory=lambda: provider,
    )

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert exit_code == 2
    assert len(lines) == 1
    receipt = json.loads(lines[0])
    assert receipt["status"] == "DATA_BLOCKED"
    assert receipt["requested_trade_date"] == TARGET
    assert receipt["target_trade_date"] == TARGET
    assert receipt["directory_count"] == 2
    assert receipt["dataset_results"] == {}
    assert "daily code coverage is partial" in receipt["reason"]
