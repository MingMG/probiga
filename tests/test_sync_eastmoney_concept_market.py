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
def calendar_engine(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    monkeypatch.setattr(market, "get_kline_engine", lambda: engine)
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


def test_partial_native_daily_fields_are_blocked_without_dml(monkeypatch, calendar_engine):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    publish_called = False

    def publish(*_args, **_kwargs):
        nonlocal publish_called
        publish_called = True

    monkeypatch.setattr(market, "publish_frames_atomically", publish)
    provider = _Provider(
        [_item("BK0001"), _item("BK0002")],
    )
    provider.items[1]["f15"] = None

    with pytest.raises(market.DataBlocked, match="BK0002.high is not numeric"):
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
    )
    provider.items[1]["f15"] = None

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
    assert "BK0002.high is not numeric" in receipt["reason"]


def test_native_closed_daily_is_exact_quote_projection_without_history_requests(monkeypatch):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    provider = _Provider([_item("BK0001"), _item("BK0002")])
    provider.fetch_daily = lambda *_args: pytest.fail("native closed-day collection does not request history")
    snapshot = market.fetch_complete_directory(provider)
    frame = market.collect_daily_frame(
        provider, snapshot, start_date=TARGET, end_date=TARGET,
        expected_dates=[TARGET], ingested_at=RUN_TIME.replace(tzinfo=None), workers=2,
    )
    assert len(frame) == 2
    for column, field in market.CLOSED_DAILY_FIELD_MAP.items():
        if column in {"index_code", "source_time"}:
            continue
        assert frame.iloc[0][column] == provider.items[0][field]
    result = market._dataset_evidence(frame, "kline")
    assert result["source_url"] == market.DIRECTORY_URL
    assert result["source_evidence"]["directory_manifest_sha256"] == snapshot.evidence["manifest_sha256"]
    assert result["source_evidence"]["raw_quote_rows_sha256"] == market._digest(list(snapshot.items))


@pytest.mark.parametrize("case", ["historical_date", "intraday", "future", "ohlc"])
def test_native_daily_never_relabels_other_date_or_unclosed_quote(monkeypatch, case):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    items = [_item("BK0001"), _item("BK0002")]
    target = TARGET
    if case == "historical_date":
        target = "2026-08-25"
    elif case == "intraday":
        items[0]["f124"] = int(SHANGHAI_CLOSE.replace(hour=14).timestamp())
    elif case == "future":
        items[0]["f124"] = int(SHANGHAI_CLOSE.replace(hour=20).timestamp())
    else:
        items[0]["f15"] = 100
    snapshot = market.fetch_complete_directory(_Provider(items))
    with pytest.raises(market.DataBlocked):
        market.build_closed_daily_frame(snapshot, target_date=target, ingested_at=RUN_TIME.replace(tzinfo=None))


def test_historical_range_requires_actual_history_rows(monkeypatch):
    monkeypatch.setattr(market, "MIN_DIRECTORY_CODES", 2)
    provider = _Provider([_item("BK0001"), _item("BK0002")], missing_daily={"BK0002"})
    snapshot = market.fetch_complete_directory(provider)
    with pytest.raises(market.DataBlocked, match="daily code coverage is partial"):
        market.collect_daily_frame(
            provider, snapshot, start_date="2026-08-25", end_date=TARGET,
            expected_dates=["2026-08-25", TARGET], ingested_at=RUN_TIME.replace(tzinfo=None), workers=2,
        )


def test_daily_native_receipt_replays_full_values_and_rejects_identity_drift(monkeypatch):
    from server.common import scheduler_validation as validation
    monkeypatch.setattr(validation, "routed_read_engine", lambda _sql, engine: engine)
    snapshot = market.fetch_complete_directory(_Provider([_item(f"BK{i:04}") for i in range(100)]))
    frame = market.build_closed_daily_frame(snapshot, target_date=TARGET, ingested_at=RUN_TIME.replace(tzinfo=None))
    engine = _atomic_engine()
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES (:target,1)"), {"target": TARGET})
    metrics = market.publish_frames_atomically(engine, {"kline": frame}, start_date=TARGET, end_date=TARGET, use_mysql_lock=False)
    receipt = market.build_receipt(
        status="PASS", datasets=["kline"], started_at=RUN_TIME,
        finished_at=RUN_TIME.replace(second=30), result={
            "datasets": ["kline"], "target_trade_date": TARGET,
            "range_start": TARGET, "range_end": TARGET,
            "open_date_count": 1, "open_dates_sha256": market._digest([TARGET]),
            "directory": snapshot.evidence,
            "dataset_results": {"kline": market._dataset_evidence(frame, "kline")},
            "db_metrics": metrics, "published": True,
        },
    )
    task_type = "eastmoney_concept_kline"
    def replay(value):
        return validation._validate_eastmoney_concept_market_receipt(
            engine, task_type=task_type, output=json.dumps(value),
            started_at=RUN_TIME.replace(tzinfo=None),
            now=RUN_TIME.replace(tzinfo=None, second=40),
        )
    assert replay(receipt)[0]
    wrong = json.loads(json.dumps(receipt))
    wrong["dataset_results"]["kline"]["source_evidence"]["field_map"]["close"] = "f17"
    wrong.pop("result_sha256")
    wrong["result_sha256"] = market._digest(wrong)
    assert validation.scheduler_output_status({"task_type": task_type}, json.dumps(wrong), return_code=0) == "failed"
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_concept_east_kline SET close=102 WHERE index_code='BK0000'"))
    valid, reason = replay(receipt)
    assert not valid and "daily values differ" in reason


def test_concept_publication_routes_history_to_its_own_database_before_any_fetch():
    primary = create_engine("sqlite+pysqlite:///:memory:")
    history = create_engine("sqlite+pysqlite:///:memory:")
    assert market._publication_engine(primary, ["current"], history_engine=history) is primary
    assert market._publication_engine(primary, ["kline"], history_engine=history) is history
    assert market._publication_engine(primary, ["minute", "kline"], history_engine=history) is history
    assert market._publication_engine(primary, ["current", "kline"], history_engine=primary) is primary
    with pytest.raises(market.DataBlocked, match="different databases"):
        market.run_publisher(primary, object(), datasets=["all"], history_engine=history)
