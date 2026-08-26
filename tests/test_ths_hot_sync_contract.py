from __future__ import annotations

from datetime import datetime, timedelta
import json
import sys
from types import ModuleType

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.common import batch_db, scheduler_validation, ths_hot_contract
from tools import fetch_hot_concept_ths_daily, fetch_hot_rank_ths


def _install_fake_adata(monkeypatch, hot_type: type) -> None:
    adata = ModuleType("adata")
    sentiment = ModuleType("adata.sentiment")
    hot = ModuleType("adata.sentiment.hot")
    hot.Hot = hot_type
    adata.sentiment = sentiment
    sentiment.hot = hot
    monkeypatch.setitem(sys.modules, "adata", adata)
    monkeypatch.setitem(sys.modules, "adata.sentiment", sentiment)
    monkeypatch.setitem(sys.modules, "adata.sentiment.hot", hot)


def test_hot_rank_uses_released_no_date_provider_api_and_stamps_run_date(monkeypatch):
    provider_calls: list[str] = []

    class FakeHot:
        def hot_rank_100_ths(self):
            provider_calls.append("rank")
            return pd.DataFrame(
                {
                    "rank": range(1, 101),
                    "stock_code": [f"{index:06d}" for index in range(1, 101)],
                    "short_name": [f"股票{index}" for index in range(1, 101)],
                    "change_pct": [1.0] * 100,
                    "hot_value": [10.0] * 100,
                    "pop_tag": ["热"] * 100,
                    "concept_tag": ["概念"] * 100,
                }
            )

    _install_fake_adata(monkeypatch, FakeHot)
    engine = object()
    writes: list[dict] = []
    captured = datetime(2026, 8, 26, 17, 20)
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "_assert_current_snapshot_date",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(fetch_hot_rank_ths, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(fetch_hot_rank_ths, "_ensure_snapshot_date_column", lambda value: None)
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "require_capture_window",
        lambda *args, **kwargs: captured,
    )
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "shanghai_now",
        lambda value=None: captured,
    )
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "replace_table_rows",
        lambda frame, table, target_engine, **kwargs: writes.append(
            {
                "frame": frame.copy(),
                "table": table,
                "engine": target_engine,
                **kwargs,
            }
        ),
    )
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "_readback_hot_rank",
        lambda *_args: writes[0]["frame"].to_dict(orient="records"),
    )

    receipt = fetch_hot_rank_ths.fetch_hot_rank_ths("2026-08-26")

    assert provider_calls == ["rank"]
    assert len(writes) == 1
    assert writes[0]["table"] == "st_hot_rank_ths"
    assert writes[0]["engine"] is engine
    assert writes[0]["where_sql"] == "snapshot_date = :d"
    assert writes[0]["params"] == {"d": "2026-08-26"}
    assert set(writes[0]["frame"]["snapshot_date"]) == {"2026-08-26"}
    assert receipt["status"] == "PASS"
    assert receipt["row_count"] == 100
    assert receipt["provider_payload_sha256"]
    assert receipt["persisted_row_sha256"]
    assert ths_hot_contract.receipt_id_valid(receipt)


def test_hot_rank_refuses_historical_label_for_current_only_provider():
    with pytest.raises(RuntimeError, match="current-snapshot endpoint"):
        fetch_hot_rank_ths._assert_current_snapshot_date(
            "2026-08-25",
            now=datetime.fromisoformat("2026-08-26T17:12:00+08:00"),
        )


def test_hot_concept_uses_released_plate_type_only_api_and_stamps_run_date(monkeypatch):
    provider_calls: list[int] = []

    class FakeHot:
        def hot_concept_20_ths(self, plate_type: int):
            provider_calls.append(plate_type)
            return pd.DataFrame(
                {
                    "rank": range(1, 21),
                    "concept_code": [f"C{plate_type}{index:02d}" for index in range(1, 21)],
                    "concept_name": [f"板块{index}" for index in range(1, 21)],
                    "change_pct": [1.0] * 20,
                    "hot_value": [10.0] * 20,
                    "hot_tag": ["热"] * 20,
                }
            )

    class FakeConnection:
        def __init__(self):
            self.executes: list[tuple[object, dict]] = []

        def execute(self, statement, params):
            self.executes.append((statement, params))

    class FakeTransaction:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self.connection

        def __exit__(self, exc_type, exc, traceback):
            return False

    class FakeEngine:
        def __init__(self):
            self.connection = FakeConnection()

        def begin(self):
            return FakeTransaction(self.connection)

    _install_fake_adata(monkeypatch, FakeHot)
    engine = FakeEngine()
    writes: list[dict] = []
    captured = datetime(2026, 8, 26, 17, 20)
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "_assert_current_snapshot_date",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(fetch_hot_concept_ths_daily, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "require_capture_window",
        lambda *args, **kwargs: captured,
    )
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "shanghai_now",
        lambda value=None: captured,
    )
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "write_frame",
        lambda frame, table, connection, **kwargs: writes.append(
            {
                "frame": frame.copy(),
                "table": table,
                "connection": connection,
                **kwargs,
            }
        ),
    )
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "_readback_hot_concept",
        lambda *_args: writes[0]["frame"].to_dict(orient="records"),
    )

    receipt = fetch_hot_concept_ths_daily.fetch_hot_concept_ths_daily(
        "2026-08-26"
    )

    assert provider_calls == [1, 2]
    assert len(writes) == 1
    assert writes[0]["table"] == "st_hot_concept_ths_daily"
    assert writes[0]["connection"] is engine.connection
    assert len(writes[0]["frame"]) == 40
    assert set(writes[0]["frame"]["snapshot_date"]) == {"2026-08-26"}
    assert engine.connection.executes[0][1] == {"d": "2026-08-26"}
    assert receipt["plate_type_counts"] == {"1": 20, "2": 20}
    assert ths_hot_contract.receipt_id_valid(receipt)


def test_hot_concept_refuses_partial_plate_snapshot(monkeypatch):
    class FakeHot:
        def hot_concept_20_ths(self, plate_type: int):
            if plate_type == 2:
                return pd.DataFrame()
            return pd.DataFrame(
                [
                    {
                        "rank": 1,
                        "concept_code": "885001",
                        "concept_name": "concept",
                        "change_pct": 1.25,
                        "hot_value": 88.0,
                        "hot_tag": "hot",
                    }
                ]
            )

    _install_fake_adata(monkeypatch, FakeHot)
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "_assert_current_snapshot_date",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(fetch_hot_concept_ths_daily, "create_batch_engine", lambda: object())
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "require_capture_window",
        lambda *args, **kwargs: datetime(2026, 8, 26, 17, 20),
    )

    with pytest.raises(RuntimeError, match="missing plate types"):
        fetch_hot_concept_ths_daily.fetch_hot_concept_ths_daily("2026-08-26")


def test_hot_concept_refuses_historical_label_for_current_only_provider():
    with pytest.raises(RuntimeError, match="current-snapshot endpoint"):
        fetch_hot_concept_ths_daily._assert_current_snapshot_date(
            "2026-08-25",
            now=datetime.fromisoformat("2026-08-26T17:10:00+08:00"),
        )


def _rank_provider_frame(*, hot_value: float = 10.0) -> pd.DataFrame:
    return pd.DataFrame({
        "rank": range(1, 101),
        "stock_code": [f"{index:06d}" for index in range(1, 101)],
        "short_name": [f"SYNTH-RANK-{index}" for index in range(1, 101)],
        "change_pct": [1.0] * 100,
        "hot_value": [hot_value] * 100,
        "pop_tag": ["synthetic"] * 100,
        "concept_tag": ["synthetic-concept"] * 100,
    })


def _concept_provider_frame(
    plate_type: int,
    *,
    hot_value: float = 10.0,
) -> pd.DataFrame:
    return pd.DataFrame({
        "rank": range(1, 21),
        "concept_code": [
            f"S{plate_type}{index:03d}" for index in range(1, 21)
        ],
        "concept_name": [
            f"SYNTH-CONCEPT-{plate_type}-{index}" for index in range(1, 21)
        ],
        "change_pct": [1.0] * 20,
        "hot_value": [hot_value] * 20,
        "hot_tag": ["synthetic"] * 20,
    })


def _sqlite_rank_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_hot_rank_ths (
                snapshot_date TEXT NOT NULL,
                rank INTEGER NOT NULL,
                stock_code TEXT NOT NULL,
                short_name TEXT NOT NULL,
                change_pct NUMERIC NULL,
                hot_value NUMERIC NULL,
                pop_tag TEXT NOT NULL,
                concept_tag TEXT NOT NULL,
                etl_sync_at TEXT NOT NULL
            )
        """))
    return engine


def _sqlite_concept_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_hot_concept_ths_daily (
                snapshot_date TEXT NOT NULL,
                plate_type INTEGER NOT NULL,
                rank INTEGER NOT NULL,
                concept_code TEXT NOT NULL,
                concept_name TEXT NOT NULL,
                change_pct NUMERIC NULL,
                hot_value NUMERIC NULL,
                hot_tag TEXT NOT NULL,
                etl_sync_at TEXT NOT NULL
            )
        """))
    return engine


@pytest.mark.parametrize(
    ("task_type", "requested_date", "now", "closed_date", "reason"),
    [
        (
            "hot_rank_ths",
            "2026-08-26",
            datetime(2026, 8, 26, 2, 0),
            "2026-08-25",
            "CURRENT_SESSION_NOT_CLOSED",
        ),
        (
            "hot_concept",
            "2026-08-30",
            datetime(2026, 8, 30, 18, 0),
            "2026-08-28",
            "REQUEST_DATE_NOT_OPEN_SESSION",
        ),
        (
            "hot_rank_ths",
            "2026-08-26",
            datetime(2026, 8, 27, 18, 0),
            "2026-08-27",
            "CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED",
        ),
    ],
)
def test_current_snapshot_capture_window_blocks_unprovable_date_labels(
    monkeypatch,
    task_type,
    requested_date,
    now,
    closed_date,
    reason,
):
    monkeypatch.setattr(
        ths_hot_contract,
        "authoritative_closed_trade_date",
        lambda *args, **kwargs: closed_date,
    )
    with pytest.raises(ths_hot_contract.ThsHotDataBlocked, match=reason):
        ths_hot_contract.require_capture_window(
            object(),
            task_type=task_type,
            requested_date=requested_date,
            now=now,
        )


def test_current_snapshot_capture_window_accepts_only_proven_postclose_session(
    monkeypatch,
):
    target = "2026-08-26"
    now = datetime(2026, 8, 26, 17, 12)
    monkeypatch.setattr(
        ths_hot_contract,
        "authoritative_closed_trade_date",
        lambda *args, **kwargs: target,
    )
    assert ths_hot_contract.require_capture_window(
        object(),
        task_type=ths_hot_contract.THS_HOT_RANK_TASK_TYPE,
        requested_date=target,
        now=now,
    ) == now


def test_ths_inventory_rejects_duplicates_and_partial_plate_types():
    rank_rows = _rank_provider_frame().to_dict(orient="records")
    rank_rows[-1]["stock_code"] = rank_rows[0]["stock_code"]
    with pytest.raises(RuntimeError, match="code/rank inventory differs"):
        ths_hot_contract.validate_rank_inventory(rank_rows)

    concept_rows = []
    for plate_type in (1, 2):
        frame = _concept_provider_frame(plate_type)
        frame["plate_type"] = plate_type
        if plate_type == 2:
            frame = frame.head(9)
        concept_rows.extend(frame.to_dict(orient="records"))
    with pytest.raises(RuntimeError, match="plate inventory size differs"):
        ths_hot_contract.validate_concept_inventory(concept_rows)


def test_hot_concept_partial_write_rolls_back_previous_complete_batch(monkeypatch):
    engine = _sqlite_concept_engine()
    target = "2026-08-26"
    old_batch = datetime(2026, 8, 26, 17, 15)
    old_parts = []
    for plate_type in (1, 2):
        frame = _concept_provider_frame(plate_type, hot_value=1.0)
        frame["snapshot_date"] = target
        frame["plate_type"] = plate_type
        frame["etl_sync_at"] = old_batch
        old_parts.append(frame)
    old_frame = pd.concat(old_parts, ignore_index=True)[[
        "snapshot_date",
        "plate_type",
        "rank",
        "concept_code",
        "concept_name",
        "change_pct",
        "hot_value",
        "hot_tag",
        "etl_sync_at",
    ]]
    batch_db.write_frame(old_frame, "st_hot_concept_ths_daily", engine)

    class FakeHot:
        def hot_concept_20_ths(self, plate_type: int):
            return _concept_provider_frame(plate_type, hot_value=99.0)

    _install_fake_adata(monkeypatch, FakeHot)
    now = datetime(2026, 8, 26, 17, 20)
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "_assert_current_snapshot_date",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "create_batch_engine",
        lambda: engine,
    )
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "require_capture_window",
        lambda *args, **kwargs: now,
    )
    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "shanghai_now",
        lambda value=None: now,
    )

    def partial_then_fail(frame, table, connection, **kwargs):
        batch_db.write_frame(frame.head(5), table, connection, **kwargs)
        raise RuntimeError("synthetic interrupted publish")

    monkeypatch.setattr(
        fetch_hot_concept_ths_daily,
        "write_frame",
        partial_then_fail,
    )
    with pytest.raises(RuntimeError, match="interrupted publish"):
        fetch_hot_concept_ths_daily.fetch_hot_concept_ths_daily(
            target,
            now=now,
        )

    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(text("""
                SELECT snapshot_date, plate_type, rank, concept_code,
                       concept_name, change_pct, hot_value, hot_tag, etl_sync_at
                  FROM st_hot_concept_ths_daily
                 ORDER BY plate_type, rank
            """)).mappings().all()
        ]
    assert len(rows) == 40
    assert {float(row["hot_value"]) for row in rows} == {1.0}
    assert ths_hot_contract.validate_concept_inventory(
        rows,
        target_date=target,
    )["plate_type_counts"] == {"1": 20, "2": 20}


def test_hot_rank_repeated_publish_replaces_atomically_and_stales_old_receipt(
    monkeypatch,
):
    engine = _sqlite_rank_engine()
    target = "2026-08-26"
    provider_calls = 0

    class FakeHot:
        def hot_rank_100_ths(self):
            nonlocal provider_calls
            provider_calls += 1
            return _rank_provider_frame(hot_value=10.0 + provider_calls)

    _install_fake_adata(monkeypatch, FakeHot)
    clock = {"now": datetime(2026, 8, 26, 17, 20)}
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "_assert_current_snapshot_date",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(fetch_hot_rank_ths, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "_ensure_snapshot_date_column",
        lambda value: None,
    )
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "require_capture_window",
        lambda *args, **kwargs: kwargs["now"],
    )
    monkeypatch.setattr(
        fetch_hot_rank_ths,
        "shanghai_now",
        lambda value=None: clock["now"],
    )

    first = fetch_hot_rank_ths.fetch_hot_rank_ths(
        target,
        now=clock["now"],
    )
    clock["now"] = datetime(2026, 8, 26, 17, 21)
    second = fetch_hot_rank_ths.fetch_hot_rank_ths(
        target,
        now=clock["now"],
    )

    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(text("""
                SELECT snapshot_date, rank, stock_code, short_name,
                       change_pct, hot_value, pop_tag, concept_tag, etl_sync_at
                  FROM st_hot_rank_ths
                 ORDER BY rank, stock_code
            """)).mappings().all()
        ]
    assert provider_calls == 2
    assert len(rows) == 100
    assert {float(row["hot_value"]) for row in rows} == {12.0}
    assert len({ths_hot_contract.batch_timestamp(rows)}) == 1
    assert first["receipt_id"] != second["receipt_id"]

    monkeypatch.setattr(
        ths_hot_contract,
        "authoritative_closed_trade_date",
        lambda *args, **kwargs: target,
    )
    old_result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "hot_rank_ths"},
        engine=engine,
        started_at=datetime(2026, 8, 26, 17, 20),
        now=datetime(2026, 8, 26, 17, 22),
        output=json.dumps(first),
    )
    latest_result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "hot_rank_ths"},
        engine=engine,
        started_at=datetime(2026, 8, 26, 17, 21),
        now=datetime(2026, 8, 26, 17, 22),
        output=json.dumps(second),
    )
    assert old_result.checked is True and old_result.ok is False
    assert "persisted inventory/hash differs" in old_result.message
    assert latest_result.checked is True and latest_result.ok is True
    assert "exact THS current snapshot verified" in latest_result.message


def test_ths_data_blocked_and_pass_receipts_are_exact_and_tamper_evident():
    started = datetime(2026, 8, 26, 2, 0)
    blocked = ths_hot_contract.build_blocked_receipt(
        task_type=ths_hot_contract.THS_HOT_RANK_TASK_TYPE,
        requested_date="2026-08-26",
        started_at=started,
        reason="CURRENT_SESSION_NOT_CLOSED",
    )
    output = json.dumps(blocked)
    task = {"task_type": "hot_rank_ths"}
    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=2,
    ) == "blocked"
    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "failed"
    tampered = {**blocked, "requested_date": "2026-08-25"}
    assert scheduler_validation.scheduler_output_status(
        task,
        json.dumps(tampered),
        return_code=2,
    ) == "failed"
