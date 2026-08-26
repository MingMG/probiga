from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import create_engine, text

from server.common import scheduler_validation
from tools import ensure_quality_gate
from tools import fetch_sector_heat_east_daily as sector
from tools import sync_eastmoney_concept_market as concept
from tools import sync_news_formal as news


def _engine():
    return create_engine("sqlite+pysqlite:///:memory:")


def test_new_formal_provider_tasks_have_one_fixed_runtime_contract():
    tasks = {task["task_type"]: task for task in ensure_quality_gate.TASKS}
    expected = {
        "eastmoney_concept_current": (
            "tools/sync_eastmoney_concept_market.py",
            "--dataset current --json",
            "18:05",
        ),
        "eastmoney_concept_kline": (
            "tools/sync_eastmoney_concept_market.py",
            "--dataset kline --json",
            "18:10",
        ),
        "eastmoney_concept_minute": (
            "tools/sync_eastmoney_concept_market.py",
            "--dataset minute --json",
            "18:15",
        ),
        "sector_heat_east": (
            "tools/fetch_sector_heat_east_daily.py",
            "--formal --json",
            "17:08",
        ),
        "news_sync": (
            "tools/sync_news_formal.py",
            "--pages 2 --json",
            "00:05",
        ),
    }
    for task_type, (path, args, cron) in expected.items():
        task = tasks[task_type]
        assert task["script_path"] == path
        assert task["script_args"] == args
        assert task["cron_time"] == cron
        assert task["enabled"] == 1


def _concept_receipt_and_engine():
    engine = _engine()
    target = "2026-08-26"
    captured = datetime(2026, 8, 26, 19, 0)
    codes = [f"BK{index:04d}" for index in range(100)]
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE si_trade_calendar "
                "(trade_date DATE, trade_status INTEGER)"
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE sm_concept_east_current (
                    index_code TEXT, trade_date DATE, trade_time DATETIME,
                    etl_sync_at DATETIME, open REAL, price REAL,
                    high REAL, low REAL, volume REAL, amount REAL
                )
                """
            )
        )
        connection.execute(
            text("INSERT INTO si_trade_calendar VALUES (:target,1)"),
            {"target": target},
        )
        connection.execute(
            text(
                """
                INSERT INTO sm_concept_east_current
                    (index_code,trade_date,trade_time,etl_sync_at,
                     open,price,high,low,volume,amount)
                VALUES (:code,:target,:trade_time,:etl_sync_at,
                        100,101,102,99,1000,2000)
                """
            ),
            [
                {
                    "code": code,
                    "target": target,
                    "trade_time": datetime(2026, 8, 26, 15, 30),
                    "etl_sync_at": captured,
                }
                for code in codes
            ],
        )
    directory = {
        "schema": concept.DIRECTORY_SCHEMA,
        "provider": concept.PROVIDER_ID,
        "source_url": concept.DIRECTORY_URL,
        "source_filter": concept.DIRECTORY_FILTER,
        "reported_count": len(codes),
        "observed_count": len(codes),
        "page_size": 100,
        "pages_expected": 1,
        "pages_fetched": 1,
        "pagination_complete": True,
        "source_dates": [target],
        "first_source_time": "2026-08-26T15:30:00+08:00",
        "last_source_time": "2026-08-26T15:30:00+08:00",
        "code_set_sha256": concept._code_set_hash(codes),
    }
    directory["manifest_sha256"] = concept._digest(directory)
    dataset_result = {
        "dataset": "current",
        "table": "sm_concept_east_current",
        "provider": concept.PROVIDER_ID,
        "source_url": concept.DIRECTORY_URL,
        "row_count": len(codes),
        "code_count": len(codes),
        "date_count": 1,
        "first_date": target,
        "last_date": target,
        "code_set_sha256": concept._code_set_hash(codes),
        "content_sha256": "a" * 64,
    }
    receipt = concept.build_receipt(
        status="PASS",
        datasets=["current"],
        started_at=datetime(2026, 8, 26, 19, 0, tzinfo=concept.SHANGHAI),
        finished_at=datetime(2026, 8, 26, 19, 0, 30, tzinfo=concept.SHANGHAI),
        result={
            "provider": concept.PROVIDER_ID,
            "datasets": ["current"],
            "target_trade_date": target,
            "range_start": target,
            "range_end": target,
            "open_date_count": 1,
            "open_dates_sha256": concept._digest([target]),
            "directory": directory,
            "dataset_results": {"current": dataset_result},
            "db_metrics": {
                "current": {
                    "row_count": len(codes),
                    "code_count": len(codes),
                    "date_count": 1,
                }
            },
            "published": True,
        },
    )
    return engine, receipt


def test_concept_current_scheduler_verifies_exact_directory_and_database_matrix():
    engine, receipt = _concept_receipt_and_engine()
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    task = {"task_type": "eastmoney_concept_current"}
    assert scheduler_validation.scheduler_output_status(
        task, output, return_code=0
    ) == "success"
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=output,
        started_at=datetime(2026, 8, 26, 19, 0),
        now=datetime(2026, 8, 26, 19, 0, 40),
    )
    assert result.checked is True
    assert result.ok is True
    assert "codes=100" in result.message

    explicit = dict(receipt)
    explicit["requested_trade_date"] = "2026-08-26"
    explicit.pop("result_sha256")
    explicit["result_sha256"] = concept._digest(explicit)
    assert scheduler_validation.scheduler_output_status(
        task,
        json.dumps(explicit, ensure_ascii=False, sort_keys=True),
        return_code=0,
    ) == "success"


def test_concept_release_validator_rejects_other_date_receipt_before_db_replay():
    engine, receipt = _concept_receipt_and_engine()
    result = scheduler_validation.validate_scheduler_task_result(
        {
            "task_type": "eastmoney_concept_current",
            "_trigger_source": "release_catchup",
            "_release_target_date": "2026-08-25",
        },
        engine=engine,
        output=json.dumps(receipt, ensure_ascii=False, sort_keys=True),
        started_at=datetime(2026, 8, 26, 19, 0),
        now=datetime(2026, 8, 26, 19, 0, 40),
    )

    assert result.checked is True
    assert result.ok is False
    assert "release target" in result.message


def _sector_rows():
    mapping = sector._industry_map()
    rows = []
    for plate_type, names in (
        (3, list(mapping)),
        (4, [child for children in mapping.values() for child in children]),
    ):
        for rank, name in enumerate(names, start=1):
            rows.append(
                {
                    "snapshot_date": "2026-08-26",
                    "plate_type": plate_type,
                    "rank": rank,
                    "concept_code": f"EM{plate_type}_{rank:03d}",
                    "concept_name": name,
                    "change_pct": 1.0,
                    "hot_value": float(1000 - rank),
                    "hot_tag": "东财正式证据",
                }
            )
    return rows


def test_sector_scheduler_recomputes_fixed_inventory_hash_from_database():
    engine = _engine()
    rows = _sector_rows()
    evidence = sector.validate_formal_sector_rows(
        rows,
        target_date="2026-08-26",
        raw_count=496,
    )
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INTEGER)")
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_hot_concept_ths_daily (
                    snapshot_date DATE, plate_type INTEGER, `rank` INTEGER,
                    concept_code TEXT, concept_name TEXT, change_pct REAL,
                    hot_value REAL, hot_tag TEXT
                )
                """
            )
        )
        connection.execute(
            text("INSERT INTO si_trade_calendar VALUES ('2026-08-26',1)")
        )
        connection.execute(
            text(
                """
                INSERT INTO st_hot_concept_ths_daily VALUES
                    (:snapshot_date,:plate_type,:rank,:concept_code,
                     :concept_name,:change_pct,:hot_value,:hot_tag)
                """
            ),
            rows,
        )
    receipt = sector._formal_sector_receipt(
        status="PASS",
        requested_date="2026-08-26",
        data_date="2026-08-26",
        started_at=datetime(2026, 8, 26, 17, 8),
        finished_at=datetime(2026, 8, 26, 17, 8, 30),
        published=True,
        evidence=evidence,
    )
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    task = {"task_type": "sector_heat_east"}
    assert scheduler_validation.scheduler_output_status(
        task, output, return_code=0
    ) == "success"
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=output,
        started_at=datetime(2026, 8, 26, 17, 8),
        now=datetime(2026, 8, 26, 17, 8, 40),
    )
    assert result.ok is True

    mismatch = scheduler_validation.validate_scheduler_task_result(
        {
            "task_type": "sector_heat_east",
            "_trigger_source": "release_catchup",
            "_release_target_date": "2026-08-25",
        },
        engine=engine,
        output=output,
        started_at=datetime(2026, 8, 26, 17, 8),
        now=datetime(2026, 8, 26, 17, 8, 40),
    )
    assert mismatch.ok is False
    assert "release target" in mismatch.message
    assert "l1=31" in result.message
    assert "l2=128" in result.message


def test_news_scheduler_recomputes_fresh_persisted_batch_hash():
    engine = _engine()
    canonical = news.canonical_news_items(
        [
            {
                "source": "cls",
                "source_id": "c1",
                "title": "快讯",
                "content": "内容",
                "publish_time": datetime(2026, 8, 26, 17, 2),
                "level": "B",
                "stocks": [],
                "subjects": [],
                "reading_num": 1,
                "is_top": False,
                "jpush": False,
            }
        ]
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_news_flash (
                    source TEXT, source_id TEXT, title TEXT, content TEXT,
                    publish_time DATETIME, level TEXT, stocks TEXT,
                    subjects TEXT, reading_num INTEGER, is_top INTEGER,
                    jpush INTEGER, etl_sync_at DATETIME
                )
                """
            )
        )
        item = canonical[0]
        connection.execute(
            text(
                """
                INSERT INTO st_news_flash VALUES
                    (:source,:source_id,:title,:content,:publish_time,:level,
                     :stocks,:subjects,:reading_num,:is_top,:jpush,:etl_sync_at)
                """
            ),
            {
                **item,
                "publish_time": datetime.fromisoformat(item["publish_time"]),
                "stocks": json.dumps(item["stocks"]),
                "subjects": json.dumps(item["subjects"]),
                "is_top": int(item["is_top"]),
                "jpush": int(item["jpush"]),
                "etl_sync_at": datetime(2026, 8, 26, 17, 30),
            },
        )
    source_results = {
        "cls": {
            "status": "SUCCESS",
            "outcome": "NONEMPTY",
            "requested_pages": 2,
            "fetched_count": 1,
        },
        "eastmoney": {
            "status": "SUCCESS",
            "outcome": "EMPTY",
            "requested_pages": 1,
            "fetched_count": 0,
        },
        "sina": {
            "status": "FAILED",
            "requested_pages": 1,
            "fetched_count": 0,
        },
    }
    receipt = news._receipt(
        status="PASS",
        started_at=datetime(2026, 8, 26, 17, 30),
        finished_at=datetime(2026, 8, 26, 17, 30, 10),
        source_results=source_results,
        pages=2,
        evidence={
            "persisted_count": 1,
            "latest_publish_time": "2026-08-26T17:02:00",
            "row_hash": news.news_row_hash(canonical),
        },
    )
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    task = {"task_type": "news_sync"}
    assert scheduler_validation.scheduler_output_status(
        task, output, return_code=0
    ) == "success"
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=output,
        started_at=datetime(2026, 8, 26, 17, 30),
        now=datetime(2026, 8, 26, 17, 30, 20),
    )
    assert result.ok is True
    assert "rows=1" in result.message
