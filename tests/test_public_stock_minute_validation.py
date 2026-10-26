from __future__ import annotations

import json
import sys
from datetime import datetime

from tools import crawl_minute_kline as crawler
from tools import import_sina_minute_history as importer
from server.common import scheduler_validation


def _payload() -> dict:
    return {
        "schema": "probiga.public-stock-minute-result.v1",
        "status": "PASS",
        "trade_date": "2026-10-26",
        "expected_codes": 100,
        "tradable_codes": 98,
        "no_trade_codes": ["000016", "600621"],
        "no_trade_count": 2,
        "complete_codes": 98,
        "failed_codes": 0,
        "source_counts": {"sina_public_1m": 23324},
        "row_count": 23324,
        "minutes_per_code": 238,
        "coverage": 1.0,
        "minimum_coverage": 0.99,
        "acquisition_status": "COMPLETE",
        "written_rows": 23324,
        "readback_rows": 23324,
        "readback_codes": 98,
        "content_sha256": "a" * 64,
        "readback_sha256": "a" * 64,
        "receipt_id": "b" * 64,
    }


def test_public_stock_minute_receipt_contract():
    task = {
        "task_type": "stock_minute",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": "--type stock --min-coverage 0.99",
        "_scheduler_target_trade_date": "2026-10-26",
        "_scheduler_effective_args": [
            "--type",
            "stock",
            "--min-coverage",
            "0.99",
            "--trade-date",
            "2026-10-26",
        ],
    }

    payload = scheduler_validation._public_stock_minute_payload(
        task,
        json.dumps(_payload()),
        0,
    )

    assert payload["trade_date"] == "2026-10-26"
    assert payload["acquisition_status"] == "COMPLETE"


def test_stock_minute_main_delegates_to_verified_public_source(monkeypatch):
    captured = []
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", "stock_minute")
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", "a" * 40)
    monkeypatch.setattr(
        crawler,
        "create_batch_engine",
        lambda: object(),
    )
    monkeypatch.setattr(
        crawler,
        "get_kline_engine",
        lambda: object(),
    )
    monkeypatch.setattr(
        "server.common.minute_acquisition_reuse.inspect_complete_partition",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        crawler,
        "_now",
        lambda: datetime(2026, 10, 26, 23, 30),
    )
    monkeypatch.setattr(
        importer,
        "main",
        lambda args: captured.append(args) or 0,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "crawl_minute_kline.py",
            "--type",
            "stock",
            "--trade-date",
            "2026-10-26",
            "--min-coverage",
            "0.99",
        ],
    )

    assert crawler.main() == 0
    assert captured
    assert captured[0][:6] == [
        "--trade-date",
        "2026-10-26",
        "--all-catalog",
        "--apply",
        "--workers",
        "2",
    ]
    assert captured[0][-1:] == ["--json"]
