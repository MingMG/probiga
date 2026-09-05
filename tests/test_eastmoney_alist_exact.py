from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from server.common.scheduler_validation import (
    scheduler_output_status,
    validate_scheduler_task_result,
)
from tools import sync_eastmoney_alist_exact as exact


def test_alist_production_identity_does_not_need_git(monkeypatch):
    sha = "a" * 40
    root = f"/opt/ProBigA-releases/{sha}"
    monkeypatch.setattr(exact, "ROOT", Path(root))
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_CODE_ROOT", root)
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", sha)
    monkeypatch.setattr(exact, "_git_head", lambda: pytest.fail("runtime Git must not run"))
    assert exact.resolve_build_sha(sha) == sha
    with pytest.raises(exact.AListDataBlocked, match="differs"):
        exact.resolve_build_sha("b" * 40)
    monkeypatch.setenv("PROBIGA_CODE_ROOT", "/opt/unbound-release")
    with pytest.raises(exact.AListDataBlocked, match="identity differs"):
        exact.resolve_build_sha(sha)


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.trust_env = True
        self.headers = {}
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append((url, dict(params), timeout))
        return _Response(self.payloads.pop(0))

    def close(self):
        return None


def _success(rows, *, count, pages):
    return {
        "success": True,
        "code": 0,
        "message": "ok",
        "result": {"count": count, "pages": pages, "data": rows},
    }


def _daily_raw(code="000001", reason="reason"):
    return {
        "TRADE_DATE": "2026-08-26 00:00:00",
        "SECURITY_NAME_ABBR": "平安银行",
        "SECURITY_CODE": code,
        "CLOSE_PRICE": 10,
        "CHANGE_RATE": 1,
        "TURNOVERRATE": 2,
        "BILLBOARD_NET_AMT": 3,
        "BILLBOARD_BUY_AMT": 5,
        "BILLBOARD_SELL_AMT": 2,
        "BILLBOARD_DEAL_AMT": 7,
        "ACCUM_AMOUNT": 100,
        "DEAL_NET_RATIO": 3,
        "DEAL_AMOUNT_RATIO": 7,
        "EXPLANATION": reason,
    }


def _info_raw(
    *,
    code="000001",
    operate="seat",
    buy=5,
    sell=2,
    reason="reason",
):
    buy_rate = None if buy is None else 5
    sell_rate = None if sell is None else 2
    return {
        "TRADE_DATE": "2026-08-26 00:00:00",
        "SECURITY_CODE": code,
        "OPERATEDEPT_CODE": "1",
        "OPERATEDEPT_NAME": operate,
        "NET": (buy or 0) - (sell or 0),
        "BUY": buy,
        "SELL": sell,
        "TOTAL_BUYRIO": buy_rate,
        "TOTAL_SELLRIO": sell_rate,
        "EXPLANATION": reason,
    }


def _evidence(report, rows, *, empty=False):
    return exact.ReportEvidence(
        report=report,
        trade_date="2026-08-26",
        rows=tuple(rows),
        declared_count=len(rows),
        declared_pages=0 if empty else 1,
        fetched_pages=1,
        authoritative_empty=empty,
        response_hash="a" * 64,
    )


def test_provider_consumes_every_declared_page():
    first = [_daily_raw(code=f"{index:06d}") for index in range(1, 501)]
    last = [_daily_raw(code="000501")]
    session = _Session(
        [
            _success(first, count=501, pages=2),
            _success(last, count=501, pages=2),
        ]
    )
    provider = exact.EastmoneyAListProvider(session=session, attempts=1)

    evidence = provider.fetch_report(exact.DAILY_REPORT, "2026-08-26")

    assert evidence.declared_count == 501
    assert evidence.fetched_pages == 2
    assert len(evidence.rows) == 501
    assert [call[1]["pageNumber"] for call in session.calls] == [1, 2]
    assert all(call[1]["pageSize"] == exact.PAGE_SIZE for call in session.calls)


def test_provider_accepts_only_explicit_9201_empty_evidence():
    provider = exact.EastmoneyAListProvider(
        session=_Session(
            [
                {
                    "success": False,
                    "code": 9201,
                    "message": "返回数据为空",
                    "result": None,
                }
            ]
        ),
        attempts=1,
    )
    evidence = provider.fetch_report(exact.DAILY_REPORT, "2026-08-26")
    assert evidence.authoritative_empty is True
    assert evidence.declared_count == 0

    malformed = exact.EastmoneyAListProvider(
        session=_Session([_success([], count=0, pages=0)]),
        attempts=1,
    )
    with pytest.raises(exact.AListDataBlocked, match="cannot prove an empty"):
        malformed.fetch_report(exact.DAILY_REPORT, "2026-08-26")


def test_daily_filters_non_equity_but_blocks_unknown_a_share():
    evidence = _evidence(
        exact.DAILY_REPORT,
        [_daily_raw("000001"), _daily_raw("118076", "bond")],
    )
    rows = exact.normalize_daily(
        evidence,
        observed_at=datetime(2026, 8, 26, 17, 40),
        build_sha="b" * 40,
        allowed_codes={"000001"},
        qmt_by_stock={"000001": "000001.SZ"},
    )
    assert [row["stock_code"] for row in rows] == ["000001"]
    assert rows[0]["qmt_code"] == "000001.SZ"

    unknown = _evidence(exact.DAILY_REPORT, [_daily_raw("000002")])
    with pytest.raises(exact.AListDataBlocked, match="absent from the immutable"):
        exact.normalize_daily(
            unknown,
            observed_at=datetime(2026, 8, 26, 17, 40),
            build_sha="b" * 40,
            allowed_codes={"000001"},
        )


def test_info_requires_exact_daily_code_set_and_deduplicates_buy_sell_overlap():
    overlap = _info_raw(code="000001", operate="overlap", buy=5, sell=2)
    buy_only = _info_raw(code="000002", operate="buy", buy=9, sell=None)
    sell_only = _info_raw(code="000002", operate="sell", buy=None, sell=4)
    reports = (
        _evidence(exact.DETAIL_REPORTS[0], [overlap, buy_only]),
        _evidence(exact.DETAIL_REPORTS[1], [overlap, sell_only]),
    )

    rows = exact.normalize_info(
        reports,
        daily_codes={"000001", "000002"},
        observed_at=datetime(2026, 8, 26, 17, 45),
        allowed_codes={"000001", "000002"},
    )

    assert len(rows) == 3
    assert exact.partition_proof(rows, dataset="info")["code_count"] == 2
    with pytest.raises(exact.AListDataBlocked, match="code set differs"):
        exact.normalize_info(
            reports,
            daily_codes={"000001", "000002", "000003"},
            observed_at=datetime(2026, 8, 26, 17, 45),
            allowed_codes={"000001", "000002", "000003"},
        )


def _sqlite_alist_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE st_a_list_daily (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              trade_date DATE NOT NULL, short_name TEXT, stock_code TEXT NOT NULL,
              close NUMERIC, change_cpt NUMERIC, turnover_ratio NUMERIC,
              a_net_amount NUMERIC, a_buy_amount NUMERIC, a_sell_amount NUMERIC,
              a_amount NUMERIC, amount NUMERIC, net_amount_rate NUMERIC,
              a_amount_rate NUMERIC, reason TEXT, etl_sync_at DATETIME NOT NULL,
              qmt_code TEXT, data_source TEXT, source_time DATETIME,
              received_at DATETIME, batch_id TEXT, data_version TEXT,
              quality_status TEXT, permission_status TEXT
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE st_a_list_info (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              trade_date DATE NOT NULL, stock_code TEXT NOT NULL,
              operate_code TEXT, operate_name TEXT, a_net_amount NUMERIC,
              a_buy_amount NUMERIC, a_sell_amount NUMERIC,
              a_buy_amount_rate NUMERIC, a_sell_amount_rate NUMERIC,
              reason TEXT, etl_sync_at DATETIME NOT NULL
            )
            """
        )
    return engine


@contextmanager
def _local_lock(engine, _name, **_kwargs):
    with engine.connect() as connection:
        yield connection


def _publish_row():
    row = {column: None for column in exact.DAILY_INSERT_COLUMNS}
    row.update(
        {
            "trade_date": "2026-08-26",
            "short_name": "平安银行",
            "stock_code": "000001",
            "close": 10.0,
            "change_cpt": 1.0,
            "turnover_ratio": 2.0,
            "a_net_amount": 3.0,
            "a_buy_amount": 5.0,
            "a_sell_amount": 2.0,
            "a_amount": 7.0,
            "amount": 100.0,
            "net_amount_rate": 3.0,
            "a_amount_rate": 7.0,
            "reason": "reason",
            "etl_sync_at": datetime(2026, 8, 26, 17, 40),
            "qmt_code": "000001.SZ",
            "data_source": exact.PROVIDER_ID,
            "source_time": datetime(2026, 8, 26, 15, 0),
            "received_at": datetime(2026, 8, 26, 17, 40),
            "batch_id": "a" * 64,
            "data_version": "b" * 40,
            "quality_status": "PROVIDER_COMPLETE",
            "permission_status": "PUBLIC",
        }
    )
    return row


def test_publish_is_atomic_and_readback_bound(monkeypatch):
    engine = _sqlite_alist_engine()
    monkeypatch.setattr(exact, "mysql_named_lock", _local_lock)
    with engine.begin() as connection:
        old = _publish_row()
        old["stock_code"] = "000002"
        connection.execute(exact._insert_statement("daily"), old)

    proof = exact.publish_partition(
        engine,
        dataset="daily",
        trade_date="2026-08-26",
        rows=[_publish_row()],
    )

    assert proof["row_count"] == 1
    assert proof["code_set_hash"] == exact.code_set_hash(["000001"])
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT stock_code FROM st_a_list_daily")
        ).scalars().all() == ["000001"]


def test_publish_rolls_back_delete_when_insert_fails(monkeypatch):
    engine = _sqlite_alist_engine()
    monkeypatch.setattr(exact, "mysql_named_lock", _local_lock)
    with engine.begin() as connection:
        connection.execute(exact._insert_statement("daily"), _publish_row())

    def mismatched_readback(*_args, **_kwargs):
        return []

    monkeypatch.setattr(exact, "_read_partition", mismatched_readback)

    with pytest.raises(exact.AListDataBlocked, match="transaction readback"):
        exact.publish_partition(
            engine,
            dataset="daily",
            trade_date="2026-08-26",
            rows=[_publish_row()],
        )

    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT stock_code FROM st_a_list_daily")
        ).scalars().all() == ["000001"]


def test_latest_session_resolution_never_selects_unclosed_current_day():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INTEGER)")
        )
        connection.execute(
            text(
                "INSERT INTO si_trade_calendar VALUES "
                "('2026-08-25',1),('2026-08-26',1)"
            )
        )

    assert exact.resolve_requested_trade_date(
        engine,
        trade_date="",
        latest_session=True,
        now=datetime(2026, 8, 26, 16, 0, tzinfo=exact.SHANGHAI),
    ) == "2026-08-25"
    assert exact.resolve_requested_trade_date(
        engine,
        trade_date="",
        latest_session=True,
        now=datetime(2026, 8, 26, 17, 0, tzinfo=exact.SHANGHAI),
    ) == "2026-08-26"
    with pytest.raises(exact.AListDataBlocked):
        exact.resolve_requested_trade_date(
            engine,
            trade_date="2026-08-26",
            latest_session=True,
            now=datetime(2026, 8, 26, 17, 0, tzinfo=exact.SHANGHAI),
        )


def test_signed_task_result_binds_provider_pagination_catalog_and_database(monkeypatch):
    evidence = _evidence(exact.DAILY_REPORT, [_daily_raw()])
    rows = exact.normalize_daily(
        evidence,
        observed_at=datetime(2026, 8, 26, 17, 40),
        build_sha="b" * 40,
        allowed_codes={"000001"},
        qmt_by_stock={"000001": "000001.SZ"},
    )
    database = exact.database_proof(rows, dataset="daily")
    payload = exact._signed(
        {
            "schema": exact.RESULT_SCHEMA,
            "status": "PASS",
            "dataset": "daily",
            "task_type": exact.TASK_TYPES["daily"],
            "executor_owner": exact.EXECUTOR_OWNER,
            "provider": exact.PROVIDER_ID,
            "trade_date": "2026-08-26",
            "build_sha": "b" * 40,
            "finished_at": "2026-08-26T17:40:00+08:00",
            "catalog": {
                "batch_id": "catalog",
                "manifest_hash": "c" * 64,
                "member_set_hash": "d" * 64,
                "captured_at": "2026-08-26 15:30:00",
                "history_complete_from": "2026-01-01",
                "eligible_code_count": 1,
                "eligible_code_set_hash": exact.code_set_hash(["000001"]),
            },
            "collection": exact._source_receipt(
                daily_report=evidence,
                daily_rows=rows,
            ),
            "database": database,
        }
    )
    assert exact.validate_task_result(payload, 0) == "complete"

    engine = _sqlite_alist_engine()
    stored_rows = [
        {
            **row,
            **{
                column: float(row[column])
                for column in exact.DAILY_NUMERIC_COLUMNS
            },
        }
        for row in rows
    ]
    with engine.begin() as connection:
        connection.execute(exact._insert_statement("daily"), stored_rows)
    catalog = type(
        "Catalog",
        (),
        {
            "batch_id": "catalog",
            "manifest_hash": "c" * 64,
            "member_set_hash": "d" * 64,
            "captured_at": "2026-08-26 15:30:00",
            "history_complete_from": "2026-01-01",
        },
    )()
    monkeypatch.setattr(exact, "_git_head", lambda: "b" * 40)
    monkeypatch.setattr(exact, "validate_runtime_schema", lambda _engine: {})
    monkeypatch.setattr(
        exact,
        "load_target_stock_catalog",
        lambda *_args, **_kwargs: (catalog, ["000001"]),
    )
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "b" * 40)
    rendered = json.dumps(payload, default=str)
    assert scheduler_output_status(
        {"task_type": "alist_daily"},
        rendered,
        return_code=0,
    ) == "success"
    assert scheduler_output_status(
        {"task_type": "alist_info"},
        rendered,
        return_code=0,
    ) == "failed"
    scheduler_proof = validate_scheduler_task_result(
        {"task_type": "alist_daily"},
        engine=engine,
        output=rendered,
        started_at=datetime(2026, 8, 26, 17, 39),
        now=datetime(2026, 8, 26, 17, 45),
    )
    assert scheduler_proof.checked and scheduler_proof.ok
    release_mismatch = validate_scheduler_task_result(
        {
            "task_type": "alist_daily",
            "_trigger_source": "release_catchup",
            "_release_target_date": "2026-08-25",
        },
        engine=engine,
        output=rendered,
        started_at=datetime(2026, 8, 26, 17, 39),
        now=datetime(2026, 8, 26, 17, 45),
    )
    assert release_mismatch.checked and not release_mismatch.ok
    assert "release target" in release_mismatch.message
    persisted = exact.validate_persisted_result(
        engine,
        payload,
        now=datetime(2026, 8, 26, 17, 45, tzinfo=exact.SHANGHAI),
    )
    assert persisted["storage_row_hash"] == database["storage_row_hash"]
    with pytest.raises(exact.AListDataBlocked, match="release target"):
        exact.validate_persisted_result(
            engine,
            payload,
            now=datetime(2026, 8, 26, 17, 45, tzinfo=exact.SHANGHAI),
            expected_session="2026-08-25",
        )

    invalid = dict(payload)
    invalid["collection"] = dict(payload["collection"])
    invalid["collection"]["daily_report"] = {
        **payload["collection"]["daily_report"],
        "fetched_pages": 0,
    }
    invalid.pop("receipt_id")
    invalid = exact._signed(invalid)
    assert exact.validate_task_result(invalid, 0) == "failed"


def test_transient_source_block_remains_scheduler_retryable():
    transient = exact._failure(
        dataset="daily",
        trade_date="2026-08-26",
        error=exact.AListDataBlocked(
            "DATA_BLOCKED: Eastmoney request failed after 4 attempts"
        ),
    )
    terminal = exact._failure(
        dataset="daily",
        trade_date="2026-08-26",
        error=exact.AListDataBlocked(
            "DATA_BLOCKED: alist runtime schema differs"
        ),
    )

    assert transient["retryable"] is True
    assert exact.validate_task_result(transient, 2) == "failed"
    assert terminal["retryable"] is False
    assert exact.validate_task_result(terminal, 2) == "blocked"
