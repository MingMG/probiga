from __future__ import annotations

import inspect
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

from tools import (
    sync_capital_flow_direct,
    sync_capital_flow_efinance,
    sync_capital_flow_efinance_all,
    sync_kline_sina,
)
from server.common import batch_db, mysql_lock
from biz.stock_market import stock_kline_akshare


ROOT = Path(__file__).resolve().parents[1]
LEGACY_DIRECT_WRITERS = (
    "tools/calc_capital_flow_approx.py",
    "tools/backfill_capital_flow.py",
    "tools/crawl_stock_kline.py",
    "tools/crawl_realtime_batch.py",
    "tools/sync_kline_adata.py",
    "tools/sync_capital_flow_batch.py",
    "tools/sync_capital_flow_direct.py",
    "tools/sync_capital_flow_efinance.py",
    "tools/sync_capital_flow_efinance_all.py",
    "tools/sync_capital_flow_push2delay.py",
    "tools/sync_capital_flow_ths.py",
    "tools/sync_capital_flow_baidu.py",
    "tools/sync_capital_flow_baidu_direct.py",
    "tools/crawl_all_stock_flow.py",
    "tools/crawl_stock_fund_flow.py",
    "tools/_kline_fill_local.py",
    "tools/_kline_fill_akshare.py",
    "tools/_kline_fetch_local.py",
    "tools/fetch_sm_stock_capital_flow_daily.py",
    "tools/fetch_sm_stock_kline_daily.py",
    "tools/sync_kline_sina.py",
    "biz/stock_market/stock_kline_akshare.py",
)


def test_legacy_sync_entrypoints_have_no_default_full_table_preclear():
    for module in (
        sync_kline_sina,
        sync_capital_flow_direct,
        sync_capital_flow_efinance,
        sync_capital_flow_efinance_all,
    ):
        source = inspect.getsource(module.main).upper()
        assert "DELETE FROM SM_STOCK_KLINE" not in source
        assert "DELETE FROM SM_STOCK_CAPITAL_FLOW_DAILY" not in source
        assert ".TO_SQL(" not in source


@pytest.mark.parametrize(
    "module",
    [
        sync_capital_flow_direct,
        sync_capital_flow_efinance,
        sync_capital_flow_efinance_all,
    ],
)
def test_flow_helpers_replace_only_fetched_code_dates(monkeypatch, module):
    calls = []
    frame = pd.DataFrame(
        [
            {"stock_code": "600000", "trade_date": "2026-08-22", "main_net_inflow": 1},
            {"stock_code": "600000", "trade_date": "2026-08-25", "main_net_inflow": 2},
        ]
    )

    def _replace(df, table_name, engine, **kwargs):
        calls.append((df.copy(), table_name, engine, kwargs))
        return len(df)

    monkeypatch.setattr(module, "replace_table_rows_exact_keys", _replace)
    engine = object()
    assert module.replace_flow_partitions(engine, frame, "600000") == 2
    assert calls[0][1:3] == ("sm_stock_capital_flow_daily", engine)
    assert calls[0][3]["key_columns"] == ("stock_code", "trade_date")
    assert calls[0][3]["lock_name"] == mysql_lock.CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME


def test_kline_helper_propagates_atomic_write_failure(monkeypatch):
    frame = pd.DataFrame(
        [
            {
                "stock_code": "600000",
                "trade_date": "2026-08-25",
                "k_type": 1,
                "adjust_type": 1,
                "close": 10,
            }
        ]
    )

    def _fail(*_args, **_kwargs):
        raise RuntimeError("atomic write failed")

    monkeypatch.setattr(sync_kline_sina, "replace_table_rows_exact_keys", _fail)
    with pytest.raises(RuntimeError, match="atomic write failed"):
        sync_kline_sina.replace_kline_partitions(object(), frame, "600000")


def test_all_legacy_direct_writers_use_exact_key_atomic_publication():
    for relative_path in LEGACY_DIRECT_WRITERS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        upper = source.upper()
        assert "DELETE FROM SM_STOCK_KLINE" not in upper, relative_path
        assert "DELETE FROM `SM_STOCK_KLINE`" not in upper, relative_path
        assert "DELETE FROM SM_STOCK_CAPITAL_FLOW_DAILY" not in upper, relative_path
        assert "DELETE FROM `SM_STOCK_CAPITAL_FLOW_DAILY`" not in upper, relative_path
        assert '.TO_SQL("SM_STOCK_KLINE"' not in upper, relative_path
        assert ".TO_SQL('SM_STOCK_KLINE'" not in upper, relative_path
        assert '.TO_SQL("SM_STOCK_CAPITAL_FLOW_DAILY"' not in upper, relative_path
        assert ".TO_SQL('SM_STOCK_CAPITAL_FLOW_DAILY'" not in upper, relative_path


def test_deletion_only_kline_range_api_is_fail_closed():
    source = inspect.getsource(stock_kline_akshare.delete_kline_range).upper()
    assert "DELETE FROM" not in source
    with pytest.raises(RuntimeError, match="deletion-only K-line range refresh is disabled"):
        stock_kline_akshare.delete_kline_range(
            object(), "600000", 1, 0, "2026-08-22", "2026-08-25",
        )


def test_baidu_direct_writer_uses_exact_flow_identity_and_canonical_lock():
    source = (ROOT / "tools/sync_capital_flow_baidu_direct.py").read_text(encoding="utf-8")
    assert "replace_table_rows_exact_keys(" in source
    assert 'key_columns=("stock_code", "trade_date")' in source
    assert "lock_name=CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME" in source


class _ExactKeyConnection:
    def __init__(self, events):
        self.events = events
        self._in_transaction = True

    def in_transaction(self):
        return self._in_transaction

    def commit(self):
        self.events.append("commit-read")
        self._in_transaction = False

    @contextmanager
    def begin(self):
        assert not self._in_transaction
        self._in_transaction = True
        self.events.append("begin-write")
        try:
            yield self
        except Exception:
            self.events.append("rollback-write")
            raise
        else:
            self.events.append("commit-write")
        finally:
            self._in_transaction = False

    def execute(self, statement, params):
        assert self._in_transaction
        self.events.append((str(statement), params))


def test_exact_key_helper_uses_lock_owner_connection_for_one_transaction(monkeypatch):
    events = []
    held_connection = _ExactKeyConnection(events)

    @contextmanager
    def _lock(engine, name, *, timeout_seconds=0, connection=None):
        assert engine == "engine"
        assert name == mysql_lock.STOCK_KLINE_FREEZE_LOCK_NAME
        events.append("lock")
        yield connection or held_connection
        events.append("unlock")

    def _write(frame, table_name, target, **kwargs):
        assert target is held_connection
        assert target.in_transaction()
        events.append(("write", table_name, len(frame), kwargs))
        return len(frame)

    monkeypatch.setattr(mysql_lock, "mysql_named_lock", _lock)
    monkeypatch.setattr(batch_db, "write_frame", _write)
    frame = pd.DataFrame([
        {"stock_code": "600000", "trade_date": "2026-08-22", "k_type": 1, "adjust_type": 0},
        {"stock_code": "600001", "trade_date": "2026-08-25", "k_type": 1, "adjust_type": 0},
    ])
    assert batch_db.replace_table_rows_exact_keys(
        frame,
        "sm_stock_kline",
        "engine",
        key_columns=("stock_code", "trade_date", "k_type", "adjust_type"),
        lock_name=mysql_lock.STOCK_KLINE_FREEZE_LOCK_NAME,
        delete_chunk_size=1,
    ) == 2
    assert events[0:3] == ["lock", "commit-read", "begin-write"]
    assert events[-2:] == ["commit-write", "unlock"]
    deletes = [event for event in events if isinstance(event, tuple) and str(event[0]).startswith("DELETE")]
    assert len(deletes) == 2
    assert all("`stock_code`" in sql and "`adjust_type`" in sql for sql, _ in deletes)


@pytest.mark.parametrize("bad_identity", [None, "", "  ", "nan", "NaT", "<NA>"])
def test_exact_key_helper_rejects_incomplete_business_identity(bad_identity):
    frame = pd.DataFrame([{"stock_code": bad_identity, "trade_date": "2026-08-25"}])
    with pytest.raises(ValueError, match="business identity"):
        batch_db.replace_table_rows_exact_keys(
            frame,
            "sm_stock_capital_flow_daily",
            object(),
            key_columns=("stock_code", "trade_date"),
            lock_name=mysql_lock.CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        )


def test_exact_key_helper_rejects_duplicate_business_identity():
    frame = pd.DataFrame([
        {"stock_code": "600000", "trade_date": "2026-08-25", "value": 1},
        {"stock_code": "600000", "trade_date": "2026-08-25", "value": 2},
    ])
    with pytest.raises(ValueError, match="duplicate business identities"):
        batch_db.replace_table_rows_exact_keys(
            frame,
            "sm_stock_capital_flow_daily",
            object(),
            key_columns=("stock_code", "trade_date"),
            lock_name=mysql_lock.CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        )


def test_exact_key_helper_rolls_back_delete_when_insert_fails(monkeypatch):
    events = []
    held_connection = _ExactKeyConnection(events)

    @contextmanager
    def _lock(*_args, **_kwargs):
        events.append("lock")
        try:
            yield held_connection
        finally:
            events.append("unlock")

    monkeypatch.setattr(mysql_lock, "mysql_named_lock", _lock)
    monkeypatch.setattr(
        batch_db,
        "write_frame",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("insert failed")),
    )
    frame = pd.DataFrame([{"stock_code": "600000", "trade_date": "2026-08-25"}])
    with pytest.raises(RuntimeError, match="insert failed"):
        batch_db.replace_table_rows_exact_keys(
            frame,
            "sm_stock_capital_flow_daily",
            object(),
            key_columns=("stock_code", "trade_date"),
            lock_name=mysql_lock.CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        )
    assert "rollback-write" in events
    assert events[-1] == "unlock"
