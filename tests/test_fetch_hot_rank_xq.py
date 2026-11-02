from types import SimpleNamespace
from datetime import datetime
import json
from unittest.mock import patch

import pytest

from server.common import hot_rank_source_contract as contract
from tools import fetch_hot_rank_xq


def _source_items():
    # Observed 2026-11-02 failure: native Top100 includes SH561010, a fund
    # (stock_type=13); filtering it leaves all 99 stocks, not a partial feed.
    stocks = [{"symbol": f"SH60{index:04d}", "name": f"stock-{index}",
               "exchange": "SH", "stock_type": 11, "current": 10.0}
              for index in range(1, 99)]
    stocks[0] = {"symbol": "SH688498", "name": "源杰科技", "exchange": "SH",
                 "stock_type": 82, "sub_type": "KSH", "current": 10.0}
    return stocks[:96] + [{"symbol": "SH561010", "name": "软件ETF华安",
                           "exchange": "SH", "stock_type": 13, "sub_type": "EBS"}] + stocks[96:] + [
        {"symbol": "BJ920001", "name": "北交所股票", "exchange": "BJ", "stock_type": 11,
         "current": 18.0}]


def test_xueqiu_preserves_native_top100_and_exact_stock_projection():
    response = SimpleNamespace(raise_for_status=lambda: None,
        json=lambda: {"error_code": 0, "data": {"items": _source_items()}})
    with patch.object(fetch_hot_rank_xq._SESSION, "get", return_value=response) as request:
        frame = fetch_hot_rank_xq._fetch_hot_rank_xq()
    assert request.call_args.kwargs["params"] == {"size": 100, "_type": 12, "type": 12}
    assert len(frame) == 99
    assert frame["rank"].tolist() == list(range(1, 100))
    assert "561010" not in set(frame["stock_code"])
    assert frame.iloc[-1]["stock_code"] == "920001"
    evidence = frame.attrs["source_evidence"]
    assert evidence["source_row_count"] == 100
    assert evidence["a_share_row_count"] == 99
    assert evidence["excluded_security_count"] == 1
    assert evidence["items"][96][0] == "SH561010"
    inventory = contract.validate_rank_inventory(frame.to_dict("records"),
        task_type=contract.HOT_RANK_XQ_TASK_TYPE, source_evidence=evidence)
    assert inventory["row_count"] == 99


def test_xueqiu_missing_source_item_remains_incomplete():
    with pytest.raises(RuntimeError, match="native Top100 inventory"):
        contract.xueqiu_source_inventory(_source_items()[:-1])


def test_xueqiu_mistyped_stock_or_duplicate_is_rejected():
    items = _source_items()
    items[96]["stock_type"] = 11
    with pytest.raises(RuntimeError, match="security type unsupported"):
        contract.xueqiu_source_inventory(items)
    items = _source_items()
    items[-1] = items[0]
    with pytest.raises(RuntimeError, match="security identity"):
        contract.xueqiu_source_inventory(items)


def test_xueqiu_projection_cannot_drop_a_stock_or_admit_the_etf():
    rows, evidence = contract.xueqiu_source_inventory(_source_items())
    with pytest.raises(RuntimeError, match="inventory differs"):
        contract.validate_rank_inventory(rows[:-1], task_type=contract.HOT_RANK_XQ_TASK_TYPE,
                                         source_evidence=evidence)
    tampered = [dict(row) for row in rows]
    tampered[-1]["stock_code"] = "600999"
    with pytest.raises(RuntimeError, match="projection differs"):
        contract.validate_rank_inventory(tampered, task_type=contract.HOT_RANK_XQ_TASK_TYPE,
                                         source_evidence=evidence)


def test_xueqiu_projection_requires_source_evidence_even_with_100_stock_rows():
    rows, _ = contract.xueqiu_source_inventory(_source_items())
    with pytest.raises(RuntimeError, match="source inventory evidence missing"):
        contract.validate_rank_inventory(rows, task_type=contract.HOT_RANK_XQ_TASK_TYPE)


def test_native_source_receipt_fits_real_scheduler_terminal_and_replay_budget(monkeypatch):
    from server.api import scheduler_runtime as runtime
    rows, evidence = contract.xueqiu_source_inventory(_source_items())
    target, batch = "2026-11-02", datetime(2026, 11, 2, 20, 0)
    persisted = [{**row, "snapshot_date": target, "etl_sync_at": batch} for row in rows]
    inventory = contract.validate_rank_inventory(persisted, task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        target_date=target, source_evidence=evidence)
    receipt = contract.build_pass_receipt(task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        provider=contract.HOT_RANK_CURRENT_PROVIDERS[contract.HOT_RANK_XQ_TASK_TYPE],
        source_capability=contract.CURRENT_SNAPSHOT_ONLY, requested_date=target,
        started_at=batch, captured_at=batch, published_at=batch,
        batch_at=batch.isoformat(sep=" "), inventory=inventory)
    stdout = json.dumps(receipt, ensure_ascii=False)
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "a" * 40)
    monkeypatch.setattr(runtime, "runtime_contract_build_sha", lambda **_kwargs: "a" * 40)
    task = {"id": 26, "task_name": "雪球热门Top100中的A股",
            "task_type": contract.HOT_RANK_XQ_TASK_TYPE, "_scheduler_target_trade_date": target}
    machine = runtime._terminal_history_output(stdout, "来源诊断\n" * 10000, stdout)
    validation = runtime._build_history_validation_evidence(task, run_uid="b" * 32,
        machine_output=stdout, status="success", exit_code=0, started_at=batch,
        validation_message="source=100 stocks=99 funds=1")
    terminal = runtime._history_output_with_validation_evidence(machine, validation)
    assert len(terminal.encode("utf8")) <= runtime._HISTORY_OUTPUT_LIMIT
    assert contract.parse_hot_rank_receipt(terminal) == receipt
