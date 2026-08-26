from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.common import hot_rank_source_contract as contract
from server.common import scheduler_validation
from tools import fetch_hot_pop_rank_east, fetch_hot_rank_sina, fetch_hot_rank_xq


TARGET = "2026-08-26"
NOW = datetime(2026, 8, 27, 4, 0)
CURRENT_NOW = datetime(2026, 8, 26, 17, 30)


def _codes() -> list[str]:
    return [f"6{index:05d}" for index in range(1, 101)]


def _east_rows() -> list[dict]:
    return [
        {
            "rank": rank,
            "stock_code": code,
            "short_name": f"EAST-{rank}",
            "rank_change": None,
            "his_rank": None,
            "price": None,
            "price_change": None,
            "change_pct": None,
            "hot_value": 101 - rank,
            "pop_tag": "历史排名",
            "concept_tag": None,
        }
        for rank, code in enumerate(_codes(), 1)
    ]


def _east_evidence(date_text: str = TARGET) -> list[dict]:
    return [
        {
            "rank": rank,
            "stock_code": code,
            "request_src_security_code": f"SH{code}",
            "provider_response": {
                "calcTime": f"{date_text} 00:00:00",
                "rank": rank,
            },
        }
        for rank, code in enumerate(_codes(), 1)
    ]


def _east_history_frame(
    batch_at: datetime,
    *,
    relabelled_current: bool = False,
) -> pd.DataFrame:
    frame = pd.DataFrame(_east_rows())
    if relabelled_current:
        frame["rank_change"] = 0
        frame["his_rank"] = 0
        frame["price"] = 10.0
        frame["price_change"] = 0.1
        frame["change_pct"] = 1.0
        frame["pop_tag"] = "排名持平"
    frame["snapshot_date"] = TARGET
    frame["etl_sync_at"] = batch_at
    return frame


def _east_history_receipt(frame: pd.DataFrame) -> dict:
    batch_at = frame.iloc[0]["etl_sync_at"]
    inventory = contract.validate_rank_inventory(
        frame.to_dict(orient="records"),
        task_type=contract.HOT_POP_EAST_TASK_TYPE,
        target_date=TARGET,
    )
    evidence = contract.validate_east_history_date_evidence(
        _east_evidence(),
        target_date=TARGET,
    )
    return contract.build_pass_receipt(
        task_type=contract.HOT_POP_EAST_TASK_TYPE,
        provider=contract.EAST_HISTORY_PROVIDER,
        source_capability=contract.AUTHORITATIVE_DATED_HISTORY,
        requested_date=TARGET,
        started_at=datetime(2026, 8, 27, 4, 0),
        captured_at=batch_at,
        published_at=datetime(2026, 8, 27, 4, 2),
        batch_at=batch_at.isoformat(sep=" "),
        inventory=inventory,
        date_evidence=evidence,
    )


def _sina_rows(count: int = 100) -> list[dict]:
    return [
        {
            "rank": rank,
            "stock_code": code,
            "short_name": f"SINA-{rank}",
            "price": 10.0,
            "price_change": 0.1,
            "change_pct": 1.0,
            "amount": 1000.0,
            "volume": 100.0,
            "market_capital": 10000.0,
            "turnover_ratio": 2.0,
        }
        for rank, code in list(enumerate(_codes(), 1))[:count]
    ]


def _xq_frame(count: int = 100) -> pd.DataFrame:
    rows = [
        {
            "rank": rank,
            "stock_code": code,
            "short_name": f"XQ-{rank}",
            "current": 10.0,
            "percent": 1.0,
            "chg": 0.1,
            "amount": 1000.0,
            "market_capital": 10000.0,
            "followers": 100,
            "sector": "sector",
            "exchange": "SH",
            "increment": 1,
            "diff": 0,
        }
        for rank, code in list(enumerate(_codes(), 1))[:count]
    ]
    return pd.DataFrame(rows)


def _install_memory_publish(monkeypatch, module, *, clock: datetime = NOW):
    engine = object()
    writes: list[pd.DataFrame] = []
    monkeypatch.setattr(module, "create_batch_engine", lambda: engine)
    guard_name = (
        "_run_ddl" if module is fetch_hot_rank_sina
        else "_ensure_snapshot_date_column"
    )
    monkeypatch.setattr(module, guard_name, lambda _engine: None)
    monkeypatch.setattr(
        module,
        "shanghai_now",
        lambda value=None: value.replace(microsecond=0) if value is not None else clock,
    )
    monkeypatch.setattr(
        module,
        "replace_table_rows",
        lambda frame, *_args, **_kwargs: writes.append(frame.copy()),
    )
    monkeypatch.setattr(
        module,
        "_readback_hot_rank",
        lambda *_args: writes[-1].to_dict(orient="records"),
    )
    return engine, writes


def test_current_capture_window_blocks_wrong_date_and_preclose(monkeypatch):
    with pytest.raises(contract.HotRankDataBlocked, match="HISTORICAL_LABEL"):
        contract.require_current_capture_window(
            object(),
            task_type=contract.HOT_RANK_SINA_TASK_TYPE,
            requested_date=TARGET,
            now=NOW,
        )

    monkeypatch.setattr(
        contract,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-25",
    )
    with pytest.raises(contract.HotRankDataBlocked, match="NOT_CLOSED"):
        contract.require_current_capture_window(
            object(),
            task_type=contract.HOT_POP_EAST_TASK_TYPE,
            requested_date=TARGET,
            now=datetime(2026, 8, 26, 16, 0),
        )


def test_exact_inventory_rejects_partial_and_duplicate_codes():
    with pytest.raises(RuntimeError, match="exact Top100"):
        contract.validate_rank_inventory(
            _sina_rows(99),
            task_type=contract.HOT_RANK_SINA_TASK_TYPE,
        )
    duplicate = _xq_frame().to_dict(orient="records")
    duplicate[-1]["stock_code"] = duplicate[0]["stock_code"]
    with pytest.raises(RuntimeError, match="code/rank inventory"):
        contract.validate_rank_inventory(
            duplicate,
            task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        )


def test_east_historical_publish_uses_only_dated_history_and_readback(monkeypatch):
    _engine, writes = _install_memory_publish(monkeypatch, fetch_hot_pop_rank_east)
    monkeypatch.setattr(
        fetch_hot_pop_rank_east,
        "_fetch_current_items",
        lambda: pytest.fail("historical publication touched current-only endpoint"),
    )
    monkeypatch.setattr(
        fetch_hot_pop_rank_east,
        "_fetch_historical_rows",
        lambda _engine, _date: (_east_rows(), _east_evidence()),
    )

    receipt = fetch_hot_pop_rank_east.fetch_hot_pop_rank_east(
        TARGET,
        now=NOW,
    )

    assert len(writes) == 1 and len(writes[0]) == 100
    assert set(writes[0]["snapshot_date"]) == {TARGET}
    assert receipt["provider"] == contract.EAST_HISTORY_PROVIDER
    assert receipt["source_capability"] == contract.AUTHORITATIVE_DATED_HISTORY
    assert receipt["provider_date_field"] == "calcTime"
    assert receipt["provider_date_count"] == 100
    assert receipt["provider_evidence_schema"] == (
        contract.EAST_HISTORY_EVIDENCE_SCHEMA
    )
    assert len(receipt["provider_response_evidence"]) == 100
    assert all(
        item["provider_calc_time"] == f"{TARGET} 00:00:00"
        for item in receipt["provider_response_evidence"]
    )
    assert len(json.dumps(receipt, ensure_ascii=False).encode("utf-8")) < 24000
    assert contract.receipt_id_valid(receipt)


def test_east_historical_publish_rejects_wrong_calc_time_before_write(monkeypatch):
    _engine, writes = _install_memory_publish(monkeypatch, fetch_hot_pop_rank_east)
    monkeypatch.setattr(
        fetch_hot_pop_rank_east,
        "_fetch_historical_rows",
        lambda _engine, _date: (_east_rows(), _east_evidence("2026-08-27")),
    )
    with pytest.raises(RuntimeError, match="calcTime differs"):
        fetch_hot_pop_rank_east.fetch_hot_pop_rank_east(TARGET, now=NOW)
    assert writes == []


def test_east_history_fetch_preserves_exact_gethislist_response_identity(
    monkeypatch,
):
    monkeypatch.setattr(
        fetch_hot_pop_rank_east,
        "_post_data",
        lambda *_args, **_kwargs: [
            {"calcTime": "2026-08-25 00:00:00", "rank": 9},
            {"calcTime": f"{TARGET} 15:01:02", "rank": "7"},
        ],
    )
    row = fetch_hot_pop_rank_east._fetch_history_date_row("600001", TARGET)
    assert row == {
        "rank": 7,
        "stock_code": "600001",
        "request_src_security_code": "SH600001",
        "provider_response": {
            "calcTime": f"{TARGET} 15:01:02",
            "rank": "7",
        },
    }


def test_east_historical_receipt_rejects_self_signed_date_digest_without_raw_response():
    frame = _east_history_frame(datetime(2026, 8, 27, 4, 1))
    inventory = contract.validate_rank_inventory(
        frame.to_dict(orient="records"),
        task_type=contract.HOT_POP_EAST_TASK_TYPE,
        target_date=TARGET,
    )
    with pytest.raises(ValueError, match="raw response evidence is missing"):
        contract.build_pass_receipt(
            task_type=contract.HOT_POP_EAST_TASK_TYPE,
            provider=contract.EAST_HISTORY_PROVIDER,
            source_capability=contract.AUTHORITATIVE_DATED_HISTORY,
            requested_date=TARGET,
            started_at=datetime(2026, 8, 27, 4, 0),
            captured_at=datetime(2026, 8, 27, 4, 1),
            published_at=datetime(2026, 8, 27, 4, 2),
            batch_at="2026-08-27 04:01:00",
            inventory=inventory,
            date_evidence={
                "provider_date_field": "calcTime",
                "provider_date_count": 100,
                "provider_date_sha256": "a" * 64,
            },
        )

    valid = _east_history_receipt(frame)
    weak = {
        key: value
        for key, value in valid.items()
        if key not in {
            "receipt_id",
            "provider_evidence_schema",
            "provider_response_evidence",
            "provider_response_evidence_sha256",
        }
    }
    weak["provider_date_sha256"] = "b" * 64
    weak["batch_id"] = contract._hot_rank_batch_id(weak)
    weak = contract.with_receipt_id(weak)
    assert contract.receipt_id_valid(weak)
    assert scheduler_validation.scheduler_output_status(
        {"task_type": contract.HOT_POP_EAST_TASK_TYPE},
        json.dumps(weak, ensure_ascii=False),
        return_code=0,
    ) == "failed"


def test_east_db_validation_rejects_current_payload_relabelled_as_history():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    frame = _east_history_frame(
        datetime(2026, 8, 27, 4, 1),
        relabelled_current=True,
    )
    frame.to_sql("st_hot_pop_rank_east", engine, index=False)
    receipt = _east_history_receipt(frame)

    with pytest.raises(RuntimeError, match="current payload relabelling prohibited"):
        contract.validate_persisted_hot_rank_receipt(
            engine,
            receipt,
            datetime(2026, 8, 27, 4, 0),
            datetime(2026, 8, 27, 4, 3),
            expected_target_date=TARGET,
        )
    result = scheduler_validation.validate_scheduler_task_result(
        {
            "task_type": contract.HOT_POP_EAST_TASK_TYPE,
            "_trigger_source": "release_catchup",
            "_release_target_date": TARGET,
        },
        engine=engine,
        started_at=datetime(2026, 8, 27, 4, 0),
        now=datetime(2026, 8, 27, 4, 3),
        output=json.dumps(receipt, ensure_ascii=False),
    )
    assert result.checked is True
    assert result.ok is False
    assert "current payload relabelling prohibited" in result.message


def test_scheduler_accepts_exact_east_gethislist_evidence_bound_to_db_batch():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    frame = _east_history_frame(datetime(2026, 8, 27, 4, 1))
    frame.to_sql("st_hot_pop_rank_east", engine, index=False)
    receipt = _east_history_receipt(frame)
    task = {
        "task_type": contract.HOT_POP_EAST_TASK_TYPE,
        "_trigger_source": "release_catchup",
        "_release_target_date": TARGET,
    }
    output = json.dumps(receipt, ensure_ascii=False)
    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "success"
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        started_at=datetime(2026, 8, 27, 4, 0),
        now=datetime(2026, 8, 27, 4, 3),
        output=output,
    )
    assert result.checked is True
    assert result.ok is True
    assert "exact persisted Top100 verified" in result.message


@pytest.mark.parametrize(
    "module,function",
    [
        (fetch_hot_rank_xq, fetch_hot_rank_xq.fetch_hot_rank_xq),
    ],
)
def test_current_only_collectors_reject_historical_date_before_database(
    monkeypatch,
    module,
    function,
):
    monkeypatch.setattr(
        module,
        "create_batch_engine",
        lambda: pytest.fail("historical current-only request reached database"),
        raising=False,
    )
    with pytest.raises(contract.HotRankDataBlocked, match="HISTORICAL_LABEL"):
        function(TARGET, now=NOW)


def test_sina_current_collector_blocks_before_provider_or_database(monkeypatch):
    monkeypatch.setattr(
        fetch_hot_rank_sina,
        "_run_ddl",
        lambda *_args, **_kwargs: pytest.fail("Sina block reached database"),
    )
    monkeypatch.setattr(
        fetch_hot_rank_sina,
        "_fetch_sina_rows",
        lambda: pytest.fail("Sina block reached unverifiable provider"),
    )

    with pytest.raises(
        contract.HotRankDataBlocked,
        match="PROVIDER_ATTENTION_SEMANTICS_UNVERIFIABLE",
    ):
        fetch_hot_rank_sina.fetch_hot_rank_sina(TARGET, now=CURRENT_NOW)

    with pytest.raises(
        contract.HotRankDataBlocked,
        match="PROVIDER_ATTENTION_SEMANTICS_UNVERIFIABLE",
    ):
        fetch_hot_rank_sina.fetch_hot_rank_sina(TARGET, now=NOW)


def test_xueqiu_exact_current_publish_has_readback_receipt(monkeypatch):
    _engine, writes = _install_memory_publish(
        monkeypatch,
        fetch_hot_rank_xq,
        clock=CURRENT_NOW,
    )
    monkeypatch.setattr(
        fetch_hot_rank_xq,
        "require_current_capture_window",
        lambda *_args, **_kwargs: CURRENT_NOW,
    )
    monkeypatch.setattr(fetch_hot_rank_xq, "_init_cookie", lambda: None)
    monkeypatch.setattr(fetch_hot_rank_xq, "_fetch_hot_rank_xq", _xq_frame)

    receipt = fetch_hot_rank_xq.fetch_hot_rank_xq(TARGET, now=CURRENT_NOW)

    assert len(writes[0]) == 100
    assert receipt["row_count"] == 100
    assert receipt["source_capability"] == contract.CURRENT_SNAPSHOT_ONLY
    assert contract.receipt_id_valid(receipt)


def test_exported_current_receipt_parser_and_db_revalidation_are_exact(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    batch_at = datetime(2026, 8, 26, 17, 21)
    frame = _xq_frame()
    frame["snapshot_date"] = TARGET
    frame["etl_sync_at"] = batch_at
    frame.to_sql("st_hot_rank_xq", engine, index=False)
    provider_inventory = contract.validate_rank_inventory(
        _xq_frame().to_dict(orient="records"),
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
    )
    persisted = frame.to_dict(orient="records")
    persisted_inventory = contract.validate_rank_inventory(
        persisted,
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        target_date=TARGET,
    )
    assert (
        provider_inventory["provider_payload_sha256"]
        == persisted_inventory["provider_payload_sha256"]
    )
    receipt = contract.build_pass_receipt(
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        provider=contract.HOT_RANK_CURRENT_PROVIDERS[
            contract.HOT_RANK_XQ_TASK_TYPE
        ],
        source_capability=contract.CURRENT_SNAPSHOT_ONLY,
        requested_date=TARGET,
        started_at=datetime(2026, 8, 26, 17, 20),
        captured_at=batch_at,
        published_at=datetime(2026, 8, 26, 17, 22),
        batch_at=batch_at.isoformat(sep=" "),
        inventory=persisted_inventory,
    )
    output = f"collector log\n{json.dumps(receipt)}\n"
    assert contract.parse_hot_rank_receipt(output) == receipt
    assert contract.basic_receipt_disposition(
        receipt,
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        return_code=0,
    ) == "success"
    monkeypatch.setattr(
        contract,
        "require_current_capture_window",
        lambda *_args, **_kwargs: batch_at,
    )
    verified = contract.validate_persisted_hot_rank_receipt(
        engine,
        receipt,
        datetime(2026, 8, 26, 17, 20),
        datetime(2026, 8, 26, 17, 23),
        expected_target_date=TARGET,
    )
    assert verified["row_count"] == 100

    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_hot_rank_xq
               SET stock_code='600001'
             WHERE `rank`=100
        """))
    with pytest.raises(RuntimeError, match="code/rank inventory"):
        contract.validate_persisted_hot_rank_receipt(
            engine,
            receipt,
            datetime(2026, 8, 26, 17, 20),
            datetime(2026, 8, 26, 17, 23),
            expected_target_date=TARGET,
        )


def test_blocked_receipt_is_formal_and_requires_exit_two():
    receipt = contract.build_blocked_receipt(
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        requested_date=TARGET,
        started_at=NOW,
        reason="CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED",
    )
    assert contract.basic_receipt_disposition(
        receipt,
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        return_code=2,
    ) == "blocked"
    assert contract.basic_receipt_disposition(
        receipt,
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        return_code=0,
    ) == "failed"


def test_scheduler_requires_and_revalidates_exact_hot_rank_receipt(monkeypatch):
    inventory = contract.validate_rank_inventory(
        _xq_frame().to_dict(orient="records"),
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
    )
    receipt = contract.build_pass_receipt(
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        provider=contract.HOT_RANK_CURRENT_PROVIDERS[
            contract.HOT_RANK_XQ_TASK_TYPE
        ],
        source_capability=contract.CURRENT_SNAPSHOT_ONLY,
        requested_date=TARGET,
        started_at=datetime(2026, 8, 26, 17, 20),
        captured_at=datetime(2026, 8, 26, 17, 21),
        published_at=datetime(2026, 8, 26, 17, 22),
        batch_at="2026-08-26 17:21:00",
        inventory={
            **inventory,
            "persisted_row_sha256": "1" * 64,
        },
    )
    output = json.dumps(receipt, ensure_ascii=False)
    task = {
        "task_type": contract.HOT_RANK_XQ_TASK_TYPE,
        "_trigger_source": "release_catchup",
        "_release_target_date": TARGET,
    }
    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "success"

    observed: dict[str, object] = {}

    def revalidate(
        engine,
        payload,
        started_at,
        now,
        expected_target_date=None,
    ):
        observed.update(
            engine=engine,
            payload=payload,
            started_at=started_at,
            now=now,
            expected_target_date=expected_target_date,
        )
        return {"row_count": 100}

    monkeypatch.setattr(
        scheduler_validation,
        "validate_persisted_hot_rank_receipt",
        revalidate,
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        started_at=datetime(2026, 8, 26, 17, 20),
        now=datetime(2026, 8, 26, 17, 23),
        output=output,
    )
    assert result.checked is True
    assert result.ok is True
    assert observed["engine"] is engine
    assert observed["payload"] == receipt
    assert observed["expected_target_date"] == TARGET

    tampered = json.dumps({**receipt, "row_count": 99})
    assert scheduler_validation.scheduler_output_status(
        task,
        tampered,
        return_code=0,
    ) == "failed"


def test_sina_only_exact_permanent_block_is_accepted_by_scheduler():
    receipt = contract.build_blocked_receipt(
        task_type=contract.HOT_RANK_SINA_TASK_TYPE,
        requested_date=TARGET,
        started_at=NOW,
        reason=contract.SINA_ATTENTION_DATA_BLOCK_REASON,
    )
    task = {"task_type": contract.HOT_RANK_SINA_TASK_TYPE}
    output = json.dumps(receipt, ensure_ascii=False)
    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=2,
    ) == "blocked"

    wrong_reason = contract.with_receipt_id({
        **{key: value for key, value in receipt.items() if key != "receipt_id"},
        "reason": "CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED",
    })
    assert scheduler_validation.scheduler_output_status(
        task,
        json.dumps(wrong_reason, ensure_ascii=False),
        return_code=2,
    ) == "failed"
    with pytest.raises(ValueError, match="permanent provider-semantics block"):
        contract.build_blocked_receipt(
            task_type=contract.HOT_RANK_SINA_TASK_TYPE,
            requested_date=TARGET,
            started_at=NOW,
            reason="CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED",
        )


def test_self_signed_sina_pass_is_rejected_before_readiness(monkeypatch):
    inventory = contract.validate_rank_inventory(
        _sina_rows(),
        task_type=contract.HOT_RANK_SINA_TASK_TYPE,
    )
    with pytest.raises(ValueError, match="PASS receipt prohibited"):
        contract.build_pass_receipt(
            task_type=contract.HOT_RANK_SINA_TASK_TYPE,
            provider=contract.HOT_RANK_CURRENT_PROVIDERS[
                contract.HOT_RANK_SINA_TASK_TYPE
            ],
            source_capability=contract.CURRENT_SNAPSHOT_ONLY,
            requested_date=TARGET,
            started_at=datetime(2026, 8, 26, 17, 20),
            captured_at=datetime(2026, 8, 26, 17, 21),
            published_at=datetime(2026, 8, 26, 17, 22),
            batch_at="2026-08-26 17:21:00",
            inventory={**inventory, "persisted_row_sha256": "1" * 64},
        )
    fake = contract.with_receipt_id({
        "schema": contract.HOT_RANK_RESULT_SCHEMA,
        "status": "PASS",
        "task_type": contract.HOT_RANK_SINA_TASK_TYPE,
        "dataset": contract.HOT_RANK_DATASETS[
            contract.HOT_RANK_SINA_TASK_TYPE
        ],
        "provider": contract.HOT_RANK_CURRENT_PROVIDERS[
            contract.HOT_RANK_SINA_TASK_TYPE
        ],
        "source_capability": contract.CURRENT_SNAPSHOT_ONLY,
        "requested_date": TARGET,
        "data_date": TARGET,
        "started_at": "2026-08-26 17:20:00",
        "captured_at": "2026-08-26 17:21:00",
        "published_at": "2026-08-26 17:22:00",
        "batch_at": "2026-08-26 17:21:00",
        **inventory,
        "persisted_row_sha256": "1" * 64,
        "batch_id": "2" * 64,
    })
    task = {
        "task_type": contract.HOT_RANK_SINA_TASK_TYPE,
        "_trigger_source": "release_catchup",
        "_release_target_date": TARGET,
    }
    output = json.dumps(fake, ensure_ascii=False)
    assert contract.receipt_id_valid(fake)
    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "failed"

    monkeypatch.setattr(
        scheduler_validation,
        "validate_persisted_hot_rank_receipt",
        lambda *_args, **_kwargs: pytest.fail("Sina PASS reached DB validation"),
    )
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=create_engine("sqlite+pysqlite:///:memory:"),
        started_at=datetime(2026, 8, 26, 17, 20),
        now=datetime(2026, 8, 26, 17, 23),
        output=output,
    )
    assert result.checked is True
    assert result.ok is False
    assert contract.SINA_ATTENTION_DATA_BLOCK_REASON in result.message


def test_scheduler_preserves_hot_rank_data_blocked_semantics():
    receipt = contract.build_blocked_receipt(
        task_type=contract.HOT_RANK_XQ_TASK_TYPE,
        requested_date=TARGET,
        started_at=NOW,
        reason="CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED",
    )
    task = {"task_type": contract.HOT_RANK_XQ_TASK_TYPE}
    output = json.dumps(receipt, ensure_ascii=False)
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


def test_legacy_history_loops_cannot_call_ths_or_publish_fused_data():
    root = Path(__file__).resolve().parents[1]
    shell = (root / "tools" / "pull_history.sh").read_text(encoding="utf-8")
    python = (root / "tools" / "pull_all.py").read_text(encoding="utf-8")
    powershell = (root / "tools" / "pull_2024.ps1").read_text(encoding="utf-8")
    fast_loop = (root / "tools" / "pull_loop_fast.py").read_text(encoding="utf-8")

    assert 'fetch_hot_rank_ths.py "$d"' not in shell
    assert 'fetch_hot_concept_ths_daily.py "$d"' not in shell
    assert "tools/merge_hot_rank.py $(date" not in shell
    assert '[PY, "tools/fetch_hot_rank_ths.py", date_str]' not in python
    assert '[PY, "tools/fetch_hot_concept_ths_daily.py", date_str]' not in python
    assert '[PY, "tools/fetch_hot_pop_rank_east.py", date_str]' not in python
    assert '[PY, "tools/merge_hot_rank.py", date_str' not in python
    assert "fetch_hot_rank_ths.py $dateStr" not in powershell
    assert "fetch_hot_concept_ths_daily.py $dateStr" not in powershell
    assert "fetch_hot_pop_rank_east.py $dateStr" not in powershell
    assert "merge_hot_rank.py $dateStr" not in powershell
    assert "fetch_hot_rank_ths.py" not in fast_loop
    assert "fetch_hot_concept_ths_daily.py" not in fast_loop
    assert "fetch_hot_pop_rank_east.py" not in fast_loop
    assert "merge_hot_rank.py" not in fast_loop


def test_legacy_xq_aggregator_stops_before_historical_fetch_or_fusion():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(root / "tools" / "run_all_changes.py"), TARGET],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 2
    assert "未生成融合榜" in result.stderr
