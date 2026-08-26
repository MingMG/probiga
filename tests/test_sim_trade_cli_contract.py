import json
import hashlib
from unittest.mock import Mock

from biz.analysis import sync_sim_trade


def _machine_receipts(stdout: str) -> list[dict]:
    return [
        json.loads(line)
        for line in stdout.splitlines()
        if line.strip().startswith("{")
    ]


def test_missing_recommendations_emit_one_blocked_receipt_and_exit_nonzero(
    monkeypatch, capsys
):
    engine = Mock(side_effect=AssertionError("signal pool must not be prepared"))
    monkeypatch.setattr(sync_sim_trade, "_previous_trade_date", lambda _date: "2026-08-25")
    monkeypatch.setattr(sync_sim_trade, "_recommendation_count", lambda _date: 0)
    monkeypatch.setattr(sync_sim_trade, "SimTradeEngine", engine)

    exit_code = sync_sim_trade.main(
        ["--prepare-signals", "--trade-date", "2026-08-26", "--json"]
    )

    receipts = _machine_receipts(capsys.readouterr().out)
    assert exit_code == 2
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["schema"] == sync_sim_trade.SIM_TRADE_TASK_RESULT_SCHEMA
    assert receipt["status"] == "DATA_BLOCKED"
    assert receipt["task_mode"] == "prepare_signals"
    assert receipt["trade_date"] == "2026-08-26"
    assert receipt["signal_date"] == "2026-08-25"
    assert receipt["recommendation_count"] == 0
    assert receipt["total_recommendations"] == 0
    assert receipt["signal_count"] == 0
    assert len(receipt["receipt_id"]) == 32
    assert len(receipt["result_sha256"]) == 64
    engine.assert_not_called()


def test_prepared_signal_pool_receipt_has_dates_and_counts(monkeypatch, capsys):
    monkeypatch.setattr(
        sync_sim_trade,
        "prepare_signals",
        lambda **_kwargs: {
            "status": "ok",
            "trade_date": "2026-08-26",
            "signal_date": "2026-08-25",
            "allowed_count": 7,
            "rejected_count": 20,
            "total_recommendations": 9,
            "recommendation_code_count": 9,
            "recommendation_code_set_hash": "a" * 64,
            "strategy_count": 3,
            "signal_identity_count": 7,
            "signal_identity_hash": "b" * 64,
            "counts": {"total": 7, "NEW": 7},
            "recommendation_prerequisite": {
                "status": "exists",
                "signal_date": "2026-08-25",
                "count": 9,
                "read_only": True,
            },
        },
    )

    exit_code = sync_sim_trade.main(
        ["--prepare-signals", "--trade-date", "2026-08-26", "--json"]
    )

    receipts = _machine_receipts(capsys.readouterr().out)
    assert exit_code == 0
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["status"] == "PASS"
    assert receipt["recommendation_count"] == 9
    assert receipt["total_recommendations"] == 9
    assert receipt["recommendation_code_count"] == 9
    assert receipt["recommendation_code_set_hash"] == "a" * 64
    assert receipt["strategy_count"] == 3
    assert receipt["signal_count"] == 7
    assert receipt["signal_identity_count"] == 7
    assert receipt["signal_identity_hash"] == "b" * 64
    assert receipt["allowed_count"] == 7
    assert receipt["rejected_count"] == 20
    unsigned = dict(receipt)
    result_sha256 = unsigned.pop("result_sha256")
    encoded = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    assert result_sha256 == hashlib.sha256(encoded).hexdigest()


def test_ok_without_actual_recommendations_is_data_blocked(monkeypatch, capsys):
    monkeypatch.setattr(
        sync_sim_trade,
        "prepare_signals",
        lambda **_kwargs: {
            "status": "ok",
            "trade_date": "2026-08-26",
            "signal_date": "2026-08-25",
            "total_recommendations": 0,
            "counts": {"total": 0},
            "recommendation_prerequisite": {
                "status": "exists",
                "signal_date": "2026-08-25",
                "count": 9,
            },
        },
    )

    exit_code = sync_sim_trade.main(
        ["--prepare-signals", "--trade-date", "2026-08-26", "--json"]
    )

    receipt = _machine_receipts(capsys.readouterr().out)[0]
    assert exit_code == 2
    assert receipt["status"] == "DATA_BLOCKED"
    assert receipt["recommendation_count"] == 9
    assert receipt["total_recommendations"] == 0


def test_prepare_pass_requires_exact_identity_manifest(monkeypatch, capsys):
    monkeypatch.setattr(
        sync_sim_trade,
        "prepare_signals",
        lambda **_kwargs: {
            "status": "ok",
            "trade_date": "2026-08-26",
            "signal_date": "2026-08-25",
            "total_recommendations": 2,
            "allowed_count": 1,
            "rejected_count": 5,
            "counts": {"total": 1},
            "recommendation_prerequisite": {
                "status": "exists",
                "signal_date": "2026-08-25",
                "count": 2,
            },
        },
    )

    exit_code = sync_sim_trade.main(
        ["--prepare-signals", "--trade-date", "2026-08-26", "--json"]
    )

    receipt = _machine_receipts(capsys.readouterr().out)[0]
    assert exit_code == 2
    assert receipt["status"] == "DATA_BLOCKED"
    assert receipt["reason"] == "signal pool identity/count contract is incomplete"
