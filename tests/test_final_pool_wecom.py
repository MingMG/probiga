from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

from biz.analysis import final_pool_wecom as sender
from integrations.wecom import delivery as wecom_delivery
from integrations.wecom.delivery import DeliveryResult


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INT)"
        ))
        connection.execute(text(
            "INSERT INTO si_trade_calendar (trade_date, trade_status) VALUES "
            "('2026-08-31', 1), ('2026-09-01', 1), "
            "('2026-09-02', 1), ('2026-09-03', 1)"
        ))
    return engine


def _receipt(day: str):
    return {
        "session": {"trade_date": day, "status": "PASS"},
        "receipt": {
            "trade_date": day,
            "status": "PASS",
            "release_id": "c" * 40,
            "governance_run_uid": ("a" if day.endswith("01") else "b") * 32,
            "analysis_run_uid": ("d" if day.endswith("01") else "e") * 32,
            "canonical_batch_status": "COMPLETED",
            "strategy_pool": {"status": "ACTIVE", "count": 1, "root": "f" * 64},
            "formal_pool": {"status": "ACTIVE", "count": 1, "root": "9" * 64},
            "api_checks": {"strategy_pool": "PASS", "formal_pool": "PASS"},
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    }


def _canonical(day: str):
    run_uid = ("a" if day.endswith("01") else "b") * 32
    item = {
        "stock_code": "000001",
        "stock_name": "平安银行",
        "rank_no": 1,
        "is_strategy_candidate": True,
        "dynamic_role": "LEADER",
        "primary_theme": "银行",
        "valid_until": "2026-09-03 09:25:59",
        "reasons": ["盘后证据通过"],
        "action_plan": {
            "label": "回踩区间且承接确认",
            "buy_range": {"low": 10.1, "high": 10.3},
            "protective_stop": 9.8,
        },
        "target": {"reference_price": 10.2},
    }
    return {
        "context": {
            "run_status": "COMPLETED",
            "decision_integrity_verified": True,
            "evidence_as_of": f"{day} 22:35:00",
            "knowledge_cutoff_at": f"{day} 22:20:00",
        },
        "run": {"status": "COMPLETED"},
        "pool": {
            "run_uid": run_uid,
            "trade_date": day,
            "run_status": "COMPLETED",
            "pool_readable": True,
            "decision_integrity_verified": True,
            "build_commit_sha": "c" * 40,
            "canonical_result_hash": "f" * 64,
            "items": [item],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    }


def _ticket(day: str, run_uid: str, build_sha: str, pool_hash: str):
    return {
        "date": day,
        "data": [{"stock_code": "000001"}],
        "total": 1,
        "identity_verified": True,
        "data_status": "READY",
        "run_uid": run_uid,
        "build_sha": build_sha,
        "canonical_pool_sha256": pool_hash,
    }


def _gate(_engine, pool, *, execution_session: str, now: datetime):
    del _engine, now
    return {
        "status": "COMPLETED",
        "session_date": execution_session,
        "cutoff_at": f"{execution_session} 09:25:59",
        "source_run_uid": pool["run_uid"],
        "gate_hash": "8" * 64,
        "assessments": [{
            "stock_code": "000001",
            "decision_rank": 1,
            "gate_status": "CONFIRMED",
            "advisory_action": "BUY_CANDIDATE",
            "quote_at": f"{execution_session} 09:25:30",
            "reasons": ["竞价价格与承接通过"],
            "alternative_set": [{
                "stock_code": "000002",
                "stock_name": "万科A",
                "relation": "OTHER_SCENARIO",
            }],
        }],
        "automatic_substitution": False,
        "order_authority": False,
    }


def test_two_exact_sessions_are_validated_before_complete_pool_delivery():
    engine = _engine()
    calls = []

    def deliver(_url, content, **kwargs):
        calls.append((content, kwargs))
        return DeliveryResult(
            delivery_id=f"delivery-{len(calls)}",
            success=True,
            segment_count=1,
            delivered_count=1,
            content_sha256=str(len(calls)) * 64,
        )

    result = sender.send_final_pool_batch(
        engine,
        target_trade_date="2026-09-02",
        now=datetime(2026, 9, 4, 1, 0),
        receipt_loader=lambda _engine, *, trade_date: _receipt(trade_date),
        canonical_loader=_canonical,
        ticket_loader=_ticket,
        gate_loader=_gate,
        delivery_fn=deliver,
        webhook_url="https://example.invalid/webhook",
    )

    assert result["status"] == "SUCCEEDED"
    assert result["covered_trade_dates"] == ["2026-09-01", "2026-09-02"]
    assert result["delivery_count"] == 2
    assert len(calls) == 2
    content, kwargs = calls[0]
    for required in (
        "盘后 #1", "竞价 #1", "dynamic_role=LEADER", "theme=银行",
        "buy_range=10.1~10.3", "reference=10.2", "protective_stop=9.8",
        "valid_until=2026-09-03 09:25:59", "gate=CONFIRMED",
        "action=BUY_CANDIDATE", "quote_at=2026-09-02 09:25:30",
        "alternative_set=[000002 万科A(OTHER_SCENARIO)]",
        "automatic_substitution=false",
    ):
        assert required in content
    identity = kwargs["audit_identity"]
    assert identity["governance_run_uid"] == "a" * 32
    assert identity["analysis_run_uid"] == "d" * 32
    assert identity["build_sha"] == "c" * 40
    assert identity["canonical_pool_sha256"] == "9" * 64
    assert identity["gate_hash"] == "8" * 64
    assert kwargs["idempotency_key"]


def test_sender_batch_replay_is_idempotent_across_calls(monkeypatch):
    engine = _engine()
    wecom_delivery.privileged_migrate_delivery_receipt_table(engine)
    outbound = []

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"errcode": 0, "errmsg": "ok"}

    class Client:
        def __init__(self, *, timeout):
            del timeout

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def post(self, url, *, json, timeout):
            outbound.append((url, json, timeout))
            return Response()

    monkeypatch.setattr(wecom_delivery.httpx, "Client", Client)
    kwargs = {
        "target_trade_date": "2026-09-02",
        "now": datetime(2026, 9, 4, 1, 0),
        "receipt_loader": lambda _engine, *, trade_date: _receipt(trade_date),
        "canonical_loader": _canonical,
        "ticket_loader": _ticket,
        "gate_loader": _gate,
        "delivery_fn": wecom_delivery.deliver_markdown,
        "webhook_url": "https://example.invalid/webhook",
    }

    first = sender.send_final_pool_batch(engine, **kwargs)
    second = sender.send_final_pool_batch(engine, **kwargs)

    assert len(outbound) == 2
    assert [item["delivery_id"] for item in first["deliveries"]] == [
        item["delivery_id"] for item in second["deliveries"]
    ]
    assert all(item["idempotent_replay"] is False for item in first["deliveries"])
    assert all(item["idempotent_replay"] is True for item in second["deliveries"])
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM sys_wecom_delivery_receipt "
            "WHERE delivery_kind='final_pool' AND status='SUCCEEDED'"
        )).scalar_one() == 2


@pytest.mark.parametrize("drift", ["governance_run", "governance_hash", "ticket_run"])
def test_hash_or_run_identity_drift_rejects_all_outbound_delivery(drift):
    engine = _engine()
    calls = []

    def canonical(day: str):
        result = _canonical(day)
        if drift == "governance_run" and day == "2026-09-02":
            result["pool"]["run_uid"] = "0" * 32
        if drift == "governance_hash" and day == "2026-09-02":
            result["pool"]["canonical_result_hash"] = "0" * 64
        return result

    def ticket(day: str, run_uid: str, build_sha: str, pool_hash: str):
        result = _ticket(day, run_uid, build_sha, pool_hash)
        if drift == "ticket_run" and day == "2026-09-02":
            result["run_uid"] = "0" * 32
        return result

    with pytest.raises(sender.FinalPoolDeliveryBlocked, match="drifted"):
        sender.send_final_pool_batch(
            engine,
            target_trade_date="2026-09-02",
            now=datetime(2026, 9, 4, 1, 0),
            receipt_loader=lambda _engine, *, trade_date: _receipt(trade_date),
            canonical_loader=canonical,
            ticket_loader=ticket,
            gate_loader=_gate,
            delivery_fn=lambda *_args, **_kwargs: calls.append(True),
            webhook_url="https://example.invalid/webhook",
        )
    assert calls == []


@pytest.mark.parametrize("mode", ["empty", "unfinished", "receipt_blocked"])
def test_empty_or_unfinished_canonical_pool_never_sends(mode):
    engine = _engine()
    calls = []

    def receipt_loader(_engine, *, trade_date: str):
        result = _receipt(trade_date)
        if mode == "receipt_blocked" and trade_date == "2026-09-02":
            result["session"]["status"] = "BLOCKED"
            result["receipt"]["status"] = "BLOCKED"
        return result

    def canonical(day: str):
        result = _canonical(day)
        if day == "2026-09-02" and mode == "empty":
            result["pool"]["items"] = []
        if day == "2026-09-02" and mode == "unfinished":
            result["run"]["status"] = "RUNNING"
        return result

    with pytest.raises(sender.FinalPoolDeliveryBlocked):
        sender.send_final_pool_batch(
            engine,
            target_trade_date="2026-09-02",
            now=datetime(2026, 9, 4, 1, 0),
            receipt_loader=receipt_loader,
            canonical_loader=canonical,
            ticket_loader=_ticket,
            gate_loader=_gate,
            delivery_fn=lambda *_args, **_kwargs: calls.append(True),
            webhook_url="https://example.invalid/webhook",
        )
    assert calls == []
