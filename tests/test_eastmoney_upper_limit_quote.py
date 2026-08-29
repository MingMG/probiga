from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, text

from server.common.analysis_pool_receipt import (
    build_preliminary_upper_subject_receipt,
)
from server.common.eastmoney_upper_limit_quote import (
    EASTMONEY_UPPER_LIMIT_CAPTURE_KIND,
    EASTMONEY_UPPER_LIMIT_PROVIDER,
    EastmoneyQuoteHttpResponse,
    EastmoneyUpperLimitQuoteBlocked,
    QmtUpperLimitQuoteTarget,
    _qmt_target_from_row,
    build_eastmoney_upper_limit_quote_run,
    build_eastmoney_upper_limit_subject,
    fetch_eastmoney_upper_limit_quote,
    parse_eastmoney_upper_limit_quote,
    publish_eastmoney_upper_limit_quote_run,
)
from server.common.qmt_attestation_contract import ATTESTATION_PROTOCOL_VERSION
from server.common.turnover_snapshot_schema import (
    FIELD_CAPTURE_ROW_TABLE,
    FIELD_CAPTURE_RUN_TABLE,
    TURNOVER_SNAPSHOT_SCHEMA,
)
from tools.sync_target_upper_limit_quote import _machine_receipt


TARGET = date(2026, 8, 28)
DECISION = datetime(2026, 8, 30, 9, 10)
CAPTURED = datetime(2026, 8, 30, 9, 0, 2)
HTTP_DATE = "Sun, 30 Aug 2026 01:00:00 GMT"
BUILD = "a" * 40


def _codes() -> list[str]:
    return [f"{index:06d}" for index in range(1, 81)]


def _candidates() -> list[dict]:
    shared = {
        "membership_snapshot_date": TARGET.isoformat(),
        "membership_snapshot_source": "gj_big_qmt_inner",
        "membership_proof_sha256": "1" * 64,
        "pit_common_receipt_root_hash": "2" * 64,
        "turnover_snapshot_run_id": "3" * 32,
        "turnover_snapshot_semantic_sha256": "4" * 64,
        "turnover_authority_identity": "truth-run",
        "turnover_authority_sha256": "5" * 64,
        "turnover_authority_set_sha256": "6" * 64,
        "turnover_collector_build_sha": BUILD,
        "turnover_collector_binary_sha256": "7" * 64,
        "turnover_full_market_count": 5547,
        "turnover_full_market_proof_root_sha256": "8" * 64,
        "flow_input_root_sha256": "9" * 64,
        "flow_input_count": 5547,
        "flow_input_min_etl_sync_at": "2026-08-28T17:30:00",
        "flow_input_max_etl_sync_at": "2026-08-28T17:31:00",
        "flow_input_decision_at": DECISION.isoformat(timespec="seconds"),
    }
    return [
        {
            **shared,
            "stock_code": code,
            "ranking_score": 80 - index / 10,
            "final_trade_score": 79 - index / 10,
            "main_wave_score": 70,
            "entry_score": 65,
            "quality_score": 72,
            "capital_score": 68,
            "signal_status": "ALLOW",
            "recommend_status": "ALLOW",
            "finance_manifest_hash": f"{index + 10:064x}"[-64:],
            "event_manifest_hash": f"{index + 100:064x}"[-64:],
            "chase_bar_window_root_sha256": f"{index + 1000:064x}"[-64:],
        }
        for index, code in enumerate(_codes())
    ]


def _receipt() -> dict:
    return build_preliminary_upper_subject_receipt(
        trade_date=TARGET,
        decision_at=DECISION,
        build_sha=BUILD,
        model_version="frozen-v4",
        min_score=62,
        candidates=_candidates(),
    )


def _subject():
    return build_eastmoney_upper_limit_subject(
        target_date=TARGET,
        decision_at=DECISION,
        preliminary_receipt=_receipt(),
    )


def _target(index: int) -> QmtUpperLimitQuoteTarget:
    pre = Decimal("10.00") + Decimal(index)
    return QmtUpperLimitQuoteTarget(
        target_row_id=index + 1,
        stock_code=_codes()[index],
        trade_date=TARGET,
        pre_close=pre,
        open=pre + Decimal("0.10"),
        high=pre + Decimal("0.40"),
        low=pre - Decimal("0.20"),
        close=pre + Decimal("0.20"),
        volume_shares=Decimal(10_000 + index * 100),
        amount=Decimal(1_000_000 + index) + Decimal("0.50"),
        received_at=datetime(2026, 8, 28, 17, 30),
        data_source="gj_big_qmt_inner",
        batch_id="qmt-batch",
        data_version=f"qmt-version-{index}",
        quality_status="QMT_ATTESTED",
        permission_status="SUPPORTED",
        attestation_id=f"{index + 1:064x}"[-64:],
        attestation_run_id="qmt-run",
        attested_at=datetime(2026, 8, 28, 17, 35),
        qmt_fact_sha256=f"{index + 101:064x}"[-64:],
    )


def _response(index: int, *, source_epoch: int | None = None) -> EastmoneyQuoteHttpResponse:
    target = _target(index)
    scale = Decimal(100)
    epoch = source_epoch or int(
        datetime(2026, 8, 28, 15, 34, tzinfo=ZoneInfo("Asia/Shanghai"))
        .astimezone(timezone.utc)
        .timestamp()
    )
    payload = {
        "rc": 0,
        "data": {
            "f13": 0,
            "f43": int(target.close * scale),
            "f44": int(target.high * scale),
            "f45": int(target.low * scale),
            "f46": int(target.open * scale),
            "f47": int(target.volume_shares / 100),
            "f48": float(target.amount),
            "f51": int((target.pre_close + Decimal("1.00")) * scale),
            "f52": int((target.pre_close - Decimal("1.00")) * scale),
            "f57": target.stock_code,
            "f58": "测试股",
            "f59": 2,
            "f60": int(target.pre_close * scale),
            "f86": epoch,
        },
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    request = json.dumps(
        {"url": f"https://push2.eastmoney.com/{target.stock_code}"},
        separators=(",", ":"),
    ).encode()
    return EastmoneyQuoteHttpResponse(
        stock_code=target.stock_code,
        request_started_at=datetime(2026, 8, 30, 9, 0),
        captured_at=CAPTURED,
        request_payload=request,
        request_payload_sha256=hashlib.sha256(request).hexdigest(),
        raw_payload=raw,
        raw_payload_sha256=hashlib.sha256(raw).hexdigest(),
        http_date_text=HTTP_DATE,
    )


def _run():
    return build_eastmoney_upper_limit_quote_run(
        subject=_subject(),
        targets=[_target(index) for index in range(80)],
        responses=[_response(index) for index in range(80)],
        collector_build_sha=BUILD,
        collector_binary_sha256="b" * 64,
        run_id="1" * 32,
    )


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        for table_name, contract in TURNOVER_SNAPSHOT_SCHEMA.items():
            definitions = []
            for column, spec in contract.columns.items():
                kind = spec.data_type.lower()
                if kind.endswith("blob"):
                    sqlite_type = "BLOB"
                elif kind in {"smallint", "int", "bigint", "tinyint"}:
                    sqlite_type = "INTEGER"
                elif kind.startswith("decimal") or kind == "decimal":
                    sqlite_type = "NUMERIC"
                else:
                    sqlite_type = "TEXT"
                definitions.append(f'"{column}" {sqlite_type}')
            primary = (
                ', PRIMARY KEY ("run_id")'
                if table_name == FIELD_CAPTURE_RUN_TABLE
                else ', PRIMARY KEY ("run_id","stock_code","trade_date","k_type","adjust_type")'
            )
            connection.execute(text(
                f'CREATE TABLE "{table_name}" (' + ",".join(definitions) + primary + ")"
            ))
    return engine


def test_quote_parser_proves_exact_date_fields_and_independent_qmt_prices() -> None:
    parsed = parse_eastmoney_upper_limit_quote(
        subject=_subject(), target=_target(0), response=_response(0)
    )
    assert parsed.upper_limit == Decimal("11.00")
    assert parsed.lower_limit == Decimal("9.00")
    assert parsed.source_pre_close == Decimal("10.00")
    assert parsed.source_trade_at.date() == TARGET
    assert parsed.provider_http_at == datetime(2026, 8, 30, 9, 0)
    assert len(parsed.raw_row_text.encode()) <= 512
    assert json.loads(parsed.raw_row_text)["fields"]["f51"] == "11"


def test_quote_parser_rejects_stale_provider_trade_date() -> None:
    stale_epoch = int(
        datetime(2026, 8, 27, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        .astimezone(timezone.utc)
        .timestamp()
    )
    with pytest.raises(EastmoneyUpperLimitQuoteBlocked, match="source_trade_date"):
        parse_eastmoney_upper_limit_quote(
            subject=_subject(),
            target=_target(0),
            response=_response(0, source_epoch=stale_epoch),
        )


def test_quote_parser_rejects_same_source_self_proof_or_price_drift() -> None:
    drifted = replace(_target(0), pre_close=Decimal("9.99"))
    with pytest.raises(EastmoneyUpperLimitQuoteBlocked, match="independent price"):
        parse_eastmoney_upper_limit_quote(
            subject=_subject(), target=drifted, response=_response(0)
        )


def test_curl_transport_keeps_tls_validation_and_exact_http_evidence(
    monkeypatch,
) -> None:
    response = _response(0)
    observed: dict[str, object] = {}

    class Completed:
        returncode = 0
        stdout = (
            b"HTTP/1.1 200 OK\r\n"
            + f"Date: {HTTP_DATE}\r\n".encode("ascii")
            + b"Content-Type: application/json\r\n\r\n"
            + response.raw_payload
        )
        stderr = b""

    def fake_run(command, **kwargs):
        observed["command"] = list(command)
        observed["kwargs"] = dict(kwargs)
        return Completed()

    monkeypatch.setattr(
        "server.common.eastmoney_upper_limit_quote.subprocess.run", fake_run
    )
    captured = fetch_eastmoney_upper_limit_quote(_target(0), attempts=1)
    command = observed["command"]
    assert "--noproxy" in command
    assert "--insecure" not in command
    assert "-k" not in command
    assert "--location" not in command
    assert "push2delay.eastmoney.com" in command[-1]
    assert captured.raw_payload == response.raw_payload
    assert captured.http_date_text == HTTP_DATE
    assert captured.raw_payload_sha256 == response.raw_payload_sha256


def test_qmt_target_requires_native_attestation_and_exact_values() -> None:
    target = _target(0)
    row = {
        "target_row_id": target.target_row_id,
        "stock_code": target.stock_code,
        "trade_date": TARGET.isoformat(),
        "k_type": 1,
        "adjust_type": 0,
        "pre_close": target.pre_close,
        "open": target.open,
        "high": target.high,
        "low": target.low,
        "close": target.close,
        "volume_shares": target.volume_shares,
        "amount": target.amount,
        "received_at": target.received_at,
        "data_source": "gj_big_qmt_inner",
        "batch_id": target.batch_id,
        "data_version": target.data_version,
        "quality_status": "QMT_ATTESTED",
        "permission_status": "SUPPORTED",
        "attestation_id": target.attestation_id,
        "attestation_run_id": target.attestation_run_id,
        "protocol_version": ATTESTATION_PROTOCOL_VERSION,
        "source_pre_close_origin": "NATIVE_QMT",
        "attested_pre_close": target.pre_close,
        "attested_open": target.open,
        "attested_high": target.high,
        "attested_low": target.low,
        "attested_close": target.close,
        "attested_volume_shares": target.volume_shares,
        "attested_amount": target.amount,
        "attested_at": target.attested_at,
        "attestation_provider": "gj_big_qmt_inner",
        "attestation_status": "COMPLETED",
        "attestation_finished_at": target.attested_at,
    }
    parsed = _qmt_target_from_row(row, target_date=TARGET, decision_at=DECISION)
    assert parsed.stock_code == "000001"
    assert len(parsed.qmt_fact_sha256) == 64
    row["attested_pre_close"] = Decimal("9.99")
    with pytest.raises(EastmoneyUpperLimitQuoteBlocked, match="contract differs"):
        _qmt_target_from_row(row, target_date=TARGET, decision_at=DECISION)


def test_target_day_run_publishes_honest_provider_and_80_rows() -> None:
    engine = _engine()
    run = _run()
    receipt = publish_eastmoney_upper_limit_quote_run(
        engine, run, published_at=datetime(2026, 8, 30, 9, 0, 3)
    )
    assert receipt["matched_count"] == 80
    assert receipt["source_trade_date_count"] == 1
    with engine.connect() as connection:
        persisted = connection.execute(text(
            f"SELECT provider, capture_kind, expected_count, matched_count "
            f"FROM {FIELD_CAPTURE_RUN_TABLE} WHERE run_id=:run_id"
        ), {"run_id": run.run_id}).mappings().one()
        count = connection.execute(text(
            f"SELECT COUNT(*) FROM {FIELD_CAPTURE_ROW_TABLE} WHERE run_id=:run_id"
        ), {"run_id": run.run_id}).scalar_one()
    assert persisted["provider"] == EASTMONEY_UPPER_LIMIT_PROVIDER
    assert persisted["capture_kind"] == EASTMONEY_UPPER_LIMIT_CAPTURE_KIND
    assert int(persisted["expected_count"]) == 80
    assert int(persisted["matched_count"]) == 80
    assert count == 80


def test_machine_receipt_does_not_claim_target_day_as_21_day_history() -> None:
    payload = _machine_receipt({"status": "COMPLETED", "matched_count": 80})
    assert payload["formal_history_required_count"] == 1680
    assert payload["formal_history_covered_count"] == 80
    assert payload["formal_history_remaining_count"] == 1600
    assert payload["formal_history_status"] == "DATA_BLOCKED_TARGET_DAY_ONLY"
