from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text

from server.common import upper_limit_snapshot as upper_module
from integrations.myquant.bridge import (
    UPPER_LIMIT_HISTORY_ACTION,
    UPPER_LIMIT_HISTORY_COLUMNS,
    UPPER_LIMIT_HISTORY_FIELDS,
)
from server.common.analysis_pool_receipt import (
    build_preliminary_upper_subject_receipt,
    build_pool_manifest,
    build_turnover_evidence,
    build_upper_limit_evidence,
    validate_upper_limit_evidence,
    validate_preliminary_upper_subject_receipt,
)
from server.common.turnover_snapshot_schema import (
    FIELD_CAPTURE_ROW_TABLE,
    FIELD_CAPTURE_RUN_TABLE,
    TURNOVER_SNAPSHOT_SCHEMA,
)
from server.common.upper_limit_snapshot import (
    UPPER_LIMIT_EXPECTED_DATE_COUNT,
    UPPER_LIMIT_EXPECTED_STOCK_COUNT,
    UpperLimitSnapshotBlocked,
    build_upper_limit_capture_run,
    build_upper_limit_subject,
    load_verified_upper_limit_evidence,
    load_latest_verified_upper_limit_evidence,
    publish_upper_limit_snapshot,
    recover_completed_upper_limit_receipt,
)


TARGET_DATE = date(2026, 8, 21)
BUILD_SHA = "a" * 40
RUN_ID = "1" * 32
DECISION_AT = datetime(2026, 8, 27, 13, 10)
CAPTURED_AT = "2026-08-27T13:06:33+08:00"


def _codes(count: int = UPPER_LIMIT_EXPECTED_STOCK_COUNT) -> list[str]:
    return [f"{number:06d}" for number in range(1, count + 1)]


def _dates(count: int = UPPER_LIMIT_EXPECTED_DATE_COUNT) -> list[date]:
    result: list[date] = []
    current = TARGET_DATE
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current -= timedelta(days=1)
    return sorted(result)


def _subject():
    return build_upper_limit_subject(
        target_date=TARGET_DATE,
        stock_codes=_codes(),
        trade_dates=_dates(),
        calendar_batch_id="calendar-batch-1",
        calendar_manifest_sha256="c" * 64,
        calendar_session_set_sha256="d" * 64,
    )


def _preliminary_candidates() -> list[dict]:
    shared = {
        "membership_snapshot_date": TARGET_DATE.isoformat(),
        "membership_snapshot_source": "gj_big_qmt_inner",
        "membership_proof_sha256": "1" * 64,
        "pit_common_receipt_root_hash": "2" * 64,
        "turnover_snapshot_run_id": "3" * 32,
        "turnover_snapshot_semantic_sha256": "4" * 64,
        "turnover_authority_identity": "truth-run",
        "turnover_authority_sha256": "5" * 64,
        "turnover_authority_set_sha256": "6" * 64,
        "turnover_collector_build_sha": BUILD_SHA,
        "turnover_collector_binary_sha256": "7" * 64,
        "turnover_full_market_count": 5205,
        "turnover_full_market_proof_root_sha256": "8" * 64,
        "flow_input_root_sha256": "9" * 64,
        "flow_input_count": 5205,
        "flow_input_min_etl_sync_at": "2026-08-21T17:30:00",
        "flow_input_max_etl_sync_at": "2026-08-21T17:31:00",
        "flow_input_decision_at": DECISION_AT.isoformat(timespec="seconds"),
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


def _bridge_result(*, artifact: bool = False) -> dict:
    subject = _subject()
    symbols = [
        ("SHSE." if code.startswith("6") else "SZSE.") + code
        for code in subject.stock_codes
    ]
    rows = []
    for code_index, (code, symbol) in enumerate(zip(subject.stock_codes, symbols)):
        for day_index, session in enumerate(subject.trade_dates):
            pre = Decimal("8.94") + Decimal(code_index % 20) + Decimal(day_index) / 100
            upper = (pre * Decimal("1.10")).quantize(Decimal("0.01"))
            lower = (pre * Decimal("0.90")).quantize(Decimal("0.01"))
            pre_value = float(pre)
            if artifact and code_index == 0 and day_index == 0:
                pre_value = 8.9399995803833
            rows.append({
                "symbol": symbol,
                "trade_date": session.isoformat() + "T00:00:00+08:00",
                "pre_close": pre_value,
                "upper_limit": float(upper),
                "lower_limit": float(lower),
                "is_suspended": 0,
            })
    worker_result = {
        "ok": True,
        "action": UPPER_LIMIT_HISTORY_ACTION,
        "fields": UPPER_LIMIT_HISTORY_FIELDS,
        "columns": list(UPPER_LIMIT_HISTORY_COLUMNS),
        "requested_symbols": symbols,
        "start_date": subject.trade_dates[0].isoformat(),
        "end_date": subject.target_date.isoformat(),
        "request_started_at": "2026-08-27T13:06:30+08:00",
        "captured_at": CAPTURED_AT,
        "timezone": "Asia/Shanghai",
        "sdk_version": "3.0.114",
        "python_version": "3.6.8",
        "entitlement_status": "SUPPORTED",
        "rows": rows,
        "errors": {},
    }
    raw = json.dumps(
        worker_result,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    request = json.dumps({
        "action": UPPER_LIMIT_HISTORY_ACTION,
        "symbols": symbols,
        "start_date": subject.trade_dates[0].isoformat(),
        "end_date": subject.target_date.isoformat(),
    }, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return {
        **worker_result,
        "raw_stdout": raw,
        "raw_stdout_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "canonical_request_json": request,
        "canonical_request_sha256": hashlib.sha256(request.encode("utf-8")).hexdigest(),
        "worker_sha256": "b" * 64,
    }


def _capture(*, artifact: bool = False):
    return build_upper_limit_capture_run(
        subject=_subject(),
        bridge_result=_bridge_result(artifact=artifact),
        decision_at=DECISION_AT,
        collector_build_sha=BUILD_SHA,
        run_id=RUN_ID,
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


def test_subject_requires_exact_80_stocks_and_21_sessions() -> None:
    assert len(_subject().stock_codes) == 80
    assert len(_subject().trade_dates) == 21
    with pytest.raises(UpperLimitSnapshotBlocked, match="exactly 80"):
        build_upper_limit_subject(
            target_date=TARGET_DATE,
            stock_codes=_codes(79),
            trade_dates=_dates(),
        )


def test_preliminary_receipt_binds_order_scores_and_input_roots() -> None:
    receipt = build_preliminary_upper_subject_receipt(
        trade_date=TARGET_DATE,
        decision_at=DECISION_AT,
        build_sha=BUILD_SHA,
        model_version="fast-eod-v3",
        min_score=62.0,
        candidates=_preliminary_candidates(),
    )

    assert validate_preliminary_upper_subject_receipt(receipt) == receipt
    assert receipt["ordered_stock_codes"] == _codes()
    assert receipt["input_proof"]["turnover_full_market_count"] == 5205
    tampered = json.loads(json.dumps(receipt))
    tampered["ranked_candidates"][0]["ranking_score"] = "99"
    with pytest.raises(ValueError, match="hash/content differs"):
        validate_preliminary_upper_subject_receipt(tampered)
    gate_tampered = json.loads(json.dumps(receipt))
    gate_tampered["ranked_candidates"][0]["signal_status"] = "BLOCK"
    with pytest.raises(ValueError, match="hash/content differs"):
        validate_preliminary_upper_subject_receipt(gate_tampered)
    bars_tampered = json.loads(json.dumps(receipt))
    bars_tampered["ranked_candidates"][0][
        "chase_bar_window_root_sha256"
    ] = "f" * 64
    with pytest.raises(ValueError, match="hash/content differs"):
        validate_preliminary_upper_subject_receipt(bars_tampered)


def test_upper_run_and_reader_require_exact_preliminary_receipt_identity() -> None:
    preliminary = build_preliminary_upper_subject_receipt(
        trade_date=TARGET_DATE,
        decision_at=DECISION_AT,
        build_sha=BUILD_SHA,
        model_version="fast-eod-v3",
        min_score=62.0,
        candidates=_preliminary_candidates(),
    )
    subject = build_upper_limit_subject(
        target_date=TARGET_DATE,
        stock_codes=_codes(),
        trade_dates=_dates(),
        calendar_batch_id="calendar-batch-1",
        calendar_manifest_sha256="c" * 64,
        calendar_session_set_sha256="d" * 64,
        preliminary_receipt_sha256=preliminary["receipt_sha256"],
    )
    run = build_upper_limit_capture_run(
        subject=subject,
        bridge_result=_bridge_result(),
        decision_at=DECISION_AT,
        collector_build_sha=BUILD_SHA,
        preliminary_receipt=preliminary,
        run_id="2" * 32,
    )
    engine = _engine()
    publish_upper_limit_snapshot(
        engine, run, published_at=datetime(2026, 8, 27, 13, 7)
    )

    evidence = load_latest_verified_upper_limit_evidence(
        engine,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        stock_codes=_codes(),
        preliminary_receipt_sha256=preliminary["receipt_sha256"],
        preliminary_build_sha=BUILD_SHA,
    )
    assert len(evidence) == 80
    assert validate_upper_limit_evidence(
        evidence["000001"]["upper_limit_evidence_json"]
    )["subject_identity"] == f"preview:{preliminary['receipt_sha256']}"
    with engine.connect() as connection:
        persisted_subject = connection.execute(text(
            f"SELECT subject_payload, subject_payload_sha256 "
            f"FROM {FIELD_CAPTURE_RUN_TABLE} WHERE run_id=:run_id"
        ), {"run_id": run.run_id}).mappings().one()
    subject_payload = bytes(persisted_subject["subject_payload"])
    assert json.loads(subject_payload) == preliminary
    assert hashlib.sha256(subject_payload).hexdigest() == str(
        persisted_subject["subject_payload_sha256"]
    )
    proof = validate_upper_limit_evidence(
        evidence["000001"]["upper_limit_evidence_json"]
    )
    assert proof["preliminary_receipt_payload_sha256"] == str(
        persisted_subject["subject_payload_sha256"]
    )
    assert load_latest_verified_upper_limit_evidence(
        engine,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        stock_codes=_codes(),
        preliminary_receipt_sha256="f" * 64,
        preliminary_build_sha=BUILD_SHA,
    ) == {}

    assert load_latest_verified_upper_limit_evidence(
        engine,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        stock_codes=_codes(),
        preliminary_receipt_sha256=preliminary["receipt_sha256"],
        preliminary_build_sha="e" * 40,
    ) == {}
    with pytest.raises(UpperLimitSnapshotBlocked, match="exactly 21"):
        build_upper_limit_subject(
            target_date=TARGET_DATE,
            stock_codes=_codes(),
            trade_dates=_dates(20),
        )


def test_capture_accepts_only_float_transport_artifact_within_one_ten_thousandth() -> None:
    run = _capture(artifact=True)
    assert run.rows[0].pre_close == Decimal("8.94")

    response = _bridge_result()
    response["rows"][0]["pre_close"] = 8.949
    worker = {key: value for key, value in response.items() if key not in {
        "raw_stdout", "raw_stdout_sha256", "canonical_request_json",
        "canonical_request_sha256", "worker_sha256",
    }}
    raw = json.dumps(worker, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    response["raw_stdout"] = raw
    response["raw_stdout_sha256"] = hashlib.sha256(raw.encode()).hexdigest()
    with pytest.raises(UpperLimitSnapshotBlocked, match="cent price contract"):
        build_upper_limit_capture_run(
            subject=_subject(), bridge_result=response,
            decision_at=DECISION_AT, collector_build_sha=BUILD_SHA,
        )


@pytest.mark.parametrize("mutation", ("missing", "duplicate", "suspended", "future"))
def test_capture_blocks_partial_duplicate_suspended_and_future_evidence(mutation: str) -> None:
    response = _bridge_result()
    if mutation == "missing":
        response["rows"].pop()
    elif mutation == "duplicate":
        response["rows"][-1] = dict(response["rows"][0])
    elif mutation == "suspended":
        response["rows"][0]["is_suspended"] = 1
    else:
        response["captured_at"] = "2026-08-27T13:11:00+08:00"
    worker = {key: value for key, value in response.items() if key not in {
        "raw_stdout", "raw_stdout_sha256", "canonical_request_json",
        "canonical_request_sha256", "worker_sha256",
    }}
    raw = json.dumps(worker, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    response["raw_stdout"] = raw
    response["raw_stdout_sha256"] = hashlib.sha256(raw.encode()).hexdigest()
    with pytest.raises(UpperLimitSnapshotBlocked, match="DATA_BLOCKED"):
        build_upper_limit_capture_run(
            subject=_subject(), bridge_result=response,
            decision_at=DECISION_AT, collector_build_sha=BUILD_SHA,
        )


def test_capture_rejects_decoded_rows_not_bound_to_raw_stdout() -> None:
    response = _bridge_result()
    response["rows"][0]["upper_limit"] += 0.01
    with pytest.raises(UpperLimitSnapshotBlocked, match="raw worker response differs"):
        build_upper_limit_capture_run(
            subject=_subject(), bridge_result=response,
            decision_at=DECISION_AT, collector_build_sha=BUILD_SHA,
        )


def test_publish_and_read_recompute_exact_1680_keys_and_receipt() -> None:
    engine = _engine()
    run = _capture()
    receipt = publish_upper_limit_snapshot(
        engine, run, published_at=datetime(2026, 8, 27, 13, 7)
    )
    assert receipt["expected_count"] == 1680
    evidence = load_verified_upper_limit_evidence(
        engine,
        run_id=run.run_id,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        stock_codes=_codes(),
        trade_dates=_dates(),
    )
    assert len(evidence) == 80
    assert len(evidence["000001"]["upper_limits"]) == 21
    proof = validate_upper_limit_evidence(
        evidence["000001"]["upper_limit_evidence_json"]
    )
    assert proof["status"] == "PASS"
    assert proof["expected_stock_count"] == 80
    assert proof["expected_date_count"] == 21


def test_completed_upper_publication_recovers_before_recapture() -> None:
    engine = _engine()
    run = _capture()
    first = publish_upper_limit_snapshot(
        engine, run, published_at=datetime(2026, 8, 27, 13, 7)
    )

    recovered = recover_completed_upper_limit_receipt(
        engine,
        subject=run.subject,
        decision_at=run.decision_at,
        collector_build_sha=run.collector_build_sha,
    )

    assert recovered is not None
    assert recovered["run_id"] == first["run_id"]
    assert recovered["recovered"] is True
    retry = publish_upper_limit_snapshot(
        engine,
        replace(run, run_id="9" * 32),
        published_at=datetime(2026, 8, 27, 13, 8),
    )
    assert retry["run_id"] == first["run_id"]


def test_upper_limit_default_clock_is_explicit_shanghai_time(monkeypatch) -> None:
    engine = _engine()
    monkeypatch.setattr(
        upper_module,
        "_now_shanghai",
        lambda: datetime(2026, 8, 27, 13, 7),
    )

    receipt = publish_upper_limit_snapshot(engine, _capture())

    assert receipt["status"] == "COMPLETED"


def test_read_blocks_row_hash_tamper_and_code_set_drift() -> None:
    engine = _engine()
    run = _capture()
    publish_upper_limit_snapshot(
        engine, run, published_at=datetime(2026, 8, 27, 13, 7)
    )
    with engine.begin() as connection:
        connection.execute(text(
            f"UPDATE {FIELD_CAPTURE_ROW_TABLE} SET field_value_decimal='99.99' "
            "WHERE run_id=:run_id AND stock_code='000001'"
        ), {"run_id": run.run_id})
    with pytest.raises(UpperLimitSnapshotBlocked, match="hash differs"):
        load_verified_upper_limit_evidence(
            engine, run_id=run.run_id, target_date=TARGET_DATE,
            decision_at=DECISION_AT, stock_codes=_codes(), trade_dates=_dates(),
        )

    with pytest.raises(
        UpperLimitSnapshotBlocked,
        match="subject requires exactly 80|subject|run contract differs",
    ):
        load_verified_upper_limit_evidence(
            engine, run_id=run.run_id, target_date=TARGET_DATE,
            decision_at=DECISION_AT,
            stock_codes=[*_codes()[:-1], "000081"], trade_dates=_dates(),
        )


def test_pool_manifest_binds_exact_upper_run_code_set_and_per_stock_root() -> None:
    engine = _engine()
    run = _capture()
    publish_upper_limit_snapshot(
        engine, run, published_at=datetime(2026, 8, 27, 13, 7)
    )
    evidence = load_verified_upper_limit_evidence(
        engine, run_id=run.run_id, target_date=TARGET_DATE,
        decision_at=DECISION_AT, stock_codes=_codes(), trade_dates=_dates(),
    )

    def turnover(code: str) -> str:
        return build_turnover_evidence({
            "status": "DATA_BLOCKED",
            "stock_code": code,
            "trade_date": TARGET_DATE.isoformat(),
            "decision_known_at": "2026-08-27 13:10:00",
            "source_table": "st_market_field_capture_row",
            "reason": "DATA_BLOCKED: fixture turnover unavailable",
        })

    recommendations = [
        {
            "stock_code": code,
            "pick_date": TARGET_DATE.isoformat(),
            "candidate_recommend_status": "BLOCK",
            "chase_risk_status": "DATA_BLOCKED",
            "candidate_ordinary_buy_eligible": 0,
            "recommend_status": "PENDING",
            "ordinary_buy_eligible": 0,
            "publication_status": "PENDING",
            "turnover_evidence_json": turnover(code),
            "upper_limit_evidence_json": evidence[code][
                "upper_limit_evidence_json"
            ],
        }
        for code in _codes()
    ]
    analysis = [
        {"stock_code": code, "analysis_date": TARGET_DATE.isoformat()}
        for code in _codes()
    ]
    first = build_pool_manifest(
        trade_date=TARGET_DATE.isoformat(),
        analysis_rows=analysis,
        recommendation_rows=recommendations,
    )
    changed_proof = validate_upper_limit_evidence(
        recommendations[0]["upper_limit_evidence_json"]
    )
    changed_proof.pop("proof_sha256")
    changed_proof["stock_rows_sha256"] = "f" * 64
    changed = [dict(item) for item in recommendations]
    changed[0]["upper_limit_evidence_json"] = build_upper_limit_evidence(
        changed_proof
    )
    second = build_pool_manifest(
        trade_date=TARGET_DATE.isoformat(),
        analysis_rows=analysis,
        recommendation_rows=changed,
    )
    assert first["canonical_pool_sha256"] != second["canonical_pool_sha256"]

    drifted = [dict(item) for item in recommendations]
    drifted[-1]["stock_code"] = "000081"
    drifted[-1]["turnover_evidence_json"] = turnover("000081")
    drifted_upper = validate_upper_limit_evidence(
        drifted[-1]["upper_limit_evidence_json"]
    )
    drifted_upper.pop("proof_sha256")
    drifted_upper["stock_code"] = "000081"
    drifted[-1]["upper_limit_evidence_json"] = build_upper_limit_evidence(
        drifted_upper
    )
    with pytest.raises(ValueError, match="immutable subject differs"):
        build_pool_manifest(
            trade_date=TARGET_DATE.isoformat(),
            analysis_rows=analysis,
            recommendation_rows=drifted,
        )
