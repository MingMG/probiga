from __future__ import annotations

import inspect

import pandas as pd
import pytest

from server.common.qmt_attestation_contract import (
    bound_stock_set_contract,
    build_qmt_v2_manifest,
    daily_market_source_batch_id,
)
from server.common.qmt_daily_no_row import (
    build_no_row_exception_contract,
    project_catalog_daily_codes,
)
from server.common import qmt_daily_market_truth as truth_module
from server.trading_v3 import backtest


class _Catalog:
    batch_id = "catalog-1"
    member_count = 3
    member_set_hash = "a" * 64
    manifest_hash = "b" * 64

    def eligible_codes(self, day):
        return {
            "2026-08-21": ["000001", "830001"],
            "2026-08-24": ["000001", "301999", "830001"],
        }[day]


class _Calendar:
    batch_id = "calendar-1"
    session_set_hash = "c" * 64
    manifest_hash = "d" * 64
    known_at = "2026-08-24 18:00:00"

    def sessions_between(self, start_date, end_date):
        return [
            day for day in ("2026-08-21", "2026-08-24")
            if start_date <= day <= end_date
        ]


def _manifest():
    catalog = _Catalog()
    calendar = _Calendar()
    source_id = daily_market_source_batch_id(
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_manifest_hash=calendar.manifest_hash,
    )
    return build_qmt_v2_manifest({
        day: bound_stock_set_contract(
            day,
            catalog.eligible_codes(day),
            catalog_batch_id=catalog.batch_id,
            catalog_member_count=catalog.member_count,
            catalog_member_set_hash=catalog.member_set_hash,
            catalog_manifest_hash=catalog.manifest_hash,
            source_batch_id=source_id,
            calendar_batch_id=calendar.batch_id,
            calendar_session_set_hash=calendar.session_set_hash,
            calendar_manifest_hash=calendar.manifest_hash,
            calendar_known_at=calendar.known_at,
        )
        for day in calendar.sessions_between("2026-08-21", "2026-08-24")
    })


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def one_or_none(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows


class _Connection:
    def __init__(self, manifest, proof_rows):
        self.manifest = manifest
        self.proof_rows = proof_rows
        self.calls = 0
        self.params = []

    def execute(self, _statement, _params=None):
        self.calls += 1
        self.params.append(dict(_params or {}))
        if self.calls == 1:
            return _Result([{
                "run_id": "attestation-1",
                "provider": truth_module.QMT_DAILY_PROVIDER,
                "start_date": "2026-08-21",
                "end_date": "2026-08-24",
                "status": "COMPLETED",
                "target_rows": 5,
                "qmt_rows": 5,
                "matched_rows": 5,
                "missing_qmt_rows": 0,
                "mismatched_rows": 0,
                "already_attested_rows": 0,
                "updated_rows": 5,
                "tolerance_json": self.manifest,
                "finished_at": "2026-08-24 19:00:00",
            }])
        return _Result(self.proof_rows)


def _bind_roots(monkeypatch):
    monkeypatch.setattr(
        truth_module, "load_stock_catalog", lambda *_a, **_k: _Catalog()
    )
    monkeypatch.setattr(
        truth_module,
        "load_trade_calendar_receipt",
        lambda *_a, **_k: _Calendar(),
    )


def test_v3_market_truth_accepts_exact_catalog_attested_rows(monkeypatch):
    _bind_roots(monkeypatch)
    connection = _Connection(_manifest(), [
        {
            "trade_date": "2026-08-21",
            "attested_row_count": 2,
            "attested_stock_count": 2,
        },
        {
            "trade_date": "2026-08-24",
            "attested_row_count": 3,
            "attested_stock_count": 3,
        },
    ])
    truth = truth_module.load_qmt_daily_market_truth(
        connection,
        start_date="2026-08-21",
        end_date="2026-08-24",
        decision_known_at="2026-08-24 20:00:00",
    )

    assert truth.attested_row_count == 5
    assert truth.catalog_batch_id == "catalog-1"
    assert len(truth.truth_hash) == 64
    assert connection.params[0]["decision_known_at"] == "2026-08-24 20:00:00"
    assert connection.params[1]["run_finished_at"] == "2026-08-24 19:00:00"
    assert connection.params[1]["selected_run_id"] == "attestation-1"


def test_v3_market_truth_blocks_missing_or_stale_attested_symbol(monkeypatch):
    _bind_roots(monkeypatch)
    connection = _Connection(_manifest(), [
        {
            "trade_date": "2026-08-21",
            "attested_row_count": 2,
            "attested_stock_count": 2,
        },
        {
            "trade_date": "2026-08-24",
            "attested_row_count": 2,
            "attested_stock_count": 2,
        },
    ])

    with pytest.raises(RuntimeError, match="rows/attestations are incomplete"):
        truth_module.load_qmt_daily_market_truth(
            connection,
            start_date="2026-08-21",
            end_date="2026-08-24",
            decision_known_at="2026-08-24 20:00:00",
        )


def test_v3_market_truth_blocks_manifest_root_tamper(monkeypatch):
    _bind_roots(monkeypatch)
    manifest = _manifest()
    manifest["daily_universe"]["2026-08-24"][
        "catalog_manifest_hash"
    ] = "e" * 64

    with pytest.raises(ValueError, match="binding"):
        truth_module.load_qmt_daily_market_truth(
            _Connection(manifest, []),
            start_date="2026-08-21",
            end_date="2026-08-24",
            decision_known_at="2026-08-24 20:00:00",
        )


def test_v3_history_loader_has_no_survivor_prefix_or_mutable_name_claim():
    source = inspect.getsource(backtest._load_history)
    truth_source = inspect.getsource(truth_module.load_qmt_daily_market_truth)

    assert "qmt_stock_catalog_member" in source
    assert "qmt_kline_attestation_row" in source
    assert "stock_code LIKE" not in source
    assert "short_name" not in source
    assert "adjust_type=0" in source
    assert "attestation.created_at<=:run_finished_at" in source
    assert "attestation.run_id=BINARY :selected_run_id" in source
    assert "attestation.source_data_version" in source
    assert "k.change_pct" not in source
    assert "_derive_attested_change_pct" in source
    assert "attestation.created_at<=:run_finished_at" in truth_source
    assert "attestation.run_id=BINARY :selected_run_id" in truth_source
    assert "attestation.source_data_version" in truth_source
    assert "finished_at<=:decision_known_at" in truth_source


def test_v3_change_pct_is_derived_only_from_attested_prices_and_bound():
    frame = pd.DataFrame({
        "close": [10.5, 9.0],
        "pre_close": [10.0, 10.0],
        # A mutable convenience value must have no influence.
        "change_pct": [999.0, 999.0],
    })

    derived = backtest._derive_attested_change_pct(frame)
    truth = backtest._bind_derived_change_pct_truth(
        {"truth_hash": "a" * 64},
        row_count=2,
    )

    assert derived.tolist() == pytest.approx([5.0, -10.0])
    binding = truth["derived_change_pct_binding"]
    assert binding["stored_change_pct_consumed"] is False
    assert binding["source_market_truth_hash"] == "a" * 64
    assert len(binding["binding_hash"]) == 64
    assert len(truth["consumer_truth_hash"]) == 64


@pytest.mark.parametrize(
    ("close", "pre_close"),
    [(10.0, 0.0), (float("inf"), 10.0), (10.0, float("nan"))],
)
def test_v3_change_pct_derivation_fails_closed_on_invalid_price_pair(
    close,
    pre_close,
):
    with pytest.raises(RuntimeError, match="cannot derive finite"):
        backtest._derive_attested_change_pct(pd.DataFrame({
            "close": [close],
            "pre_close": [pre_close],
        }))


def test_consumer_truth_rejects_reused_row_proofs(monkeypatch):
    _bind_roots(monkeypatch)
    connection = _Connection(_manifest(), [
        {
            "trade_date": "2026-08-21",
            "attested_row_count": 2,
            "attested_stock_count": 2,
        },
        {
            "trade_date": "2026-08-24",
            "attested_row_count": 3,
            "attested_stock_count": 3,
        },
    ])
    original_execute = connection.execute

    def execute(statement, params=None):
        result = original_execute(statement, params)
        if connection.calls == 1:
            result.rows[0]["already_attested_rows"] = 1
            result.rows[0]["updated_rows"] = 4
        return result

    connection.execute = execute
    with pytest.raises(RuntimeError, match="counters differ"):
        truth_module.load_qmt_daily_market_truth(
            connection,
            start_date="2026-08-21",
            end_date="2026-08-24",
            decision_known_at="2026-08-24 20:00:00",
        )


class _NoRowCatalog:
    batch_id = "catalog-no-row"
    member_count = 2
    member_set_hash = "1" * 64
    manifest_hash = "2" * 64
    members = (
        {
            "stock_code": "000001",
            "qmt_code": "000001.SZ",
            "list_date": "1991-04-03",
            "expire_date": None,
        },
        {
            "stock_code": "301688",
            "qmt_code": "301688.SZ",
            "list_date": "1970-01-01",
            "expire_date": None,
        },
    )

    def eligible_codes(self, day):
        assert day in {"2026-08-21", "2026-08-24"}
        return ["000001", "301688"]


class _NoRowCalendar(_Calendar):
    batch_id = "calendar-no-row"
    session_set_hash = "3" * 64
    manifest_hash = "4" * 64


def _no_row_manifest():
    catalog = _NoRowCatalog()
    calendar = _NoRowCalendar()
    proof = build_no_row_exception_contract(
        catalog=catalog,
        calendar=calendar,
        start_date="2026-08-21",
        end_date="2026-08-24",
        not_yet_listed_codes=["301688"],
        target_rows_by_code={"301688": 0},
        history_rows_by_code={"301688": 0},
    )
    projected = project_catalog_daily_codes(
        catalog=catalog,
        calendar=calendar,
        start_date="2026-08-21",
        end_date="2026-08-24",
        contract=proof,
    )
    source_id = daily_market_source_batch_id(
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_manifest_hash=calendar.manifest_hash,
    )
    daily = {
        day: bound_stock_set_contract(
            day,
            codes,
            catalog_batch_id=catalog.batch_id,
            catalog_member_count=catalog.member_count,
            catalog_member_set_hash=catalog.member_set_hash,
            catalog_manifest_hash=catalog.manifest_hash,
            source_batch_id=source_id,
            calendar_batch_id=calendar.batch_id,
            calendar_session_set_hash=calendar.session_set_hash,
            calendar_manifest_hash=calendar.manifest_hash,
            calendar_known_at=calendar.known_at,
        )
        for day, codes in projected.items()
    }
    return build_qmt_v2_manifest(
        daily,
        no_row_exception_contract=proof,
    )


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _NoRowConnection:
    def __init__(self, *, excluded_target_rows=0):
        self.calls = 0
        self.excluded_target_rows = excluded_target_rows

    def execute(self, _statement, _params=None):
        self.calls += 1
        if self.calls == 1:
            return _Result([{
                "run_id": "attestation-no-row",
                "provider": truth_module.QMT_DAILY_PROVIDER,
                "start_date": "2026-08-21",
                "end_date": "2026-08-24",
                "status": "COMPLETED",
                "target_rows": 2,
                "qmt_rows": 2,
                "matched_rows": 2,
                "missing_qmt_rows": 0,
                "mismatched_rows": 0,
                "already_attested_rows": 0,
                "updated_rows": 2,
                "tolerance_json": _no_row_manifest(),
                "finished_at": "2026-08-24 19:00:00",
            }])
        if self.calls == 2:
            return _ScalarResult(self.excluded_target_rows)
        return _Result([
            {
                "trade_date": "2026-08-21",
                "attested_row_count": 1,
                "attested_stock_count": 1,
            },
            {
                "trade_date": "2026-08-24",
                "attested_row_count": 1,
                "attested_stock_count": 1,
            },
        ])


def _bind_no_row_roots(monkeypatch):
    monkeypatch.setattr(
        truth_module,
        "load_stock_catalog",
        lambda *_a, **_k: _NoRowCatalog(),
    )
    monkeypatch.setattr(
        truth_module,
        "load_trade_calendar_receipt",
        lambda *_a, **_k: _NoRowCalendar(),
    )


def test_consumer_truth_accepts_and_binds_reviewed_no_row_manifest(monkeypatch):
    _bind_no_row_roots(monkeypatch)
    truth = truth_module.load_qmt_daily_market_truth(
        _NoRowConnection(),
        start_date="2026-08-21",
        end_date="2026-08-24",
        decision_known_at="2026-08-24 20:00:00",
    )

    assert truth.attested_row_count == 2
    assert len(truth.no_row_exception_proof_sha256 or "") == 64
    assert truth.as_dict()["no_row_exception_proof_sha256"] == (
        truth.no_row_exception_proof_sha256
    )


def test_consumer_truth_rejects_row_appearing_under_no_row_exception(
    monkeypatch,
):
    _bind_no_row_roots(monkeypatch)
    with pytest.raises(RuntimeError, match="target rows appeared"):
        truth_module.load_qmt_daily_market_truth(
            _NoRowConnection(excluded_target_rows=1),
            start_date="2026-08-21",
            end_date="2026-08-24",
            decision_known_at="2026-08-24 20:00:00",
        )
