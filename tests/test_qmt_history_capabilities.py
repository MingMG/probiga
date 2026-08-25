from __future__ import annotations

from typing import Any

import pytest

from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID
from integrations.qmt.diagnostics import PROVIDER_ID as NATIVE_QMT_PROVIDER_ID
from server.common.qmt_history_capabilities import (
    QMT_HISTORY_DATASET_KEYS,
    QMT_HISTORY_DATASET_SPECS,
    assess_qmt_history_capabilities,
)


class _Mappings:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _Result:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def mappings(self) -> _Mappings:
        return _Mappings(self._rows)


class _Connection:
    def __init__(
        self,
        *,
        capability_rows: list[dict[str, Any]],
        coverage_rows: list[dict[str, Any]],
        fail_capability: bool = False,
        fail_coverage: bool = False,
    ):
        self.capability_rows = capability_rows
        self.coverage_rows = coverage_rows
        self.fail_capability = fail_capability
        self.fail_coverage = fail_coverage
        self.statements: list[str] = []

    def execute(self, statement, _params=None) -> _Result:
        sql = str(statement)
        self.statements.append(sql)
        if "FROM qmt_api_capability" in sql:
            if self.fail_capability:
                raise RuntimeError("ledger unavailable")
            return _Result(self.capability_rows)
        if "FROM qmt_history_coverage_manifest" in sql:
            if self.fail_coverage:
                raise RuntimeError("coverage unavailable")
            return _Result(self.coverage_rows)
        raise AssertionError(sql)


def _all_capability_keys() -> list[str]:
    return sorted(
        {
            key
            for spec in QMT_HISTORY_DATASET_SPECS
            for key in spec.capability_keys
        }
    )


def _supported_row(key: str, **overrides: Any) -> dict[str, Any]:
    row = {
        "provider": "gj_qmt",
        "capability_key": key,
        "capability_status": "SUPPORTED",
        "available": 1,
        "returned_rows": 3,
        "error_message": None,
        "connection_port": 58610,
        "sdk_module": "xtquant.xtdata",
        "sdk_version": "test-version",
        "probed_at": "2026-08-25 08:00:00",
        "probe_age_seconds": 60,
    }
    row.update(overrides)
    return row


def _supported_rows() -> list[dict[str, Any]]:
    return [_supported_row(key) for key in _all_capability_keys()]


def _coverage_row(
    dataset: str,
    trade_date: str = "2024-01-02",
    **overrides: Any,
) -> dict[str, Any]:
    spec = next(
        item for item in QMT_HISTORY_DATASET_SPECS if item.dataset == dataset
    )
    row = {
        "manifest_hash": "a" * 64,
        "dataset": spec.coverage_dataset,
        "period": spec.period,
        "provider": spec.coverage_provider,
        "trade_date": trade_date,
        "status": "EXACT",
        "strategy_eligible": 1,
        "captured_at": "2026-08-24 18:00:00",
        "capture_age_seconds": 3600,
    }
    row.update(overrides)
    return row


def _dataset(payload: dict[str, Any], key: str) -> dict[str, Any]:
    return next(item for item in payload["datasets"] if item["dataset"] == key)


def test_fixed_matrix_covers_every_required_history_family():
    assert len(QMT_HISTORY_DATASET_KEYS) == 19
    assert len(QMT_HISTORY_DATASET_KEYS) == len(set(QMT_HISTORY_DATASET_KEYS))
    assert set(QMT_HISTORY_DATASET_KEYS) == {
        "stock_daily",
        "stock_minute",
        "index_daily",
        "index_minute",
        "sector_membership_pit",
        "industry_membership_pit",
        "concept_membership_pit",
        "index_weight_pit",
        "stock_funds_flow_daily",
        "stock_funds_flow_minute",
        "stock_orderflow_daily",
        "stock_orderflow_minute",
        "northbound_flow_daily",
        "northbound_flow_minute",
        "stock_l2_quote",
        "stock_l2_order",
        "stock_l2_transaction",
        "stock_l2_transaction_count",
        "stock_l2_order_queue",
    }
    pit = {
        spec.dataset
        for spec in QMT_HISTORY_DATASET_SPECS
        if spec.point_in_time_required
    }
    assert pit == {
        "sector_membership_pit",
        "industry_membership_pit",
        "concept_membership_pit",
        "index_weight_pit",
    }
    assert all(spec.capability_keys for spec in QMT_HISTORY_DATASET_SPECS)
    assert all(spec.coverage_dataset for spec in QMT_HISTORY_DATASET_SPECS)


def test_stock_bar_provider_contract_matches_real_full_history_runner():
    from tools import run_guojin_qmt_full_market_history_2024 as runner

    daily = next(
        spec for spec in QMT_HISTORY_DATASET_SPECS
        if spec.dataset == "stock_daily"
    )
    minute = next(
        spec for spec in QMT_HISTORY_DATASET_SPECS
        if spec.dataset == "stock_minute"
    )

    assert runner.BIGQMT_PROVIDER_ID == BIGQMT_PROVIDER_ID
    assert daily.capability_provider == NATIVE_QMT_PROVIDER_ID
    assert daily.coverage_provider == BIGQMT_PROVIDER_ID
    assert minute.capability_provider == NATIVE_QMT_PROVIDER_ID
    assert minute.coverage_provider == NATIVE_QMT_PROVIDER_ID


def test_supported_api_and_exact_scope_are_both_required_for_eligibility():
    scopes = {
        dataset: ("2024-01-02", "2024-01-03")
        for dataset in QMT_HISTORY_DATASET_KEYS
    }
    coverage = [
        _coverage_row(dataset, trade_date, manifest_hash=f"{index:064x}")
        for index, (dataset, trade_date) in enumerate(
            (
                (dataset, trade_date)
                for dataset in QMT_HISTORY_DATASET_KEYS
                for trade_date in scopes[dataset]
            ),
            start=1,
        )
    ]
    connection = _Connection(
        capability_rows=_supported_rows(), coverage_rows=coverage
    )

    ok, payload = assess_qmt_history_capabilities(
        connection,
        required_trade_dates_by_dataset=scopes,
    )

    assert ok is True
    assert payload["status"] == "HEALTHY"
    assert payload["strategy_eligible_dataset_count"] == 19
    assert all(item["status"] == "AVAILABLE" for item in payload["datasets"])
    assert all(item["coverage"]["status"] == "EXACT" for item in payload["datasets"])


def test_supported_api_without_explicit_coverage_scope_never_guesses_eligibility():
    connection = _Connection(
        capability_rows=_supported_rows(), coverage_rows=[]
    )

    ok, payload = assess_qmt_history_capabilities(connection)

    assert ok is True
    assert payload["strategy_eligible_dataset_count"] == 0
    stock = _dataset(payload, "stock_daily")
    assert stock["api_status"] == "AVAILABLE"
    assert stock["status"] == "UNAVAILABLE"
    assert stock["coverage"]["status"] == "UNASSESSED"
    assert "coverage_scope_missing" in stock["blockers"]


def test_one_exact_day_cannot_cover_a_two_day_required_scope():
    connection = _Connection(
        capability_rows=_supported_rows(),
        coverage_rows=[_coverage_row("stock_daily", "2024-01-02")],
    )

    ok, payload = assess_qmt_history_capabilities(
        connection,
        required_trade_dates_by_dataset={
            "stock_daily": ("2024-01-02", "2024-01-03")
        },
    )

    assert ok is True
    stock = _dataset(payload, "stock_daily")
    assert stock["strategy_eligible"] is False
    assert stock["coverage"]["status"] == "INCOMPLETE"
    assert stock["coverage"]["missing_trade_dates"] == ["2024-01-03"]
    assert "coverage_exact_missing" in stock["blockers"]


@pytest.mark.parametrize(
    "provider_status",
    ["NO_DATA", "NOT_AUTHORIZED", "UNSUPPORTED_CLIENT"],
)
def test_explicit_provider_unavailability_is_healthy_evidence_but_closes_strategy(
    provider_status: str,
):
    rows = _supported_rows()
    target = "probe:stock_l2_order:-"
    row = next(item for item in rows if item["capability_key"] == target)
    row.update(
        {
            "capability_status": provider_status,
            "available": 0,
            "returned_rows": 0,
            "error_message": provider_status.lower(),
        }
    )
    connection = _Connection(
        capability_rows=rows,
        coverage_rows=[_coverage_row("stock_l2_order")],
    )

    ok, payload = assess_qmt_history_capabilities(
        connection,
        required_trade_dates_by_dataset={"stock_l2_order": ("2024-01-02",)},
    )

    assert ok is True
    item = _dataset(payload, "stock_l2_order")
    assert item["evidence_status"] == "HEALTHY"
    assert item["api_status"] == "UNAVAILABLE"
    assert item["strategy_eligible"] is False
    capability = next(
        detail
        for detail in item["capabilities"]
        if detail["capability_key"] == target
    )
    assert capability["provider_status"] == provider_status
    assert capability["freshness"] == "FRESH"
    assert f"provider_status:{provider_status}" in item["blockers"]


@pytest.mark.parametrize(
    ("status", "available", "age", "blocker"),
    [
        ("SUPPORTED", 1, 96 * 60 * 60 + 1, "capability_probe_stale"),
        ("SUPPORTED", 1, -1, "capability_probe_future"),
        ("FAILED", 0, 60, "capability_probe_failed"),
        ("PENDING_PERMISSION_PROBE", None, 60, "capability_probe_pending"),
    ],
)
def test_stale_future_failed_and_pending_probes_are_unhealthy_and_unavailable(
    status: str,
    available: Any,
    age: int,
    blocker: str,
):
    rows = _supported_rows()
    target = "probe:stock_daily_bar:-"
    next(item for item in rows if item["capability_key"] == target).update(
        {
            "capability_status": status,
            "available": available,
            "probe_age_seconds": age,
        }
    )
    connection = _Connection(
        capability_rows=rows,
        coverage_rows=[_coverage_row("stock_daily")],
    )

    ok, payload = assess_qmt_history_capabilities(
        connection,
        required_trade_dates_by_dataset={"stock_daily": ("2024-01-02",)},
    )

    assert ok is False
    item = _dataset(payload, "stock_daily")
    assert item["api_status"] == "UNAVAILABLE"
    assert item["strategy_eligible"] is False
    assert blocker in item["blockers"]


def test_missing_or_duplicate_capability_evidence_is_ambiguous_and_fail_closed():
    rows = _supported_rows()
    missing_key = "probe:index_daily_bar:-"
    rows = [item for item in rows if item["capability_key"] != missing_key]
    duplicate_key = "probe:index_minute_bar:-"
    rows.append(_supported_row(duplicate_key))
    connection = _Connection(capability_rows=rows, coverage_rows=[])

    ok, payload = assess_qmt_history_capabilities(connection)

    assert ok is False
    assert "capability_evidence_missing" in _dataset(
        payload, "index_daily"
    )["blockers"]
    assert "capability_evidence_ambiguous" in _dataset(
        payload, "index_minute"
    )["blockers"]


@pytest.mark.parametrize(
    ("overrides", "blocker"),
    [
        ({"strategy_eligible": 0}, "coverage_eligibility_flag_mismatch"),
        ({"capture_age_seconds": -1}, "coverage_evidence_future"),
        ({"manifest_hash": "not-a-hash"}, "coverage_manifest_hash_invalid"),
        ({"period": "5m"}, "coverage_period_mismatch"),
    ],
)
def test_malformed_or_future_exact_coverage_never_grants_eligibility(
    overrides: dict[str, Any],
    blocker: str,
):
    connection = _Connection(
        capability_rows=_supported_rows(),
        coverage_rows=[_coverage_row("stock_daily", **overrides)],
    )

    ok, payload = assess_qmt_history_capabilities(
        connection,
        required_trade_dates_by_dataset={"stock_daily": ("2024-01-02",)},
    )

    assert ok is False
    item = _dataset(payload, "stock_daily")
    assert item["strategy_eligible"] is False
    assert blocker in item["blockers"]


def test_incomplete_and_unavailable_manifests_are_visible_but_not_exact():
    connection = _Connection(
        capability_rows=_supported_rows(),
        coverage_rows=[
            _coverage_row(
                "stock_daily",
                status="INCOMPLETE",
                strategy_eligible=0,
            ),
            _coverage_row(
                "stock_minute",
                status="UNAVAILABLE",
                strategy_eligible=0,
                manifest_hash="b" * 64,
            ),
        ],
    )

    ok, payload = assess_qmt_history_capabilities(
        connection,
        required_trade_dates_by_dataset={
            "stock_daily": ("2024-01-02",),
            "stock_minute": ("2024-01-02",),
        },
    )

    assert ok is True
    daily = _dataset(payload, "stock_daily")
    minute = _dataset(payload, "stock_minute")
    assert daily["coverage"]["status"] == "INCOMPLETE"
    assert minute["coverage"]["status"] == "UNAVAILABLE"
    assert daily["strategy_eligible"] is False
    assert minute["strategy_eligible"] is False


def test_query_failures_and_invalid_scope_are_reported_not_raised():
    connection = _Connection(
        capability_rows=[],
        coverage_rows=[],
        fail_capability=True,
        fail_coverage=True,
    )

    ok, payload = assess_qmt_history_capabilities(
        connection,
        required_trade_dates_by_dataset={
            "not_a_dataset": ("2024-01-02",),
            "stock_daily": ("not-a-date",),
        },
    )

    assert ok is False
    assert set(payload["errors"]) == {
        "coverage_scope_unknown_dataset:not_a_dataset",
        "coverage_scope_invalid:stock_daily",
        "capability_ledger_query_failed",
        "coverage_query_failed",
    }
    assert all(item["strategy_eligible"] is False for item in payload["datasets"])


def test_assessor_executes_selects_only():
    connection = _Connection(
        capability_rows=_supported_rows(), coverage_rows=[]
    )

    assess_qmt_history_capabilities(connection)

    assert len(connection.statements) == 2
    assert all(
        statement.lstrip().upper().startswith("SELECT")
        for statement in connection.statements
    )
    forbidden = ("INSERT ", "UPDATE ", "DELETE ", "CREATE ", "ALTER ", "DROP ")
    assert all(
        token not in statement.upper()
        for statement in connection.statements
        for token in forbidden
    )
