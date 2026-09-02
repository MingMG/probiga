from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from server.common.qmt_attestation_contract import (
    bound_stock_set_contract,
    build_qmt_v2_manifest,
    daily_market_source_batch_id,
)
from server.common.daily_stock_universe import (
    DailyStockUniverse,
    validate_daily_stock_coverage,
)
from server.common import daily_stock_universe
from server.common import scheduler_validation


def _universe(*codes: str) -> DailyStockUniverse:
    return DailyStockUniverse(
        target_date="2026-08-26",
        catalog_batch_id="catalog-20260826",
        catalog_manifest_hash="a" * 64,
        catalog_member_set_hash="b" * 64,
        expected_codes=tuple(codes),
        expected_code_set_hash="c" * 64,
    )


def _bar(code: str, *, volume: int = 100, amount: int = 1_000) -> dict:
    return {"stock_code": code, "volume": volume, "amount": amount}


def _catalog(batch_id: str, manifest_hash: str, *, captured_at: str):
    codes = ("000001", "000002")
    return SimpleNamespace(
        batch_id=batch_id,
        manifest_hash=manifest_hash,
        member_set_hash="b" * 64,
        member_count=len(codes),
        captured_at=captured_at,
        history_complete_from="1990-01-01",
        members=tuple(
            {
                "stock_code": code,
                "list_date": "1991-01-01",
                "expire_date": None,
                "instrument_batch_id": "instrument-1",
                "instrument_type": "STOCK",
            }
            for code in codes
        ),
    )


def _completed_receipt(catalog, *, target_date: str = "2026-09-01") -> dict:
    codes = [member["stock_code"] for member in catalog.members]
    calendar_manifest_hash = "c" * 64
    entry = bound_stock_set_contract(
        target_date,
        codes,
        catalog_batch_id=catalog.batch_id,
        catalog_member_count=catalog.member_count,
        catalog_member_set_hash=catalog.member_set_hash,
        catalog_manifest_hash=catalog.manifest_hash,
        source_batch_id=daily_market_source_batch_id(
            catalog_manifest_hash=catalog.manifest_hash,
            calendar_manifest_hash=calendar_manifest_hash,
        ),
        calendar_batch_id="calendar-1",
        calendar_session_set_hash="d" * 64,
        calendar_manifest_hash=calendar_manifest_hash,
        calendar_known_at="2026-09-01 18:00:00",
    )
    count = len(codes)
    return {
        "run_id": "attestation-a",
        "start_date": target_date,
        "end_date": target_date,
        "target_rows": count,
        "qmt_rows": count,
        "matched_rows": count,
        "missing_qmt_rows": 0,
        "mismatched_rows": 0,
        "already_attested_rows": count,
        "updated_rows": 0,
        "tolerance_json": build_qmt_v2_manifest({target_date: entry}),
    }


def _engine_with_receipt(receipt: dict | None) -> MagicMock:
    engine = MagicMock()
    result = (
        engine.connect.return_value.__enter__.return_value.execute.return_value
    )
    result.mappings.return_value.one_or_none.return_value = receipt
    return engine


def test_partial_kline_cannot_pass_against_a_larger_catalog() -> None:
    universe = _universe("000001", "000002", "000003", "000004", "000005")

    with pytest.raises(RuntimeError, match=r"expected=5, actual=3"):
        validate_daily_stock_coverage(
            universe,
            kline_rows=[_bar("000001"), _bar("000002"), _bar("000003")],
        )


def test_equal_row_counts_with_shifted_flow_codes_fail_closed() -> None:
    universe = _universe("000001", "000002", "000003")

    with pytest.raises(RuntimeError, match="K-line/capital-flow sets are misaligned"):
        validate_daily_stock_coverage(
            universe,
            kline_rows=[_bar("000001"), _bar("000002"), _bar("000003")],
            flow_rows=[
                {"stock_code": "000001"},
                {"stock_code": "000002"},
                {"stock_code": "600000"},
            ],
        )


def test_only_zero_volume_zero_amount_suspension_may_omit_flow() -> None:
    universe = _universe("000001", "000002", "000003")

    audit = validate_daily_stock_coverage(
        universe,
        kline_rows=[
            _bar("000001"),
            _bar("000002"),
            _bar("000003", volume=0, amount=0),
        ],
        flow_rows=[{"stock_code": "000001"}, {"stock_code": "000002"}],
    )

    assert audit["kline_coverage"] == 1.0
    assert audit["traded_flow_coverage"] == 1.0
    assert audit["suspended_count"] == 1
    assert audit["suspended_without_flow_count"] == 1


def test_zero_volume_with_nonzero_amount_is_not_a_suspension_exception() -> None:
    universe = _universe("000001")

    with pytest.raises(RuntimeError, match=r"missing_traded_sample=\['000001'\]"):
        validate_daily_stock_coverage(
            universe,
            kline_rows=[_bar("000001", volume=0, amount=10)],
            flow_rows=[],
        )


@pytest.mark.parametrize("source", ["kline", "flow"])
def test_duplicate_codes_fail_before_publication(source: str) -> None:
    universe = _universe("000001")
    kline_rows = [_bar("000001")]
    flow_rows = [{"stock_code": "000001"}]
    if source == "kline":
        kline_rows.append(_bar("000001"))
    else:
        flow_rows.append({"stock_code": "000001"})

    with pytest.raises(RuntimeError, match="duplicate stock codes"):
        validate_daily_stock_coverage(
            universe,
            kline_rows=kline_rows,
            flow_rows=flow_rows,
        )


@pytest.mark.parametrize("bad_code", [None, "", "   "])
def test_empty_code_never_normalizes_to_000000(bad_code) -> None:
    universe = _universe("000001")

    with pytest.raises(RuntimeError, match="empty stock code"):
        validate_daily_stock_coverage(
            universe,
            kline_rows=[{"stock_code": bad_code, "volume": 1, "amount": 1}],
        )


def test_post_target_catalog_without_effective_range_proof_is_blocked(
    monkeypatch,
) -> None:
    catalog = SimpleNamespace(
        batch_id="catalog-late",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        captured_at="2026-08-26 16:00:00",
        history_complete_from="2026-08-22",
        members=(
            {
                "stock_code": "000001",
                "list_date": "1991-01-01",
                "expire_date": None,
                "instrument_batch_id": "instrument-1",
                "instrument_type": "STOCK",
            },
        ),
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda engine, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "_load_attested_target_receipt",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "load_target_stock_catalog",
        lambda *args, **kwargs: (catalog, ["000001"]),
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "_load_attested_target_projection",
        lambda *args, **kwargs: (("000001",), "", ()),
    )

    with pytest.raises(RuntimeError, match="lacks native effective-range proof"):
        daily_stock_universe.load_daily_stock_universe(
            object(), "2026-08-21"
        )


def test_post_target_catalog_with_native_effective_ranges_is_auditable(
    monkeypatch,
) -> None:
    catalog = SimpleNamespace(
        batch_id="catalog-late",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        captured_at="2026-08-26 16:00:00",
        history_complete_from="2024-01-01",
        members=(
            {
                "stock_code": "000001",
                "list_date": "1991-01-01",
                "expire_date": None,
                "instrument_batch_id": "instrument-1",
                "instrument_type": "STOCK",
            },
        ),
    )
    schema_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda engine, **kwargs: schema_calls.append(kwargs),
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "_load_attested_target_receipt",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "load_target_stock_catalog",
        lambda *args, **kwargs: (catalog, ["000001"]),
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "_load_attested_target_projection",
        lambda *args, **kwargs: (("000001",), "", ()),
    )

    universe = daily_stock_universe.load_daily_stock_universe(
        object(), "2026-08-21"
    )

    assert universe.catalog_knowledge_mode == (
        "RETROSPECTIVE_NATIVE_EFFECTIVE_RANGE"
    )
    assert universe.catalog_history_complete_from == "2024-01-01"
    assert schema_calls == [{"require_triggers": False}]


def test_daily_universe_applies_attested_no_row_projection(monkeypatch) -> None:
    catalog = SimpleNamespace(
        batch_id="catalog-target",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        member_count=2,
        captured_at="2026-08-26 16:00:00",
        history_complete_from="2024-01-01",
        members=tuple(
            {
                "stock_code": code,
                "list_date": "1991-01-01",
                "expire_date": None,
                "instrument_batch_id": "instrument-1",
                "instrument_type": "STOCK",
            }
            for code in ("000001", "301688")
        ),
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda engine, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "_load_attested_target_receipt",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "load_target_stock_catalog",
        lambda *args, **kwargs: (catalog, ["000001", "301688"]),
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "_load_attested_target_projection",
        lambda *args, **kwargs: (
            ("000001",),
            "c" * 64,
            ("301688",),
        ),
    )

    universe = daily_stock_universe.load_daily_stock_universe(
        object(), "2026-08-26"
    )

    assert universe.expected_codes == ("000001",)
    assert universe.no_row_exception_proof_sha256 == "c" * 64
    assert universe.excluded_no_row_codes == ("301688",)


def test_daily_universe_pins_catalog_from_latest_completed_receipt(
    monkeypatch,
) -> None:
    catalog_a = _catalog(
        "catalog-a", "a" * 64, captured_at="2026-09-01 23:09:36"
    )
    catalog_b = _catalog(
        "catalog-b", "e" * 64, captured_at="2026-09-02 03:20:19"
    )
    engine = _engine_with_receipt(_completed_receipt(catalog_a))
    loaded_batch_ids: list[str | None] = []

    def load_catalog(*args, batch_id=None, **kwargs):
        loaded_batch_ids.append(batch_id)
        selected = catalog_a if batch_id == catalog_a.batch_id else catalog_b
        return selected, [member["stock_code"] for member in selected.members]

    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "load_target_stock_catalog",
        load_catalog,
    )

    universe = daily_stock_universe.load_daily_stock_universe(
        engine,
        "2026-09-01",
        decision_known_at=datetime(2026, 9, 2, 8, 0),
    )

    assert loaded_batch_ids == ["catalog-a"]
    assert universe.catalog_batch_id == "catalog-a"
    assert universe.catalog_manifest_hash == "a" * 64
    assert universe.expected_codes == ("000001", "000002")


def test_daily_universe_uses_latest_catalog_only_without_receipt(
    monkeypatch,
) -> None:
    catalog_b = _catalog(
        "catalog-b", "e" * 64, captured_at="2026-09-02 03:20:19"
    )
    engine = _engine_with_receipt(None)
    loaded_batch_ids: list[str | None] = []

    def load_catalog(*args, batch_id=None, **kwargs):
        loaded_batch_ids.append(batch_id)
        return catalog_b, [member["stock_code"] for member in catalog_b.members]

    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "load_target_stock_catalog",
        load_catalog,
    )

    universe = daily_stock_universe.load_daily_stock_universe(
        engine,
        "2026-09-01",
        decision_known_at=datetime(2026, 9, 2, 8, 0),
    )

    assert loaded_batch_ids == [None]
    assert universe.catalog_batch_id == "catalog-b"


def test_daily_universe_fails_closed_when_receipt_catalog_is_missing(
    monkeypatch,
) -> None:
    catalog_a = _catalog(
        "catalog-a", "a" * 64, captured_at="2026-09-01 23:09:36"
    )
    engine = _engine_with_receipt(_completed_receipt(catalog_a))
    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda *args, **kwargs: None,
    )

    def missing_catalog(*args, batch_id=None, **kwargs):
        assert batch_id == "catalog-a"
        raise RuntimeError("no complete independent QMT stock catalog batch")

    monkeypatch.setattr(
        daily_stock_universe,
        "load_target_stock_catalog",
        missing_catalog,
    )

    with pytest.raises(RuntimeError, match="no complete independent"):
        daily_stock_universe.load_daily_stock_universe(
            engine,
            "2026-09-01",
            decision_known_at=datetime(2026, 9, 2, 8, 0),
        )


def test_daily_universe_fails_closed_before_catalog_on_tampered_receipt(
    monkeypatch,
) -> None:
    catalog_a = _catalog(
        "catalog-a", "a" * 64, captured_at="2026-09-01 23:09:36"
    )
    receipt = deepcopy(_completed_receipt(catalog_a))
    receipt["tolerance_json"]["daily_universe"]["2026-09-01"][
        "catalog_manifest_hash"
    ] = "f" * 64
    engine = _engine_with_receipt(receipt)
    load_catalog = MagicMock()
    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        daily_stock_universe,
        "load_target_stock_catalog",
        load_catalog,
    )

    with pytest.raises(RuntimeError, match="receipt manifest is invalid"):
        daily_stock_universe.load_daily_stock_universe(
            engine,
            "2026-09-01",
            decision_known_at=datetime(2026, 9, 2, 8, 0),
        )
    load_catalog.assert_not_called()


def test_market_overview_validator_binds_total_to_catalog(monkeypatch) -> None:
    universe = _universe("000001", "000002")
    monkeypatch.setattr(
        scheduler_validation,
        "load_daily_stock_universe",
        lambda *args, **kwargs: universe,
    )

    def fake_read_all(engine, sql, params=None):
        normalized = " ".join(sql.split())
        if "FROM sm_stock_kline" in normalized:
            return [_bar("000001"), _bar("000002")]
        if "FROM sm_market_overview_daily" in normalized:
            return [{"total": 1}]
        raise AssertionError(normalized)

    monkeypatch.setattr(scheduler_validation, "_read_all", fake_read_all)

    ok, message = scheduler_validation._validate_daily_universe_coverage(
        object(),
        task_type="market_overview_daily",
        target_date=date(2026, 8, 26),
        decision_known_at=datetime(2026, 8, 26, 18, 0),
    )

    assert ok is False
    assert "total=1 expected=2" in message


def test_morning_analysis_validator_uses_daily_kline_and_flow_sets(
    monkeypatch,
) -> None:
    universe = _universe("000001", "000002")
    monkeypatch.setattr(
        scheduler_validation,
        "load_daily_stock_universe",
        lambda *args, **kwargs: universe,
    )

    def fake_read_all(engine, sql, params=None):
        normalized = " ".join(sql.split())
        if "FROM sm_stock_kline" in normalized:
            return [_bar("000001"), _bar("000002")]
        if "FROM sm_stock_capital_flow_daily" in normalized:
            return [{"stock_code": "000001"}, {"stock_code": "000002"}]
        raise AssertionError(normalized)

    monkeypatch.setattr(scheduler_validation, "_read_all", fake_read_all)

    ok, message = scheduler_validation._validate_daily_universe_coverage(
        object(),
        task_type="analysis_morning_strict",
        target_date=date(2026, 8, 26),
        decision_known_at=datetime(2026, 8, 27, 8, 30),
    )

    assert ok is True
    assert "exact capital-flow/catalog coverage verified" in message


def test_snapshot_validator_rejects_shifted_source_sets(monkeypatch) -> None:
    universe = _universe("000001", "000002")
    monkeypatch.setattr(
        scheduler_validation,
        "load_daily_stock_universe",
        lambda *args, **kwargs: universe,
    )

    def fake_read_all(engine, sql, params=None):
        normalized = " ".join(sql.split())
        if "FROM sm_stock_kline" in normalized:
            return [_bar("000001"), _bar("000002")]
        if "FROM sm_stock_capital_flow_daily" in normalized:
            return [{"stock_code": "000001"}, {"stock_code": "600000"}]
        if "FROM sm_stock_snapshot" in normalized:
            return [_bar("000001"), _bar("000002")]
        raise AssertionError(normalized)

    monkeypatch.setattr(scheduler_validation, "_read_all", fake_read_all)

    with pytest.raises(RuntimeError, match="sets are misaligned"):
        scheduler_validation._validate_daily_universe_coverage(
            object(),
            task_type="stock_snapshot_daily",
            target_date=date(2026, 8, 26),
            decision_known_at=datetime(2026, 8, 26, 18, 0),
        )
