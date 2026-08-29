from __future__ import annotations

import pytest
from datetime import date, datetime
from types import SimpleNamespace

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
        lambda engine: None,
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
    monkeypatch.setattr(
        daily_stock_universe,
        "validate_stock_catalog_runtime_schema",
        lambda engine: None,
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
        lambda engine: None,
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
