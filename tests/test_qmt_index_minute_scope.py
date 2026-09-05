from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from server.common.qmt_history_coverage import QMT_MINUTE_GRID_PROFILE, minute_time_grid
from tools import sync_qmt_index_edge as publisher


SESSION = "2026-09-04"
CATALOG = [publisher.IndexCatalogMember(
    index_code="980001", qmt_code="980001.SZ", name="cross-market index",
    list_date=None, expire_date=None, batch_id="reference-batch",
)]


def _row(minute, **overrides):
    return {
        "stock_code": "980001", "qmt_code": "980001.SZ",
        "trade_date": SESSION, "trade_time": f"{SESSION} {minute}",
        "price": 123.4567, "volume": 0, "amount": 0, **overrides,
    }


def _native_rows():
    return [_row(minute) for minute in minute_time_grid()] + [
        _row(minute) for minute in ("11:31:00", "12:00:00", "15:01:00", "16:09:00")
    ]


def _validate(rows):
    return publisher.validate_minute_frame(
        pd.DataFrame(rows), catalog=CATALOG,
        expected_by_session={SESSION: ("980001",)},
        captured_at=datetime(2026, 9, 5, 20, 0),
    )


def test_cross_market_native_day_is_projected_to_existing_a_share_contract():
    validated = _validate(_native_rows())
    assert len(validated) == 241
    assert set(validated.trade_time.dt.strftime("%H:%M:%S")) == set(minute_time_grid())
    assert set(validated.price) == {123.4567}


def test_manifest_explicitly_identifies_a_share_scope_not_full_native_day():
    manifest = publisher._manifest(
        dataset="minute", build_sha="a" * 40,
        release={key: "b" * 64 for key in (
            "strategy_git_blob", "strategy_source_sha256",
            "strategy_artifact_sha256", "strategy_loaded_identity_sha256",
        )},
        calendar=SimpleNamespace(batch_id="calendar-batch", manifest_hash="c" * 64,
                                 session_set_hash="d" * 64),
        catalog=CATALOG, expected_by_session={SESSION: ("980001",)},
        row_count=241, source_frame_hash="e" * 64, capture_receipts=[],
        captured_at=datetime(2026, 9, 5, 20, 0), applied=False,
    )
    assert manifest["minute_scope"] == QMT_MINUTE_GRID_PROFILE
    assert manifest["minute_grid_count"] == 241


@pytest.mark.parametrize("minute", ["09:30:00", "15:00:00"])
def test_extra_native_points_cannot_replace_missing_a_share_points(minute):
    rows = [row for row in _native_rows() if row["trade_time"] != f"{SESSION} {minute}"]
    with pytest.raises(publisher.IndexDataBlocked, match="241-bar grid is incomplete"):
        _validate(rows)


@pytest.mark.parametrize("minute", ["09:30:00", "16:09:00"])
def test_duplicate_native_keys_are_rejected_inside_and_outside_scope(minute):
    with pytest.raises(publisher.IndexDataBlocked, match="inventory differs"):
        _validate(_native_rows() + [_row(minute)])


def test_out_of_scope_time_does_not_hide_unexpected_code():
    with pytest.raises(publisher.IndexDataBlocked):
        _validate(_native_rows() + [_row("16:09:00", stock_code="999999", qmt_code="999999.SZ")])


def test_out_of_scope_time_does_not_hide_unrequested_session():
    with pytest.raises(publisher.IndexDataBlocked, match="inventory differs"):
        _validate(_native_rows() + [_row("16:09:00", trade_date="2026-09-03", trade_time="2026-09-03 16:09:00")])


def test_out_of_scope_time_does_not_hide_inconsistent_date_fields():
    with pytest.raises(publisher.IndexDataBlocked, match="date fields differ"):
        _validate(_native_rows() + [_row("16:08:00", trade_date="2026-09-03")])
