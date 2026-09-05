from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace

import pandas as pd
import pytest

from tools import sync_qmt_index_edge as publisher


NOW = datetime(2026, 9, 4, 16, 0)
SESSION = "2026-09-04"
CATALOG = [publisher.IndexCatalogMember("000001", "000001.SH", "index", None, None, "catalog-1")]


def _raw(dataset):
    times = publisher.minute_time_grid() if dataset == "minute" else ["15:00:00"]
    return pd.DataFrame([{
        "stock_code": "000001", "qmt_code": "000001.SH",
        "trade_time": SESSION + " " + minute, "trade_date": SESSION,
        "snapshot_at": SESSION + " " + minute,
        "open": 3930.11, "high": 3935.22, "low": 3928.01,
        "close": 3930.0, "price": 3930.0, "avg_price": None,
        "volume": 12345.1234567, "amount": 67890.7654325,
        "change": -11.972000000000207, "change_pct": -0.303696924066642,
    } for minute in times])


def _validate(dataset, raw):
    kwargs = {"catalog": CATALOG, "captured_at": NOW}
    if dataset == "current":
        return publisher.validate_current_frame(raw, trade_date=SESSION, **kwargs)
    function = publisher.validate_kline_frame if dataset == "kline" else publisher.validate_minute_frame
    return function(raw, expected_by_session={SESSION: ["000001"]}, **kwargs)


def _decimal_readback(frame):
    """Emulate PyMySQL's DECIMAL(50,6) values, not SQLite FLOAT storage."""
    result = frame.copy()
    for column in ("open", "close", "price", "high", "low", "avg_price", "volume", "amount", "change", "change_pct"):
        if column in result:
            result[column] = result[column].map(
                lambda value: None if pd.isna(value) else Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            )
    result["stock_code"] = "000001"
    result["qmt_code"] = "000001.SH"
    return result


@pytest.mark.parametrize("dataset", ["current", "kline", "minute"])
def test_decimal_roundtrip_preserves_source_hash_and_detects_stored_change(monkeypatch, dataset):
    source = _validate(dataset, _raw(dataset))
    assert source.iloc[0]["change"] == -11.972
    assert source.iloc[0]["change_pct"] == -0.303697
    assert source.iloc[0]["amount"] == 67890.765433
    stored = _decimal_readback(source)
    assert publisher._frame_hash(_validate(dataset, stored)) == publisher._frame_hash(source)

    calendar = SimpleNamespace(batch_id="calendar-1", manifest_hash="5" * 64, session_set_hash="6" * 64)
    manifest = publisher._manifest(
        dataset=dataset, build_sha="7" * 40,
        release={"strategy_git_blob": "1" * 40, "strategy_source_sha256": "2" * 64,
                 "strategy_artifact_sha256": "3" * 64, "strategy_loaded_identity_sha256": "4" * 64},
        calendar=calendar, catalog=CATALOG, expected_by_session={SESSION: ["000001"]},
        row_count=len(source), source_frame_hash=publisher._frame_hash(source),
        capture_receipts=[{"request_id": "source-1"}], captured_at=NOW, applied=True,
    )
    result = publisher.build_complete_result(dataset=dataset, manifest=manifest,
                                             written_rows=len(source), verified_rows=len(source))
    monkeypatch.setattr(publisher, "_expected_build_sha", lambda: "7" * 40)
    monkeypatch.setattr(publisher, "_resolve_sessions", lambda *a, **kw: (calendar, [SESSION]))
    monkeypatch.setattr(publisher, "_load_index_catalog", lambda *a, **kw: CATALOG)
    monkeypatch.setattr(publisher, "get_kline_engine", lambda: object())
    monkeypatch.setattr(publisher, "_read_published", lambda **kw: stored)
    assert publisher.validate_persisted_result(object(), result, now=NOW)["row_count"] == len(source)

    # One stored unit remains a real mismatch, not a broad epsilon exemption.
    stored.loc[0, "change_pct"] += Decimal("0.000001")
    with pytest.raises(publisher.IndexDataBlocked, match="partition differs"):
        publisher.validate_persisted_result(object(), result, now=NOW)


def test_storage_rounding_is_mysql_half_away_from_zero_and_normalizes_signed_zero():
    frame = publisher._storage_frame(pd.DataFrame({"change": [1.2345665, -1.2345665, -0.00000001]}))
    assert frame["change"].tolist() == [1.234567, -1.234567, 0.0]
    assert str(frame.iloc[2]["change"]) == "0.0"


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), 1e44])
def test_storage_contract_rejects_nonfinite_or_overflow(value):
    with pytest.raises(publisher.IndexDataBlocked, match="storage contract"):
        publisher._storage_frame(pd.DataFrame({"change": [value]}))
