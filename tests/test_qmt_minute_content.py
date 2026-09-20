from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pytest

from server.common import qmt_minute_content as content

DAY = "2026-09-01"
LAYOUT = {field: [50, 6] for field in content.NUMBERS}


def raw_row():
    return dict(stock_code="000001", trade_date=DAY, trade_time=DAY + " 10:00:00",
                source_time=DAY + " 10:00:00", data_source="gj_big_qmt_inner", batch_id="native-run",
                price=10.1234565, avg_price=None, change=-10.1234565, change_pct=-0.0000001,
                volume=100, amount=99999999.9999995)


def test_input_normalization_matches_mysql_decimal_cast_wire_tuple():
    # Verified with read-only MySQL CAST/JSON_ARRAY/SHA2, including float
    # parameter binding; never derive expected content from a persisted row.
    values = content.normalized_tuple(raw_row(), layout=LAYOUT, trade_date=DAY, run_id="native-run")
    assert values == ["000001", DAY + " 10:00:00", DAY, "10.123457", None, "-10.123457",
                      "0.000000", "100.000000", "100000000.000000", "gj_big_qmt_inner", DAY + " 10:00:00", "native-run"]
    persisted = {**raw_row(), "price": Decimal("10.123457"), "change": Decimal("-10.123457"),
                 "change_pct": Decimal("0.000000"), "volume": Decimal("100.000000"), "amount": Decimal("100000000.000000")}
    assert content.input_content_rows([raw_row()], layout=LAYOUT, trade_date=DAY, run_id="native-run") == content.input_content_rows(
        [persisted], layout=LAYOUT, trade_date=DAY, run_id="native-run")


@pytest.mark.parametrize("change", [
    {"price": True}, {"price": float("inf")}, {"amount": "nan"}, {"volume": "1e60"},
    {"stock_code": "1"}, {"source_time": DAY + " 10:01:00"}, {"trade_time": DAY + " 10:00:00.001"},
    {"trade_date": "2026-09-02"}, {"data_source": "public"}, {"batch_id": "other-run"},
])
def test_invalid_input_cannot_produce_expected_content(change):
    with pytest.raises(ValueError):
        content.input_content_rows([{**raw_row(), **change}], layout=LAYOUT, trade_date=DAY, run_id="native-run")


@pytest.mark.parametrize("field", content.FIELDS)
def test_every_canonical_field_is_bound_by_sql_hash(field):
    assert f"CAST(`{field}` AS CHAR)" in content.sql_row_hash()
    assert "etl_sync_at" not in content.sql_row_hash()
    assert "`id`" not in content.sql_row_hash()


def test_physical_read_is_bounded_and_restores_session_limit(monkeypatch):
    monkeypatch.setattr(content, "load_numeric_layout", lambda _connection: LAYOUT)
    codes = [f"{value:06d}" for value in range(1, 206)]
    entities = [{"stock_code": code, "bar_count": 241, "expected_state": "TRADED"} for code in codes]
    manifest = {"trade_date": DAY, "run_id": "native-run", "manifest_hash": "a" * 64, "bar_count": len(codes) * 241}
    calls, settings = [], []
    state = {"truncate": False, "fail": False}
    class Connection:
        def exec_driver_sql(self, sql):
            settings.append(sql)
            return SimpleNamespace(scalar_one=lambda: 1024)
        def execute(self, sql, params):
            assert "trade_time>=:day" in str(sql) and "trade_time<:next_day" in str(sql)
            assert params["next_day"] == "2026-09-02"
            calls.append(params["codes"])
            if state["fail"]: raise RuntimeError("read interrupted")
            rows = [{"stock_code": code, "row_count": 241, "row_hash": "b" * 64,
                     "hashed_bytes": 100 if state["truncate"] else 241 * 64} for code in params["codes"]]
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
    kwargs = dict(table="sm_stock_minute_qmt_stage_123", manifest=manifest, entities=entities, layout=LAYOUT)
    proof = content.read_content_proof(Connection(), **kwargs)
    assert proof["row_count"] == 205 * 241
    assert [len(batch) for batch in calls] == [100, 100, 5]
    assert settings[-1] == "SET SESSION group_concat_max_len=1024"
    state["truncate"] = True
    with pytest.raises(ValueError, match="truncated"):
        content.read_content_proof(Connection(), **kwargs)
    state["fail"] = True
    with pytest.raises(RuntimeError, match="interrupted"):
        content.read_content_proof(Connection(), **kwargs)
    assert settings[-1] == "SET SESSION group_concat_max_len=1024"
    with pytest.raises(ValueError, match="table differs"):
        content.read_content_proof(Connection(), **{**kwargs, "table": "unexpected"})


def test_proof_cannot_change_input_manifest_or_storage_layout():
    rows = content.input_content_rows([raw_row()], layout=LAYOUT, trade_date=DAY, run_id="native-run")
    entities = [{"stock_code": "000001", "bar_count": 1, "expected_state": "TRADED"}]
    manifest = {"trade_date": DAY, "run_id": "native-run", "manifest_hash": "a" * 64, "bar_count": 1}
    proof = content.content_proof(rows, layout=LAYOUT, manifest=manifest, entities=entities)
    assert content.validate_content_proof(proof, manifest=manifest, entities=entities) == proof
    for change in ({"run_id": "other"}, {"coverage_manifest_hash": "b" * 64}, {"numeric_layout": {}},
                   {"content_root_sha256": "invalid"}, {"row_count": 2}):
        with pytest.raises(ValueError):
            content.validate_content_proof({**proof, **change}, manifest=manifest, entities=entities)
