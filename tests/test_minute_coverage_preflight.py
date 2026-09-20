"""Coverage may reject cheaply, but can never replace the canonical proof."""
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from server.common import minute_acquisition_reuse as reuse
from tools import repair_qmt_canonical_history_gaps as repair
from test_minute_acquisition_reuse import inventory_row

DAY = "2026-08-25"
NOW = datetime(2026, 9, 21, 4, 42)


class RecordingConnection:
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def execute(self, statement, params):
        self.calls.append((str(statement), params))
        return self.connection.execute(statement, params)


@pytest.fixture
def database():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE sm_stock_capital_flow_min (stock_code TEXT,trade_time TEXT)"))
    yield engine
    engine.dispose()


def put_minutes(connection, code, count, *, duplicate=False):
    start = datetime.fromisoformat(DAY + " 09:30:00")
    rows = [{"code": code, "at": (start + timedelta(minutes=offset)).isoformat(sep=" ")}
            for offset in range(count)]
    if duplicate:
        rows[-1]["at"] = rows[-2]["at"]
    connection.execute(text("INSERT INTO sm_stock_capital_flow_min VALUES (:code,:at)"), rows)


def probe(connection, codes, counts=(240, 241)):
    return reuse.has_minute_code_coverage(
        connection, table=reuse.TABLES["flow"], trade_date=DAY,
        expected_codes=codes, allowed_counts=counts,
    )


def test_missing_first_batch_stops_after_one_index_bounded_select(database):
    codes = [f"{value:06d}" for value in range(1, 202)]
    with database.connect() as connection:
        observed = RecordingConnection(connection)
        assert probe(observed, codes) is False
    assert len(observed.calls) == 1
    sql, params = observed.calls[0]
    assert params["codes"] == codes[:100]
    assert params["day"] == DAY and params["next_day"] == "2026-08-26"
    assert "stock_code IN" in sql and "trade_time>=:day" in sql
    assert "SHA2" not in sql and "GROUP_CONCAT" not in sql


@pytest.mark.parametrize("counts,expected", [((240, 241), True), ((241,), False)])
def test_public_and_native_counts_remain_distinct_for_native_fallback(database, counts, expected):
    with database.begin() as connection:
        put_minutes(connection, "000001", 240)
        put_minutes(connection, "000002", 241)
        assert probe(connection, ["000001", "000002"], counts) is expected


@pytest.mark.parametrize("count,duplicate", [(239, False), (241, True)])
def test_missing_or_duplicate_time_never_passes_coverage(database, count, duplicate):
    with database.begin() as connection:
        put_minutes(connection, "000001", count, duplicate=duplicate)
        assert probe(connection, ["000001"]) is False


class InventoryConnection:
    def __init__(self, coverage, inventory):
        self.coverage = coverage
        self.inventory = inventory
        self.calls = []
        self.settings = []

    def __enter__(self): return self
    def __exit__(self, *_args): pass

    def exec_driver_sql(self, statement):
        self.settings.append(statement)
        return SimpleNamespace(scalar_one=lambda: 1024)

    def execute(self, statement, params):
        hashed = "SHA2(" in str(statement)
        self.calls.append((hashed, params["codes"]))
        selected = self.inventory if hashed else self.coverage
        rows = [row for row in selected if row["stock_code"] in params["codes"]]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))


def configure_catalog(monkeypatch, codes, *, no_trade=()):
    from server.common import qmt_stock_catalog
    from tools import crawl_minute_kline
    monkeypatch.setattr(crawl_minute_kline, "_is_trade_day", lambda *_args: True)
    monkeypatch.setattr(qmt_stock_catalog, "load_target_stock_catalog", lambda *_args, **_kwargs: (
        SimpleNamespace(batch_id="catalog", manifest_hash="a" * 64), codes,
    ))
    monkeypatch.setattr(crawl_minute_kline, "verified_no_trade_codes", lambda *_args, **_kwargs: (
        set(no_trade), {"proof_sha256": "b" * 64} if no_trade else None,
    ))


@pytest.mark.parametrize("kind", ["stock", "flow"])
def test_inspector_does_not_hash_or_modify_session_settings_when_first_batch_is_missing(monkeypatch, kind):
    codes = [f"{value:06d}" for value in range(1, 202)]
    configure_catalog(monkeypatch, codes)
    connection = InventoryConnection([], [])
    engine = SimpleNamespace(connect=lambda: connection)
    assert reuse.inspect_complete_partition(object(), engine, kind=kind, trade_date=DAY, now=NOW) is None
    assert connection.calls == [(False, codes[:100])]
    assert connection.settings == []


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("native", [False, True])
def test_complete_source_inventory_and_attested_no_trade_still_receive_full_proof(monkeypatch, kind, native):
    configure_catalog(monkeypatch, ["000001", "000002"], no_trade=["000002"])
    row = inventory_row(kind=kind, native=native)
    connection = InventoryConnection([row], [row])
    proof_calls = []
    monkeypatch.setattr(reuse, "native_stock_closes_match", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(reuse, "load_numeric_layout", lambda *_args: {})
    monkeypatch.setattr(reuse, "_native_stock_proof", lambda *_args, **_kwargs: (
        proof_calls.append("native_content") or {"verified": True}
    ))
    result = reuse.inspect_complete_partition(object(), SimpleNamespace(connect=lambda: connection),
                                              kind=kind, trade_date=DAY, now=NOW)
    assert result["row_count"] == (241 if native else 240)
    assert result["source_rows"] == {row["data_source"]: row["row_count"]}
    assert result["no_trade_count"] == 1
    assert connection.calls == [(False, ["000001"]), (True, ["000001", "000002"])]
    assert proof_calls == (["native_content"] if kind == "stock" and native else [])


@pytest.mark.parametrize("change", [
    {"grid_hash": "0" * 64}, {"invalid_count": 1}, {"source_count": 2},
    {"data_source": "unknown"}, {"hashed_bytes": 1024}, {"row_count": 239},
])
def test_positive_coverage_cannot_bypass_grid_values_source_or_later_change(monkeypatch, change):
    configure_catalog(monkeypatch, ["000001"])
    row = inventory_row()
    connection = InventoryConnection([row], [{**row, **change}])
    assert reuse.inspect_complete_partition(object(), SimpleNamespace(connect=lambda: connection),
                                            kind="stock", trade_date=DAY, now=NOW) is None
    assert [hashed for hashed, _codes in connection.calls] == [False, True]


def test_no_trade_rows_are_not_hidden_by_the_coverage_probe(monkeypatch):
    configure_catalog(monkeypatch, ["000001", "000002"], no_trade=["000002"])
    row = inventory_row()
    connection = InventoryConnection([row], [row, inventory_row("000002")])
    assert reuse.inspect_complete_partition(object(), SimpleNamespace(connect=lambda: connection),
                                            kind="stock", trade_date=DAY, now=NOW) is None


def test_native_content_tampering_still_rejects_a_full_grid(monkeypatch):
    configure_catalog(monkeypatch, ["000001"])
    row = inventory_row(native=True)
    connection = InventoryConnection([row], [row])
    monkeypatch.setattr(reuse, "native_stock_closes_match", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(reuse, "load_numeric_layout", lambda *_args: {})
    calls = []
    monkeypatch.setattr(reuse, "_native_stock_proof", lambda *_args, **_kwargs: calls.append("mismatch") or None)
    assert reuse.inspect_complete_partition(object(), SimpleNamespace(connect=lambda: connection),
                                            kind="stock", trade_date=DAY, now=NOW) is None
    assert calls == ["mismatch"]


def test_native_fallback_confirms_its_own_universe_gap_without_full_table_scan(database, monkeypatch):
    from tools import sync_qmt_minute_flow_exact as flow
    codes = [f"{value:06d}" for value in range(1, 202)]
    monkeypatch.setattr(flow, "validate_runtime_schema", lambda *_args: None)
    monkeypatch.setattr(flow, "load_flow_universe", lambda *_args, **_kwargs: SimpleNamespace(
        qmt_by_stock={code: code + ".SZ" for code in codes},
    ))
    monkeypatch.setattr(flow, "_stream_table_proof", lambda *_args, **_kwargs: pytest.fail("full-table fallback entered"))
    inspector = repair.CanonicalPartitionInspector(object(), object(), database,
        window=SimpleNamespace(sessions=[DAY]), decision_time=NOW)
    # Collection reuse may have a later catalog. Its negative cannot stand in
    # for this branch's independently attested historical native universe.
    monkeypatch.setattr(inspector, "_complete_minute_acquisition", lambda *_args, **_kwargs: None)
    statements = []
    event.listen(database, "before_cursor_execute", lambda _conn, _cursor, statement, params, *_rest:
                 statements.append((statement, params)))
    with pytest.raises(repair.CanonicalGapRepairBlocked, match="code/grid coverage incomplete"):
        inspector(repair.PartitionRef("stock_minute_flow", DAY))
    assert len(statements) == 1
    assert "stock_code IN" in statements[0][0]


def test_native_fallback_can_validate_older_catalog_after_newer_catalog_rejects(database, monkeypatch):
    from tools import sync_qmt_minute_flow_exact as flow
    configure_catalog(monkeypatch, ["000001", "000002"])
    with database.begin() as connection:
        connection.execute(text("INSERT INTO sm_stock_capital_flow_min VALUES (:code,:at)"),
            [{"code": "000001", "at": DAY + " " + minute} for minute in flow.GRID])
    monkeypatch.setattr(flow, "validate_runtime_schema", lambda *_args: None)
    monkeypatch.setattr(flow, "load_flow_universe", lambda *_args, **_kwargs: SimpleNamespace(
        qmt_by_stock={"000001": "000001.SZ"}, traded_stock_count=1,
        traded_stock_set_hash="c" * 64, catalog={"manifest_hash": "d" * 64},
        daily_truth={"truth_hash": "e" * 64},
    ))
    full_proof_calls = []
    def full_proof(_connection, *, table, trade_date):
        full_proof_calls.append((table, trade_date))
        return dict(row_count=241, row_hash="f" * 64, code_count=1, code_set_hash="c" * 64,
                    minute_grid_profile=flow.QMT_MINUTE_GRID_PROFILE,
                    minute_grid_count=241, minute_grid_hash=flow.GRID_HASH, nonzero_code_ratio=1)
    monkeypatch.setattr(flow, "_stream_table_proof", full_proof)
    inspector = repair.CanonicalPartitionInspector(object(), object(), database,
        window=SimpleNamespace(sessions=[DAY]), decision_time=NOW)
    result = inspector(repair.PartitionRef("stock_minute_flow", DAY))
    assert result["row_count"] == 241
    assert result["catalog_manifest_hash"] == "d" * 64
    assert full_proof_calls == [(flow.TABLE, DAY)]


def test_coverage_cannot_address_an_arbitrary_table():
    with pytest.raises(ValueError, match="table differs"):
        reuse.has_minute_code_coverage(object(), table="arbitrary", trade_date=DAY,
                                       expected_codes=["000001"], allowed_counts=(241,))
