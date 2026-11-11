"""Scheduled minute publishers open existing partitions before using DAT."""
from datetime import date
import json

import pytest
from sqlalchemy import create_engine, text

from integrations.qmt.datadir_import import SOURCE, Universe
from server.common import qmt_datadir_minute_coverage as stock_coverage
from server.common import qmt_datadir_result as results
from server.common.qmt_minute_publication_scope import load_verified_minute_security_rows
from tests.test_qmt_datadir_partition_reuse import DAY as STOCK_DAY, existing_partition
from tests.test_tactical_minute_pit import _bar, _bars_fixture, _create_receipt_tables
from tools import import_qmt_datadir_history as stocks
from tools import import_qmt_datadir_index as indexes
from tools.sync_qmt_index_edge import IndexCatalogMember


def _forbid_source(*_args, **_kwargs):
    pytest.fail("a proven existing partition must not read or replace native bars")


def _read_stock_rows(engine):
    with engine.connect() as connection:
        return connection.execute(text("SELECT * FROM sm_stock_minute ORDER BY stock_code,trade_time")).all()


def _stock_cli(item, monkeypatch):
    actual = stock_coverage.persisted_matrix(item.engine, str(STOCK_DAY), scope=item.scope)
    with item.engine.begin() as connection:
        stocks.write_receipt(
            connection, session=STOCK_DAY, root="original-cache", status="COMPLETE",
            run_id="original-minute-run", code_count=2, row_count=482,
            content_sha256="c" * 64, detail=stocks._completion_detail(actual), finished=True,
        )
        connection.execute(text(
            "UPDATE qmt_datadir_import_partition SET finished_at=:original "
            "WHERE dataset='stock_minute'"
        ), {"original": str(STOCK_DAY) + " 19:00:00"})
        connection.execute(text(
            "INSERT INTO qmt_history_coverage_manifest "
            "(trade_date,manifest_hash,dataset,provider,schema_version,status,strategy_eligible,captured_at) "
            "VALUES (:day,:digest,'stock_daily','qmt_datadir',:schema,'EXACT',1,:captured)"
        ), {
            "day": str(STOCK_DAY), "digest": item.scope["full_market_manifest_hash"],
            "schema": stock_coverage.DAILY_SCHEMA, "captured": str(STOCK_DAY) + " 18:00:00",
        })
    monkeypatch.setattr(stocks, "load_project_env", lambda: None)
    monkeypatch.setattr(stocks, "get_minute_mysql_url", lambda: "sqlite://")
    monkeypatch.setattr(stocks, "create_pooled_engine", lambda *_a, **_k: item.engine)
    monkeypatch.setattr(stocks, "_sessions", lambda *_a: [STOCK_DAY])
    monkeypatch.setattr(stocks, "load_universe", lambda *_a, **_k: Universe(
        {member.qmt_code: member for member in item.universe}, {},
    ))
    monkeypatch.setattr(stocks, "require_schema", lambda *_a: None)
    monkeypatch.setattr(stocks, "existing_sources", lambda *_a: {STOCK_DAY: [SOURCE]})
    monkeypatch.setattr(stocks, "harvest", _forbid_source)
    monkeypatch.setattr(stocks, "_source_matrix", _forbid_source)
    monkeypatch.setattr(stocks, "publish_session", _forbid_source)
    return [
        "--start", str(STOCK_DAY), "--end", str(STOCK_DAY), "--apply", "--json",
        "--datadir-root", str(item.root / "missing-DAT-cache"),
    ]


def test_stock_cli_skip_complete_seals_missing_proof_and_reuses_its_actual_time(
    existing_partition, monkeypatch, capsys,
):
    item = existing_partition
    argv = _stock_cli(item, monkeypatch)
    before = _read_stock_rows(item.engine)
    assert stocks.main(argv) == 0
    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert payload["status"] == "PASS" and payload["sessions"] == [str(STOCK_DAY)]
    assert _read_stock_rows(item.engine) == before
    receipt = results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(STOCK_DAY))
    assert receipt["run_id"] == "original-minute-run"
    assert receipt["source_root"] == "original-cache" and receipt["content_sha256"] == "c" * 64
    with item.engine.connect() as connection:
        old, _ = load_verified_minute_security_rows(
            connection, connection, dataset="stock_minute", trade_date=str(STOCK_DAY),
            decision_known_at=str(STOCK_DAY) + " 22:20:00", codes=["000001"],
        )
        proof, rows = load_verified_minute_security_rows(
            connection, connection, dataset="stock_minute", trade_date=str(STOCK_DAY),
            decision_known_at="2026-11-03 05:00:00", codes=["000001"],
        )
        assert old == {}
        assert proof["known_at"] == "2026-11-03 04:40:00" and len(rows["000001"]) == 241
        connection.connection.driver_connection.create_function("NOW", 0, lambda: "2026-11-03 05:40:00")
    monkeypatch.setattr(stocks, "trade_scope", _forbid_source)
    monkeypatch.setattr(stocks, "persisted_matrix", _forbid_source)
    assert stocks.main(argv) == 0
    capsys.readouterr()
    assert results.load_receipt(item.engine, dataset="stock_minute", trade_date=str(STOCK_DAY)) == receipt
    with item.engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM qmt_history_coverage_manifest "
            "WHERE dataset='stock_minute_security_publication'"
        )).scalar_one() == 1


def test_stock_cli_skip_complete_requires_the_original_daily_close_anchors(
    existing_partition, monkeypatch,
):
    item = existing_partition
    argv = _stock_cli(item, monkeypatch)
    item.scope["daily_close_by_code"]["000001"] = "11"
    with pytest.raises(SystemExit) as blocked:
        stocks.main(argv)
    assert blocked.value.code == 2
    with item.engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM qmt_history_coverage_manifest "
            "WHERE dataset='stock_minute_security_publication'"
        )).scalar_one() == 0


@pytest.mark.parametrize("damage", ["changed", "deleted"])
def test_stock_cli_rechecks_sampled_native_rows_before_reusing_existing_proof(
    existing_partition, monkeypatch, capsys, damage,
):
    item = existing_partition
    argv = _stock_cli(item, monkeypatch)
    assert stocks.main(argv) == 0
    capsys.readouterr()
    with item.engine.begin() as connection:
        statement = (
            "UPDATE sm_stock_minute SET price=9 WHERE stock_code='000001'"
            if damage == "changed" else "DELETE FROM sm_stock_minute WHERE stock_code='000001'"
        )
        connection.execute(text(statement))
    with pytest.raises(SystemExit) as blocked:
        stocks.main(argv)
    assert blocked.value.code == 2


@pytest.fixture
def existing_index_partition(tmp_path, monkeypatch, request):
    primary = create_engine("sqlite://")
    history = create_engine("sqlite://")
    _create_receipt_tables(primary)
    day = date(2026, 11, 10)
    partial_kind = getattr(request, "param", "")
    partial = partial_kind.startswith("95-percent")
    expected_codes = tuple(f"{number:06d}" for number in range(1, 21)) if partial else ("000001", "399001", "399006")
    codes = (
        expected_codes[1:] if partial_kind == "95-percent-first-missing"
        else expected_codes[:-1] if partial else expected_codes
    )
    _bars_fixture(history, "sm_index_minute", [
        {**_bar(moment), "index_code": code, "avg_price": None, "snapshot_at": f"{day} {moment}"}
        for code in codes for moment in stock_coverage.CORE_GRID
    ])
    fingerprint = results.persisted_fingerprint(
        history, dataset="index_minute", trade_date=str(day), include_security_fingerprints=True,
    )
    detail = {
        "readback_rows": fingerprint["row_count"], "readback_codes": fingerprint["code_count"],
        "row_hash": fingerprint["row_hash"], "eligible_code_count": len(expected_codes),
        "coverage": len(codes) / len(expected_codes),
    }
    with primary.begin() as connection:
        connection.connection.driver_connection.create_function("NOW", 0, lambda: "2026-11-10 20:15:00")
        connection.execute(text(
            "INSERT INTO qmt_datadir_import_partition VALUES "
            "('index_minute',:day,'original-index-cache','COMPLETE',:codes,:rows,:raw,'index-original',"
            "'2026-11-10 15:35:00','2026-11-10 16:00:00',:detail)"
        ), {"day": str(day), "raw": "b" * 64, "detail": json.dumps(detail),
            "codes": len(codes), "rows": fingerprint["row_count"]})
    catalog = [IndexCatalogMember(
        code, code + (".SH" if code == "000001" else ".SZ"), code, "2000-01-01", None, "catalog",
    ) for code in expected_codes]
    monkeypatch.setattr(indexes, "load_project_env", lambda: None)
    monkeypatch.setattr(indexes, "create_batch_engine", lambda: primary)
    monkeypatch.setattr(indexes, "get_kline_engine", lambda: history)
    monkeypatch.setattr(indexes, "_sessions", lambda *_a: [day])
    monkeypatch.setattr(indexes, "_load_catalog", lambda *_a: catalog)
    monkeypatch.setattr(indexes, "_harvest", _forbid_source)
    monkeypatch.setattr(indexes, "replace_table_rows", _forbid_source)
    argv = [
        "--dataset", "minute", "--start-date", str(day), "--end-date", str(day),
        "--datadir-root", str(tmp_path / "missing-DAT-cache"), "--apply", "--json",
    ]
    yield primary, history, day, catalog, argv
    primary.dispose()
    history.dispose()


def test_index_cli_reuses_original_partition_before_datadir_and_keeps_provenance(
    existing_index_partition, capsys, monkeypatch,
):
    primary, history, day, _catalog, argv = existing_index_partition
    with history.connect() as connection:
        before = connection.execute(text("SELECT * FROM sm_index_minute ORDER BY index_code,trade_time")).all()
    assert indexes.main(argv) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "PASS"
    with history.connect() as connection:
        assert connection.execute(text("SELECT * FROM sm_index_minute ORDER BY index_code,trade_time")).all() == before
    receipt = results.load_receipt(primary, dataset="index_minute", trade_date=str(day))
    assert receipt["run_id"] == "index-original" and receipt["content_sha256"] == "b" * 64
    assert receipt["source_root"] == "original-index-cache"
    assert receipt["detail"]["prior_completion"]["finished_at"] == "2026-11-10 16:00:00"
    with primary.connect() as connection, history.connect() as data_connection:
        old, _ = load_verified_minute_security_rows(
            connection, data_connection, dataset="index_minute", trade_date=str(day),
            decision_known_at="2026-11-10 19:00:00", codes=["000001"],
        )
        proof, rows = load_verified_minute_security_rows(
            connection, data_connection, dataset="index_minute", trade_date=str(day),
            decision_known_at="2026-11-10 22:20:00", codes=["000001"],
        )
        assert old == {}
        assert proof["known_at"] == "2026-11-10 20:15:00" and len(rows["000001"]) == 241
        connection.connection.driver_connection.create_function("NOW", 0, lambda: "2026-11-10 21:00:00")
    monkeypatch.setattr(indexes, "persisted_fingerprint", _forbid_source)
    assert indexes.main(argv) == 0
    capsys.readouterr()
    assert results.load_receipt(primary, dataset="index_minute", trade_date=str(day)) == receipt


@pytest.mark.parametrize("existing_index_partition", ["95-percent"], indirect=True)
def test_index_cli_keeps_existing_95_percent_coverage_contract(existing_index_partition, capsys):
    primary, _history, day, _catalog, argv = existing_index_partition
    assert indexes.main(argv) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "PASS"
    receipt = results.load_receipt(primary, dataset="index_minute", trade_date=str(day))
    assert receipt["code_count"] == 19 and receipt["detail"]["eligible_code_count"] == 20
    assert receipt["detail"]["coverage"] == .95


@pytest.mark.parametrize("existing_index_partition", ["95-percent-first-missing"], indirect=True)
def test_index_cli_samples_a_covered_security_when_first_catalog_member_is_missing(
    existing_index_partition, capsys,
):
    _primary, history, _day, _catalog, argv = existing_index_partition
    assert indexes.main(argv) == 0
    assert indexes.main(argv) == 0
    capsys.readouterr()
    with history.begin() as connection:
        connection.execute(text("UPDATE sm_index_minute SET price=9 WHERE index_code='000002'"))
    assert indexes.main(argv) == 2
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "BLOCKED"


@pytest.mark.parametrize("damage", ["changed", "deleted"])
def test_index_cli_rechecks_sampled_native_rows_before_reusing_existing_proof(
    existing_index_partition, capsys, damage,
):
    _primary, history, _day, _catalog, argv = existing_index_partition
    assert indexes.main(argv) == 0
    capsys.readouterr()
    with history.begin() as connection:
        statement = (
            "UPDATE sm_index_minute SET price=9 WHERE index_code='000001'"
            if damage == "changed" else "DELETE FROM sm_index_minute WHERE index_code='000001'"
        )
        connection.execute(text(statement))
    assert indexes.main(argv) == 2
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "BLOCKED"


@pytest.mark.parametrize("damage", ["missing-receipt", "row-hash", "changed-native", "same-count-other-catalog"])
def test_index_cli_requires_existing_receipt_content_and_actual_membership(
    existing_index_partition, capsys, damage,
):
    primary, history, day, catalog, argv = existing_index_partition
    if damage == "same-count-other-catalog":
        catalog[0] = IndexCatalogMember("000300", "000300.SH", "other", "2000-01-01", None, "catalog")
    elif damage == "changed-native":
        with history.begin() as connection:
            connection.execute(text("UPDATE sm_index_minute SET price=9 WHERE index_code='399006'"))
    else:
        with primary.begin() as connection:
            if damage == "missing-receipt":
                connection.execute(text("DELETE FROM qmt_datadir_import_partition"))
            else:
                receipt = results.load_receipt(connection, dataset="index_minute", trade_date=str(day))
                detail = {**receipt["detail"], "row_hash": "f" * 64}
                connection.execute(text("UPDATE qmt_datadir_import_partition SET detail_json=:detail"),
                                   {"detail": json.dumps(detail)})
    assert indexes.main(argv) == 2
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "BLOCKED"
    with primary.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_history_coverage_manifest")).scalar_one() == 0
