"""Installation report tests use temporary SQLite databases only."""

from pathlib import Path
import json

import pytest
from sqlalchemy import Column, Date, Index, Integer, MetaData, String, Table, UniqueConstraint, create_engine, inspect

import acquisition.config as config_module
from acquisition.config import Config
from acquisition.datasets import get_spec
from acquisition.store import STATE
from server.common.batch_db import routed_read_engine
from server.common import minute_data
from tools.prepare_direct_acquisition_schema import inspect_configuration, main


def installation(tmp_path, monkeypatch, datasets=None):
    urls = {name: f"sqlite:///{(tmp_path / (name + '.db')).as_posix()}" for name in ("primary", "history", "minute")}
    for name, url in urls.items():
        monkeypatch.setenv("TEST_DIRECT_SCHEMA_" + name.upper(), url)
    data = {"start_date": "2026-09-01", "state_dir": str(tmp_path.resolve()), "write_enabled": False,
            "datasets": datasets or ["stock_daily"],
            "database_env": {name: "TEST_DIRECT_SCHEMA_" + name.upper() for name in urls}}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return Config.load(path), urls


def stock_table(engine, *, narrow=False, legacy_required=False):
    metadata = MetaData()
    columns = [Column("id", Integer, primary_key=True), Column("stock_code", String(6), nullable=False),
               Column("trade_date", Date, nullable=False), Column("k_type", Integer, nullable=False),
               Column("adjust_type", Integer, nullable=False),
               UniqueConstraint("stock_code", "trade_date", "k_type", "adjust_type", name="uq_business")]
    if narrow:
        columns.append(UniqueConstraint("stock_code", "trade_date", name="uq_old"))
    if legacy_required:
        columns.append(Column("legacy_privileged_seal_id", String(64), nullable=False))
    Table("sm_stock_kline", metadata, *columns)
    metadata.create_all(engine)


def test_existing_database_profiles_are_explicit_and_need_no_task_secret_copy(tmp_path, monkeypatch):
    captured = []
    marker = object()
    monkeypatch.setattr(config_module, "get_mysql_url", lambda required=True: "mysql+pymysql://primary")
    monkeypatch.setattr(config_module, "get_kline_mysql_url", lambda: "mysql+pymysql://history")
    monkeypatch.setattr(config_module, "get_minute_mysql_url", lambda: "mysql+pymysql://minute")
    monkeypatch.setattr(config_module, "create_pooled_engine",
                        lambda value, **kwargs: captured.append((value, kwargs)) or marker)
    config = Config({"database_profiles": {"primary": "primary", "history": "kline", "minute": "minute"}}, tmp_path / "config.json")
    assert config.engine("primary") is marker
    assert config.engine("history") is marker
    assert config.engine("minute") is marker
    assert [item[0] for item in captured] == [
        "mysql+pymysql://primary", "mysql+pymysql://history", "mysql+pymysql://minute",
    ]


def test_check_is_read_only_and_reports_missing_progress(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch)
    engine = create_engine(urls["history"])
    stock_table(engine)
    before = inspect(engine).get_table_names()
    report = inspect_configuration(config)
    assert inspect(engine).get_table_names() == before
    assert report["datasets"][0]["status"] == "compatible"
    assert report["status"] == "migration_required"
    assert report["databases"]["history"]["progress_table"]["exists"] is False
    assert "primary" not in report["databases"]


def test_apply_only_creates_progress_and_is_idempotent(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch)
    engine = create_engine(urls["history"])
    stock_table(engine)
    before_indexes = inspect(engine).get_unique_constraints("sm_stock_kline")
    first = inspect_configuration(config, apply=True)
    second = inspect_configuration(config, apply=True)
    assert first["status"] == second["status"] == "compatible"
    assert first["databases"]["history"]["created_progress_table"] is True
    assert second["databases"]["history"]["created_progress_table"] is False
    assert set(inspect(engine).get_table_names()) == {"sm_stock_kline", STATE.name}
    assert inspect(engine).get_unique_constraints("sm_stock_kline") == before_indexes
    assert config.data["write_enabled"] is False


def test_apply_does_not_create_missing_business_table(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch)
    report = inspect_configuration(config, apply=True)
    assert report["status"] == "migration_required"
    assert inspect(create_engine(urls["history"])).get_table_names() == [STATE.name]
    assert any(item.get("table") == "sm_stock_kline" and item["reason"] == "missing_table" for item in report["migration_required"])


def test_legacy_unique_and_required_seal_are_reported_not_changed(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch)
    engine = create_engine(urls["history"])
    stock_table(engine, narrow=True, legacy_required=True)
    report = inspect_configuration(config, apply=True)
    issues = report["migration_required"]
    assert any(item["reason"] == "legacy_unique_collapses_business_identity" for item in issues)
    assert any(item.get("column") == "legacy_privileged_seal_id" and item["reason"] == "required_column_not_supplied" for item in issues)
    assert len(inspect(engine).get_unique_constraints("sm_stock_kline")) == 2
    columns = report["datasets"][0]["schema"]["required_input_columns"]
    assert {item["column"] for item in columns} >= {"stock_code", "trade_date", "legacy_privileged_seal_id"}
    assert all(item["column"] != "id" for item in columns)


def test_schema_missing_key_columns_are_explicit(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch)
    metadata = MetaData()
    Table("sm_stock_kline", metadata, Column("id", Integer, primary_key=True), Column("stock_code", String(6)))
    metadata.create_all(create_engine(urls["history"]))
    report = inspect_configuration(config)
    assert any(item["reason"] == "missing_business_key_columns" and "adjust_type" in item["columns"] for item in report["migration_required"])
    assert any(item["reason"] == "missing_business_identity_index" for item in report["migration_required"])


def test_existing_covering_nonunique_index_is_reused_for_single_writer(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch, ["stock_minute"])
    metadata = MetaData()
    table = Table("sm_stock_minute", metadata, Column("id", Integer, primary_key=True),
                  Column("stock_code", String(6), nullable=False), Column("trade_time", Date),
                  Column("etl_sync_at", Date, nullable=False))
    Index("idx_existing_identity", table.c.stock_code, table.c.trade_time)
    engine = create_engine(urls["history"])
    metadata.create_all(engine)
    STATE.create(engine)
    report = inspect_configuration(config)
    assert report["datasets"][0]["status"] == "compatible"


def test_existing_index_prefix_is_enough_without_collapsing_index_k_type(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch, ["index_daily"])
    metadata = MetaData()
    table = Table("sm_index_kline", metadata, Column("id", Integer, primary_key=True),
                  Column("index_code", String(6), nullable=False), Column("trade_date", Date),
                  Column("k_type", Integer), Column("etl_sync_at", Date, nullable=False))
    Index("idx_existing_index_day", table.c.index_code, table.c.trade_date)
    engine = create_engine(urls["history"])
    metadata.create_all(engine)
    STATE.create(engine)
    report = inspect_configuration(config)
    assert report["datasets"][0]["status"] == "compatible"
    assert report["datasets"][0]["schema"]["expected_unique"] == ["index_code", "trade_date", "k_type"]


def test_qmt_daily_flow_schema_recognizes_its_five_native_derived_fields(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch, ["capital_flow_daily"])
    metadata = MetaData()
    table = Table("sm_stock_capital_flow_daily", metadata,
                  Column("stock_code", String(6), primary_key=True),
                  Column("trade_date", Date, primary_key=True),
                  *(Column(name, Integer, nullable=False) for name in (
                      "main_net_inflow", "sm_net_inflow", "mid_net_inflow",
                      "lg_net_inflow", "max_net_inflow")))
    engine = create_engine(urls["minute"])
    metadata.create_all(engine)
    STATE.create(engine)
    report = inspect_configuration(config)
    assert report["datasets"][0]["status"] == "compatible"
    assert report["datasets"][0]["database"] == "minute"
    assert "primary" not in report["databases"]


def test_daily_flow_writer_progress_and_pure_consumer_read_share_minute_database(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch, ["capital_flow_daily"])
    primary = create_engine(urls["primary"])
    minute = create_engine(urls["minute"])
    metadata = MetaData()
    Table("sm_stock_capital_flow_daily", metadata,
          Column("stock_code", String(6), primary_key=True),
          Column("trade_date", Date, primary_key=True),
          *(Column(name, Integer, nullable=False) for name in (
              "main_net_inflow", "sm_net_inflow", "mid_net_inflow",
              "lg_net_inflow", "max_net_inflow")))
    metadata.create_all(minute)

    report = inspect_configuration(config, apply=True)
    monkeypatch.setattr(minute_data, "get_minute_engine", lambda: minute)
    consumer = routed_read_engine(
        "SELECT MAX(etl_sync_at) AS data_time FROM sm_stock_capital_flow_daily",
        primary,
    )

    assert get_spec("capital_flow_daily").database == "minute"
    assert report["status"] == "compatible"
    assert consumer is minute
    assert STATE.name in inspect(minute).get_table_names()
    assert inspect(primary).get_table_names() == []


def test_finance_revision_legacy_nonnull_fact_parent_is_not_faked(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch, ["finance"])
    metadata = MetaData()
    Table("si_stock_finance", metadata, Column("stock_code", String(6), primary_key=True), Column("report_date", Date, primary_key=True))
    Table("st_pit_finance_revision", metadata, Column("revision_id", String(64), primary_key=True),
          Column("source_coverage_id", String(64), nullable=False), Column("published_at", Date, nullable=False))
    engine = create_engine(urls["primary"])
    metadata.create_all(engine)
    report = inspect_configuration(config, apply=True)
    issues = report["datasets"][0]["revision_schema"]["migration_required"]
    assert {item.get("column") for item in issues} >= {"source_coverage_id", "published_at"}
    assert report["status"] == "migration_required"


@pytest.mark.parametrize("apply", [False, True])
def test_finance_missing_source_version_reports_ddl_without_altering_business_table(tmp_path, monkeypatch, apply):
    config, urls = installation(tmp_path, monkeypatch, ["finance"])
    engine = create_engine(urls["primary"])
    metadata = MetaData()
    Table("si_stock_finance", metadata, Column("stock_code", String(6), primary_key=True),
          Column("report_date", Date, primary_key=True))
    Table("st_pit_finance_revision", metadata, Column("revision_id", String(64), primary_key=True))
    metadata.create_all(engine)
    STATE.create(engine)
    before = inspect(engine).get_columns("si_stock_finance")
    for _ in range(2):
        report = inspect_configuration(config, apply=apply)
        issues = report["migration_required"]
        assert len(issues) == 1
        assert issues[0] == {
            "dataset": "finance", "database": "primary",
            "reason": "missing_finance_source_update_date", "table": "si_stock_finance",
            "column": "source_update_date", "type": "VARCHAR(64)", "nullable": True,
            "suggested_ddl": "ALTER TABLE `si_stock_finance` ADD COLUMN `source_update_date` VARCHAR(64) NULL;",
        }
        assert report["datasets"][0]["status"] == "migration_required"
        assert report["business_tables_modified"] is False
        assert [item["name"] for item in inspect(engine).get_columns("si_stock_finance")] == [item["name"] for item in before]


def test_finance_existing_nullable_source_version_needs_no_repeated_migration(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch, ["finance"])
    engine = create_engine(urls["primary"])
    metadata = MetaData()
    Table("si_stock_finance", metadata, Column("stock_code", String(6), primary_key=True),
          Column("report_date", Date, primary_key=True), Column("source_update_date", String(64), nullable=True))
    Table("st_pit_finance_revision", metadata, Column("revision_id", String(64), primary_key=True))
    metadata.create_all(engine)
    STATE.create(engine)
    report = inspect_configuration(config)
    assert report["status"] == "compatible"
    assert report["migration_required"] == []
    actual = next(item for item in inspect(engine).get_columns("si_stock_finance") if item["name"] == "source_update_date")
    assert actual["nullable"] is True
    assert actual["type"].length == 64


def test_only_enabled_database_is_opened(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch)
    monkeypatch.delenv("TEST_DIRECT_SCHEMA_PRIMARY")
    stock_table(create_engine(urls["history"]))
    assert inspect_configuration(config, apply=True)["status"] == "compatible"


def test_existing_progress_table_is_not_silently_altered(tmp_path, monkeypatch):
    config, urls = installation(tmp_path, monkeypatch)
    engine = create_engine(urls["history"])
    stock_table(engine)
    metadata = MetaData()
    Table(STATE.name, metadata, Column("dataset", String(32), primary_key=True),
          Column("source", String(32), primary_key=True), Column("target_date", Date, primary_key=True),
          Column("partition_key", String(64), primary_key=True))
    metadata.create_all(engine)
    report = inspect_configuration(config, apply=True)
    assert any(item["reason"] == "missing_progress_columns" for item in report["migration_required"])
    assert len(inspect(engine).get_columns(STATE.name)) == 4


def test_database_error_does_not_expose_credentials():
    class BrokenConfig:
        data = {"datasets": ["stock_daily"]}
        def engine(self, name):
            raise RuntimeError("mysql://root:SECRET_PASSWORD@private.example/db")
    report = inspect_configuration(BrokenConfig())
    assert "SECRET" not in json.dumps(report) and "private.example" not in json.dumps(report)
    assert report["status"] == "migration_required"


def test_cli_modes_report_exit_status_without_implicit_apply(tmp_path, monkeypatch, capsys):
    config, urls = installation(tmp_path, monkeypatch)
    stock_table(create_engine(urls["history"]))
    assert main(["--config", str(config.path), "--check"]) == 2
    assert json.loads(capsys.readouterr().out)["mode"] == "check"
    assert main(["--config", str(config.path), "--apply"]) == 0
    assert json.loads(capsys.readouterr().out)["business_tables_modified"] is False
    with pytest.raises(SystemExit):
        main(["--config", str(config.path)])
