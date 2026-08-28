from __future__ import annotations

import inspect
import json
from datetime import date, datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.common.qmt_attestation_contract import (
    bound_stock_set_contract,
    build_qmt_v2_manifest,
    canonical_digest,
    daily_market_source_batch_id,
    validated_universe_manifest,
)
from server.common.qmt_stock_catalog import (
    A_SHARE_STOCK_CODE_SQL_REGEXP,
    CATALOG_STATUS_COMPLETE,
    NATIVE_A_SHARE_SECTORS,
    a_share_stock_code_sql,
    build_catalog_discovery,
    build_catalog_manifest,
    canonical_catalog_members,
    load_stock_catalog,
    privileged_migrate_stock_catalog_schema,
    ensure_stock_catalog_tables,
    is_a_share_stock_code,
    validate_stock_catalog_immutability,
    validate_catalog_manifest,
)
from server.common.qmt_trade_calendar import (
    CALENDAR_STATUS_COMPLETE,
    build_calendar_manifest,
    calendar_source_batch_id,
    load_trade_calendar_receipt,
    privileged_migrate_trade_calendar_schema,
    ensure_trade_calendar_tables,
    validate_trade_calendar_immutability,
    validate_calendar_manifest,
)
from tools import attest_qmt_daily_kline as attester
from tools import fetch_sm_stock_kline_daily as daily_fetch
from tools import sync_guojin_qmt_reference_data as reference_sync
from tools import prepare_strategy_governance_qmt_history as history_preparation
from biz.stock_market import sync_stock_market


def _members():
    return [
        {
            "qmt_code": "000001.SZ",
            "stock_code": "000001",
            "list_date": "1991-04-03",
            "expire_date": None,
            "instrument_batch_id": "instrument-batch-1",
            "instrument_type": "STOCK",
        },
        {
            "qmt_code": "301999.SZ",
            "stock_code": "301999",
            "list_date": "2026-08-24",
            "expire_date": None,
            "instrument_batch_id": "instrument-batch-1",
            "instrument_type": "STOCK",
        },
        {
            "qmt_code": "600001.SH",
            "stock_code": "600001",
            "list_date": "1990-12-19",
            "expire_date": "2026-08-24",
            "instrument_batch_id": "instrument-batch-1",
            "instrument_type": "STOCK",
        },
    ]


def test_exact_a_share_code_contract_includes_920_and_excludes_non_a_families():
    for code in (
        "000001", "301999", "600519", "688001", "430001", "830799",
        "870001", "920001",
    ):
        assert is_a_share_stock_code(code)
    for code in ("900901", "200001", "810001", "820001", "880001", "92001"):
        assert not is_a_share_stock_code(code)

    assert a_share_stock_code_sql("t.stock_code") == (
        f"t.stock_code REGEXP '{A_SHARE_STOCK_CODE_SQL_REGEXP}'"
    )
    with pytest.raises(ValueError, match="column is invalid"):
        a_share_stock_code_sql("stock_code OR 1=1")

    member = {
        "qmt_code": "920001.BJ",
        "stock_code": "920001",
        "list_date": "2022-12-27",
        "expire_date": None,
        "instrument_batch_id": "instrument-batch-920",
        "instrument_type": "STOCK",
    }
    assert canonical_catalog_members([member])[0]["qmt_code"] == "920001.BJ"
    with pytest.raises(ValueError, match="instrument code"):
        canonical_catalog_members([{**member, "qmt_code": "920001.SH"}])


def _discovery(members):
    sector_by_exchange = {
        "SH": "上证A股",
        "SZ": "深证A股",
        "BJ": "京市A股",
    }
    return build_catalog_discovery(
        current_sectors=("上证A股", "深证A股", "京市A股", "沪深A股"),
        expired_sectors=(),
        sector_members=[
            {
                "sector_name": sector_by_exchange[
                    str(member["qmt_code"]).split(".", 1)[1]
                ],
                "qmt_code": member["qmt_code"],
            }
            for member in members
        ],
    )


def _catalog_row(manifest):
    return {
        "batch_id": manifest["batch_id"],
        "captured_at": manifest["captured_at"],
        "history_complete_from": manifest["history_complete_from"],
        "status": CATALOG_STATUS_COMPLETE,
        "member_count": manifest["member_count"],
        "member_set_hash": manifest["member_set_hash"],
        "manifest_hash": canonical_digest(manifest),
    }


def _calendar_row(manifest):
    return {
        "batch_id": manifest["batch_id"],
        "source_batch_id": manifest["source_batch_id"],
        "known_at": manifest["known_at"],
        "start_date": manifest["start_date"],
        "end_date": manifest["end_date"],
        "status": CALENDAR_STATUS_COMPLETE,
        "session_count": manifest["session_count"],
        "session_set_hash": manifest["session_set_hash"],
        "manifest_hash": canonical_digest(manifest),
    }


def test_qmt_calendar_receipt_freezes_sessions_known_at_and_source_batch():
    source_batch_id = calendar_source_batch_id(
        start_date="2026-08-01",
        end_date="2026-08-31",
        sessions=["2026-08-21", "2026-08-24"],
    )
    manifest, sessions = build_calendar_manifest(
        batch_id="calendar-batch-1",
        source_batch_id=source_batch_id,
        known_at="2026-08-24 03:20:00",
        start_date="2026-08-01",
        end_date="2026-08-31",
        sessions=["2026-08-21", "2026-08-24"],
    )
    receipt = validate_calendar_manifest(
        manifest, row=_calendar_row(manifest), sessions=sessions
    )

    assert receipt.sessions_between("2026-08-22", "2026-08-24") == [
        "2026-08-24"
    ]
    assert receipt.source_batch_id == source_batch_id
    assert receipt.known_at == "2026-08-24 03:20:00"

    tampered = {**manifest, "session_count": 1}
    with pytest.raises(ValueError, match="manifest content differs"):
        validate_calendar_manifest(
            tampered, row=_calendar_row(manifest), sessions=sessions
        )

    with pytest.raises(ValueError, match="payload-bound"):
        build_calendar_manifest(
            batch_id="calendar-batch-2",
            source_batch_id="f" * 64,
            known_at="2026-08-24 03:20:00",
            start_date="2026-08-01",
            end_date="2026-08-31",
            sessions=sessions,
        )


def _bigqmt_calendar_release_proof():
    return {
        "strategy_release_protocol": "probiga.bigqmt-strategy-release.v2",
        "strategy_identity_protocol": (
            "probiga.bigqmt-loaded-strategy-identity.v1"
        ),
        "strategy_identity_frozen": True,
        "strategy_identity_status": "BOUND",
        "strategy_build_sha": "a" * 40,
        "strategy_git_blob": "b" * 40,
        "strategy_source_sha256": "c" * 64,
        "strategy_artifact_sha256": "d" * 64,
        "strategy_loaded_identity_sha256": "e" * 64,
    }


def test_formal_reference_calendar_uses_bigqmt_and_limits_proven_range(
    monkeypatch,
):
    proof = _bigqmt_calendar_release_proof()
    monkeypatch.setattr(
        reference_sync,
        "validate_strategy_release_payload",
        lambda *_args, **_kwargs: proof,
    )
    monkeypatch.setattr(
        reference_sync.bigqmt_bridge,
        "capabilities",
        lambda **_kwargs: {"status": "ok"},
    )

    def capture(_market, *, start_date, end_date, **_kwargs):
        assert start_date == "2026-01-01"
        assert end_date == "2026-08-26"
        rows = [
            {
                "market": "SH",
                "calendar_year": 2026,
                "trade_date": day,
                "trade_status": 1,
                "day_week": pd.Timestamp(day).isoweekday(),
            }
            for day in ("2026-08-24", "2026-08-25", "2026-08-26")
        ]
        return {
            **proof,
            "status": "ok",
            "source": "gj_big_qmt_inner",
            "action": "trading_calendar",
            "source_method": "ContextInfo.get_trading_dates",
            "source_stock_code": "000001.SH",
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "observed_start_date": "2026-08-24",
            "observed_end_date": "2026-08-26",
            "rows": rows,
        }

    monkeypatch.setattr(
        reference_sync.bigqmt_bridge,
        "trading_calendar_capture",
        capture,
    )
    monkeypatch.setattr(
        reference_sync.bridge,
        "trading_calendar",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("formal calendar must not use direct QMT")
        ),
        raising=False,
    )

    frame, evidence = reference_sync._fetch_trading_calendar(
        2026,
        2027,
        expected_build_sha="a" * 40,
        as_of_date=date(2026, 8, 26),
    )

    assert frame["trade_date"].tolist() == [
        "2026-08-24", "2026-08-25", "2026-08-26",
    ]
    assert evidence["proven_start_date"] == "2026-08-24"
    assert evidence["proven_end_date"] == "2026-08-26"
    assert evidence["requested_end_date"] == "2027-12-31"
    assert evidence["unproven_after_date"] == "2026-08-27"
    assert evidence["future_range_status"] == (
        "NOT_COVERED_NO_AUTHORITATIVE_FUTURE_CALENDAR"
    )
    manifest, _source_id = reference_sync._build_proven_calendar_manifest(
        batch_id="calendar-bound-test",
        captured_at=datetime(2026, 8, 26, 16, 0, 0),
        calendar=frame,
        capture_evidence=evidence,
    )
    assert manifest["start_date"] == "2026-08-24"
    assert manifest["end_date"] == "2026-08-26"
    assert manifest["end_date"] != evidence["requested_end_date"]


def test_bigqmt_calendar_response_identity_drift_fails_closed(monkeypatch):
    proof = _bigqmt_calendar_release_proof()
    monkeypatch.setattr(
        reference_sync,
        "validate_strategy_release_payload",
        lambda *_args, **_kwargs: proof,
    )
    monkeypatch.setattr(
        reference_sync.bigqmt_bridge,
        "capabilities",
        lambda **_kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        reference_sync.bigqmt_bridge,
        "trading_calendar_capture",
        lambda *_args, **_kwargs: {
            **proof,
            "strategy_build_sha": "d" * 40,
        },
    )

    with pytest.raises(RuntimeError, match="release identity differs"):
        reference_sync._fetch_trading_calendar(
            2026,
            2026,
            expected_build_sha="a" * 40,
            as_of_date=date(2026, 8, 26),
        )


def test_formal_calendar_without_build_identity_fails_before_database(
    monkeypatch,
):
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    monkeypatch.setattr(
        reference_sync,
        "create_engine",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("database must follow build identity")
        ),
    )
    with pytest.raises(RuntimeError, match="requires a release build SHA"):
        reference_sync.sync_reference_data(
            start_year=2026,
            end_year=2027,
            iscomplete=True,
            refresh_timeout=1,
            skip_calendar=False,
        )


def _calendar_trigger_rows():
    return [
        {
            "TRIGGER_NAME": name,
            "EVENT_MANIPULATION": event,
            "ACTION_TIMING": "BEFORE",
            "ACTION_STATEMENT": (
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
                f"'{table_name} is append-only'"
            ),
            "EVENT_OBJECT_TABLE": table_name,
        }
        for name, table_name, event in (
            (
                "trg_qmt_calendar_batch_no_update",
                "qmt_trade_calendar_batch",
                "UPDATE",
            ),
            (
                "trg_qmt_calendar_batch_no_delete",
                "qmt_trade_calendar_batch",
                "DELETE",
            ),
            (
                "trg_qmt_calendar_session_no_update",
                "qmt_trade_calendar_session",
                "UPDATE",
            ),
            (
                "trg_qmt_calendar_session_no_delete",
                "qmt_trade_calendar_session",
                "DELETE",
            ),
        )
    ]


class _CalendarDdlConnection:
    def __init__(self, *, trigger_rows=None):
        self.statements = []
        self.trigger_rows = (
            _calendar_trigger_rows() if trigger_rows is None else trigger_rows
        )

    def execute(self, statement, _params=None):
        sql = str(statement)
        self.statements.append(sql)
        trigger_rows = self.trigger_rows

        class _Result:
            def mappings(self):
                return self

            def all(self):
                return trigger_rows

        return _Result()


class _CalendarDdlEngine:
    def __init__(self):
        self.connection = _CalendarDdlConnection()

    def begin(self):
        connection = self.connection

        class _Scope:
            def __enter__(self):
                return connection

            def __exit__(self, *_args):
                return False

        return _Scope()

    def connect(self):
        return self.begin()


def test_qmt_calendar_schema_installs_database_append_only_guards():
    engine = _CalendarDdlEngine()
    privileged_migrate_trade_calendar_schema(engine)
    ddl = "\n".join(engine.connection.statements)

    assert ddl.index("CREATE TABLE IF NOT EXISTS qmt_trade_calendar_session") < (
        ddl.index("trg_qmt_calendar_session_no_update")
    )
    assert ddl.count("BEFORE UPDATE ON qmt_trade_calendar_") == 2
    assert ddl.count("BEFORE DELETE ON qmt_trade_calendar_") == 2
    assert ddl.count("SIGNAL SQLSTATE '45000'") == 4
    assert ddl.count("DROP TRIGGER IF EXISTS `trg_qmt_calendar_") == 4
    assert ddl.index("DROP TRIGGER IF EXISTS `trg_qmt_calendar_batch_no_update`") < (
        ddl.index("CREATE TRIGGER IF NOT EXISTS trg_qmt_calendar_batch_no_update")
    )


def test_qmt_calendar_trigger_attestation_rejects_missing_inventory():
    rows = _calendar_trigger_rows()[:-1]

    with pytest.raises(RuntimeError, match="triggers are incomplete"):
        validate_trade_calendar_immutability(
            _CalendarDdlConnection(trigger_rows=rows)
        )


def test_qmt_calendar_trigger_attestation_rejects_noop_body_mutation():
    rows = _calendar_trigger_rows()
    rows[0] = {
        **rows[0],
        "ACTION_STATEMENT": (
            "BEGIN IF 1=0 THEN SIGNAL SQLSTATE '45000' "
            "SET MESSAGE_TEXT='qmt_trade_calendar_batch is append-only'; "
            "END IF; END"
        ),
    }

    with pytest.raises(RuntimeError, match="trigger differs"):
        validate_trade_calendar_immutability(
            _CalendarDdlConnection(trigger_rows=rows)
        )


def test_qmt_calendar_loader_hides_receipts_unknown_at_decision_time():
    engine = create_engine("sqlite:///:memory:", future=True)
    start_date = "2026-08-01"
    end_date = "2026-08-31"
    receipts = []
    for batch_id, known_at, sessions in (
        ("known-batch", "2026-08-24 10:00:00", ["2026-08-21"]),
        (
            "future-batch",
            "2026-08-25 10:00:00",
            ["2026-08-21", "2026-08-24"],
        ),
    ):
        source_id = calendar_source_batch_id(
            start_date=start_date,
            end_date=end_date,
            sessions=sessions,
        )
        manifest, normalized = build_calendar_manifest(
            batch_id=batch_id,
            source_batch_id=source_id,
            known_at=known_at,
            start_date=start_date,
            end_date=end_date,
            sessions=sessions,
        )
        receipts.append((manifest, normalized))
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_batch (
                batch_id TEXT PRIMARY KEY, source_batch_id TEXT,
                known_at TEXT, start_date TEXT, end_date TEXT, status TEXT,
                session_count INTEGER, session_set_hash TEXT,
                manifest_json TEXT, manifest_hash TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_session (
                batch_id TEXT, trade_date TEXT
            )
        """))
        for manifest, sessions in receipts:
            connection.execute(text("""
                INSERT INTO qmt_trade_calendar_batch
                VALUES (:batch_id, :source_batch_id, :known_at, :start_date,
                        :end_date, 'COMPLETE', :session_count,
                        :session_set_hash, :manifest_json, :manifest_hash)
            """), {
                **manifest,
                "manifest_json": json.dumps(
                    manifest, sort_keys=True, separators=(",", ":")
                ),
                "manifest_hash": canonical_digest(manifest),
            })
            connection.execute(text("""
                INSERT INTO qmt_trade_calendar_session
                (batch_id, trade_date) VALUES (:batch_id, :trade_date)
            """), [
                {"batch_id": manifest["batch_id"], "trade_date": day}
                for day in sessions
            ])

    with engine.connect() as connection:
        receipt = load_trade_calendar_receipt(
            connection,
            start_date=start_date,
            end_date=end_date,
            decision_known_at="2026-08-24 12:00:00",
        )

    assert receipt.batch_id == "known-batch"
    assert receipt.sessions == ("2026-08-21",)


def test_governance_history_window_cannot_drift_with_mutable_calendar():
    source = inspect.getsource(history_preparation)
    session_source = inspect.getsource(
        history_preparation._latest_closed_sessions
    )
    close_source = inspect.getsource(
        history_preparation.authoritative_closed_trade_date
    )

    assert "si_trade_calendar" not in source
    assert "load_trade_calendar_receipt" in session_source
    assert "decision_known_at=" in session_source
    assert "load_trade_calendar_receipt" in close_source
    assert "decision_known_at=" in close_source


def test_target_date_catalog_adds_new_ipo_and_excludes_expired_member():
    manifest, members = build_catalog_manifest(
        batch_id="catalog-batch-1",
        captured_at="2026-08-24 18:00:00",
        history_complete_from="1990-01-01",
        members=_members(),
        discovery=_discovery(_members()),
    )
    catalog = validate_catalog_manifest(
        json.dumps(manifest), row=_catalog_row(manifest), members=members
    )

    assert catalog.eligible_codes("2026-08-23") == ["000001", "600001"]
    assert catalog.eligible_codes("2026-08-24") == ["000001", "301999"]
    with pytest.raises(RuntimeError, match="does not prove"):
        catalog.eligible_codes("1989-12-31")


def test_expired_code_collision_rejects_index_as_equity():
    index_member = {
        "qmt_code": "000001.SH",
        "stock_code": "000001",
        "list_date": "1991-07-15",
        "expire_date": "2026-08-24",
        "instrument_batch_id": "qmt-history-contracts",
        "instrument_type": "INDEX",
    }
    with pytest.raises(ValueError, match="not proven as equity"):
        build_catalog_manifest(
            batch_id="collision-batch",
            captured_at="2026-08-24 18:00:00",
            history_complete_from="1991-07-15",
            members=[index_member],
            discovery=build_catalog_discovery(
                current_sectors=NATIVE_A_SHARE_SECTORS,
                expired_sectors=["过期指数"],
                sector_members=[{
                    "sector_name": "过期指数",
                    "qmt_code": "000001.SH",
                }],
            ),
        )


def _catalog_trigger_rows():
    return [
        {
            "TRIGGER_NAME": name,
            "EVENT_MANIPULATION": event,
            "ACTION_TIMING": "BEFORE",
            "ACTION_STATEMENT": (
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
                f"'{table_name} is append-only'"
            ),
            "EVENT_OBJECT_TABLE": table_name,
        }
        for name, table_name, event in (
            (
                "trg_qmt_stock_catalog_batch_no_update",
                "qmt_stock_catalog_batch",
                "UPDATE",
            ),
            (
                "trg_qmt_stock_catalog_batch_no_delete",
                "qmt_stock_catalog_batch",
                "DELETE",
            ),
            (
                "trg_qmt_stock_catalog_member_no_update",
                "qmt_stock_catalog_member",
                "UPDATE",
            ),
            (
                "trg_qmt_stock_catalog_member_no_delete",
                "qmt_stock_catalog_member",
                "DELETE",
            ),
        )
    ]


class _CatalogDdlConnection:
    def __init__(self, *, trigger_rows=None, columns_by_table=None):
        self.statements = []
        self.trigger_rows = (
            _catalog_trigger_rows() if trigger_rows is None else trigger_rows
        )
        self.columns_by_table = columns_by_table or {}

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "information_schema.COLUMNS" in sql:
            rows = [
                {"COLUMN_NAME": name}
                for name in self.columns_by_table.get(
                    str((params or {}).get("table_name") or ""),
                    (),
                )
            ]
        else:
            rows = self.trigger_rows

        class _Result:
            def mappings(self):
                return self

            def all(self):
                return rows

        return _Result()


class _CatalogDdlEngine:
    def __init__(self, *, columns_by_table=None):
        self.connection = _CatalogDdlConnection(
            columns_by_table=columns_by_table
        )

    def begin(self):
        connection = self.connection

        class _Scope:
            def __enter__(self):
                return connection

            def __exit__(self, *_args):
                return False

        return _Scope()

    def connect(self):
        return self.begin()


def test_qmt_stock_catalog_schema_installs_database_append_only_guards():
    engine = _CatalogDdlEngine()
    privileged_migrate_stock_catalog_schema(engine)
    ddl = "\n".join(engine.connection.statements)

    assert ddl.index("CREATE TABLE IF NOT EXISTS qmt_stock_catalog_member") < (
        ddl.index(
            "CREATE TRIGGER IF NOT EXISTS "
            "trg_qmt_stock_catalog_member_no_update"
        )
    )
    assert ddl.count("BEFORE UPDATE ON qmt_stock_catalog_") == 2
    assert ddl.count("BEFORE DELETE ON qmt_stock_catalog_") == 2
    assert ddl.count("SIGNAL SQLSTATE '45000'") == 4
    assert ddl.count("DROP TRIGGER IF EXISTS `trg_qmt_stock_catalog_") == 4
    assert ddl.index(
        "DROP TRIGGER IF EXISTS `trg_qmt_stock_catalog_batch_no_update`"
    ) < ddl.index("UPDATE qmt_stock_catalog_batch")


def test_qmt_stock_catalog_additive_migration_is_mysql_compatible_and_idempotent():
    missing = _CatalogDdlEngine()
    privileged_migrate_stock_catalog_schema(missing, install_triggers=False)
    missing_sql = "\n".join(missing.connection.statements)

    assert "ADD COLUMN IF NOT EXISTS" not in missing_sql
    assert "ADD COLUMN history_complete_from" in missing_sql
    assert "ADD COLUMN instrument_type" in missing_sql

    existing = _CatalogDdlEngine(columns_by_table={
        "qmt_stock_catalog_batch": {"history_complete_from"},
        "qmt_stock_catalog_member": {"instrument_type"},
    })
    privileged_migrate_stock_catalog_schema(existing, install_triggers=False)
    existing_sql = "\n".join(existing.connection.statements)

    assert "ADD COLUMN history_complete_from" not in existing_sql
    assert "ADD COLUMN instrument_type" not in existing_sql


def test_qmt_stock_catalog_trigger_attestation_rejects_missing_or_noop():
    with pytest.raises(RuntimeError, match="triggers are incomplete"):
        validate_stock_catalog_immutability(
            _CatalogDdlConnection(trigger_rows=_catalog_trigger_rows()[:-1])
        )

    rows = _catalog_trigger_rows()
    rows[0] = {
        **rows[0],
        "ACTION_STATEMENT": (
            "BEGIN IF 1=0 THEN SIGNAL SQLSTATE '45000' "
            "SET MESSAGE_TEXT='qmt_stock_catalog_batch is append-only'; "
            "END IF; END"
        ),
    }
    with pytest.raises(RuntimeError, match="trigger differs"):
        validate_stock_catalog_immutability(
            _CatalogDdlConnection(trigger_rows=rows)
        )


def test_qmt_stock_catalog_loader_hides_future_metadata():
    engine = create_engine("sqlite:///:memory:", future=True)
    batches = []
    for batch_id, captured_at, history_from in (
        ("known-catalog", "2026-08-24 10:00:00", "2026-08-01"),
        ("future-catalog", "2026-08-25 10:00:00", "1990-01-01"),
    ):
        manifest, members = build_catalog_manifest(
            batch_id=batch_id,
            captured_at=captured_at,
            history_complete_from=history_from,
            members=_members(),
            discovery=_discovery(_members()),
        )
        batches.append((manifest, members))
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_batch (
                batch_id TEXT PRIMARY KEY, captured_at TEXT,
                history_complete_from TEXT, status TEXT, member_count INTEGER,
                member_set_hash TEXT, manifest_json TEXT, manifest_hash TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_member (
                batch_id TEXT, qmt_code TEXT, stock_code TEXT, list_date TEXT,
                expire_date TEXT, instrument_batch_id TEXT,
                instrument_type TEXT
            )
        """))
        for manifest, members in batches:
            connection.execute(text("""
                INSERT INTO qmt_stock_catalog_batch VALUES
                (:batch_id, :captured_at, :history_complete_from, 'COMPLETE',
                 :member_count, :member_set_hash, :manifest_json, :manifest_hash)
            """), {
                **manifest,
                "manifest_json": json.dumps(manifest),
                "manifest_hash": canonical_digest(manifest),
            })
            connection.execute(text("""
                    INSERT INTO qmt_stock_catalog_member VALUES
                    (:batch_id, :qmt_code, :stock_code, :list_date, :expire_date,
                     :instrument_batch_id, :instrument_type)
            """), [{"batch_id": manifest["batch_id"], **member}
                      for member in members])
    with engine.connect() as connection:
        catalog = load_stock_catalog(
            connection,
            decision_known_at="2026-08-24 12:00:00",
        )

    assert catalog.batch_id == "known-catalog"
    assert catalog.history_complete_from == "2026-08-01"


def test_historical_archive_imports_expired_instrument_and_rejects_tamper(
    tmp_path,
):
    raw_member = {
        "qmt_code": "600001.SH",
        "stock_code": "600001",
        "list_date": "1990-12-19",
        "expire_date": "2026-08-24",
        "instrument_type": "STOCK",
    }
    unsigned = {
        "schema": reference_sync.HISTORICAL_INSTRUMENT_ARCHIVE_SCHEMA,
        "source_export_id": "native-qmt-export-20260824",
        "history_complete_from": "1990-12-19",
        "members": [raw_member],
    }
    payload = {**unsigned, "payload_hash": canonical_digest(unsigned)}
    archive_path = tmp_path / "qmt-instrument-archive.json"
    archive_path.write_text(json.dumps(payload), encoding="utf-8")

    history_from, members = reference_sync._load_historical_instrument_archive(
        str(archive_path)
    )
    assert history_from == "1990-12-19"
    assert members[0]["expire_date"] == "2026-08-24"
    assert members[0]["instrument_batch_id"] == payload["payload_hash"]

    payload["members"][0]["expire_date"] = None
    archive_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="root differs"):
        reference_sync._load_historical_instrument_archive(str(archive_path))


def test_scheduled_reference_capture_is_ddl_free_and_schema_is_privileged():
    runtime_source = inspect.getsource(reference_sync.sync_reference_data)
    prepare_source = inspect.getsource(reference_sync.prepare_reference_tables)
    migration_source = inspect.getsource(
        reference_sync.privileged_migrate_reference_schema
    )
    validate_source = inspect.getsource(reference_sync.validate_reference_tables)

    assert "validate_reference_tables(engine)" in runtime_source
    assert "prepare_reference_tables(engine)" not in runtime_source
    assert "CREATE TABLE" not in runtime_source
    assert "CREATE TRIGGER" not in runtime_source
    assert "CREATE TABLE" not in prepare_source
    assert "ALTER TABLE" not in prepare_source
    assert "validate_reference_tables(engine)" in prepare_source
    assert "privileged_migrate_stock_catalog_schema" in migration_source
    assert "privileged_migrate_trade_calendar_schema" in migration_source
    assert "qmt_reference_schema_contract" in validate_source
    assert "REFERENCE_SCHEMA_CONTRACT_HASH" in validate_source
    assert "information_schema.TRIGGERS" not in validate_source


def test_legacy_ensure_aliases_are_read_only_runtime_validators():
    for function in (
        ensure_stock_catalog_tables,
        ensure_trade_calendar_tables,
    ):
        source = inspect.getsource(function).upper()
        assert "CREATE TABLE" not in source
        assert "ALTER TABLE" not in source
        assert "CREATE TRIGGER" not in source
        assert "VALIDATE_" in source


def test_privileged_reference_migration_can_defer_triggers_to_release_broker(
    monkeypatch,
):
    class _Connection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, _params=None):
            self.statements.append(str(statement))

            class _Result:
                def mappings(self):
                    return self

                def all(self):
                    return []

            return _Result()

    class _Engine:
        def __init__(self):
            self.connection = _Connection()

        def begin(self):
            connection = self.connection

            class _Scope:
                def __enter__(self):
                    return connection

                def __exit__(self, *_args):
                    return False

            return _Scope()

    engine = _Engine()
    monkeypatch.setattr(reference_sync, "_table_columns", lambda *_args: set())

    result = reference_sync.privileged_migrate_reference_schema(
        engine,
        install_triggers=False,
        attest_schema=False,
    )

    sql = "\n".join(engine.connection.statements).upper()
    assert "CREATE TABLE" in sql
    assert "ALTER TABLE" in sql
    assert "CREATE TRIGGER" not in sql
    assert result["privileged_migration"] is True
    assert result["triggers_installed"] is False
    assert result["schema_attested"] is False


def test_missing_bar_never_removes_member_from_next_target_universe():
    manifest, members = build_catalog_manifest(
        batch_id="catalog-batch-1",
        captured_at="2026-08-24 18:00:00",
        history_complete_from="1990-01-01",
        members=_members(),
        discovery=_discovery(_members()),
    )
    catalog = validate_catalog_manifest(
        manifest, row=_catalog_row(manifest), members=members
    )
    prior_day_bar_codes = {"000001"}

    assert "301999" not in prior_day_bar_codes
    assert "301999" in catalog.eligible_codes("2026-08-25")
    source = inspect.getsource(daily_fetch._read_stock_codes)
    assert "load_target_stock_catalog" in source
    assert "sm_stock_kline" not in source


def test_native_qmt_sector_discovery_includes_code_absent_from_si_all_code(
    monkeypatch,
):
    rows = []
    for sector_name, code in (
        ("上证A股", "600000.SH"),
        ("深证A股", "000001.SZ"),
        ("京市A股", "830001.BJ"),
        ("沪深A股", "301999.SZ"),
    ):
        rows.append({
            "sector_name": sector_name,
            "qmt_code": code,
            "stock_code": code[:6],
        })
    monkeypatch.setattr(
        reference_sync.bridge,
        "sector_members_many",
        lambda *_args, **_kwargs: pd.DataFrame(rows),
    )

    assert reference_sync._discover_native_stock_qmt_codes() == [
        "000001.SZ",
        "301999.SZ",
        "600000.SH",
        "830001.BJ",
    ]
    assert "si_all_code" not in inspect.getsource(
        reference_sync._discover_native_stock_qmt_codes
    )


def test_ninety_percent_threshold_cannot_authorize_a_daily_write(
    monkeypatch, capsys,
):
    monkeypatch.delenv("KLINE_DAILY_MAX_STOCKS", raising=False)
    monkeypatch.setattr(daily_fetch, "_mysql_url", lambda: "mysql://test")
    monkeypatch.setattr(daily_fetch, "create_engine", lambda *_a, **_k: object())
    monkeypatch.setattr(
        daily_fetch,
        "_read_stock_codes",
        lambda *_a, **_k: (
            ["000001"],
            "qmt_stock_catalog:batch",
            {
                "catalog_batch_id": "batch",
                "catalog_manifest_hash": "a" * 64,
                "catalog_member_count": 1,
                "catalog_member_set_hash": "b" * 64,
                "stock_count": 1,
                "stock_set_hash": "c" * 64,
            },
        ),
    )

    assert daily_fetch.fetch_daily_kline(
        "2026-08-24", min_coverage=0.90
    ) == 2
    assert "100%" in capsys.readouterr().out


def test_source_target_same_missing_member_is_rejected_by_catalog_gap_gate():
    source = inspect.getsource(attester.attest_range)

    assert "catalog_missing_target_rows" in source
    assert "catalog_missing_source_rows" in source
    assert "target_not_catalog_rows" in source
    assert "source_not_catalog_rows" in source
    assert "universe_gap_rows == 0" in source
    assert "and universe_sets_exact" in source
    assert "bound_stock_set_contract" in source
    assert "source_batch_by_date" in source
    assert "BINARY selected_batch.batch_id=BINARY raw.batch_id" in source


def test_canonical_qmt_daily_writer_requires_exact_sets_and_atomic_replace():
    source = inspect.getsource(sync_stock_market._step_stock_kline_qmt)

    assert "load_stock_catalog" in source
    assert "member_by_code" in source
    assert "observed != expected" in source
    assert "_create_temporary_stage(" in source
    assert "_publish_temporary_stage(" in source
    assert "persist_daily_kline_capture(" in source
    assert "load_trade_calendar_receipt" in source
    assert "capture_batch_id = daily_market_source_batch_id(" in source
    assert "coverage <" not in source
    assert "_upsert_qmt_kline_frame" not in source

    attestation_source = inspect.getsource(attester.attest_range)
    assert "daily_market_source_batch_id(" in (
        attestation_source
    )


def test_catalog_manifest_and_bound_attestation_tampering_fail_closed():
    manifest, members = build_catalog_manifest(
        batch_id="catalog-batch-1",
        captured_at="2026-08-24 18:00:00",
        history_complete_from="1990-01-01",
        members=_members(),
        discovery=_discovery(_members()),
    )
    tampered_catalog = {**manifest, "member_count": manifest["member_count"] - 1}
    with pytest.raises(ValueError, match="manifest content differs"):
        validate_catalog_manifest(
            tampered_catalog,
            row=_catalog_row(manifest),
            members=members,
        )

    daily = bound_stock_set_contract(
        "2026-08-24",
        ["000001", "301999"],
        catalog_batch_id=manifest["batch_id"],
        catalog_member_count=manifest["member_count"],
        catalog_member_set_hash=manifest["member_set_hash"],
        catalog_manifest_hash=canonical_digest(manifest),
        source_batch_id=daily_market_source_batch_id(
            catalog_manifest_hash=canonical_digest(manifest),
            calendar_manifest_hash="e" * 64,
        ),
        calendar_batch_id="calendar-batch-1",
        calendar_session_set_hash="d" * 64,
        calendar_manifest_hash="e" * 64,
        calendar_known_at="2026-08-24 18:00:00",
    )
    bound_manifest = build_qmt_v2_manifest({"2026-08-24": daily})
    bound_manifest["daily_universe"]["2026-08-24"][
        "source_stock_count"
    ] -= 1
    with pytest.raises(ValueError, match="catalog/source/target"):
        validated_universe_manifest(
            bound_manifest,
            start_date="2026-08-24",
            end_date="2026-08-24",
        )


class _AtomicConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params))


class _AtomicTransaction:
    def __init__(self, connection):
        self.connection = connection
        self.rolled_back = False

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, _exc, _tb):
        self.rolled_back = exc_type is not None
        return False


class _AtomicEngine:
    def __init__(self):
        self.connection = _AtomicConnection()
        self.transaction = _AtomicTransaction(self.connection)

    def begin(self):
        return self.transaction


def test_daily_kline_delegates_exact_business_keys_to_atomic_helper(monkeypatch):
    engine = _AtomicEngine()
    frame = pd.DataFrame([{
        "stock_code": "000001",
        "short_name": "平安银行",
        "trade_time": datetime(2026, 8, 24, 15, 0),
        "trade_date": "2026-08-24",
        "k_type": 1,
        "adjust_type": 0,
        "open": 10.0,
        "close": 10.1,
        "high": 10.2,
        "low": 9.9,
        "volume": 1000.0,
        "amount": 10000.0,
        "change": 0.1,
        "change_pct": 1.0,
        "turnover_ratio": 0.1,
        "pre_close": 10.0,
    }])

    def fail_write(_frame, table_name, target_engine, **kwargs):
        assert target_engine is engine
        assert table_name == "sm_stock_kline"
        assert kwargs["key_columns"] == (
            "stock_code", "trade_date", "k_type", "adjust_type",
        )
        assert kwargs["lock_name"] == "probiga:stock_kline"
        raise RuntimeError("insert failed")

    monkeypatch.setattr(daily_fetch, "replace_table_rows_exact_keys", fail_write)
    with pytest.raises(RuntimeError, match="insert failed"):
        daily_fetch._write_daily_kline(engine, "2026-08-24", frame)
