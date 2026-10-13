"""Native capture -> immutable source seal -> transactional publication -> gate.

Everything lives in tmp_path: neither the production bridge, database, strategy
process nor formal bootstrap receipt is used. Identity, codecs, manifests,
immutable INSERTs, independent loaders and the pure bootstrap gate stay real.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta
import gzip
import json
import os
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from integrations.bigqmt import reference, reference_capture as capture
from integrations.bigqmt import release_identity as identity
from tools import run_qmt_windows_edge_release_bootstrap as bootstrap
from tools import sync_guojin_qmt_reference_data as publisher


SERVER_NOW = "2031-02-03 04:05:06"
STRATEGY_PATH = "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
IMMUTABLE_TABLES = (
    "qmt_stock_catalog_batch", "qmt_stock_catalog_member",
    "qmt_trade_calendar_batch", "qmt_trade_calendar_session",
)


def _git(root, *args, input=None, env=None):
    result = subprocess.run(
        ["git", "-C", str(root), *args], input=input, capture_output=True,
        check=True, timeout=10, env=env,
    )
    return result.stdout.decode("utf-8").strip()


def _test_release_repository(tmp_path):
    """Two real app commits containing precisely the same native strategy."""
    root = tmp_path / "release-proof-repository"
    root.mkdir()
    source = publisher.BIGQMT_STRATEGY_SOURCE.read_bytes()
    _git(root, "init", "--quiet")
    blob = _git(root, "hash-object", "-w", "--stdin", input=source)
    _git(root, "update-index", "--add", "--cacheinfo",
         "100644", blob, STRATEGY_PATH)
    tree = _git(root, "write-tree")
    commit_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Isolated reference test",
        "GIT_AUTHOR_EMAIL": "reference-test@example.invalid",
        "GIT_COMMITTER_NAME": "Isolated reference test",
        "GIT_COMMITTER_EMAIL": "reference-test@example.invalid",
    }
    old = _git(root, "commit-tree", tree, "-m", "native collector release",
               env=commit_env)
    new = _git(root, "commit-tree", tree, "-p", old,
               "-m", "publisher-only release", env=commit_env)
    artifact = identity.git_strategy_artifact(
        root=root, source_path=root / STRATEGY_PATH, build_sha=old,
    )
    rendered = identity.render_strategy_artifact(
        artifact["source_bytes"], build_sha=old, git_blob=artifact["git_blob"],
        source_sha256=artifact["source_sha256"],
    )
    manifest = identity.build_strategy_release_manifest(
        build_sha=old, git_blob=artifact["git_blob"],
        source_sha256=artifact["source_sha256"],
        artifact_sha256=rendered["artifact_sha256"],
        identity_sha256=rendered["identity_sha256"],
    )
    return root, old, new, manifest


def _native_index_row(symbol):
    """One native index instrument-detail row, in the shape QMT returns."""
    code, _, exchange = symbol.partition(".")
    return {"qmt_code": symbol, "stock_code": code,
            "short_name": f"原生指数{code}", "exchange": exchange,
            "product_type": "INDEX", "list_date": "20100104",
            "expire_date": None}


class NativeSource:
    """Only the actual QMT transport boundary is replaced by native-shaped I/O."""

    def __init__(self, manifest):
        self.calls = []
        self.offline = False
        self.payload = {
            **manifest, "status": "ok", "source": "gj_big_qmt_inner",
            "bridge_version": "bigqmt_inner_v2",
            "strategy_identity_frozen": True, "strategy_identity_status": "BOUND",
            "read_only": True, "simulation_only": True,
            "automatic_real_order_submission": False, "real_order_authority": False,
            "model_instance_id": "isolated-same-native-model",
            "actions": ["capabilities", "trading_calendar"],
            "native_capabilities": [
                {"capability": "trading_calendar", "action": "trading_calendar",
                 "available": True, "source_method": "ContextInfo.get_trading_dates"},
                {"capability": "index_weight", "available": False,
                 "source_method": "membership_only_no_native_weight"},
            ],
        }
        self.details = pd.DataFrame({
            "qmt_code": ["000001.SZ", "600000.SH", "920001.BJ"],
            "stock_code": ["000001", "600000", "920001"],
            "short_name": ["深圳原生证券", "上海原生证券", "北京原生证券"],
            "exchange": ["SZ", "SH", "BJ"],
            "product_type": ["STOCK", "STOCK", None],
            "list_date": ["19910403", "19991110", "20240101"],
            "expire_date": [None, None, None],
            "volume_unit": pd.array([100, pd.NA, 100], dtype="Int64"),
        })
        self.details.attrs = {
            "native_evidence": {"model_instance_id": self.payload["model_instance_id"],
                                "optional_native_field": None},
            "nullable_scalar": pd.NA,
        }

    def capabilities(self, **_kwargs):
        self.calls.append(("capabilities",))
        if self.offline:
            raise ConnectionError("isolated native source offline")
        return deepcopy(self.payload)

    def sector_members_many(self, sectors, **_kwargs):
        self.calls.append(("sector_members_many", tuple(sectors)))
        assert set(sectors) == set(publisher.NATIVE_A_SHARE_SECTORS)
        return pd.DataFrame([
            {"sector_name": sector, "qmt_code": code, "stock_code": code[:6]}
            for sector, code in (
                ("深证A股", "000001.SZ"), ("上证A股", "600000.SH"),
                ("京市A股", "920001.BJ"), ("沪深A股", "000001.SZ"),
                ("沪深A股", "600000.SH"),
            )
        ])

    def instrument_details(self, codes, **_kwargs):
        self.calls.append(("instrument_details", tuple(codes)))
        requested = [str(code).upper() for code in codes]
        known = self.details.loc[self.details["qmt_code"].isin(requested)]
        # A QMT instrument-detail request covers whatever the caller asks for,
        # indexes included, so an index catalog can never be published from a
        # silently truncated response.
        unknown = [symbol for symbol in requested
                   if symbol not in set(known["qmt_code"])]
        if unknown:
            known = pd.concat([known, pd.DataFrame([_native_index_row(symbol)
                                                    for symbol in unknown])],
                              ignore_index=True)
        return known.copy()

    def trading_calendar_capture(self, market, *, start_date, end_date, **_kwargs):
        self.calls.append(("trading_calendar_capture", market, start_date, end_date))
        assert market == "SH"
        last = date.fromisoformat(end_date)
        first = max(date(last.year, 1, 1), last - timedelta(days=1))
        sessions = sorted({first, last})
        return {
            **deepcopy(self.payload), "action": "trading_calendar",
            "source_method": "ContextInfo.get_trading_dates",
            "source_stock_code": "000001.SH", "requested_start_date": start_date,
            "requested_end_date": end_date,
            "observed_start_date": sessions[0].isoformat(),
            "observed_end_date": sessions[-1].isoformat(),
            "rows": [
                {"market": "SH", "calendar_year": day.year,
                 "trade_date": day.isoformat(), "trade_status": 1,
                 "day_week": day.isoweekday()} for day in sessions
            ],
        }


def _sqlite_pair_engine(path):
    # File-backed: production sync disposes the pool after each invocation,
    # including failed/commit-unknown attempts. Durable rows must still exist.
    engine = create_engine(f"sqlite:///{path.as_posix()}")

    @event.listens_for(engine, "connect")
    def native_server_clock_and_fk(dbapi, _record):
        dbapi.create_function("NOW", 0, lambda: SERVER_NOW)
        dbapi.execute("PRAGMA foreign_keys=ON")

    statements = (
        """CREATE TABLE qmt_stock_catalog_batch (
            batch_id TEXT PRIMARY KEY, captured_at TEXT, history_complete_from TEXT,
            status TEXT, member_count INTEGER, member_set_hash TEXT,
            manifest_json TEXT, manifest_hash TEXT, created_at TEXT NOT NULL)""",
        """CREATE TABLE qmt_stock_catalog_member (
            batch_id TEXT, qmt_code TEXT, stock_code TEXT, list_date TEXT,
            expire_date TEXT, instrument_batch_id TEXT, instrument_type TEXT,
            created_at TEXT NOT NULL, PRIMARY KEY(batch_id,qmt_code),
            UNIQUE(batch_id,stock_code),
            FOREIGN KEY(batch_id) REFERENCES qmt_stock_catalog_batch(batch_id))""",
        """CREATE TABLE qmt_trade_calendar_batch (
            batch_id TEXT PRIMARY KEY, source_batch_id TEXT, known_at TEXT,
            start_date TEXT, end_date TEXT, status TEXT, session_count INTEGER,
            session_set_hash TEXT, manifest_json TEXT, manifest_hash TEXT,
            created_at TEXT NOT NULL)""",
        """CREATE TABLE qmt_trade_calendar_session (
            batch_id TEXT, trade_date TEXT, created_at TEXT NOT NULL,
            PRIMARY KEY(batch_id,trade_date),
            FOREIGN KEY(batch_id) REFERENCES qmt_trade_calendar_batch(batch_id))""",
        """CREATE TABLE si_all_code (
            stock_code TEXT PRIMARY KEY, short_name TEXT, qmt_code TEXT,
            batch_id TEXT)""",
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
    return engine


@pytest.fixture
def chain(tmp_path, monkeypatch):
    root, old, new, manifest = _test_release_repository(tmp_path)
    # Bind the real identity validator to an isolated repository, not fake
    # validator return values. Both app SHAs have the same strategy content.
    monkeypatch.setattr(reference, "_REFERENCE_ROOT", root)
    monkeypatch.setattr(reference, "_REFERENCE_SOURCE", root / STRATEGY_PATH)
    monkeypatch.setattr(capture, "ROOT", root)
    monkeypatch.setattr(capture, "SOURCE", root / STRATEGY_PATH)
    monkeypatch.setattr(publisher, "ROOT", root)
    monkeypatch.setattr(publisher, "BIGQMT_STRATEGY_SOURCE", root / STRATEGY_PATH)
    source = NativeSource(manifest)
    for method in ("capabilities", "sector_members_many", "instrument_details",
                   "trading_calendar_capture"):
        monkeypatch.setattr(reference.bridge, method, getattr(source, method))
    # Any accidental additional collector or user-auth/UI operation fails.
    def unused_native(*_args, **_kwargs):
        pytest.fail("catalog-only attempted an unrelated native action")
    for method in ("sector_list", "sector_members", "index_weight_many"):
        monkeypatch.setattr(reference.bridge, method, unused_native)
    monkeypatch.setattr(publisher.bridge, "refresh_reference_data", unused_native)

    engine = _sqlite_pair_engine(tmp_path / "immutable-pair.sqlite")
    monkeypatch.setattr(publisher, "get_mysql_url", lambda **_kwargs: str(engine.url))
    monkeypatch.setattr(publisher, "create_batch_engine", lambda *_a, **_k: engine)
    # This metadata validator describes MySQL DDL/triggers; the isolated SQLite
    # schema above enforces real PK/unique/FK constraints instead.
    monkeypatch.setattr(publisher, "validate_reference_tables", lambda _engine: None)
    ordinary_calls = []
    def ordinary_tables(_engine, *, table_name, frame, **_kwargs):
        ordinary_calls.append((table_name, len(frame)))
        return {"status": "ISOLATED_UNUSED_COMPATIBILITY_TABLE",
                "accepted_rows": len(frame)}
    monkeypatch.setattr(publisher, "_safe_upsert_frame", ordinary_tables)

    stores = []
    store_class = capture.ReferenceCaptureStore
    def store(scope, **kwargs):
        instance = store_class(scope, root=tmp_path / "source-captures", **kwargs)
        stores.append(instance)
        return instance
    monkeypatch.setattr(publisher, "ReferenceCaptureStore", store)

    def run(build=old):
        for name in ("PROBIGA_SCHEDULER_BUILD_SHA", "PROBIGA_BUILD_COMMIT_SHA",
                     "PROBIGA_EXPECTED_GIT_SHA"):
            monkeypatch.setenv(name, build)
        return publisher.sync_reference_data(
            start_year=date.today().year, end_year=date.today().year,
            iscomplete=True, refresh_timeout=1, skip_refresh=True,
            skip_calendar=False, dry_run=False, historical_instrument_archive="",
            release_build_sha=build, catalog_only=True,
        )

    value = SimpleNamespace(engine=engine, source=source, stores=stores,
                            run=run, old=old, new=new, ordinary_calls=ordinary_calls)
    yield value
    engine.dispose()


def _counts(chain):
    with chain.engine.connect() as connection:
        return {table: connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                .scalar_one() for table in IMMUTABLE_TABLES}


def _assert_real_readback_and_gate(chain, result, build):
    with chain.engine.connect() as connection:
        assert connection.execute(text("PRAGMA foreign_keys")).scalar_one() == 1
        catalog = publisher.load_stock_catalog(
            connection, batch_id=result["batch_id"], decision_known_at=datetime.now(),
        )
        calendar = publisher.load_trade_calendar_receipt(
            connection, batch_id=result["batch_id"],
            start_date=result["trade_calendar"]["start_date"],
            end_date=result["trade_calendar"]["end_date"],
            decision_known_at=datetime.now(),
        )
        for table in IMMUTABLE_TABLES:
            assert connection.execute(text(f"SELECT DISTINCT created_at FROM {table}"))\
                .scalars().all() == [SERVER_NOW]
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_stock_catalog_member "
                                       "WHERE expire_date IS NULL")).scalar_one() == 3
    assert catalog.manifest_hash == publisher.canonical_digest(result["stock_catalog"])
    assert calendar.manifest_hash == publisher.canonical_digest(result["trade_calendar"])
    validated = bootstrap._validated_reference_capture(result, expected_build_sha=build)
    assert validated[-1]["workload"] == "STOCK_CATALOG_NATIVE_CALENDAR"
    assert validated[-1]["source_capture"] == result["reference_capture"]
    return catalog.manifest_hash, calendar.manifest_hash


# The isolated schema below reproduces only the ordinary tables a catalog-only
# refresh writes, so the real index publisher gate can be driven against real
# rows instead of a mocked writer.
_ORDINARY_DDL = (
    """CREATE TABLE qmt_instrument_detail (
        qmt_code TEXT PRIMARY KEY, stock_code TEXT, short_name TEXT,
        exchange TEXT, product_type TEXT, list_date TEXT, expire_date TEXT,
        instrument_type TEXT, batch_id TEXT, data_source TEXT,
        permission_status TEXT, quality_status TEXT,
        etl_sync_at TEXT, received_at TEXT)""",
    """CREATE TABLE si_all_index_code (
        index_code TEXT PRIMARY KEY, concept_code TEXT, name TEXT, source TEXT,
        etl_sync_at TEXT)""",
    """CREATE TABLE si_trade_calendar (
        calendar_year INTEGER, trade_date TEXT, trade_status INTEGER,
        day_week INTEGER, batch_id TEXT, data_source TEXT,
        permission_status TEXT, quality_status TEXT, etl_sync_at TEXT,
        received_at TEXT, PRIMARY KEY (calendar_year, trade_date))""",
)
# The batch id the production database was left holding: the formal index rows
# in `qmt_instrument_detail` carry it while no `qmt_stock_catalog_batch` row for
# it was ever sealed. The isolated seed mirrors that shape at gate scale.
_STRANDED_BATCH = "qmt_rel_f251cc514aa8409f3bec1562b42ea3db11ba9ff5_20261011130145"


def _isolated_index_codes():
    """The formal index codes a catalog-only refresh has to cover."""
    codes = [f"{1000 + offset:06d}" for offset in range(30)]
    codes.extend(f"{399100 + offset:06d}" for offset in range(39))
    codes.extend(symbol.split(".")[0] for symbol in publisher.CORE_INDEXES)
    return sorted(set(codes))


def test_catalog_only_refresh_rebinds_index_details_to_its_sealed_batch(chain, monkeypatch):
    """Reproduces the production index failure instead of asserting its fix text.

    A catalog-only refresh used to skip index instrument details, so the formal
    index rows kept a batch whose catalog was never sealed and every index task
    died in seconds with `no complete independent QMT stock catalog batch`.
    """
    from tools import sync_qmt_index_edge as index_edge

    seeded = _isolated_index_codes()
    with chain.engine.begin() as connection:
        for statement in _ORDINARY_DDL:
            connection.execute(text(statement))
        connection.execute(text(
            "INSERT INTO si_all_index_code (index_code, name, source) VALUES "
            "(:code, :name, 'qmt')"),
            [{"code": code, "name": f"隔离指数{code}"} for code in seeded])
        connection.execute(text(
            "INSERT INTO qmt_instrument_detail (qmt_code, stock_code, short_name, "
            "exchange, product_type, list_date, expire_date, instrument_type, "
            "batch_id, data_source, permission_status, quality_status) VALUES "
            "(:qmt, :code, :name, :exchange, 'INDEX', '2010-01-04', NULL, 'INDEX', "
            ":batch, 'gj_qmt', 'SUPPORTED', 'PENDING')"),
            [{"qmt": publisher.to_qmt_index_symbol(code), "code": code,
              "name": f"隔离指数{code}", "batch": _STRANDED_BATCH,
              "exchange": publisher.to_qmt_index_symbol(code).split(".")[1]}
             for code in seeded])

    # Observed production failure: the index gate cannot resolve a catalog for
    # the batch the formal index rows point at.
    with chain.engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM qmt_stock_catalog_batch WHERE batch_id=:batch"),
            {"batch": _STRANDED_BATCH}).scalar_one() == 0
    with pytest.raises(RuntimeError,
                       match="no complete independent QMT stock catalog batch"):
        index_edge._load_index_catalog(chain.engine)

    # One catalog-only refresh, through the real publication path, with the
    # ordinary tables actually written.
    def real_ordinary_tables(engine, *, table_name, frame, key_columns,
                             batch_id, **_kwargs):
        chain.ordinary_calls.append((table_name, len(frame)))
        rows = publisher._records(frame)
        if not rows:
            return {"status": "EMPTY", "source_rows": 0,
                    "accepted_rows": 0, "duplicate_rows": 0}

        def bindable(value):
            # MySQL DATETIME columns carry real timestamps; the isolated TEXT
            # schema above stores the same instant in its ISO form.
            return value.isoformat(sep=" ") if isinstance(value, datetime) else value

        with engine.begin() as connection:
            table_columns = [row[1] for row in connection.execute(
                text(f"PRAGMA table_info({table_name})"))]
            assert table_columns, f"isolated schema is missing {table_name}"
            present = set().union(*(row.keys() for row in rows))
            columns = [column for column in table_columns if column in present]
            assert set(key_columns).issubset(columns)
            updates = [column for column in columns if column not in set(key_columns)]
            connection.execute(
                text(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
                     f"({', '.join(f':{column}' for column in columns)}) "
                     f"ON CONFLICT ({', '.join(key_columns)}) DO UPDATE SET "
                     + ", ".join(f"{column}=excluded.{column}" for column in updates)),
                [{column: bindable(row.get(column)) for column in columns}
                 for row in rows])
        return {"status": "UPSERTED", "source_rows": len(rows),
                "accepted_rows": len(rows), "duplicate_rows": 0}

    monkeypatch.setattr(publisher, "_safe_upsert_frame", real_ordinary_tables)

    published = chain.run()
    assert published["status"] == "success"
    assert published["mode"] == "catalog_only"

    # The same gate now resolves one sealed catalog and every formal index row
    # is bound to it.
    members = index_edge._load_index_catalog(chain.engine)
    assert {member.batch_id for member in members} == {published["batch_id"]}
    assert {member.index_code for member in members} == set(seeded)
    with chain.engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM qmt_instrument_detail WHERE batch_id=:batch"),
            {"batch": _STRANDED_BATCH}).scalar_one() == 0


def test_native_nullable_frames_seal_publish_validate_and_old_sha_replay(chain):
    assert str(chain.source.details["volume_unit"].dtype) == "Int64"
    first = chain.run()
    assert first["status"] == "success"
    assert first["mode"] == "catalog_only"
    assert first["reference_capture"]["reused"] is False
    store = chain.stores[-1]
    with store.locked():
        decoded = store.load_sealed()
    raw = json.loads(gzip.decompress((store.root / "sealed.json.gz").read_bytes()))
    assert raw["snapshot"]["stock_details"]["__frame__"] is True
    assert raw["snapshot"]["calendar"]["attrs"] == decoded["calendar"].attrs
    assert decoded["stock_details"]["expire_date"].tolist() == [None, None, None]
    assert decoded["stock_details"].attrs == {
        "native_evidence": {"model_instance_id": "isolated-same-native-model",
                            "optional_native_field": None}, "nullable_scalar": None,
    }
    instrument_partitions = [
        json.loads(gzip.decompress(path.read_bytes()))
        for path in store.root.glob("partition-*.json.gz")
    ]
    instrument = next(partition for partition in instrument_partitions
                      if partition["request"]["method"] == "instrument_details"
                      and "600000.SH" in partition["request"]["args"][0])
    assert instrument["value"]["rows"][1]["volume_unit"] is None
    assert instrument["value"]["rows"][0]["volume_unit"] == 100
    assert decoded["calendar_capture_evidence"]["source_method"] == "ContextInfo.get_trading_dates"
    assert decoded["calendar_capture_evidence"]["partitions"][0]["session_count"] == len(decoded["calendar"])
    assert decoded["calendar_manifest"]["start_date"] == decoded["calendar_capture_evidence"]["proven_start_date"]
    assert decoded["calendar_manifest"]["end_date"] == decoded["calendar_capture_evidence"]["proven_end_date"]
    hashes = _assert_real_readback_and_gate(chain, first, chain.old)
    counts = _counts(chain)
    assert counts["qmt_stock_catalog_batch"] == counts["qmt_trade_calendar_batch"] == 1
    assert counts["qmt_stock_catalog_member"] == 3
    assert counts["qmt_trade_calendar_session"] > 0
    calls = list(chain.source.calls)

    replay = chain.run(chain.new)
    assert chain.old != chain.new
    assert chain.source.calls == calls  # No native call, including identity probes.
    assert replay["release_build_sha"] == chain.new
    assert replay["batch_id"] == first["batch_id"]
    assert replay["reference_capture"]["collector_build_sha"] == chain.old
    assert replay["reference_capture"]["reused"] is True
    assert replay["reference_capture"]["sha256"] == first["reference_capture"]["sha256"]
    assert chain.stores[-1].root == store.root
    assert _counts(chain) == counts
    assert _assert_real_readback_and_gate(chain, replay, chain.new) == hashes


def test_failed_pair_publish_retries_sealed_source_without_native_reads(chain):
    def fail_calendar_session(_connection, _cursor, statement, _params, _context, _many):
        if "INSERT INTO qmt_trade_calendar_session" in statement:
            raise RuntimeError("isolated calendar INSERT failed after catalog INSERT")
    event.listen(chain.engine, "before_cursor_execute", fail_calendar_session)
    with pytest.raises(RuntimeError, match="isolated calendar INSERT failed"):
        chain.run()
    assert (chain.stores[-1].root / "sealed.json.gz").is_file()
    assert _counts(chain) == dict.fromkeys(IMMUTABLE_TABLES, 0)
    assert not list(chain.stores[-1].root.glob("published-*.json.gz"))
    calls = list(chain.source.calls)
    event.remove(chain.engine, "before_cursor_execute", fail_calendar_session)
    # Even an unavailable native source cannot stop the durable publisher retry.
    chain.source.offline = True
    result = chain.run()
    assert chain.source.calls == calls
    assert result["reference_capture"]["reused"] is True
    _assert_real_readback_and_gate(chain, result, chain.old)
    assert _counts(chain)["qmt_stock_catalog_member"] == 3


def test_missing_source_seal_cannot_report_success_from_mutable_tables(chain):
    published = chain.run()
    counts = _counts(chain)
    with chain.engine.begin() as connection:
        connection.execute(text("INSERT INTO si_all_code "
            "(stock_code,short_name,qmt_code,batch_id) VALUES "
            "('000001','mutable table is not source proof','000001.SZ',:batch)"),
            {"batch": published["batch_id"]})
    store = chain.stores[-1]
    # Only isolated pytest evidence is removed, never production artifacts.
    (store.root / "sealed.json.gz").unlink()
    existing_receipts = sorted(path.name for path in store.root.glob("published-*.json.gz"))
    ordinary_before = list(chain.ordinary_calls)
    calls = list(chain.source.calls)
    chain.source.offline = True
    with pytest.raises(reference.ReferenceTransportUnavailable, match="QMT_REFERENCE_TRANSPORT_UNAVAILABLE"):
        chain.run()
    assert chain.source.calls == [*calls, ("capabilities",)]
    assert chain.ordinary_calls == ordinary_before
    assert _counts(chain) == counts
    assert not (store.root / "sealed.json.gz").exists()
    assert sorted(path.name for path in store.root.glob("published-*.json.gz")) == existing_receipts
    with chain.engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM si_all_code")).scalar_one() == 1
