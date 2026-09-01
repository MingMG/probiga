from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from threading import Barrier, Lock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from server.common import turnover_snapshot as turnover_module
from server.common import production_runtime_schema_bundle as bundle
from server.common.analysis_pool_receipt import (
    build_pool_manifest,
    build_turnover_evidence,
    build_upper_limit_evidence,
    validate_turnover_evidence,
)
from server.common.qmt_attestation_contract import expected_stock_set_contract
from server.common.turnover_snapshot import (
    TURNOVER_SNAPSHOT_VERSION,
    PinnedCurlEastmoneyTurnoverCollector,
    TurnoverSnapshotBlocked,
    TurnoverTransportError,
    TurnoverUniverseAuthority,
    build_capture_run,
    collect_turnover_snapshot,
    freeze_qmt_turnover_targets,
    load_verified_turnover_evidence,
    _historical_qmt_row_matches_capture,
    _revalidate_replayable_turnover_authority,
    load_turnover_universe_authority,
    parse_eastmoney_turnover_response,
    publish_turnover_snapshot,
    recover_completed_turnover_receipt,
)
from server.common.turnover_snapshot_schema import (
    TURNOVER_SNAPSHOT_ROW_TABLE,
    TURNOVER_SNAPSHOT_RUN_TABLE,
    TURNOVER_SNAPSHOT_SCHEMA,
    _expected_trigger_contracts,
    market_field_capture_trigger_ddl_statements,
    privileged_migrate_market_field_capture_schema,
    validate_market_field_capture_immutability,
)
from tools import sync_target_turnover_snapshot as turnover_command


ROOT = Path(__file__).resolve().parents[1]
TARGET_DATE = "2026-08-21"
DECISION_AT = datetime(2026, 8, 27, 18, 50)
CAPTURED_AT = datetime(2026, 8, 27, 18, 40)
HTTP_DATE = "Thu, 27 Aug 2026 10:39:00 GMT"
BUILD_SHA = "a" * 40
BINARY_SHA = "b" * 64


class _OneMappingResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self._row


class _AuthorityConnection:
    dialect = SimpleNamespace(name="sqlite")

    def __init__(self, row):
        self.row = row

    def execute(self, _statement, _params=None):
        return _OneMappingResult(self.row)


def test_turnover_authority_reuses_attested_no_row_projection(monkeypatch) -> None:
    truth = SimpleNamespace(
        run_id="attestation-run-1",
        run_start_date="2026-03-06",
        run_end_date="2026-08-28",
        catalog_batch_id="catalog-1",
        calendar_batch_id="calendar-1",
        catalog_manifest_hash="1" * 64,
        catalog_member_set_hash="2" * 64,
        requested_sessions=("2026-08-28",),
        attested_row_count=2,
        truth_hash="3" * 64,
        no_row_exception_proof_sha256="4" * 64,
    )
    catalog = SimpleNamespace(
        manifest_hash="1" * 64,
        member_set_hash="2" * 64,
        eligible_codes=lambda _day: ["000001", "301688", "600000"]
    )
    no_row_contract = {"proof_sha256": "4" * 64}
    observed = {}

    monkeypatch.setattr(
        turnover_module, "load_qmt_daily_market_truth", lambda *_a, **_k: truth
    )
    monkeypatch.setattr(
        turnover_module, "load_stock_catalog", lambda *_a, **_k: catalog
    )
    monkeypatch.setattr(
        turnover_module,
        "validated_no_row_exception_contract",
        lambda *_a, **_k: no_row_contract,
    )
    monkeypatch.setattr(
        turnover_module,
        "load_trade_calendar_receipt",
        lambda *_a, **_k: SimpleNamespace(batch_id="calendar-1"),
    )

    def project(**kwargs):
        observed.update(kwargs)
        return {"2026-08-28": ["000001", "600000"]}

    monkeypatch.setattr(turnover_module, "project_catalog_daily_codes", project)
    connection = _AuthorityConnection({
            "start_date": "2026-03-06",
            "end_date": "2026-08-28",
            "tolerance_json": "sealed-manifest",
        })
    connection.dialect = SimpleNamespace(name="mysql")
    catalog_guard = MagicMock()
    calendar_guard = MagicMock()
    monkeypatch.setattr(
        turnover_module, "validate_stock_catalog_immutability", catalog_guard
    )
    monkeypatch.setattr(
        turnover_module, "validate_trade_calendar_immutability", calendar_guard
    )
    authority = load_turnover_universe_authority(
        connection,
        target_date="2026-08-28",
        decision_at="2026-08-30 09:00:00",
        require_triggers=False,
    )

    assert authority.expected_codes == ("000001", "600000")
    assert authority.stock_set_sha256 == expected_stock_set_contract(
        "2026-08-28", authority.expected_codes
    )["stock_set_hash"]
    assert observed["contract"] is no_row_contract
    catalog_guard.assert_not_called()
    calendar_guard.assert_not_called()


def test_turnover_authority_rejects_no_row_proof_relabel(monkeypatch) -> None:
    truth = SimpleNamespace(
        run_id="attestation-run-1",
        run_start_date="2026-03-06",
        run_end_date="2026-08-28",
        catalog_batch_id="catalog-1",
        calendar_batch_id="calendar-1",
        catalog_manifest_hash="1" * 64,
        catalog_member_set_hash="2" * 64,
        requested_sessions=("2026-08-28",),
        attested_row_count=2,
        truth_hash="3" * 64,
        no_row_exception_proof_sha256="4" * 64,
    )
    monkeypatch.setattr(
        turnover_module, "load_qmt_daily_market_truth", lambda *_a, **_k: truth
    )
    monkeypatch.setattr(
        turnover_module,
        "load_stock_catalog",
        lambda *_a, **_k: SimpleNamespace(eligible_codes=lambda _day: []),
    )
    monkeypatch.setattr(
        turnover_module,
        "validated_no_row_exception_contract",
        lambda *_a, **_k: {"proof_sha256": "5" * 64},
    )

    with pytest.raises(TurnoverSnapshotBlocked, match="no-row authority differs"):
        load_turnover_universe_authority(
            _AuthorityConnection({
                "start_date": "2026-03-06",
                "end_date": "2026-08-28",
                "tolerance_json": "tampered-manifest",
            }),
            target_date="2026-08-28",
            decision_at="2026-08-30 09:00:00",
        )


class _TriggerRows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _TriggerConnection:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _statement, _params):
        return _TriggerRows(self.rows)


def _valid_field_capture_trigger_rows():
    return [
        {
            "TRIGGER_NAME": name,
            "EVENT_OBJECT_TABLE": table_name,
            "EVENT_MANIPULATION": event,
            "ACTION_TIMING": "BEFORE",
            "ACTION_STATEMENT": body,
        }
        for name, (table_name, event, body)
        in _expected_trigger_contracts().items()
    ]


def test_market_field_capture_trigger_attestation_is_exact() -> None:
    rows = _valid_field_capture_trigger_rows()
    validate_market_field_capture_immutability(_TriggerConnection(rows))

    with pytest.raises(RuntimeError, match="inventory differs"):
        validate_market_field_capture_immutability(
            _TriggerConnection(rows[:-1])
        )

    changed = [dict(row) for row in rows]
    changed[0]["ACTION_STATEMENT"] = (
        "BEGIN SET NEW.status='COMPLETED'; END"
    )
    with pytest.raises(RuntimeError, match="contract differs"):
        validate_market_field_capture_immutability(
            _TriggerConnection(changed)
        )


def test_market_field_capture_trigger_attestation_uses_behavior_when_hidden(
    monkeypatch,
) -> None:
    connection = _TriggerConnection([])
    calls = []
    monkeypatch.setattr(
        "server.common.turnover_snapshot_schema."
        "_validate_market_field_capture_trigger_behavior",
        lambda received: calls.append(received),
    )

    validate_market_field_capture_immutability(connection)

    assert calls == [connection]


def test_market_field_capture_migration_never_runs_trigger_ddl(
    monkeypatch,
) -> None:
    statements: list[str] = []

    class _Rows:
        def mappings(self):
            return self

        def all(self):
            return []

    class _Connection:
        def execute(self, statement, _params=None):
            statements.append(str(statement))
            return _Rows()

    class _Context:
        def __enter__(self):
            return _Connection()

        def __exit__(self, *_args):
            return False

    class _Engine:
        dialect = type("Dialect", (), {"name": "mysql"})()

        def begin(self):
            return _Context()

    validator_calls: list[bool] = []
    monkeypatch.setattr(
        "server.common.turnover_snapshot_schema.privileged_normalize_mysql_storage",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "server.common.turnover_snapshot_schema.validate_market_field_capture_runtime",
        lambda _engine, *, connection, require_triggers: (
            validator_calls.append(require_triggers)
        ),
    )

    result = privileged_migrate_market_field_capture_schema(_Engine())

    normalized = [statement.lstrip().upper() for statement in statements]
    assert not any(statement.startswith("DROP TRIGGER") for statement in normalized)
    assert not any(statement.startswith("CREATE TRIGGER") for statement in normalized)
    assert validator_calls == [False]
    assert result["triggers_installed"] is False
    assert len(market_field_capture_trigger_ddl_statements()) == 5

    with pytest.raises(RuntimeError, match="frozen release broker"):
        privileged_migrate_market_field_capture_schema(
            _Engine(),
            install_triggers=True,
        )


def _create_tables(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
              id INTEGER PRIMARY KEY,
              stock_code TEXT NOT NULL,
              trade_date TEXT NOT NULL,
              k_type INTEGER NOT NULL,
              adjust_type INTEGER NOT NULL,
              open NUMERIC NOT NULL,
              high NUMERIC NOT NULL,
              low NUMERIC NOT NULL,
              close NUMERIC NOT NULL,
              volume NUMERIC NOT NULL,
              amount NUMERIC NOT NULL,
              turnover_ratio NUMERIC,
              received_at TEXT NOT NULL,
              data_source TEXT NOT NULL,
              batch_id TEXT NOT NULL,
              data_version TEXT NOT NULL,
              quality_status TEXT NOT NULL,
              permission_status TEXT NOT NULL
            )
        """))
        for table_name, contract in TURNOVER_SNAPSHOT_SCHEMA.items():
            definitions = []
            for column, spec in contract.columns.items():
                sqlite_type = "BLOB" if spec.data_type.endswith("blob") else "TEXT"
                definitions.append(f'"{column}" {sqlite_type}')
            primary = (
                ', PRIMARY KEY ("run_id")'
                if table_name == TURNOVER_SNAPSHOT_RUN_TABLE
                else ', PRIMARY KEY ("run_id","stock_code","trade_date","k_type","adjust_type")'
            )
            connection.execute(text(
                f'CREATE TABLE "{table_name}" ('
                + ",".join(definitions)
                + primary
                + ")"
            ))


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _create_tables(engine)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_stock_kline (
              id,stock_code,trade_date,k_type,adjust_type,
              open,high,low,close,volume,amount,turnover_ratio,
              received_at,data_source,batch_id,data_version,
              quality_status,permission_status
            ) VALUES
              (1,'000001',:target,1,0,10,10.5,9.8,10.2,1000,10000,NULL,
               '2026-08-23 08:00:00','gj_big_qmt_inner','batch-1','version-1',
               'QMT_ATTESTED','SUPPORTED'),
              (2,'600000',:target,1,0,8,8.3,7.9,8.1,2000,20000,NULL,
               '2026-08-23 08:00:01','gj_big_qmt_inner','batch-2','version-2',
               'QMT_ATTESTED','SUPPORTED')
        """), {"target": TARGET_DATE})
    return engine


def _raw_payload(code: str) -> bytes:
    if code == "000001":
        line = f"{TARGET_DATE},10,10.2,10.5,9.8,10,10000,7,2,0.2,0.45"
    else:
        # f59/f60 are signed provider fields and must accept a down day.
        line = f"{TARGET_DATE},8,8.1,8.3,7.9,20,20000,5,-1.25,-0.1,0.15"
    return json.dumps(
        {"rc": 0, "data": {"code": code, "klines": [line]}},
        separators=(",", ":"),
    ).encode("utf-8")


def _targets(engine):
    with engine.connect() as connection:
        return freeze_qmt_turnover_targets(
            connection,
            target_date=TARGET_DATE,
            decision_at=DECISION_AT,
            authority=_authority(),
            min_expected_count=2,
        )


def _authority() -> TurnoverUniverseAuthority:
    codes = ("000001", "600000")
    return TurnoverUniverseAuthority(
        target_date=date.fromisoformat(TARGET_DATE),
        decision_at=DECISION_AT,
        truth_run_id="qmt-truth-run-1",
        truth_sha256="c" * 64,
        stock_set_sha256=expected_stock_set_contract(
            TARGET_DATE, codes
        )["stock_set_hash"],
        expected_codes=codes,
    )


def _capture(engine):
    targets = _targets(engine)
    rows = tuple(
        parse_eastmoney_turnover_response(
            target=target,
            raw_payload=_raw_payload(target.stock_code),
            provider_http_date=HTTP_DATE,
            captured_at=CAPTURED_AT,
            decision_at=DECISION_AT,
        )
        for target in targets
    )
    return build_capture_run(
        targets=targets,
        rows=rows,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        collector_build_sha=BUILD_SHA,
        collector_binary_sha256=BINARY_SHA,
        authority=_authority(),
        request_started_at=datetime(2026, 8, 27, 18, 39),
        run_id="1" * 32,
    )


def _clone_completed_turnover_run(engine, *, value_root: str | None = None) -> None:
    run_columns = tuple(TURNOVER_SNAPSHOT_SCHEMA[TURNOVER_SNAPSHOT_RUN_TABLE].columns)
    row_columns = tuple(TURNOVER_SNAPSHOT_SCHEMA[TURNOVER_SNAPSHOT_ROW_TABLE].columns)
    with engine.begin() as connection:
        persisted_run = dict(connection.execute(text(
            f"SELECT * FROM {TURNOVER_SNAPSHOT_RUN_TABLE} "
            "WHERE run_id='11111111111111111111111111111111'"
        )).mappings().one())
        persisted_run["run_id"] = "2" * 32
        persisted_run["decision_at"] = "2026-08-27 18:49:00.000000"
        if value_root is not None:
            persisted_run["field_value_root_sha256"] = value_root
        connection.execute(text(
            f"INSERT INTO {TURNOVER_SNAPSHOT_RUN_TABLE} ("
            + ",".join(f'`{column}`' for column in run_columns)
            + ") VALUES ("
            + ",".join(f":{column}" for column in run_columns)
            + ")"
        ), persisted_run)
        persisted_rows = connection.execute(text(
            f"SELECT * FROM {TURNOVER_SNAPSHOT_ROW_TABLE} "
            "WHERE run_id='11111111111111111111111111111111'"
        )).mappings().all()
        for raw in persisted_rows:
            cloned = dict(raw)
            cloned["run_id"] = "2" * 32
            connection.execute(text(
                f"INSERT INTO {TURNOVER_SNAPSHOT_ROW_TABLE} ("
                + ",".join(f'`{column}`' for column in row_columns)
                + ") VALUES ("
                + ",".join(f":{column}" for column in row_columns)
                + ")"
            ), cloned)
def test_parse_f61_retains_raw_payload_and_matches_qmt_fingerprint() -> None:
    engine = _engine()
    target = _targets(engine)[0]

    row = parse_eastmoney_turnover_response(
        target=target,
        raw_payload=_raw_payload(target.stock_code),
        provider_http_date=HTTP_DATE,
        captured_at=CAPTURED_AT,
        decision_at=DECISION_AT,
    )

    assert str(row.turnover_percent) == "0.45"
    assert row.source_volume_shares == target.volume_shares
    assert row.raw_payload == _raw_payload(target.stock_code)
    assert len(row.raw_payload_sha256) == 64
    assert len(row.snapshot_row_sha256) == 64


def test_completed_turnover_authority_survives_next_session_projection(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "server.common.turnover_snapshot.load_turnover_universe_authority",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("current QMT target rows/attestations are incomplete")
        ),
    )

    result = _revalidate_replayable_turnover_authority(
        object(),
        target_date=date.fromisoformat(TARGET_DATE),
        decision_at=DECISION_AT,
    )

    assert result is None


def test_completed_turnover_replay_ignores_replaced_row_identity() -> None:
    engine = _engine()
    target = _targets(engine)[0]
    capture = parse_eastmoney_turnover_response(
        target=target,
        raw_payload=_raw_payload(target.stock_code),
        provider_http_date=HTTP_DATE,
        captured_at=CAPTURED_AT,
        decision_at=DECISION_AT,
    )
    refreshed = {
        "id": target.target_row_id + 10,
        "stock_code": target.stock_code,
        "trade_date": target.trade_date,
        "k_type": target.k_type,
        "adjust_type": target.adjust_type,
        "open": target.open,
        "high": target.high,
        "low": target.low,
        "close": target.close,
        "volume": target.volume_shares,
        "amount": target.amount,
        "turnover_ratio": None,
        "received_at": DECISION_AT,
        "data_source": target.data_source,
        "batch_id": "next-session-refresh",
        "data_version": "next-session-version",
        "quality_status": target.quality_status,
        "permission_status": target.permission_status,
    }

    assert _historical_qmt_row_matches_capture(refreshed, capture) is True
    assert _historical_qmt_row_matches_capture(
        {**refreshed, "close": target.close + Decimal("0.01")},
        capture,
    ) is False


def test_turnover_completed_replays_converge_only_on_exact_value_roots() -> None:
    engine = _engine()
    run = _capture(engine)
    publish_turnover_snapshot(
        engine, run, min_expected_count=2,
        published_at=datetime(2026, 8, 27, 18, 45),
    )
    _clone_completed_turnover_run(engine)
    evidence = load_verified_turnover_evidence(
        engine, target_date=TARGET_DATE, decision_at=DECISION_AT,
        min_expected_count=2,
    )
    assert set(evidence) == {"000001", "600000"}

    engine = _engine()
    run = _capture(engine)
    publish_turnover_snapshot(
        engine, run, min_expected_count=2,
        published_at=datetime(2026, 8, 27, 18, 45),
    )
    _clone_completed_turnover_run(engine, value_root="f" * 64)
    with pytest.raises(TurnoverSnapshotBlocked, match="replays disagree"):
        load_verified_turnover_evidence(
            engine, target_date=TARGET_DATE, decision_at=DECISION_AT,
            min_expected_count=2,
        )


def test_pinned_curl_transport_keeps_tls_verification_and_exact_raw_body(
    monkeypatch,
) -> None:
    engine = _engine()
    target = _targets(engine)[0]
    raw = _raw_payload(target.stock_code)
    observed: dict[str, object] = {}

    class Completed:
        returncode = 0
        stdout = (
            b"HTTP/1.1 200 OK\r\n"
            + f"Date: {HTTP_DATE}\r\n".encode("ascii")
            + b"Content-Type: application/json\r\n\r\n"
            + raw
        )
        stderr = b""

    def fake_run(command, **kwargs):
        observed["command"] = list(command)
        observed["kwargs"] = dict(kwargs)
        return Completed()

    monkeypatch.setattr(
        "server.common.turnover_snapshot.subprocess.run",
        fake_run,
    )
    collector = PinnedCurlEastmoneyTurnoverCollector(
        curl_binary="curl",
        now=lambda: CAPTURED_AT,
    )
    row = collector.fetch(target, decision_at=DECISION_AT)

    command = observed["command"]
    assert "--resolve" in command
    assert "push2his.eastmoney.com:443:61.129.129.48" in command
    assert "--insecure" not in command
    assert "-k" not in command
    assert "--location" not in command
    assert row.raw_payload == raw


def test_full_market_collector_retries_only_transport_failures() -> None:
    engine = _engine()
    targets = _targets(engine)
    captured = {
        target.stock_code: parse_eastmoney_turnover_response(
            target=target,
            raw_payload=_raw_payload(target.stock_code),
            provider_http_date=HTTP_DATE,
            captured_at=CAPTURED_AT,
            decision_at=DECISION_AT,
        )
        for target in targets
    }
    calls: dict[str, int] = {}
    sleeps: list[float] = []

    class FlakyTransport:
        transport_contract = "HTTPS_TLS_VERIFIED_PINNED_RESOLVE_V1"
        resolved_endpoint = "push2his.eastmoney.com:443:61.129.129.48"
        now = staticmethod(lambda: datetime(2026, 8, 27, 18, 39))

        def fetch(self, target, *, decision_at):
            calls[target.stock_code] = calls.get(target.stock_code, 0) + 1
            if target.stock_code == "000001" and calls[target.stock_code] < 3:
                raise TurnoverTransportError("DATA_BLOCKED: transient curl failure")
            return captured[target.stock_code]

    run = collect_turnover_snapshot(
        targets=targets,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        collector_build_sha=BUILD_SHA,
        collector_binary_sha256=BINARY_SHA,
        authority=_authority(),
        collector=FlakyTransport(),
        delay_seconds=0,
        batch_pause_seconds=0,
        transport_attempts=3,
        transport_backoff_seconds=0.5,
        sleep=sleeps.append,
    )

    assert len(run.rows) == 2
    assert calls == {"000001": 3, "600000": 1}
    assert sleeps == [0.5, 1.0]

    class SemanticDrift(FlakyTransport):
        def fetch(self, target, *, decision_at):
            calls[target.stock_code] = calls.get(target.stock_code, 0) + 1
            raise TurnoverSnapshotBlocked("DATA_BLOCKED: OHLCV differs")

    calls.clear()
    with pytest.raises(TurnoverSnapshotBlocked, match="OHLCV differs"):
        collect_turnover_snapshot(
            targets=targets,
            target_date=TARGET_DATE,
            decision_at=DECISION_AT,
            collector_build_sha=BUILD_SHA,
            collector_binary_sha256=BINARY_SHA,
            authority=_authority(),
            collector=SemanticDrift(),
            delay_seconds=0,
            batch_pause_seconds=0,
            transport_attempts=3,
            sleep=lambda _seconds: None,
        )
    assert calls == {"000001": 1}


def test_full_market_collector_parallelizes_without_reordering_frozen_rows() -> None:
    engine = _engine()
    targets = _targets(engine)
    captured = {
        target.stock_code: parse_eastmoney_turnover_response(
            target=target,
            raw_payload=_raw_payload(target.stock_code),
            provider_http_date=HTTP_DATE,
            captured_at=CAPTURED_AT,
            decision_at=DECISION_AT,
        )
        for target in targets
    }
    rendezvous = Barrier(2, timeout=2)
    lock = Lock()
    active = 0
    peak = 0

    class ParallelTransport:
        transport_contract = "HTTPS_TLS_VERIFIED_PINNED_RESOLVE_V1"
        resolved_endpoint = "push2his.eastmoney.com:443:61.129.129.48"
        now = staticmethod(lambda: datetime(2026, 8, 27, 18, 39))

        def fetch(self, target, *, decision_at):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            rendezvous.wait()
            with lock:
                active -= 1
            return captured[target.stock_code]

    run = collect_turnover_snapshot(
        targets=targets,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        collector_build_sha=BUILD_SHA,
        collector_binary_sha256=BINARY_SHA,
        authority=_authority(),
        collector=ParallelTransport(),
        workers=2,
        delay_seconds=0,
        batch_pause_seconds=0,
    )

    assert peak == 2
    assert [row.target.stock_code for row in run.rows] == [
        row.stock_code for row in targets
    ]


@pytest.mark.parametrize(
    "mutation",
    (
        lambda raw: raw.replace(b"10.2,10.5", b"10.3,10.5"),
        lambda raw: raw.replace(b",0.45\"]", b",-\"]"),
        lambda raw: raw.replace(TARGET_DATE.encode(), b"2026-08-20"),
    ),
)
def test_parse_f61_fails_closed_on_ohlcv_schema_or_date_drift(mutation) -> None:
    engine = _engine()
    target = _targets(engine)[0]

    with pytest.raises(TurnoverSnapshotBlocked, match="DATA_BLOCKED"):
        parse_eastmoney_turnover_response(
            target=target,
            raw_payload=mutation(_raw_payload(target.stock_code)),
            provider_http_date=HTTP_DATE,
            captured_at=CAPTURED_AT,
            decision_at=DECISION_AT,
        )


def test_capture_requires_exact_frozen_universe_and_recomputes_hashes() -> None:
    engine = _engine()
    run = _capture(engine)

    with pytest.raises(TurnoverSnapshotBlocked, match="full-universe coverage"):
        build_capture_run(
            targets=_targets(engine),
            rows=run.rows[:1],
            target_date=TARGET_DATE,
            decision_at=DECISION_AT,
            collector_build_sha=BUILD_SHA,
            collector_binary_sha256=BINARY_SHA,
            authority=_authority(),
            request_started_at=datetime(2026, 8, 27, 18, 39),
        )

    tampered = replace(run.rows[0], raw_payload=run.rows[0].raw_payload + b" ")
    with pytest.raises(TurnoverSnapshotBlocked, match="raw payload changed"):
        build_capture_run(
            targets=_targets(engine),
            rows=(tampered, run.rows[1]),
            target_date=TARGET_DATE,
            decision_at=DECISION_AT,
            collector_build_sha=BUILD_SHA,
            collector_binary_sha256=BINARY_SHA,
            authority=_authority(),
            request_started_at=datetime(2026, 8, 27, 18, 39),
        )


def test_turnover_universe_must_match_catalog_bound_daily_truth() -> None:
    engine = _engine()
    incomplete_codes = ("000001",)
    incomplete_authority = replace(
        _authority(),
        expected_codes=incomplete_codes,
        stock_set_sha256=expected_stock_set_contract(
            TARGET_DATE, incomplete_codes
        )["stock_set_hash"],
    )

    with engine.connect() as connection, pytest.raises(
        TurnoverSnapshotBlocked, match="universe authority differs"
    ):
        freeze_qmt_turnover_targets(
            connection,
            target_date=TARGET_DATE,
            decision_at=DECISION_AT,
            authority=incomplete_authority,
            min_expected_count=2,
        )

def test_atomic_null_only_promotion_and_verified_proof_round_trip() -> None:
    engine = _engine()
    run = _capture(engine)

    receipt = publish_turnover_snapshot(
        engine,
        run,
        min_expected_count=2,
        published_at=datetime(2026, 8, 27, 18, 45),
    )

    assert receipt["schema"] == TURNOVER_SNAPSHOT_VERSION
    assert receipt["expected_count"] == receipt["promoted_count"] == 2
    with engine.connect() as connection:
        values = connection.execute(text(
            "SELECT stock_code,turnover_ratio FROM sm_stock_kline ORDER BY stock_code"
        )).fetchall()
        assert [(row[0], float(row[1])) for row in values] == [
            ("000001", 0.45),
            ("600000", 0.15),
        ]
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {TURNOVER_SNAPSHOT_RUN_TABLE}"
        )).scalar() == 1
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {TURNOVER_SNAPSHOT_ROW_TABLE}"
        )).scalar() == 2

    evidence = load_verified_turnover_evidence(
        engine,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        min_expected_count=2,
    )
    assert set(evidence) == {"000001", "600000"}
    proof = validate_turnover_evidence(evidence["000001"]["turnover_evidence_json"])
    assert proof["status"] == "PASS"
    assert proof["unit"] == "PERCENT"
    assert proof["snapshot_run_id"] == run.run_id
    assert proof["snapshot_semantic_sha256"] == run.semantic_sha256
    assert proof["collector_build_sha"] == BUILD_SHA
    assert proof["collector_binary_sha256"] == BINARY_SHA
    assert proof["authority_proof_identity"] == _authority().truth_run_id


def test_completed_turnover_publish_and_process_retry_are_idempotent() -> None:
    engine = _engine()
    run = _capture(engine)
    first = publish_turnover_snapshot(
        engine,
        run,
        min_expected_count=2,
        published_at=datetime(2026, 8, 27, 18, 45),
    )

    same_process = publish_turnover_snapshot(
        engine,
        run,
        min_expected_count=2,
        published_at=datetime(2026, 8, 27, 18, 46),
    )
    restarted = recover_completed_turnover_receipt(
        engine,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        collector_build_sha=BUILD_SHA,
        collector_binary_sha256=BINARY_SHA,
        authority=_authority(),
        min_expected_count=2,
    )

    assert same_process == first
    assert restarted is not None
    assert restarted["run_id"] == first["run_id"]
    assert restarted["recovered"] is True
    assert restarted["decision_at"] == DECISION_AT.isoformat(timespec="seconds")
    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {TURNOVER_SNAPSHOT_RUN_TABLE}"
        )).scalar() == 1
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {TURNOVER_SNAPSHOT_ROW_TABLE}"
        )).scalar() == 2


def test_turnover_default_clock_is_explicit_shanghai_time(monkeypatch) -> None:
    engine = _engine()
    run = _capture(engine)
    monkeypatch.setattr(
        turnover_module,
        "_now_shanghai",
        lambda: datetime(2026, 8, 27, 18, 45),
    )

    receipt = publish_turnover_snapshot(engine, run, min_expected_count=2)

    assert receipt["status"] == "COMPLETED"


def test_turnover_build_identity_rejects_dirty_checkout(monkeypatch) -> None:
    monkeypatch.setattr(turnover_command, "_git_head", lambda: BUILD_SHA)
    monkeypatch.setattr(
        turnover_command, "_git_status_porcelain", lambda: " M collector.py"
    )
    monkeypatch.delenv("PROBIGA_SCHEDULER_BUILD_SHA", raising=False)
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)

    with pytest.raises(RuntimeError, match="checkout is dirty"):
        turnover_command.resolve_build_sha(BUILD_SHA)

    monkeypatch.setattr(turnover_command, "_git_status_porcelain", lambda: "")
    assert turnover_command.resolve_build_sha(BUILD_SHA) == BUILD_SHA
    assert len(turnover_command.collector_bundle_sha256()) == 64


def test_turnover_target_uses_source_specific_postclose_readiness(
    monkeypatch,
) -> None:
    engine = _engine()
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE si_trade_calendar ("
            "trade_date TEXT PRIMARY KEY, trade_status INTEGER NOT NULL)"
        ))
        connection.execute(text(
            "INSERT INTO si_trade_calendar VALUES (:target, 1)"
        ), {"target": TARGET_DATE})
    observed = {}

    def closed(_engine, *, now, close_ready_time):
        observed["now"] = now
        observed["close_ready_time"] = close_ready_time
        return TARGET_DATE

    monkeypatch.setattr(
        turnover_command, "authoritative_closed_trade_date", closed
    )
    turnover_command._require_open_closed_target(
        engine,
        TARGET_DATE,
        now=datetime(2026, 8, 21, 15, 50),
    )

    assert observed["close_ready_time"] == turnover_command.time(15, 30)


def test_turnover_cli_after_cutoff_only_recovers_and_never_collects(
    monkeypatch, capsys,
) -> None:
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = object()
    authority = _authority()
    recovered = {
        "schema": TURNOVER_SNAPSHOT_VERSION,
        "status": "COMPLETED",
        "run_id": "f" * 32,
        "target_date": TARGET_DATE,
        "decision_at": datetime(2026, 8, 21, 23, 55).isoformat(
            timespec="seconds"
        ),
        "expected_count": 5205,
        "promoted_count": 5205,
        "collector_build_sha": BUILD_SHA,
        "collector_binary_sha256": BINARY_SHA,
        "recovered": True,
    }
    monkeypatch.setattr(turnover_command, "load_project_env", lambda: None)
    monkeypatch.setattr(turnover_command, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        turnover_command,
        "validate_market_field_capture_runtime",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        turnover_command, "_require_open_closed_target", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        turnover_command, "resolve_build_sha", lambda _value: BUILD_SHA
    )
    monkeypatch.setattr(
        turnover_command, "collector_bundle_sha256", lambda: BINARY_SHA
    )
    monkeypatch.setattr(
        turnover_command,
        "load_turnover_universe_authority",
        lambda *_a, **_k: authority,
    )
    recovery = MagicMock(return_value=recovered)
    monkeypatch.setattr(
        turnover_command, "recover_completed_turnover_receipt", recovery
    )
    frozen = MagicMock(side_effect=AssertionError("must not freeze after cutoff"))
    monkeypatch.setattr(turnover_command, "freeze_qmt_turnover_targets", frozen)

    argv = [
        "--target-date", TARGET_DATE,
        "--decision-at", "2026-08-21T23:55:00",
    ]
    assert turnover_command.main(argv) == 0
    assert '"recovered":true' in capsys.readouterr().out
    frozen.assert_not_called()

    recovery.return_value = None
    with pytest.raises(RuntimeError, match="cutoff has elapsed"):
        turnover_command.main(argv)
    frozen.assert_not_called()


def test_pool_receipt_hash_binds_generic_field_capture_root() -> None:
    engine = _engine()
    run = _capture(engine)
    publish_turnover_snapshot(
        engine,
        run,
        min_expected_count=2,
        published_at=datetime(2026, 8, 27, 18, 45),
    )
    evidence = load_verified_turnover_evidence(
        engine,
        target_date=TARGET_DATE,
        decision_at=DECISION_AT,
        min_expected_count=2,
    )["000001"]["turnover_evidence_json"]
    proof = validate_turnover_evidence(evidence)
    changed = dict(proof)
    changed.pop("proof_sha256")
    changed["snapshot_semantic_sha256"] = "f" * 64
    changed_evidence = build_turnover_evidence(changed)
    recommendation = {
        "stock_code": "000001",
        "pick_date": TARGET_DATE,
        "turnover_evidence_json": evidence,
        "upper_limit_evidence_json": build_upper_limit_evidence({
            "status": "DATA_BLOCKED",
            "stock_code": "000001",
            "trade_date": TARGET_DATE,
            "decision_known_at": "2026-08-27 18:50:00",
            "source_table": "st_market_field_capture_row",
            "reason": "DATA_BLOCKED: upper-limit evidence unavailable",
        }),
        "chase_risk_status": "DATA_BLOCKED",
        "candidate_recommend_status": "BLOCK",
        "candidate_ordinary_buy_eligible": 0,
        "recommend_status": "PENDING",
        "ordinary_buy_eligible": 0,
        "publication_status": "PENDING",
    }
    first = build_pool_manifest(
        trade_date=TARGET_DATE,
        analysis_rows=[{"stock_code": "000001", "analysis_date": TARGET_DATE}],
        recommendation_rows=[recommendation],
    )
    second = build_pool_manifest(
        trade_date=TARGET_DATE,
        analysis_rows=[{"stock_code": "000001", "analysis_date": TARGET_DATE}],
        recommendation_rows=[
            {**recommendation, "turnover_evidence_json": changed_evidence}
        ],
    )

    assert first["canonical_pool_sha256"] != second["canonical_pool_sha256"]


def test_qmt_drift_before_publish_rolls_back_run_rows_and_promotion() -> None:
    engine = _engine()
    run = _capture(engine)
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE sm_stock_kline SET close=10.3 WHERE stock_code='000001'"
        ))

    with pytest.raises(TurnoverSnapshotBlocked, match="QMT target universe changed"):
        publish_turnover_snapshot(
            engine,
            run,
            min_expected_count=2,
            published_at=datetime(2026, 8, 27, 18, 45),
        )

    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {TURNOVER_SNAPSHOT_RUN_TABLE}"
        )).scalar() == 0
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {TURNOVER_SNAPSHOT_ROW_TABLE}"
        )).scalar() == 0
        assert connection.execute(text(
            "SELECT COUNT(*) FROM sm_stock_kline WHERE turnover_ratio IS NOT NULL"
        )).scalar() == 0


def test_nonnull_race_is_blocked_without_overwrite_or_stage_commit() -> None:
    engine = _engine()
    run = _capture(engine)
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE sm_stock_kline SET turnover_ratio=9.99 WHERE stock_code='000001'"
        ))

    with pytest.raises(TurnoverSnapshotBlocked, match="already populated"):
        publish_turnover_snapshot(
            engine,
            run,
            min_expected_count=2,
            published_at=datetime(2026, 8, 27, 18, 45),
        )

    with engine.connect() as connection:
        assert float(connection.execute(text(
            "SELECT turnover_ratio FROM sm_stock_kline WHERE stock_code='000001'"
        )).scalar()) == 9.99
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {TURNOVER_SNAPSHOT_RUN_TABLE}"
        )).scalar() == 0


def test_persisted_raw_payload_tamper_is_rejected() -> None:
    engine = _engine()
    run = _capture(engine)
    publish_turnover_snapshot(
        engine,
        run,
        min_expected_count=2,
        published_at=datetime(2026, 8, 27, 18, 45),
    )
    with engine.begin() as connection:
        connection.execute(text(
            f"UPDATE {TURNOVER_SNAPSHOT_ROW_TABLE} SET raw_payload=:payload "
            "WHERE stock_code='000001'"
        ), {"payload": b"{}"})

    with pytest.raises(TurnoverSnapshotBlocked, match="raw payload|JSON|response status"):
        load_verified_turnover_evidence(
            engine,
            target_date=TARGET_DATE,
            decision_at=DECISION_AT,
            min_expected_count=2,
        )


def test_turnover_schema_is_privileged_and_collector_has_no_runtime_ddl() -> None:
    assert "market_field_capture" in [name for name, _ in bundle._MIGRATIONS]
    assert "market_field_capture" in [name for name, _ in bundle._VALIDATORS]
    schema_source = (
        ROOT / "server/common/turnover_snapshot_schema.py"
    ).read_text(encoding="utf-8")
    collector_source = (
        ROOT / "tools/sync_target_turnover_snapshot.py"
    ).read_text(encoding="utf-8").upper()
    assert "BEFORE UPDATE" in schema_source
    assert "BEFORE DELETE" in schema_source
    assert TURNOVER_SNAPSHOT_RUN_TABLE == "st_market_field_capture_run"
    assert TURNOVER_SNAPSHOT_ROW_TABLE == "st_market_field_capture_row"
    row_contract = TURNOVER_SNAPSHOT_SCHEMA[TURNOVER_SNAPSHOT_ROW_TABLE]
    assert row_contract.columns["field_value_decimal"].nullable is False
    assert row_contract.columns["source_open"].nullable is True
    run_contract = TURNOVER_SNAPSHOT_SCHEMA[TURNOVER_SNAPSHOT_RUN_TABLE]
    assert {
        "capture_kind",
        "window_start_date",
        "window_end_date",
        "transport_contract",
        "expected_keyset_sha256",
        "target_fingerprint_root_sha256",
    } <= set(run_contract.columns)
    assert "CREATE TABLE" not in collector_source
    assert "ALTER TABLE" not in collector_source


def test_eastmoney_secid_keeps_beijing_920_symbols_on_market_zero() -> None:
    assert turnover_module._eastmoney_secid("600000") == "1.600000"
    assert turnover_module._eastmoney_secid("000001") == "0.000001"
    assert turnover_module._eastmoney_secid("920000") == "0.920000"
