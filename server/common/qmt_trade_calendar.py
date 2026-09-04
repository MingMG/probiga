"""Append-only QMT trade-calendar receipts and immutable session roots."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.legacy_table_surface import validate_required_table_surface
from server.common.qmt_attestation_contract import canonical_digest


CALENDAR_MANIFEST_SCHEMA = "probiga.qmt-trade-calendar.v1"
AUTHORITATIVE_CALENDAR_MANIFEST_SCHEMA = "probiga.trade-calendar.v2"
CALENDAR_SESSION_SET_SCHEMA = "probiga.qmt-trade-calendar-sessions.v1"
CALENDAR_SOURCE_PAYLOAD_SCHEMA = "probiga.qmt-trade-calendar-source-payload.v1"
AUTHORITATIVE_CALENDAR_SOURCE_PAYLOAD_SCHEMA = (
    "probiga.trade-calendar-source-payload.v2"
)
CALENDAR_STATUS_COMPLETE = "COMPLETE"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRODUCTION_TIMEZONE = ZoneInfo("Asia/Shanghai")
_IMMUTABILITY_TRIGGERS = {
    "trg_qmt_calendar_batch_no_update": (
        "qmt_trade_calendar_batch", "UPDATE",
    ),
    "trg_qmt_calendar_batch_no_delete": (
        "qmt_trade_calendar_batch", "DELETE",
    ),
    "trg_qmt_calendar_session_no_update": (
        "qmt_trade_calendar_session", "UPDATE",
    ),
    "trg_qmt_calendar_session_no_delete": (
        "qmt_trade_calendar_session", "DELETE",
    ),
}
_MANIFEST_KEYS = frozenset({
    "schema",
    "batch_id",
    "source_batch_id",
    "known_at",
    "start_date",
    "end_date",
    "session_count",
    "session_set_hash",
})
_AUTHORITATIVE_MANIFEST_KEYS = _MANIFEST_KEYS | {"source_provider"}
TRADE_CALENDAR_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "qmt_trade_calendar_batch": frozenset({
        "batch_id",
        "source_batch_id",
        "known_at",
        "start_date",
        "end_date",
        "status",
        "session_count",
        "session_set_hash",
        "manifest_json",
        "manifest_hash",
        "created_at",
    }),
    "qmt_trade_calendar_session": frozenset({
        "batch_id",
        "trade_date",
        "created_at",
    }),
}


@dataclass(frozen=True)
class QmtTradeCalendarReceipt:
    batch_id: str
    source_batch_id: str
    known_at: str
    start_date: str
    end_date: str
    session_count: int
    session_set_hash: str
    manifest_hash: str
    sessions: tuple[str, ...]
    source_provider: str = "GUOJIN_QMT"

    def sessions_between(self, start_date: str, end_date: str) -> list[str]:
        start = _iso_date(start_date)
        end = _iso_date(end_date)
        if start > end or start < self.start_date or end > self.end_date:
            raise ValueError("QMT calendar receipt does not cover target range")
        return [day for day in self.sessions if start <= day <= end]


def _iso_date(value: Any) -> str:
    raw = value.isoformat() if isinstance(value, date) else str(value or "")
    raw = raw[:10]
    try:
        normalized = datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError("QMT calendar date is invalid") from exc
    if raw != normalized:
        raise ValueError("QMT calendar date is invalid")
    return raw


def _known_at(value: Any) -> str:
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is not None:
            normalized = normalized.astimezone(_PRODUCTION_TIMEZONE).replace(
                tzinfo=None
            )
        return normalized.replace(microsecond=0).isoformat(sep=" ")
    raw = str(value or "").strip()[:19]
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").isoformat(sep=" ")
    except ValueError as exc:
        raise ValueError("QMT calendar known_at is invalid") from exc


def canonical_sessions(sessions: Iterable[Any]) -> list[str]:
    result = sorted({_iso_date(value) for value in sessions})
    if not result:
        raise ValueError("QMT calendar session list is empty")
    return result


def calendar_session_set_hash(
    *, start_date: str, end_date: str, sessions: Iterable[Any],
) -> str:
    normalized = canonical_sessions(sessions)
    return canonical_digest({
        "schema": CALENDAR_SESSION_SET_SCHEMA,
        "start_date": _iso_date(start_date),
        "end_date": _iso_date(end_date),
        "sessions": normalized,
    })


def calendar_source_batch_id(
    *,
    start_date: str,
    end_date: str,
    sessions: Iterable[Any],
    source_provider: str = "GUOJIN_QMT",
) -> str:
    """Return the independently reproducible root of the native QMT payload.

    ``source_batch_id`` is deliberately content-addressed.  A caller cannot
    self-report an unrelated identifier and still produce a valid receipt.
    """

    normalized = canonical_sessions(sessions)
    provider = str(source_provider or "").strip().upper()
    if not provider or len(provider) > 64:
        raise ValueError("trade calendar source_provider is invalid")
    return canonical_digest({
        "schema": (
            CALENDAR_SOURCE_PAYLOAD_SCHEMA
            if provider == "GUOJIN_QMT"
            else AUTHORITATIVE_CALENDAR_SOURCE_PAYLOAD_SCHEMA
        ),
        "provider": provider,
        "method": "get_trading_calendar",
        "start_date": _iso_date(start_date),
        "end_date": _iso_date(end_date),
        "sessions": normalized,
    })


def build_calendar_manifest(
    *,
    batch_id: str,
    source_batch_id: str,
    known_at: Any,
    start_date: str,
    end_date: str,
    sessions: Iterable[Any],
    source_provider: str = "GUOJIN_QMT",
) -> tuple[dict[str, Any], list[str]]:
    normalized_batch = str(batch_id or "").strip()
    normalized_source = str(source_batch_id or "").strip()
    if not normalized_batch or len(normalized_batch) > 64:
        raise ValueError("QMT calendar batch_id is invalid")
    if not _SHA256_RE.fullmatch(normalized_source):
        raise ValueError("QMT calendar source_batch_id is invalid")
    start = _iso_date(start_date)
    end = _iso_date(end_date)
    if start > end:
        raise ValueError("QMT calendar range is invalid")
    normalized_sessions = canonical_sessions(sessions)
    if normalized_sessions[0] < start or normalized_sessions[-1] > end:
        raise ValueError("QMT calendar session is outside source range")
    provider = str(source_provider or "").strip().upper()
    if not provider or len(provider) > 64:
        raise ValueError("trade calendar source_provider is invalid")
    if normalized_source != calendar_source_batch_id(
        start_date=start,
        end_date=end,
        sessions=normalized_sessions,
        source_provider=provider,
    ):
        raise ValueError("QMT calendar source_batch_id is not payload-bound")
    manifest = {
        "schema": (
            CALENDAR_MANIFEST_SCHEMA
            if provider == "GUOJIN_QMT"
            else AUTHORITATIVE_CALENDAR_MANIFEST_SCHEMA
        ),
        "batch_id": normalized_batch,
        "source_batch_id": normalized_source,
        "known_at": _known_at(known_at),
        "start_date": start,
        "end_date": end,
        "session_count": len(normalized_sessions),
        "session_set_hash": calendar_session_set_hash(
            start_date=start, end_date=end, sessions=normalized_sessions
        ),
    }
    if provider != "GUOJIN_QMT":
        manifest["source_provider"] = provider
    return manifest, normalized_sessions


def validate_calendar_manifest(
    manifest_json: Any,
    *,
    row: Mapping[str, Any],
    sessions: Iterable[Any],
) -> QmtTradeCalendarReceipt:
    try:
        payload = (
            manifest_json
            if type(manifest_json) is dict
            else json.loads(str(manifest_json or ""))
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("QMT calendar manifest is not valid JSON") from exc
    if type(payload) is not dict or set(payload) not in {
        _MANIFEST_KEYS,
        _AUTHORITATIVE_MANIFEST_KEYS,
    }:
        raise ValueError("QMT calendar manifest fields differ")
    schema = str(payload.get("schema") or "")
    if schema == CALENDAR_MANIFEST_SCHEMA:
        source_provider = "GUOJIN_QMT"
    elif schema == AUTHORITATIVE_CALENDAR_MANIFEST_SCHEMA:
        source_provider = str(payload.get("source_provider") or "")
    else:
        raise ValueError("QMT calendar manifest schema differs")
    expected, normalized_sessions = build_calendar_manifest(
        batch_id=str(row.get("batch_id") or ""),
        source_batch_id=str(row.get("source_batch_id") or ""),
        known_at=row.get("known_at"),
        start_date=str(row.get("start_date") or "")[:10],
        end_date=str(row.get("end_date") or "")[:10],
        sessions=sessions,
        source_provider=source_provider,
    )
    if payload != expected:
        raise ValueError("QMT calendar manifest content differs")
    manifest_hash = str(row.get("manifest_hash") or "")
    if (
        str(row.get("status") or "") != CALENDAR_STATUS_COMPLETE
        or type(row.get("session_count")) is not int
        or int(row["session_count"]) != len(normalized_sessions)
        or str(row.get("session_set_hash") or "")
        != expected["session_set_hash"]
        or not _SHA256_RE.fullmatch(manifest_hash)
        or canonical_digest(payload) != manifest_hash
    ):
        raise ValueError("QMT calendar receipt counters/hash differ")
    return QmtTradeCalendarReceipt(
        batch_id=expected["batch_id"],
        source_batch_id=expected["source_batch_id"],
        known_at=expected["known_at"],
        start_date=expected["start_date"],
        end_date=expected["end_date"],
        session_count=expected["session_count"],
        session_set_hash=expected["session_set_hash"],
        manifest_hash=manifest_hash,
        sessions=tuple(normalized_sessions),
        source_provider=source_provider,
    )


def trade_calendar_schema_statements() -> tuple[str, ...]:
    return (
        """
        CREATE TABLE IF NOT EXISTS qmt_trade_calendar_batch (
            batch_id VARCHAR(64) NOT NULL PRIMARY KEY,
            source_batch_id VARCHAR(64) NOT NULL,
            known_at DATETIME NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            status VARCHAR(16) NOT NULL,
            session_count INT NOT NULL,
            session_set_hash CHAR(64) NOT NULL,
            manifest_json MEDIUMTEXT NOT NULL,
            manifest_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL,
            KEY idx_qmt_calendar_complete
                (status, start_date, end_date, known_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_trade_calendar_session (
            batch_id VARCHAR(64) NOT NULL,
            trade_date DATE NOT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (batch_id, trade_date),
            KEY idx_qmt_calendar_session_date (trade_date, batch_id),
            CONSTRAINT fk_qmt_calendar_session_batch
                FOREIGN KEY (batch_id) REFERENCES qmt_trade_calendar_batch(batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        ALTER TABLE qmt_trade_calendar_batch
        CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """,
        """
        ALTER TABLE qmt_trade_calendar_session
        CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_calendar_batch_no_update
        BEFORE UPDATE ON qmt_trade_calendar_batch
        FOR EACH ROW
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_trade_calendar_batch is append-only'
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_calendar_batch_no_delete
        BEFORE DELETE ON qmt_trade_calendar_batch
        FOR EACH ROW
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_trade_calendar_batch is append-only'
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_calendar_session_no_update
        BEFORE UPDATE ON qmt_trade_calendar_session
        FOR EACH ROW
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_trade_calendar_session is append-only'
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_calendar_session_no_delete
        BEFORE DELETE ON qmt_trade_calendar_session
        FOR EACH ROW
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_trade_calendar_session is append-only'
        """,
    )


def trade_calendar_table_ddl_statements() -> tuple[str, ...]:
    return trade_calendar_schema_statements()[:2]


def trade_calendar_migration_statements() -> tuple[str, ...]:
    return trade_calendar_schema_statements()[2:4]


def trade_calendar_trigger_ddl_statements() -> tuple[str, ...]:
    return trade_calendar_schema_statements()[4:]


def _normalized_trigger_body(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("`", "").lower()


def _expected_trigger_contracts() -> dict[str, tuple[str, str, str]]:
    """Derive the exact metadata contract from the privileged trigger DDL."""

    contracts: dict[str, tuple[str, str, str]] = {}
    pattern = re.compile(
        r"^CREATE TRIGGER(?: IF NOT EXISTS)?\s+`?([^`\s]+)`?\s+"
        r"BEFORE\s+(INSERT|UPDATE|DELETE)\s+ON\s+`?([^`\s]+)`?\s+"
        r"FOR EACH ROW\s+(.*)$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for statement in trade_calendar_trigger_ddl_statements():
        matched = pattern.match(statement.strip())
        if matched is None:
            raise RuntimeError("QMT calendar trigger DDL is unparsable")
        name, event, table_name, body = matched.groups()
        if name in contracts:
            raise RuntimeError("QMT calendar trigger DDL inventory differs")
        contracts[name] = (
            table_name.lower(),
            event.upper(),
            _normalized_trigger_body(body),
        )
    if set(contracts) != set(_IMMUTABILITY_TRIGGERS):
        raise RuntimeError("QMT calendar trigger DDL inventory differs")
    return contracts


def privileged_migrate_trade_calendar_schema(
    engine: Any,
    *,
    install_triggers: bool = True,
    trigger_ddl_executor: Any | None = None,
) -> dict[str, Any]:
    """Install the persistent calendar schema during a fenced release.

    Production may set ``install_triggers=False`` so its allow-listed trigger
    broker can install and attest the frozen trigger contract later in the
    same fenced cutover.
    """

    if type(install_triggers) is not bool:
        raise TypeError("install_triggers must be bool")
    if trigger_ddl_executor is not None and not callable(trigger_ddl_executor):
        raise TypeError("trigger_ddl_executor must be callable")

    with engine.begin() as connection:
        for statement in (
            *trade_calendar_table_ddl_statements(),
            *trade_calendar_migration_statements(),
        ):
            connection.execute(text(statement))
    if install_triggers:
        repair_statements = tuple(
            f"DROP TRIGGER IF EXISTS `{name}`"
            for name in _expected_trigger_contracts()
        )
        if trigger_ddl_executor is None:
            with engine.begin() as connection:
                for statement in repair_statements:
                    connection.execute(text(statement))
                for statement in trade_calendar_trigger_ddl_statements():
                    connection.execute(text(statement))
        else:
            for statement in repair_statements:
                trigger_ddl_executor(statement)
            for statement in trade_calendar_trigger_ddl_statements():
                trigger_ddl_executor(statement)
        with engine.connect() as connection:
            validate_trade_calendar_immutability(connection)
    return {
        "tables": tuple(sorted(TRADE_CALENDAR_REQUIRED_COLUMNS)),
        "trigger_names": tuple(sorted(_IMMUTABILITY_TRIGGERS)),
        "triggers_installed": install_triggers,
        "privileged_migration": True,
        "runtime_ddl_required": False,
    }


def validate_trade_calendar_runtime_schema(engine: Any) -> dict[str, Any]:
    """Validate the runtime surface and immutable guards without DDL."""

    surface = validate_required_table_surface(
        engine,
        TRADE_CALENDAR_REQUIRED_COLUMNS,
        context="QMT trade calendar",
        required_columns=TRADE_CALENDAR_REQUIRED_COLUMNS,
    )
    with engine.connect() as connection:
        validate_trade_calendar_immutability(connection)
    return {
        **surface,
        "trigger_names": tuple(sorted(_IMMUTABILITY_TRIGGERS)),
        "append_only_verified": True,
    }


def ensure_trade_calendar_tables(engine: Any) -> dict[str, Any]:
    """Backward-compatible, read-only runtime validation alias."""

    return validate_trade_calendar_runtime_schema(engine)


def _validate_trade_calendar_trigger_behavior(connection: Any) -> None:
    """Attest append-only guards when trigger metadata is not grant-visible."""

    sample = connection.execute(text("""
        SELECT b.`batch_id`, s.`trade_date`
        FROM `qmt_trade_calendar_batch` b
        JOIN `qmt_trade_calendar_session` s ON s.`batch_id`=b.`batch_id`
        WHERE b.`status`='COMPLETE'
        LIMIT 1
    """)).mappings().first()
    if sample is None:
        raise RuntimeError(
            "QMT calendar triggers are not visible and no completed immutable "
            "sample exists for behavioral attestation"
        )
    params = {
        "batch_id": sample["batch_id"],
        "trade_date": sample["trade_date"],
    }
    probes = (
        (
            "qmt_trade_calendar_batch is append-only",
            text(
                "UPDATE `qmt_trade_calendar_batch` SET `status`=`status` "
                "WHERE `batch_id`=:batch_id"
            ),
        ),
        (
            "qmt_trade_calendar_batch is append-only",
            text(
                "DELETE FROM `qmt_trade_calendar_batch` "
                "WHERE `batch_id`=:batch_id"
            ),
        ),
        (
            "qmt_trade_calendar_session is append-only",
            text(
                "UPDATE `qmt_trade_calendar_session` "
                "SET `trade_date`=`trade_date` "
                "WHERE `batch_id`=:batch_id AND `trade_date`=:trade_date"
            ),
        ),
        (
            "qmt_trade_calendar_session is append-only",
            text(
                "DELETE FROM `qmt_trade_calendar_session` "
                "WHERE `batch_id`=:batch_id AND `trade_date`=:trade_date"
            ),
        ),
    )
    for expected_message, statement in probes:
        savepoint = connection.begin_nested()
        observed_message = ""
        try:
            connection.execute(statement, params)
        except Exception as exc:  # MySQL SIGNAL is surfaced by the DBAPI wrapper.
            observed_message = str(getattr(exc, "orig", exc))
        finally:
            savepoint.rollback()
        if expected_message not in observed_message:
            raise RuntimeError("QMT calendar trigger behavioral contract differs")


def validate_trade_calendar_immutability(connection: Any) -> None:
    expected = _expected_trigger_contracts()
    rows = connection.execute(text("""
        SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING,
               ACTION_STATEMENT
        FROM information_schema.TRIGGERS
        WHERE TRIGGER_SCHEMA=DATABASE()
          AND TRIGGER_NAME IN (
              'trg_qmt_calendar_batch_no_update',
              'trg_qmt_calendar_batch_no_delete',
              'trg_qmt_calendar_session_no_update',
              'trg_qmt_calendar_session_no_delete'
          )
    """)).mappings().all()
    observed = {
        str(row.get("TRIGGER_NAME") or row.get("trigger_name") or ""): row
        for row in rows
    }
    if not observed:
        _validate_trade_calendar_trigger_behavior(connection)
        return
    if set(observed) != set(expected):
        raise RuntimeError("QMT calendar append-only triggers are incomplete")
    for name, (table_name, event, action_body) in expected.items():
        row = observed[name]
        actual_table = str(
            row.get("EVENT_OBJECT_TABLE") or row.get("event_object_table") or ""
        ).lower()
        normalized_event = str(
            row.get("EVENT_MANIPULATION") or row.get("event_manipulation") or ""
        ).upper()
        timing = str(
            row.get("ACTION_TIMING") or row.get("action_timing") or ""
        ).upper()
        if (
            actual_table != table_name
            or normalized_event != event
            or timing != "BEFORE"
            or _normalized_trigger_body(
                row.get("ACTION_STATEMENT")
                or row.get("action_statement")
                or ""
            )
            != action_body
        ):
            raise RuntimeError("QMT calendar append-only trigger differs")


def insert_trade_calendar_receipt(
    connection: Any,
    *,
    batch_id: str,
    source_batch_id: str,
    known_at: Any,
    start_date: str,
    end_date: str,
    sessions: Iterable[Any],
    source_provider: str = "GUOJIN_QMT",
) -> dict[str, Any]:
    manifest, normalized_sessions = build_calendar_manifest(
        batch_id=batch_id,
        source_batch_id=source_batch_id,
        known_at=known_at,
        start_date=start_date,
        end_date=end_date,
        sessions=sessions,
        source_provider=source_provider,
    )
    manifest_hash = canonical_digest(manifest)
    connection.execute(text("""
        INSERT INTO qmt_trade_calendar_batch
        (batch_id, source_batch_id, known_at, start_date, end_date, status,
         session_count, session_set_hash, manifest_json, manifest_hash,
         created_at)
        VALUES
        (:batch_id, :source_batch_id, :known_at, :start_date, :end_date,
         :status, :session_count, :session_set_hash, :manifest_json,
         :manifest_hash, NOW())
    """), {
        **manifest,
        "status": CALENDAR_STATUS_COMPLETE,
        "manifest_json": json.dumps(
            manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
        "manifest_hash": manifest_hash,
    })
    connection.execute(text("""
        INSERT INTO qmt_trade_calendar_session
        (batch_id, trade_date, created_at)
        VALUES (:batch_id, :trade_date, NOW())
    """), [
        {"batch_id": manifest["batch_id"], "trade_date": trade_date}
        for trade_date in normalized_sessions
    ])
    return {**manifest, "manifest_hash": manifest_hash}


def load_trade_calendar_receipt(
    connection: Any,
    *,
    start_date: str,
    end_date: str,
    decision_known_at: Any,
    batch_id: str | None = None,
) -> QmtTradeCalendarReceipt:
    start = _iso_date(start_date)
    end = _iso_date(end_date)
    decision_time = _known_at(decision_known_at)
    params: dict[str, Any] = {
        "start_date": start,
        "end_date": end,
        "decision_known_at": decision_time,
    }
    where = (
        "status='COMPLETE' AND start_date<=:start_date "
        "AND end_date>=:end_date AND known_at<=:decision_known_at"
    )
    order = "ORDER BY known_at DESC, batch_id DESC LIMIT 1"
    if batch_id is not None:
        where += " AND batch_id=:batch_id"
        params["batch_id"] = str(batch_id)
        order = ""
    row = connection.execute(text(f"""
        SELECT batch_id, source_batch_id, known_at, start_date, end_date,
               status, session_count, session_set_hash, manifest_json,
               manifest_hash
        FROM qmt_trade_calendar_batch
        WHERE {where}
        {order}
    """), params).mappings().one_or_none()
    if row is None:
        raise RuntimeError("no immutable QMT calendar receipt covers target range")
    session_rows = connection.execute(text("""
        SELECT trade_date
        FROM qmt_trade_calendar_session
        WHERE batch_id=:batch_id
        ORDER BY trade_date
    """), {"batch_id": str(row["batch_id"])}).fetchall()
    return validate_calendar_manifest(
        row["manifest_json"],
        row=dict(row),
        sessions=[item[0] for item in session_rows],
    )


__all__ = [
    "AUTHORITATIVE_CALENDAR_MANIFEST_SCHEMA",
    "AUTHORITATIVE_CALENDAR_SOURCE_PAYLOAD_SCHEMA",
    "CALENDAR_MANIFEST_SCHEMA",
    "CALENDAR_SESSION_SET_SCHEMA",
    "CALENDAR_SOURCE_PAYLOAD_SCHEMA",
    "CALENDAR_STATUS_COMPLETE",
    "QmtTradeCalendarReceipt",
    "build_calendar_manifest",
    "calendar_source_batch_id",
    "calendar_session_set_hash",
    "canonical_sessions",
    "ensure_trade_calendar_tables",
    "insert_trade_calendar_receipt",
    "load_trade_calendar_receipt",
    "privileged_migrate_trade_calendar_schema",
    "TRADE_CALENDAR_REQUIRED_COLUMNS",
    "validate_trade_calendar_immutability",
    "validate_trade_calendar_runtime_schema",
    "trade_calendar_table_ddl_statements",
    "trade_calendar_migration_statements",
    "trade_calendar_trigger_ddl_statements",
    "validate_calendar_manifest",
]
