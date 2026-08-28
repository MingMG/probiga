"""Independent, immutable QMT A-share catalog contracts.

The daily bar table is deliberately not an input to this module.  A catalog
batch is discovered from QMT's native A-share sectors, bound to QMT instrument
details, and then reused by daily/history writers and the attestation process.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text

from server.common.legacy_table_surface import validate_required_table_surface
from server.common.qmt_attestation_contract import canonical_digest


CATALOG_MANIFEST_SCHEMA = "probiga.qmt-stock-catalog.v1"
CATALOG_MEMBER_SET_SCHEMA = "probiga.qmt-stock-catalog-members.v1"
CATALOG_DISCOVERY_SCHEMA = "probiga.qmt-stock-catalog-discovery.v1"
CATALOG_STATUS_COMPLETE = "COMPLETE"
NATIVE_A_SHARE_SECTORS = ("上证A股", "深证A股", "京市A股", "沪深A股")

_A_SHARE_CODE_RE = re.compile(r"^(?:0|3|4|6|8|9)\d{5}$")
_QMT_CODE_RE = re.compile(r"^(?:0|3|4|6|8|9)\d{5}\.(?:SH|SZ|BJ)$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STOCK_INSTRUMENT_TYPES = frozenset({"stock", "equity", "股票"})
_IMMUTABILITY_TRIGGERS = {
    "trg_qmt_stock_catalog_batch_no_update": (
        "qmt_stock_catalog_batch", "UPDATE",
    ),
    "trg_qmt_stock_catalog_batch_no_delete": (
        "qmt_stock_catalog_batch", "DELETE",
    ),
    "trg_qmt_stock_catalog_member_no_update": (
        "qmt_stock_catalog_member", "UPDATE",
    ),
    "trg_qmt_stock_catalog_member_no_delete": (
        "qmt_stock_catalog_member", "DELETE",
    ),
}
_MANIFEST_KEYS = frozenset({
    "schema",
    "batch_id",
    "captured_at",
    "history_complete_from",
    "native_sectors",
    "discovery",
    "member_count",
    "member_set_hash",
})
STOCK_CATALOG_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "qmt_stock_catalog_batch": frozenset({
        "batch_id",
        "captured_at",
        "history_complete_from",
        "status",
        "member_count",
        "member_set_hash",
        "manifest_json",
        "manifest_hash",
        "created_at",
    }),
    "qmt_stock_catalog_member": frozenset({
        "batch_id",
        "qmt_code",
        "stock_code",
        "list_date",
        "expire_date",
        "instrument_batch_id",
        "instrument_type",
        "created_at",
    }),
}


def build_catalog_discovery(
    *,
    current_sectors: Iterable[str],
    expired_sectors: Iterable[str],
    sector_members: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    current = sorted({str(value or "").strip() for value in current_sectors
                      if str(value or "").strip()})
    expired = sorted({str(value or "").strip() for value in expired_sectors
                      if str(value or "").strip()})
    if not set(NATIVE_A_SHARE_SECTORS[:3]).issubset(current):
        raise ValueError("QMT native A-share discovery sectors are incomplete")
    allowed_sectors = set(current) | set(expired)
    members = sorted({
        (
            str(row.get("sector_name") or "").strip(),
            str(row.get("qmt_code") or "").strip().upper(),
        )
        for row in sector_members
    })
    if (
        not members
        or any(sector not in allowed_sectors or not _QMT_CODE_RE.fullmatch(code)
               for sector, code in members)
    ):
        raise ValueError("QMT catalog discovery member payload is invalid")
    unsigned = {
        "schema": CATALOG_DISCOVERY_SCHEMA,
        "current_sectors": current,
        "expired_sectors": expired,
        "sector_members": [
            {"sector_name": sector, "qmt_code": code}
            for sector, code in members
        ],
    }
    return {**unsigned, "payload_hash": canonical_digest(unsigned)}


def validate_catalog_discovery(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {
        "schema", "current_sectors", "expired_sectors", "sector_members",
        "payload_hash",
    }:
        raise ValueError("QMT catalog discovery fields differ")
    rebuilt = build_catalog_discovery(
        current_sectors=value.get("current_sectors") or (),
        expired_sectors=value.get("expired_sectors") or (),
        sector_members=value.get("sector_members") or (),
    )
    if value != rebuilt:
        raise ValueError("QMT catalog discovery payload/hash differs")
    return rebuilt


@dataclass(frozen=True)
class StockCatalogBatch:
    batch_id: str
    captured_at: str
    history_complete_from: str
    member_count: int
    member_set_hash: str
    manifest_hash: str
    native_sectors: tuple[str, ...]
    members: tuple[dict[str, Any], ...]

    def eligible_codes(self, target_date: str) -> list[str]:
        _require_iso_date(target_date)
        if target_date < self.history_complete_from:
            raise RuntimeError(
                "QMT stock catalog does not prove the historical target date"
            )
        return sorted({
            str(member["stock_code"])
            for member in self.members
            if str(member["list_date"]) <= target_date
            and (
                member.get("expire_date") in (None, "")
                or target_date <= str(member["expire_date"])
            )
        })


def _require_iso_date(value: Any) -> str:
    if type(value) is not str:
        raise ValueError("QMT stock catalog date must be an exact string")
    try:
        normalized = datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError("QMT stock catalog date is invalid") from exc
    if normalized != value:
        raise ValueError("QMT stock catalog date is invalid")
    return value


def _captured_at(value: Any) -> str:
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    raw = str(value or "").strip()[:19]
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError("QMT stock catalog captured_at is invalid") from exc
    return parsed.isoformat(sep=" ")


def canonical_catalog_members(
    members: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for raw in members:
        stock_code = str(raw.get("stock_code") or "").strip().zfill(6)
        qmt_code = str(raw.get("qmt_code") or "").strip().upper()
        list_date = str(raw.get("list_date") or "")[:10]
        raw_expire = raw.get("expire_date")
        expire_date = (
            str(raw_expire)[:10]
            if raw_expire not in (None, "", "NaT")
            else None
        )
        instrument_batch_id = str(
            raw.get("instrument_batch_id") or raw.get("batch_id") or ""
        ).strip()
        instrument_type_raw = str(raw.get("instrument_type") or "").strip()
        if instrument_type_raw.casefold() not in _STOCK_INSTRUMENT_TYPES:
            raise ValueError("QMT catalog member is not proven as equity")
        if not _A_SHARE_CODE_RE.fullmatch(stock_code):
            raise ValueError(f"invalid QMT A-share stock code: {stock_code!r}")
        if not _QMT_CODE_RE.fullmatch(qmt_code) or qmt_code[:6] != stock_code:
            raise ValueError(f"invalid QMT A-share instrument code: {qmt_code!r}")
        _require_iso_date(list_date)
        if expire_date is not None:
            _require_iso_date(expire_date)
            if expire_date < list_date:
                raise ValueError(
                    "QMT instrument expire_date cannot precede list_date"
                )
        if not instrument_batch_id:
            raise ValueError("QMT instrument detail batch proof is missing")
        member = {
            "qmt_code": qmt_code,
            "stock_code": stock_code,
            "list_date": list_date,
            "expire_date": expire_date,
            "instrument_batch_id": instrument_batch_id,
            "instrument_type": "STOCK",
        }
        previous = normalized.get(qmt_code)
        if previous is not None and previous != member:
            raise ValueError(f"conflicting QMT catalog member: {qmt_code}")
        normalized[qmt_code] = member
    result = sorted(normalized.values(), key=lambda row: row["qmt_code"])
    if not result:
        raise ValueError("QMT native A-share catalog is empty")
    stock_codes = [row["stock_code"] for row in result]
    if len(set(stock_codes)) != len(stock_codes):
        raise ValueError("QMT catalog maps one stock code more than once")
    return result


def catalog_member_set_hash(members: Sequence[Mapping[str, Any]]) -> str:
    return canonical_digest({
        "schema": CATALOG_MEMBER_SET_SCHEMA,
        "members": list(members),
    })


def build_catalog_manifest(
    *,
    batch_id: str,
    captured_at: Any,
    history_complete_from: str,
    members: Iterable[Mapping[str, Any]],
    discovery: Mapping[str, Any],
    native_sectors: Iterable[str] = NATIVE_A_SHARE_SECTORS,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    normalized_batch = str(batch_id or "").strip()
    if not normalized_batch or len(normalized_batch) > 64:
        raise ValueError("QMT catalog batch_id is invalid")
    normalized_members = canonical_catalog_members(members)
    normalized_discovery = validate_catalog_discovery(dict(discovery))
    if {
        row["qmt_code"] for row in normalized_discovery["sector_members"]
    } != {row["qmt_code"] for row in normalized_members}:
        raise ValueError("QMT discovery and instrument-detail sets differ")
    sectors = tuple(dict.fromkeys(
        str(value or "").strip() for value in native_sectors
        if str(value or "").strip()
    ))
    if not sectors or not set(NATIVE_A_SHARE_SECTORS[:3]).issubset(sectors):
        raise ValueError("QMT native A-share sector proof is incomplete")
    normalized_captured_at = _captured_at(captured_at)
    normalized_history_from = _require_iso_date(history_complete_from)
    if normalized_history_from > normalized_captured_at[:10]:
        raise ValueError("QMT catalog history boundary follows capture time")
    manifest = {
        "schema": CATALOG_MANIFEST_SCHEMA,
        "batch_id": normalized_batch,
        "captured_at": normalized_captured_at,
        "history_complete_from": normalized_history_from,
        "native_sectors": list(sectors),
        "discovery": normalized_discovery,
        "member_count": len(normalized_members),
        "member_set_hash": catalog_member_set_hash(normalized_members),
    }
    return manifest, normalized_members


def validate_catalog_manifest(
    manifest_json: Any,
    *,
    row: Mapping[str, Any],
    members: Iterable[Mapping[str, Any]],
) -> StockCatalogBatch:
    try:
        payload = (
            manifest_json
            if type(manifest_json) is dict
            else json.loads(str(manifest_json or ""))
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("QMT catalog manifest is not valid JSON") from exc
    if type(payload) is not dict or set(payload) != _MANIFEST_KEYS:
        raise ValueError("QMT catalog manifest fields differ")
    normalized_members = canonical_catalog_members(members)
    expected_manifest, _ = build_catalog_manifest(
        batch_id=str(row.get("batch_id") or ""),
        captured_at=row.get("captured_at"),
        history_complete_from=str(
            row.get("history_complete_from")
            or payload.get("history_complete_from")
            or ""
        )[:10],
        members=normalized_members,
        discovery=payload.get("discovery") or {},
        native_sectors=payload.get("native_sectors") or (),
    )
    if payload != expected_manifest:
        raise ValueError("QMT catalog manifest content differs")
    manifest_hash = str(row.get("manifest_hash") or "")
    if not _SHA256_RE.fullmatch(manifest_hash):
        raise ValueError("QMT catalog manifest hash is invalid")
    if canonical_digest(payload) != manifest_hash:
        raise ValueError("QMT catalog manifest hash differs")
    if (
        str(row.get("status") or "") != CATALOG_STATUS_COMPLETE
        or type(row.get("member_count")) is not int
        or int(row["member_count"]) != len(normalized_members)
        or str(row.get("member_set_hash") or "")
        != expected_manifest["member_set_hash"]
    ):
        raise ValueError("QMT catalog batch counters/hash differ")
    return StockCatalogBatch(
        batch_id=expected_manifest["batch_id"],
        captured_at=expected_manifest["captured_at"],
        history_complete_from=expected_manifest["history_complete_from"],
        member_count=len(normalized_members),
        member_set_hash=expected_manifest["member_set_hash"],
        manifest_hash=manifest_hash,
        native_sectors=tuple(expected_manifest["native_sectors"]),
        members=tuple(normalized_members),
    )


def stock_catalog_schema_statements() -> tuple[str, ...]:
    return (
        """
        CREATE TABLE IF NOT EXISTS qmt_stock_catalog_batch (
            batch_id VARCHAR(64) NOT NULL PRIMARY KEY,
            captured_at DATETIME NOT NULL,
            history_complete_from DATE NOT NULL,
            status VARCHAR(16) NOT NULL,
            member_count INT NOT NULL,
            member_set_hash CHAR(64) NOT NULL,
            manifest_json MEDIUMTEXT NOT NULL,
            manifest_hash CHAR(64) NOT NULL,
            created_at DATETIME NOT NULL,
            KEY idx_qmt_stock_catalog_complete (status, captured_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_stock_catalog_member (
            batch_id VARCHAR(64) NOT NULL,
            qmt_code VARCHAR(64) NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            list_date DATE NOT NULL,
            expire_date DATE NULL,
            instrument_batch_id VARCHAR(64) NOT NULL,
            instrument_type VARCHAR(32) NOT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (batch_id, qmt_code),
            UNIQUE KEY uk_qmt_stock_catalog_stock (batch_id, stock_code),
            KEY idx_qmt_stock_catalog_target
                (batch_id, list_date, expire_date, stock_code),
            CONSTRAINT fk_qmt_stock_catalog_member_batch
                FOREIGN KEY (batch_id) REFERENCES qmt_stock_catalog_batch(batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        ALTER TABLE qmt_stock_catalog_batch
        ADD COLUMN IF NOT EXISTS history_complete_from DATE NOT NULL
            DEFAULT '9999-12-31' AFTER captured_at
        """,
        """
        ALTER TABLE qmt_stock_catalog_member
        ADD COLUMN IF NOT EXISTS instrument_type VARCHAR(32) NOT NULL
            DEFAULT 'UNVERIFIED' AFTER instrument_batch_id
        """,
        """
        UPDATE qmt_stock_catalog_batch
        SET status='LEGACY_UNBOUND'
        WHERE status='COMPLETE'
          AND (
              history_complete_from='9999-12-31'
              OR EXISTS (
                  SELECT 1 FROM qmt_stock_catalog_member member
                  WHERE member.batch_id=qmt_stock_catalog_batch.batch_id
                    AND member.instrument_type='UNVERIFIED'
              )
          )
        """,
        """
        ALTER TABLE qmt_stock_catalog_batch
        MODIFY COLUMN history_complete_from DATE NOT NULL
        """,
        """
        ALTER TABLE qmt_stock_catalog_member
        MODIFY COLUMN instrument_type VARCHAR(32) NOT NULL
        """,
        """
        ALTER TABLE qmt_stock_catalog_batch
        CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """,
        """
        ALTER TABLE qmt_stock_catalog_member
        CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_stock_catalog_batch_no_update
        BEFORE UPDATE ON qmt_stock_catalog_batch
        FOR EACH ROW SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_stock_catalog_batch is append-only'
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_stock_catalog_batch_no_delete
        BEFORE DELETE ON qmt_stock_catalog_batch
        FOR EACH ROW SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_stock_catalog_batch is append-only'
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_stock_catalog_member_no_update
        BEFORE UPDATE ON qmt_stock_catalog_member
        FOR EACH ROW SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_stock_catalog_member is append-only'
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_qmt_stock_catalog_member_no_delete
        BEFORE DELETE ON qmt_stock_catalog_member
        FOR EACH ROW SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='qmt_stock_catalog_member is append-only'
        """,
    )


def stock_catalog_table_ddl_statements() -> tuple[str, ...]:
    return stock_catalog_schema_statements()[:2]


def stock_catalog_migration_statements() -> tuple[str, ...]:
    return stock_catalog_schema_statements()[2:9]


def stock_catalog_trigger_ddl_statements() -> tuple[str, ...]:
    return stock_catalog_schema_statements()[9:]


def _normalized_trigger_body(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("`", "").lower()


def _expected_trigger_contracts() -> dict[str, tuple[str, str, str]]:
    contracts: dict[str, tuple[str, str, str]] = {}
    pattern = re.compile(
        r"^CREATE TRIGGER(?: IF NOT EXISTS)?\s+`?([^`\s]+)`?\s+"
        r"BEFORE\s+(INSERT|UPDATE|DELETE)\s+ON\s+`?([^`\s]+)`?\s+"
        r"FOR EACH ROW\s+(.*)$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for statement in stock_catalog_trigger_ddl_statements():
        matched = pattern.match(statement.strip())
        if matched is None:
            raise RuntimeError("QMT stock catalog trigger DDL is unparsable")
        name, event, table_name, body = matched.groups()
        if name in contracts:
            raise RuntimeError("QMT stock catalog trigger DDL inventory differs")
        contracts[name] = (
            table_name.lower(),
            event.upper(),
            _normalized_trigger_body(body),
        )
    if set(contracts) != set(_IMMUTABILITY_TRIGGERS):
        raise RuntimeError("QMT stock catalog trigger DDL inventory differs")
    return contracts


def _stock_catalog_column_names(connection: Any, table_name: str) -> set[str]:
    rows = connection.execute(text("""
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name
    """), {"table_name": table_name}).mappings().all()
    return {
        str(row.get("COLUMN_NAME") or row.get("column_name") or "").casefold()
        for row in rows
        if str(row.get("COLUMN_NAME") or row.get("column_name") or "")
    }


def _mysql_add_column_statement(statement: str) -> str:
    marker = "ADD COLUMN IF NOT EXISTS"
    if statement.upper().count(marker) != 1:
        raise RuntimeError("QMT stock catalog additive DDL contract differs")
    return re.sub(
        marker,
        "ADD COLUMN",
        statement,
        count=1,
        flags=re.IGNORECASE,
    )


def privileged_migrate_stock_catalog_schema(
    engine: Any,
    *,
    install_triggers: bool = True,
    trigger_ddl_executor: Any | None = None,
) -> dict[str, Any]:
    """Install the persistent catalog schema during a fenced release.

    Production may set ``install_triggers=False`` so its allow-listed trigger
    broker can install and attest the frozen trigger contract later in the
    same fenced cutover.
    """

    if type(install_triggers) is not bool:
        raise TypeError("install_triggers must be bool")
    if trigger_ddl_executor is not None and not callable(trigger_ddl_executor):
        raise TypeError("trigger_ddl_executor must be callable")

    repair_statements = tuple(
        f"DROP TRIGGER IF EXISTS `{name}`"
        for name in _expected_trigger_contracts()
    )
    if install_triggers:
        if trigger_ddl_executor is None:
            with engine.begin() as connection:
                for statement in repair_statements:
                    connection.execute(text(statement))
        else:
            for statement in repair_statements:
                trigger_ddl_executor(statement)
    with engine.begin() as connection:
        for statement in stock_catalog_table_ddl_statements():
            connection.execute(text(statement))
        migration_statements = stock_catalog_migration_statements()
        additive_migrations = (
            (
                "qmt_stock_catalog_batch",
                "history_complete_from",
                migration_statements[0],
            ),
            (
                "qmt_stock_catalog_member",
                "instrument_type",
                migration_statements[1],
            ),
        )
        for table_name, column_name, statement in additive_migrations:
            if column_name.casefold() in _stock_catalog_column_names(
                connection, table_name
            ):
                continue
            connection.execute(text(_mysql_add_column_statement(statement)))
        for statement in migration_statements[2:]:
            connection.execute(text(statement))
    if install_triggers:
        if trigger_ddl_executor is None:
            with engine.begin() as connection:
                for statement in stock_catalog_trigger_ddl_statements():
                    connection.execute(text(statement))
        else:
            for statement in stock_catalog_trigger_ddl_statements():
                trigger_ddl_executor(statement)
        with engine.connect() as connection:
            validate_stock_catalog_immutability(connection)
    return {
        "tables": tuple(sorted(STOCK_CATALOG_REQUIRED_COLUMNS)),
        "trigger_names": tuple(sorted(_IMMUTABILITY_TRIGGERS)),
        "triggers_installed": install_triggers,
        "privileged_migration": True,
        "runtime_ddl_required": False,
    }


def validate_stock_catalog_runtime_schema(engine: Any) -> dict[str, Any]:
    """Validate the runtime surface and immutable guards without DDL."""

    surface = validate_required_table_surface(
        engine,
        STOCK_CATALOG_REQUIRED_COLUMNS,
        context="QMT stock catalog",
        required_columns=STOCK_CATALOG_REQUIRED_COLUMNS,
    )
    with engine.connect() as connection:
        validate_stock_catalog_immutability(connection)
    return {
        **surface,
        "trigger_names": tuple(sorted(_IMMUTABILITY_TRIGGERS)),
        "append_only_verified": True,
    }


def ensure_stock_catalog_tables(engine: Any) -> dict[str, Any]:
    """Backward-compatible, read-only runtime validation alias."""

    return validate_stock_catalog_runtime_schema(engine)


def validate_stock_catalog_immutability(connection: Any) -> None:
    expected = _expected_trigger_contracts()
    rows = connection.execute(text("""
        SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING,
               ACTION_STATEMENT
        FROM information_schema.TRIGGERS
        WHERE TRIGGER_SCHEMA=DATABASE()
          AND TRIGGER_NAME IN (
              'trg_qmt_stock_catalog_batch_no_update',
              'trg_qmt_stock_catalog_batch_no_delete',
              'trg_qmt_stock_catalog_member_no_update',
              'trg_qmt_stock_catalog_member_no_delete'
          )
    """)).mappings().all()
    observed = {
        str(row.get("TRIGGER_NAME") or row.get("trigger_name") or ""): row
        for row in rows
    }
    if set(observed) != set(expected):
        raise RuntimeError("QMT stock catalog append-only triggers are incomplete")
    for name, (table_name, event, action_body) in expected.items():
        row = observed[name]
        actual_table = str(
            row.get("EVENT_OBJECT_TABLE") or row.get("event_object_table") or ""
        ).lower()
        actual_event = str(
            row.get("EVENT_MANIPULATION") or row.get("event_manipulation") or ""
        ).upper()
        timing = str(
            row.get("ACTION_TIMING") or row.get("action_timing") or ""
        ).upper()
        if (
            actual_table != table_name
            or actual_event != event
            or timing != "BEFORE"
            or _normalized_trigger_body(
                row.get("ACTION_STATEMENT")
                or row.get("action_statement")
                or ""
            )
            != action_body
        ):
            raise RuntimeError("QMT stock catalog append-only trigger differs")


def insert_catalog_batch(
    connection: Any,
    *,
    batch_id: str,
    captured_at: Any,
    history_complete_from: str,
    members: Iterable[Mapping[str, Any]],
    discovery: Mapping[str, Any],
    native_sectors: Iterable[str] = NATIVE_A_SHARE_SECTORS,
) -> dict[str, Any]:
    """Insert one append-only batch through the caller's transaction."""

    manifest, normalized_members = build_catalog_manifest(
        batch_id=batch_id,
        captured_at=captured_at,
        history_complete_from=history_complete_from,
        members=members,
        discovery=discovery,
        native_sectors=native_sectors,
    )
    manifest_json = json.dumps(
        manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    manifest_hash = canonical_digest(manifest)
    connection.execute(text("""
        INSERT INTO qmt_stock_catalog_batch
        (batch_id, captured_at, history_complete_from, status, member_count, member_set_hash,
         manifest_json, manifest_hash, created_at)
        VALUES
        (:batch_id, :captured_at, :history_complete_from, :status, :member_count, :member_set_hash,
         :manifest_json, :manifest_hash, NOW())
    """), {
        "batch_id": manifest["batch_id"],
        "captured_at": manifest["captured_at"],
        "history_complete_from": manifest["history_complete_from"],
        "status": CATALOG_STATUS_COMPLETE,
        "member_count": manifest["member_count"],
        "member_set_hash": manifest["member_set_hash"],
        "manifest_json": manifest_json,
        "manifest_hash": manifest_hash,
    })
    connection.execute(text("""
        INSERT INTO qmt_stock_catalog_member
        (batch_id, qmt_code, stock_code, list_date, expire_date,
         instrument_batch_id, instrument_type, created_at)
        VALUES
        (:batch_id, :qmt_code, :stock_code, :list_date, :expire_date,
         :instrument_batch_id, :instrument_type, NOW())
    """), [
        {"batch_id": manifest["batch_id"], **member}
        for member in normalized_members
    ])
    return {**manifest, "manifest_hash": manifest_hash}


def load_stock_catalog(
    connection: Any,
    *,
    decision_known_at: Any,
    batch_id: str | None = None,
) -> StockCatalogBatch:
    decision_time = _captured_at(decision_known_at)
    params: dict[str, Any] = {"decision_known_at": decision_time}
    where = "status='COMPLETE' AND captured_at<=:decision_known_at"
    order = "ORDER BY captured_at DESC, batch_id DESC LIMIT 1"
    if batch_id is not None:
        where = (
            "batch_id=:batch_id AND status='COMPLETE' "
            "AND captured_at<=:decision_known_at"
        )
        order = ""
        params["batch_id"] = str(batch_id)
    batch = connection.execute(text(f"""
        SELECT batch_id, captured_at, history_complete_from, status, member_count,
               member_set_hash, manifest_json, manifest_hash
        FROM qmt_stock_catalog_batch
        WHERE {where}
        {order}
    """), params).mappings().one_or_none()
    if batch is None:
        raise RuntimeError("no complete independent QMT stock catalog batch")
    rows = connection.execute(text("""
        SELECT qmt_code, stock_code, list_date, expire_date,
               instrument_batch_id, instrument_type
        FROM qmt_stock_catalog_member
        WHERE batch_id=:batch_id
        ORDER BY qmt_code
    """), {"batch_id": str(batch["batch_id"])}).mappings().all()
    return validate_catalog_manifest(
        batch["manifest_json"], row=dict(batch), members=[dict(row) for row in rows]
    )


def load_target_stock_catalog(
    engine: Any,
    *,
    target_date: str,
    decision_known_at: Any,
    batch_id: str | None = None,
) -> tuple[StockCatalogBatch, list[str]]:
    _require_iso_date(target_date)
    with engine.connect() as connection:
        batch = load_stock_catalog(
            connection,
            batch_id=batch_id,
            decision_known_at=decision_known_at,
        )
    codes = batch.eligible_codes(target_date)
    if not codes:
        raise RuntimeError("independent QMT target-date stock catalog is empty")
    return batch, codes


__all__ = [
    "CATALOG_MANIFEST_SCHEMA",
    "CATALOG_MEMBER_SET_SCHEMA",
    "CATALOG_DISCOVERY_SCHEMA",
    "CATALOG_STATUS_COMPLETE",
    "NATIVE_A_SHARE_SECTORS",
    "StockCatalogBatch",
    "build_catalog_manifest",
    "build_catalog_discovery",
    "canonical_catalog_members",
    "catalog_member_set_hash",
    "ensure_stock_catalog_tables",
    "insert_catalog_batch",
    "load_stock_catalog",
    "load_target_stock_catalog",
    "privileged_migrate_stock_catalog_schema",
    "STOCK_CATALOG_REQUIRED_COLUMNS",
    "validate_catalog_manifest",
    "validate_catalog_discovery",
    "validate_stock_catalog_immutability",
    "validate_stock_catalog_runtime_schema",
    "stock_catalog_table_ddl_statements",
    "stock_catalog_migration_statements",
    "stock_catalog_trigger_ddl_statements",
]
