from __future__ import annotations

import uuid
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from integrations.qmt.diagnostics import PROVIDER_ID


CHINA_STANDARD_TIME = timezone(timedelta(hours=8), name="Asia/Shanghai")
RETRYABLE_MYSQL_WRITE_ERRORS = {1205, 1213}


@dataclass(frozen=True)
class SafeUpsertResult:
    table_name: str
    status: str
    source_rows: int
    accepted_rows: int
    duplicate_rows: int
    inserted_temp_rows: int
    key_columns: list[str]
    update_columns: list[str]
    batch_id: str | None


def _quote_identifier(identifier: str) -> str:
    text_value = str(identifier or "").strip()
    if not text_value.replace("_", "").isalnum() or not text_value:
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f"`{text_value}`"


def _fetch_table_columns(conn: Any, table_name: str) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
            """
        ),
        {"table_name": table_name},
    ).fetchall()
    return [str(row[0]) for row in rows]


def _normalize_row(
    row: Mapping[str, Any],
    *,
    table_columns: set[str],
    batch_id: str | None,
    received_at: datetime,
    permission_status: str,
    quality_status: str,
) -> dict[str, Any]:
    normalized = {str(key): value for key, value in dict(row).items() if str(key) in table_columns}
    if "data_source" in table_columns and not normalized.get("data_source"):
        normalized["data_source"] = PROVIDER_ID
    if "received_at" in table_columns and not normalized.get("received_at"):
        normalized["received_at"] = received_at
    if "batch_id" in table_columns and batch_id and not normalized.get("batch_id"):
        normalized["batch_id"] = batch_id
    if "permission_status" in table_columns and not normalized.get("permission_status"):
        normalized["permission_status"] = permission_status
    if "quality_status" in table_columns and not normalized.get("quality_status"):
        normalized["quality_status"] = quality_status
    return normalized


def prepare_qmt_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    table_columns: Sequence[str],
    key_columns: Sequence[str],
    batch_id: str | None = None,
    permission_status: str = "SUPPORTED",
    quality_status: str = "PENDING",
) -> tuple[list[dict[str, Any]], int]:
    """Normalize QMT rows, drop unknown fields, add provenance defaults and de-duplicate by key."""
    columns = set(table_columns)
    keys = [str(column) for column in key_columns]
    missing_keys = [column for column in keys if column not in columns]
    if missing_keys:
        raise ValueError(f"Key columns do not exist in target table: {missing_keys}")

    received_at = datetime.now(CHINA_STANDARD_TIME).replace(tzinfo=None)
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    source_count = 0
    duplicate_count = 0
    for item in rows:
        source_count += 1
        normalized = _normalize_row(
            item,
            table_columns=columns,
            batch_id=batch_id,
            received_at=received_at,
            permission_status=permission_status,
            quality_status=quality_status,
        )
        key = tuple(normalized.get(column) for column in keys)
        if any(value is None or value == "" for value in key):
            raise ValueError(f"Missing required key columns {keys} in row: {item}")
        if key in deduped:
            duplicate_count += 1
        deduped[key] = normalized
    return list(deduped.values()), duplicate_count


def _ordered_insert_columns(rows: Sequence[Mapping[str, Any]], table_columns: Sequence[str]) -> list[str]:
    present = set().union(*(row.keys() for row in rows)) if rows else set()
    return [column for column in table_columns if column in present]


def _mysql_error_code(exc: BaseException) -> int | None:
    orig = getattr(exc, "orig", None)
    args = getattr(orig, "args", None) or getattr(exc, "args", None) or ()
    if not args:
        return None
    try:
        return int(args[0])
    except (TypeError, ValueError):
        return None


def _safe_upsert_rows_once(
    engine: Engine,
    *,
    table_name: str,
    rows: Iterable[Mapping[str, Any]],
    key_columns: Sequence[str],
    batch_id: str | None = None,
    update_columns: Sequence[str] | None = None,
    permission_status: str = "SUPPORTED",
    quality_status: str = "PENDING",
) -> SafeUpsertResult:
    """Write QMT business rows through a temporary table, then upsert without truncating target tables."""
    quoted_table = _quote_identifier(table_name)
    source_rows = list(rows)
    if not source_rows:
        return SafeUpsertResult(
            table_name=table_name,
            status="EMPTY",
            source_rows=0,
            accepted_rows=0,
            duplicate_rows=0,
            inserted_temp_rows=0,
            key_columns=[str(column) for column in key_columns],
            update_columns=[],
            batch_id=batch_id,
        )

    temp_table = f"tmp_qmt_{uuid.uuid4().hex[:24]}"
    quoted_temp = _quote_identifier(temp_table)
    with engine.begin() as conn:
        table_columns = _fetch_table_columns(conn, table_name)
        if not table_columns:
            raise ValueError(f"Target table does not exist or has no columns: {table_name}")
        prepared_rows, duplicate_rows = prepare_qmt_rows(
            source_rows,
            table_columns=table_columns,
            key_columns=key_columns,
            batch_id=batch_id,
            permission_status=permission_status,
            quality_status=quality_status,
        )
        if not prepared_rows:
            return SafeUpsertResult(
                table_name=table_name,
                status="EMPTY",
                source_rows=len(source_rows),
                accepted_rows=0,
                duplicate_rows=duplicate_rows,
                inserted_temp_rows=0,
                key_columns=[str(column) for column in key_columns],
                update_columns=[],
                batch_id=batch_id,
            )

        insert_columns = _ordered_insert_columns(prepared_rows, table_columns)
        update_set = (
            [str(column) for column in update_columns]
            if update_columns is not None
            else [column for column in insert_columns if column not in set(key_columns)]
        )
        update_set = [column for column in update_set if column in insert_columns and column not in set(key_columns)]
        if not update_set:
            raise ValueError("No update columns available for ON DUPLICATE KEY UPDATE")

        conn.execute(text(f"CREATE TEMPORARY TABLE {quoted_temp} LIKE {quoted_table}"))
        column_sql = ", ".join(_quote_identifier(column) for column in insert_columns)
        value_sql = ", ".join(f":{column}" for column in insert_columns)
        conn.execute(
            text(f"INSERT INTO {quoted_temp} ({column_sql}) VALUES ({value_sql})"),
            [{column: row.get(column) for column in insert_columns} for row in prepared_rows],
        )
        inserted_temp_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {quoted_temp}")).scalar() or 0)
        if inserted_temp_rows != len(prepared_rows):
            raise RuntimeError(
                f"Temporary table row count mismatch: expected {len(prepared_rows)}, got {inserted_temp_rows}"
            )

        update_sql = ", ".join(f"{_quote_identifier(column)} = VALUES({_quote_identifier(column)})" for column in update_set)
        conn.execute(
            text(
                f"INSERT INTO {quoted_table} ({column_sql}) "
                f"SELECT {column_sql} FROM {quoted_temp} "
                f"ON DUPLICATE KEY UPDATE {update_sql}"
            )
        )

    return SafeUpsertResult(
        table_name=table_name,
        status="UPSERTED",
        source_rows=len(source_rows),
        accepted_rows=len(prepared_rows),
        duplicate_rows=duplicate_rows,
        inserted_temp_rows=len(prepared_rows),
        key_columns=[str(column) for column in key_columns],
        update_columns=update_set,
        batch_id=batch_id,
    )


def safe_upsert_rows(
    engine: Engine,
    *,
    table_name: str,
    rows: Iterable[Mapping[str, Any]],
    key_columns: Sequence[str],
    batch_id: str | None = None,
    update_columns: Sequence[str] | None = None,
    permission_status: str = "SUPPORTED",
    quality_status: str = "PENDING",
) -> SafeUpsertResult:
    """Retry transient MySQL write conflicts around the temp-table upsert."""
    source_rows = list(rows)
    attempts = max(1, int(os.environ.get("QMT_SAFE_UPSERT_RETRIES", "3")))
    base_sleep = max(0.05, float(os.environ.get("QMT_SAFE_UPSERT_RETRY_SLEEP", "0.25")))
    for attempt in range(attempts):
        try:
            return _safe_upsert_rows_once(
                engine,
                table_name=table_name,
                rows=source_rows,
                key_columns=key_columns,
                batch_id=batch_id,
                update_columns=update_columns,
                permission_status=permission_status,
                quality_status=quality_status,
            )
        except OperationalError as exc:
            code = _mysql_error_code(exc)
            if code not in RETRYABLE_MYSQL_WRITE_ERRORS or attempt >= attempts - 1:
                raise
            time.sleep(base_sleep * (2**attempt))
    raise RuntimeError("unreachable safe_upsert retry state")


def result_dict(result: SafeUpsertResult) -> dict[str, Any]:
    return asdict(result)
