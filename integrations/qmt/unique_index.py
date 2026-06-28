from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Iterable, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine


IDENTIFIER = re.compile(r"^[0-9A-Za-z_]+$")


@dataclass(frozen=True)
class UniqueIndexDefinition:
    table_name: str
    index_name: str
    columns: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class UniqueIndexResult:
    table_name: str
    index_name: str
    columns: list[str]
    status: str
    duplicate_groups: int
    error: str | None = None


QMT_UNIQUE_INDEXES: tuple[UniqueIndexDefinition, ...] = (
    UniqueIndexDefinition(
        table_name="sm_stock_current",
        index_name="uk_qmt_sm_stock_current_code",
        columns=("stock_code",),
        reason="当前行情表每只股票只保留一条最新记录，供安全Upsert触发更新。",
    ),
)


def _quote_identifier(identifier: str) -> str:
    if not IDENTIFIER.match(str(identifier or "")):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _index_exists(conn, table_name: str, index_name: str) -> bool:
    value = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
              AND INDEX_NAME = :index_name
            """
        ),
        {"table_name": table_name, "index_name": index_name},
    ).scalar()
    return bool(value)


def _duplicate_groups(conn, table_name: str, columns: Sequence[str]) -> int:
    group_sql = ", ".join(_quote_identifier(column) for column in columns)
    value = conn.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {group_sql}
                FROM {_quote_identifier(table_name)}
                GROUP BY {group_sql}
                HAVING COUNT(*) > 1
            ) qmt_dup
            """
        )
    ).scalar()
    return int(value or 0)


def ensure_unique_index(
    engine: Engine,
    definition: UniqueIndexDefinition,
    *,
    dry_run: bool = False,
) -> UniqueIndexResult:
    try:
        _quote_identifier(definition.table_name)
        _quote_identifier(definition.index_name)
        for column in definition.columns:
            _quote_identifier(column)
        with engine.begin() as conn:
            if _index_exists(conn, definition.table_name, definition.index_name):
                return UniqueIndexResult(
                    table_name=definition.table_name,
                    index_name=definition.index_name,
                    columns=list(definition.columns),
                    status="UNCHANGED",
                    duplicate_groups=0,
                )
            duplicate_groups = _duplicate_groups(conn, definition.table_name, definition.columns)
            if duplicate_groups > 0:
                return UniqueIndexResult(
                    table_name=definition.table_name,
                    index_name=definition.index_name,
                    columns=list(definition.columns),
                    status="BLOCKED_DUPLICATES",
                    duplicate_groups=duplicate_groups,
                )
            if not dry_run:
                column_sql = ", ".join(_quote_identifier(column) for column in definition.columns)
                conn.execute(
                    text(
                        f"ALTER TABLE {_quote_identifier(definition.table_name)} "
                        f"ADD UNIQUE KEY {_quote_identifier(definition.index_name)} ({column_sql})"
                    )
                )
        return UniqueIndexResult(
            table_name=definition.table_name,
            index_name=definition.index_name,
            columns=list(definition.columns),
            status="DRY_RUN" if dry_run else "CREATED",
            duplicate_groups=0,
        )
    except Exception as exc:
        return UniqueIndexResult(
            table_name=definition.table_name,
            index_name=definition.index_name,
            columns=list(definition.columns),
            status="ERROR",
            duplicate_groups=0,
            error=str(exc),
        )


def ensure_qmt_unique_indexes(
    engine: Engine,
    *,
    definitions: Iterable[UniqueIndexDefinition] | None = None,
    dry_run: bool = False,
) -> list[UniqueIndexResult]:
    return [ensure_unique_index(engine, definition, dry_run=dry_run) for definition in (definitions or QMT_UNIQUE_INDEXES)]


def result_dicts(results: Iterable[UniqueIndexResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]
