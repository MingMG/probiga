"""Streaming transforms used to make legacy dumps safer for MySQL 8.4.

MySQL 8.4.11 on Windows has reproduced access violations while maintaining
secondary InnoDB indexes during a large logical seed.  This module does not
change row payloads.  For explicitly selected tables it removes secondary
index definitions from the ``CREATE TABLE`` block, then adds the exact same
definitions after that table's data has been loaded.

The transform is deliberately opt-in.  A caller must name every table it
wants to defer, and the stream reports which requested tables were actually
matched.  An incomplete dump therefore cannot silently appear successful.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field


class BulkTransformError(ValueError):
    """Raised when a requested table/index transform is unsafe or incomplete."""


_USE_RE = re.compile(rb"^\s*USE\s+(?P<name>`[^`]+`|[A-Za-z0-9_$]+)\s*;", re.I)
_CREATE_RE = re.compile(
    rb"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rb"(?P<name>(?:`[^`]+`|[A-Za-z0-9_$]+)(?:\s*\.\s*(?:`[^`]+`|[A-Za-z0-9_$]+))?)",
    re.I,
)
_CREATE_END_RE = re.compile(rb"^\s*\)\s*(?:ENGINE\b|;|/\*|$)", re.I)
_INLINE_CREATE_END_RE = re.compile(rb"\)\s*(?:ENGINE\b|;)", re.I)
_INDEX_RE = re.compile(
    rb"^\s*(?P<definition>(?:(?:UNIQUE|FULLTEXT|SPATIAL)\s+)?KEY\b.*?)(?:,\s*)?(?:\r?\n)?$",
    re.I,
)
_UNLOCK_RE = re.compile(rb"^\s*UNLOCK\s+TABLES\s*;", re.I)


def _create_block_complete(line: bytes) -> bool:
    return bool(_CREATE_END_RE.match(line) or _INLINE_CREATE_END_RE.search(line))


def _strip_identifier(raw: bytes) -> str:
    value = raw.decode("utf-8", errors="strict").strip()
    if value.startswith("`") and value.endswith("`"):
        value = value[1:-1].replace("``", "`")
    return value


def normalize_table_name(value: str) -> str:
    """Normalize ``schema.table`` or ``table`` to lowercase dotted form."""

    pieces = [piece.strip() for piece in value.split(".")]
    if len(pieces) not in (1, 2) or any(not piece for piece in pieces):
        raise BulkTransformError(f"invalid table name: {value!r}")
    cleaned = []
    for piece in pieces:
        if piece.startswith("`") and piece.endswith("`"):
            piece = piece[1:-1].replace("``", "`")
        if "`" in piece or "\\x00" in piece:
            raise BulkTransformError(f"invalid table name: {value!r}")
        cleaned.append(piece.casefold())
    return ".".join(cleaned)


def _qualified_name(schema: str | None, table: str) -> str:
    if schema:
        return f"`{schema.replace('`', '``')}`.`{table.replace('`', '``')}`"
    return f"`{table.replace('`', '``')}`"


@dataclass(frozen=True)
class DeferredTableReport:
    table: str
    secondary_indexes: tuple[str, ...]


@dataclass
class BulkTransformStats:
    requested_tables: tuple[str, ...]
    defer_all: bool = False
    matched_tables: list[DeferredTableReport] = field(default_factory=list)
    added_index_statements: int = 0

    @property
    def matched_names(self) -> frozenset[str]:
        return frozenset(item.table for item in self.matched_tables)


def _table_identity(raw_name: bytes, current_schema: str | None) -> str:
    parts = [part.strip() for part in raw_name.split(b".")]
    if len(parts) == 1:
        return normalize_table_name(
            f"{current_schema}.{_strip_identifier(parts[0])}"
            if current_schema
            else _strip_identifier(parts[0])
        )
    if len(parts) == 2:
        return normalize_table_name(
            f"{_strip_identifier(parts[0])}.{_strip_identifier(parts[1])}"
        )
    raise BulkTransformError(f"invalid CREATE TABLE name: {raw_name!r}")


def _rewrite_create_block(
    block: Sequence[bytes], *, table: str
) -> tuple[list[bytes], DeferredTableReport | None]:
    index_definitions: list[str] = []
    retained: list[bytes] = []
    for line in block:
        match = _INDEX_RE.fullmatch(line)
        if match is None:
            retained.append(line)
            continue
        definition = match.group("definition").rstrip()
        if definition.endswith(b","):
            definition = definition[:-1].rstrip()
        index_definitions.append(definition.decode("utf-8", errors="strict"))

    if not index_definitions:
        return list(block), None

    # The last retained column/constraint line may still carry the comma that
    # separated it from the first removed key.  Remove only that delimiter.
    closing_index = next(
        (index for index, line in enumerate(retained) if _CREATE_END_RE.match(line)),
        None,
    )
    if closing_index is None:
        raise BulkTransformError(f"CREATE TABLE {table} has no closing line")
    previous = closing_index - 1
    while previous >= 0 and not retained[previous].strip():
        previous -= 1
    if previous < 0:
        raise BulkTransformError(f"CREATE TABLE {table} has no definition body")
    line = retained[previous]
    newline = b""
    if line.endswith(b"\r\n"):
        line, newline = line[:-2], b"\r\n"
    elif line.endswith((b"\n", b"\r")):
        line, newline = line[:-1], line[-1:]
    stripped = line.rstrip()
    if stripped.endswith(b","):
        retained[previous] = stripped[:-1].rstrip() + newline
    elif not stripped.endswith(b")"):
        raise BulkTransformError(
            f"CREATE TABLE {table} has an unexpected closing definition"
        )
    return retained, DeferredTableReport(table=table, secondary_indexes=tuple(index_definitions))


def _add_index_lines(report: DeferredTableReport) -> Iterator[bytes]:
    if "." in report.table:
        schema, table = report.table.split(".", 1)
        qualified = _qualified_name(schema, table)
    else:
        qualified = _qualified_name(None, report.table)
    for definition in report.secondary_indexes:
        yield f"ALTER TABLE {qualified} ADD {definition};\n".encode("utf-8")


def transform_dump_lines(
    lines: Iterable[bytes],
    requested_tables: Iterable[str],
    *,
    require_table_data_completion: bool = True,
) -> tuple[Iterator[bytes], BulkTransformStats]:
    """Return a lazy transformed stream and its mutable stats object.

    The iterator must be consumed before callers inspect ``stats.matched``.
    ``requested_tables`` accepts explicit ``schema.table`` names; an
    unqualified name matches the current ``USE`` schema at the CREATE block.
    """

    raw_requested = tuple(str(item).strip() for item in requested_tables)
    sentinel = tuple(item for item in raw_requested if item.casefold() in {"all", "*"})
    if sentinel and len(sentinel) != len(raw_requested):
        raise BulkTransformError("'all' cannot be combined with explicit table names")
    defer_all = bool(sentinel)
    requested = (
        ("all",)
        if defer_all
        else tuple(sorted({normalize_table_name(item) for item in raw_requested}))
    )
    requested_set = frozenset(requested)
    stats = BulkTransformStats(requested_tables=requested, defer_all=defer_all)

    def iterator() -> Iterator[bytes]:
        current_schema: str | None = None
        pending: list[bytes] | None = None
        pending_table: str | None = None
        active: DeferredTableReport | None = None
        for line in lines:
            if pending is not None:
                pending.append(line)
                if _create_block_complete(line):
                    assert pending_table is not None
                    selected = defer_all or pending_table in requested_set or any(
                        "." not in requested and pending_table.endswith("." + requested)
                        for requested in requested_set
                    )
                    if selected:
                        rewritten, report = _rewrite_create_block(pending, table=pending_table)
                        if report is not None:
                            stats.matched_tables.append(report)
                            active = report
                            yield from rewritten
                        else:
                            yield from pending
                    else:
                        yield from pending
                    pending = None
                    pending_table = None
                continue

            use_match = _USE_RE.match(line)
            if use_match:
                current_schema = _strip_identifier(use_match.group("name"))
                yield line
                continue

            create_match = _CREATE_RE.match(line)
            if create_match:
                pending = [line]
                pending_table = _table_identity(create_match.group("name"), current_schema)
                # A one-line CREATE is uncommon but valid.  It is still
                # handled by the same closing-line check on the next branch.
                if _create_block_complete(line):
                    rewritten, report = _rewrite_create_block(pending, table=pending_table)
                    selected = defer_all or pending_table in requested_set or any(
                        "." not in requested and pending_table.endswith("." + requested)
                        for requested in requested_set
                    )
                    if report is not None and selected:
                        stats.matched_tables.append(report)
                        active = report
                        yield from rewritten
                    else:
                        yield from pending
                    pending = None
                    pending_table = None
                continue

            if active is not None and _UNLOCK_RE.match(line):
                yield line
                yield from _add_index_lines(active)
                stats.added_index_statements += len(active.secondary_indexes)
                active = None
                continue
            yield line

        if pending is not None:
            raise BulkTransformError("dump ended inside a CREATE TABLE block")
        if active is not None and require_table_data_completion:
            raise BulkTransformError(
                f"dump ended before deferred table data completed: {active.table}"
            )
        matched_requests = {
            requested
            for requested in requested_set
            if any(
                matched == requested
                or ("." not in requested and matched.endswith("." + requested))
                for matched in stats.matched_names
            )
        }
        missing = frozenset() if defer_all and stats.matched_tables else requested_set - matched_requests
        if defer_all and not stats.matched_tables:
            raise BulkTransformError("requested all tables but found no secondary indexes")
        if missing:
            raise BulkTransformError(
                "requested tables had no removable secondary indexes: "
                + ", ".join(sorted(missing))
            )

    return iterator(), stats
