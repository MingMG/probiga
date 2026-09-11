"""Lossless dictionary/column storage for complete analysis snapshot JSON.

Values are whole JSON cells, never business-field projections.  The logical
payload and its canonical hash are independent of this storage representation.
"""
from __future__ import annotations

from copy import deepcopy
import json
from typing import Any


SNAPSHOT_DICTIONARY_ENCODING = "zlib-base64-dictionary-columnar-json-v1"
SNAPSHOT_EXPANDED_MAX_BYTES = 256 * 1024 * 1024
SNAPSHOT_MAX_CELLS = 4_000_000
SNAPSHOT_MAX_ROWS = 10_000
SNAPSHOT_MAX_COLUMNS = 512
_REQUIRED_TABLES = frozenset({"analysis_rows", "scored_rows"})
_TABLES = _REQUIRED_TABLES | {"candidate_rows"}


def _validate_json_types(value: Any) -> None:
    kind = type(value)
    if kind is dict:
        for key, child in value.items():
            if type(key) is not str:
                raise ValueError("analysis snapshot JSON key is not a string")
            _validate_json_types(child)
    elif kind is list:
        for child in value:
            _validate_json_types(child)
    elif kind not in (str, int, float, bool, type(None)):
        raise ValueError("analysis snapshot contains a non-JSON value")


def _canonical_bytes(value: Any) -> bytes:
    _validate_json_types(value)
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("analysis snapshot JSON contains a duplicate key")
        result[key] = value
    return result


def _invalid_constant(_value: str) -> None:
    raise ValueError("analysis snapshot JSON contains a non-finite number")


def _check_table_names(names: set[str]) -> None:
    if not _REQUIRED_TABLES <= names <= _TABLES:
        raise ValueError("analysis snapshot table scope differs")


def _check_expanded_size(size: int) -> None:
    if size > SNAPSHOT_EXPANDED_MAX_BYTES:
        raise ValueError(
            "analysis snapshot expanded size exceeds contract: "
            f"expanded_bytes={size}, limit_bytes={SNAPSHOT_EXPANDED_MAX_BYTES}"
        )


def _validate_wire(wire: Any) -> None:
    """Validate all references and budget the full JSON before making rows."""
    if type(wire) is not dict or set(wire) != {"metadata", "values", "tables"}:
        raise ValueError("analysis snapshot storage shape differs")
    metadata, values, tables = wire["metadata"], wire["values"], wire["tables"]
    if type(metadata) is not dict or set(metadata) & _TABLES:
        raise ValueError("analysis snapshot metadata aliases a table")
    if type(values) is not list or type(tables) is not dict:
        raise ValueError("analysis snapshot storage types differ")
    _check_table_names(set(tables))

    cells = 0
    for table in tables.values():
        if type(table) is not dict or set(table) != {"columns", "row_count", "indices"}:
            raise ValueError("analysis snapshot table shape differs")
        columns, count, indices = table["columns"], table["row_count"], table["indices"]
        if type(count) is not int or not 0 <= count <= SNAPSHOT_MAX_ROWS:
            raise ValueError("analysis snapshot row count differs")
        if (
            type(columns) is not list or len(columns) > SNAPSHOT_MAX_COLUMNS
            or any(type(column) is not str for column in columns)
            or columns != sorted(set(columns))
        ):
            raise ValueError("analysis snapshot columns differ")
        cells += count * len(columns)
        if cells > SNAPSHOT_MAX_CELLS:
            raise ValueError("analysis snapshot cell count exceeds contract")
        if type(indices) is not list or len(indices) != len(columns):
            raise ValueError("analysis snapshot column index count differs")
        if any(type(column) is not list or len(column) != count for column in indices):
            raise ValueError("analysis snapshot column length differs")
    if len(values) > cells:
        raise ValueError("analysis snapshot value pool contains unused values")

    value_lengths = []
    seen_values = set()
    for value in values:
        raw = _canonical_bytes(value)
        if raw in seen_values:
            raise ValueError("analysis snapshot value pool contains duplicates")
        seen_values.add(raw)
        value_lengths.append(len(raw))

    # Braces, top-level commas, metadata members, then each table's list and
    # row braces.  Every present cell adds its quoted key, colon, value and
    # (after the first member of that row) a comma.  Missing cells add nothing.
    member_count = len(metadata) + len(tables)
    expanded = 2 + max(0, member_count - 1)
    for key, value in metadata.items():
        expanded += len(_canonical_bytes(key)) + 1 + len(_canonical_bytes(value))
    _check_expanded_size(expanded)
    next_identity = 1
    for name in sorted(tables):
        table = tables[name]
        count = table["row_count"]
        expanded += len(_canonical_bytes(name)) + 1 + 2 + max(0, count - 1) + 2 * count
        _check_expanded_size(expanded)
        populated = [False] * count
        for column, indices in zip(table["columns"], table["indices"]):
            key_bytes = len(_canonical_bytes(column)) + 1
            used_column = False
            for row_index, identity in enumerate(indices):
                if type(identity) is not int or not 0 <= identity <= len(values):
                    raise ValueError("analysis snapshot cell index differs")
                if identity == 0:
                    continue
                used_column = True
                if identity >= next_identity:
                    if identity != next_identity:
                        raise ValueError("analysis snapshot value pool order differs")
                    next_identity += 1
                expanded += key_bytes + value_lengths[identity - 1] + int(populated[row_index])
                populated[row_index] = True
                _check_expanded_size(expanded)
            if not used_column:
                raise ValueError("analysis snapshot contains an unused column")
    if next_identity != len(values) + 1:
        raise ValueError("analysis snapshot value pool contains unused values")


def encode_snapshot_payload(payload: dict[str, Any]) -> bytes:
    """Return the unique canonical storage JSON for one complete payload."""
    try:
        if type(payload) is not dict or any(type(key) is not str for key in payload):
            raise ValueError("analysis snapshot payload is not a JSON object")
        table_names = set(payload) & _TABLES
        _check_table_names(table_names)
        pool = []
        identities: dict[bytes, int] = {}
        wire = {
            "metadata": {key: value for key, value in payload.items() if key not in _TABLES},
            "values": pool, "tables": {},
        }
        cells = 0
        for name in sorted(table_names):
            rows = payload[name]
            if type(rows) is not list or len(rows) > SNAPSHOT_MAX_ROWS:
                raise ValueError("analysis snapshot row count differs")
            if any(type(row) is not dict or any(type(key) is not str for key in row) for row in rows):
                raise ValueError("analysis snapshot row is not a JSON object")
            columns = sorted({key for row in rows for key in row})
            cells += len(rows) * len(columns)
            if len(columns) > SNAPSHOT_MAX_COLUMNS or cells > SNAPSHOT_MAX_CELLS:
                raise ValueError("analysis snapshot table dimensions exceed contract")
            table = {"columns": columns, "row_count": len(rows), "indices": []}
            for column in columns:
                indices = []
                for row in rows:
                    if column not in row:
                        indices.append(0)
                        continue
                    value = row[column]
                    raw = _canonical_bytes(value)
                    identity = identities.get(raw)
                    if identity is None:
                        pool.append(value)
                        identity = len(pool)
                        identities[raw] = identity
                    indices.append(identity)
                table["indices"].append(indices)
            wire["tables"][name] = table
        del identities
        _validate_wire(wire)
        return _canonical_bytes(wire)
    except (TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ValueError("analysis snapshot JSON is invalid") from exc


def decode_snapshot_payload(raw: bytes) -> dict[str, Any]:
    """Validate storage and expand independent mutable cells into logical rows."""
    try:
        if type(raw) is not bytes:
            raise ValueError("analysis snapshot storage must be UTF-8 bytes")
        wire = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        if raw != _canonical_bytes(wire):
            raise ValueError("analysis snapshot storage JSON is not canonical")
        _validate_wire(wire)
        result = dict(wire["metadata"])
        values = wire["values"]
        for name, table in wire["tables"].items():
            rows = [{} for _ in range(table["row_count"])]
            for column, indices in zip(table["columns"], table["indices"]):
                for row, identity in zip(rows, indices):
                    if identity:
                        row[column] = deepcopy(values[identity - 1])
            result[name] = rows
        return result
    except (TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ValueError("analysis snapshot JSON is invalid") from exc
