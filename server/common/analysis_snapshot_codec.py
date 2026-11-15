"""Lossless dictionary/column storage for complete analysis snapshot JSON.

Values are whole JSON cells, never business-field projections.  The logical
payload and its canonical hash are independent of this storage representation.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
from typing import Any


SNAPSHOT_DICTIONARY_ENCODING = "zlib-base64-dictionary-columnar-json-v1"
SNAPSHOT_EXPANDED_MAX_BYTES = 256 * 1024 * 1024
SNAPSHOT_MAX_CELLS = 4_000_000
SNAPSHOT_MAX_ROWS = 10_000
SNAPSHOT_MAX_COLUMNS = 512
_REQUIRED_TABLES = frozenset({"analysis_rows", "scored_rows"})
_TABLES = _REQUIRED_TABLES | {"candidate_rows"}
_SCORE_SCHEMA = "probiga.analysis-score-snapshot.v1"
SCORE_RECONSTRUCTION_STORAGE_KEY = "_score_reconstruction_storage"
_SCORE_RECONSTRUCTION_SCHEMA = "probiga.score-reconstruction-storage.v1"
_RECONSTRUCTION_SCHEMA = "probiga.qmt-announcement-historical-reconstruction.v2"
_RECONSTRUCTION_FIELD = "pit_reconstruction_provenance"
_RECONSTRUCTION_MARKER = f'"{_RECONSTRUCTION_FIELD}":'
_SCORE_SHARED_PROOF_MAX_BYTES = 64 * 1024
_SCORE_REHYDRATION_TEXT_MAX_BYTES = 256 * 1024 * 1024
_CODE = re.compile(r"[0-9]{6}")


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


def _paired_score_rows(payload: dict[str, Any]) -> tuple[list, list]:
    if type(payload) is not dict or payload.get("schema") != _SCORE_SCHEMA or "candidate_rows" in payload:
        raise ValueError("score reconstruction payload scope differs")
    analysis, scores = payload.get("analysis_rows"), payload.get("scored_rows")
    if (
        type(analysis) is not list or type(scores) is not list
        or not 0 < len(analysis) == len(scores) <= SNAPSHOT_MAX_ROWS
        or type(payload.get("stock_count")) is not int
        or payload["stock_count"] != len(scores)
    ):
        raise ValueError("score reconstruction universe differs")
    previous = ""
    for item, score in zip(analysis, scores):
        if type(item) is not dict or type(score) is not dict:
            raise ValueError("score reconstruction row shape differs")
        code = item.get("stock_code")
        if type(code) is not str or not _CODE.fullmatch(code) or code <= previous or score.get("stock_code") != code:
            raise ValueError("score reconstruction row identity differs")
        previous = code
    return analysis, scores


def _score_proof_reference(proof: dict[str, Any], target: Any) -> dict[str, str]:
    if (
        type(proof) is not dict
        or type(target) is not str
        or proof.get("schema") != _RECONSTRUCTION_SCHEMA
        or proof.get("mode") != "HISTORICAL_RECONSTRUCTION"
        or proof.get("target_trade_date") != target
        or proof.get("provider") != "cninfo.announcement"
        or proof.get("source") != "cninfo.announcement"
        or type(proof.get("reconstructed_at")) is not str
        or not proof["reconstructed_at"]
        or proof.get("known_at") != proof["reconstructed_at"]
        or proof.get("automatic_real_order_submission") is not False
        or proof.get("real_order_authority") is not False
    ):
        raise ValueError("score reconstruction provenance scope differs")
    core = {key: value for key, value in proof.items() if key != "reconstruction_sha256"}
    digest = hashlib.sha256(_canonical_bytes(core)).hexdigest()
    if proof.get("reconstruction_sha256") != digest:
        raise ValueError("score reconstruction provenance seal differs")
    return {
        "pit_reconstruction_mode": proof["mode"],
        "pit_reconstruction_sha256": digest,
        "pit_reconstructed_at": proof["reconstructed_at"],
    }


def _score_detail_span(detail: str) -> int:
    if type(detail) is not str:
        raise ValueError("score reconstruction event detail is not a string")
    marker = detail.find(_RECONSTRUCTION_MARKER)
    if marker < 0 or detail.find(_RECONSTRUCTION_MARKER, marker + len(_RECONSTRUCTION_MARKER)) >= 0:
        raise ValueError("score reconstruction provenance is absent or ambiguous")
    start = marker + len(_RECONSTRUCTION_MARKER)
    while start < len(detail) and detail[start] in " \t\r\n":
        start += 1
    return start


def _validate_score_fragments(
    prefix: str, suffix: str, *, decoder: json.JSONDecoder, reference: dict[str, str],
) -> None:
    # Parse the stock-specific JSON with a sentinel instead of reparsing the
    # shared proof thousands of times. Its separately validated object fits
    # exactly at the same top-level member value.
    stub = prefix + "null" + suffix
    if _score_detail_span(stub) != len(prefix):
        raise ValueError("score reconstruction fragment boundary differs")
    parsed = decoder.decode(stub)
    if (
        type(parsed) is not dict or _RECONSTRUCTION_FIELD not in parsed or parsed[_RECONSTRUCTION_FIELD] is not None
        or any(parsed.get(key) != value for key, value in reference.items())
    ):
        raise ValueError("score reconstruction event detail binding differs")


def _score_proof_container_slots(value: Any) -> int:
    if type(value) is dict:
        children = value.values()
    elif type(value) is list:
        children = value
    else:
        return 0
    return 1 + len(value) + sum(_score_proof_container_slots(child) for child in children)


def _score_rehydration_text_bytes(prefix: str, suffix: str, proof_bytes: int) -> int:
    # Each pair of output rows shares one immutable text value. Mutable
    # proof containers have a separate repeated-cell bound.
    return len(prefix.encode("utf-8")) + proof_bytes + len(suffix.encode("utf-8"))


def compact_score_reconstruction(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Store one shared reconstruction, preserving every logical row byte."""
    try:
        analysis, scores = _paired_score_rows(payload)
        if SCORE_RECONSTRUCTION_STORAGE_KEY in payload:
            return None
        decoder = json.JSONDecoder(object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        first = analysis[0].get("event_risk_detail")
        start = _score_detail_span(first)
        proof, proof_length = decoder.raw_decode(first[start:start + _SCORE_SHARED_PROOF_MAX_BYTES])
        proof_json = first[start:start + proof_length]
        proof_bytes = len(proof_json.encode("utf-8"))
        if proof_bytes > _SCORE_SHARED_PROOF_MAX_BYTES:
            return None
        reference = _score_proof_reference(proof, payload.get("trade_date"))
        canonical_proof = _canonical_bytes(proof)
        if len(scores) * _score_proof_container_slots(proof) > SNAPSHOT_MAX_CELLS:
            return None
        compact_analysis, compact_scores = [], []
        added = 0
        for item, score in zip(analysis, scores):
            detail = item.get("event_risk_detail")
            if (
                _RECONSTRUCTION_FIELD in item
                or detail != score.get("event_risk_detail")
                or _canonical_bytes(score.get(_RECONSTRUCTION_FIELD)) != canonical_proof
                or any(score.get(key) != value for key, value in reference.items())
            ):
                return None
            start = _score_detail_span(detail)
            if not detail.startswith(proof_json, start):
                return None
            prefix, suffix = detail[:start], detail[start + len(proof_json):]
            added += _score_rehydration_text_bytes(prefix, suffix, proof_bytes)
            if added > _SCORE_REHYDRATION_TEXT_MAX_BYTES:
                return None
            _validate_score_fragments(prefix, suffix, decoder=decoder, reference=reference)
            compact_analysis.append({**item, "event_risk_detail": [prefix, suffix]})
            compact_scores.append({
                key: value for key, value in score.items()
                if key not in ("event_risk_detail", _RECONSTRUCTION_FIELD)
            })
        return {
            **payload,
            SCORE_RECONSTRUCTION_STORAGE_KEY: {"schema": _SCORE_RECONSTRUCTION_SCHEMA, "provenance_json": proof_json},
            "analysis_rows": compact_analysis,
            "scored_rows": compact_scores,
        }
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError):
        return None


def expand_score_reconstruction(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate compact score storage and recover the original logical rows."""
    try:
        return _expand_score_reconstruction(payload)
    except (TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ValueError("score reconstruction stored JSON is invalid") from exc


def _expand_score_reconstruction(payload: dict[str, Any]) -> dict[str, Any]:
    analysis, scores = _paired_score_rows(payload)
    shared = payload.get(SCORE_RECONSTRUCTION_STORAGE_KEY)
    if (
        type(shared) is not dict or set(shared) != {"schema", "provenance_json"}
        or shared["schema"] != _SCORE_RECONSTRUCTION_SCHEMA
        or type(shared["provenance_json"]) is not str
    ):
        raise ValueError("score reconstruction storage shape differs")
    proof_json = shared["provenance_json"]
    if not proof_json.startswith("{") or not proof_json.endswith("}") or _RECONSTRUCTION_MARKER in proof_json:
        raise ValueError("score reconstruction shared proof boundary differs")
    proof_bytes = len(proof_json.encode("utf-8"))
    if proof_bytes > _SCORE_SHARED_PROOF_MAX_BYTES:
        raise ValueError("score reconstruction shared proof exceeds contract")
    decoder = json.JSONDecoder(object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    proof = decoder.decode(proof_json)
    reference = _score_proof_reference(proof, payload.get("trade_date"))
    if len(scores) * _score_proof_container_slots(proof) > SNAPSHOT_MAX_CELLS:
        raise ValueError("score reconstruction repeated proof cells exceed contract")
    canonical_proof = _canonical_bytes(proof)
    added = 0
    for item, score in zip(analysis, scores):
        fragments = item.get("event_risk_detail")
        if (
            type(fragments) is not list or len(fragments) != 2
            or any(type(fragment) is not str for fragment in fragments)
            or _RECONSTRUCTION_FIELD in item
            or "event_risk_detail" in score or _RECONSTRUCTION_FIELD in score
            or any(score.get(key) != value for key, value in reference.items())
        ):
            raise ValueError("score reconstruction fragments or reference differ")
        added += _score_rehydration_text_bytes(*fragments, proof_bytes)
        if added > _SCORE_REHYDRATION_TEXT_MAX_BYTES:
            raise ValueError("score reconstruction rehydrated size exceeds contract")
        _validate_score_fragments(*fragments, decoder=decoder, reference=reference)

    normalized_proof = json.loads(canonical_proof)
    expanded_analysis, expanded_scores = [], []
    for item, score in zip(analysis, scores):
        prefix, suffix = item["event_risk_detail"]
        detail = prefix + proof_json + suffix
        expanded_analysis.append({**item, "event_risk_detail": detail})
        expanded_scores.append({**score, "event_risk_detail": detail, _RECONSTRUCTION_FIELD: deepcopy(normalized_proof)})
    return {
        **{key: value for key, value in payload.items() if key != SCORE_RECONSTRUCTION_STORAGE_KEY},
        "analysis_rows": expanded_analysis,
        "scored_rows": expanded_scores,
    }
