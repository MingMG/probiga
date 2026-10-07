"""Permanent native minute inventory evidence shared by planning and writing.

This proves normalized acquisition inventory, not whole-market publication or
daily-anchor attestation. Grid definitions have one canonical owner.
"""
import hashlib
import json
import re

from server.common.qmt_index_minute_grid import CORE_GRID, index_minute_grids


SCHEMA = "probiga.direct-qmt-minute-grid.v1"


def _digest(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str,
    ).encode()).hexdigest()


def grids(spec, code):
    if spec.name == "stock_minute":
        required, allowed = CORE_GRID, CORE_GRID
    elif spec.name == "index_minute":
        required, allowed = index_minute_grids(code)
    else:
        raise ValueError("no canonical native minute product")
    return tuple(sorted(required)), tuple(sorted(allowed))


def identity(spec, code, target_date):
    required, allowed = grids(spec, code)
    return {
        "schema": SCHEMA, "dataset": spec.name, "source": spec.source,
        "qmt_code": code, "target_date": str(target_date)[:10],
        "period": spec.period, "adjustment": "none",
        "required_grid_sha256": _digest(required),
        "allowed_grid_sha256": _digest(allowed),
    }


def proof(spec, unit, rows):
    if (unit.dataset != spec.name or unit.source != spec.source
            or unit.period != spec.period or unit.adjustment != "none"):
        raise ValueError("minute work unit differs from canonical product")
    required, allowed = grids(spec, unit.code)
    if any(row["trade_time"].tzinfo is not None
           or row["trade_time"].second or row["trade_time"].microsecond
           or row["trade_time"].date().isoformat() != unit.target_date
           or row[spec.code_column] != unit.code.split(".")[0] for row in rows):
        raise ValueError("minute inventory identity or timestamp differs")
    observed = {row["trade_time"].strftime("%H:%M:%S") for row in rows}
    if not set(required) <= observed <= set(allowed) or len(rows) != len(observed):
        raise ValueError("minute inventory is not complete and unique")
    # At most 341 bits, rather than repeating hundreds of timestamp strings in
    # every partition. Readers reconstruct and validate the actual time set.
    mask = sum(1 << i for i, value in enumerate(allowed) if value in observed)
    return {
        **identity(spec, unit.code, unit.target_date),
        "row_count": len(rows), "observed_grid_sha256": _digest(sorted(observed)),
        "observed_bitmap": format(mask, f"0{(len(allowed) + 3) // 4}x"),
        "content_sha256": _digest(sorted(rows, key=lambda row: row["trade_time"])),
    }


def verified_complete(spec, code, state):
    if state.get("status") != "complete":
        return False
    if spec.period != "1m":
        return True
    try:
        detail = state.get("detail_json") or {}
        if isinstance(detail, str):
            detail = json.loads(detail)
        value = detail["minute_grid_proof"]
        required, allowed = grids(spec, code)
        expected = identity(spec, code, state["target_date"])
        if (not isinstance(value, dict)
                or set(value) != set(expected) | {
                    "row_count", "observed_grid_sha256", "observed_bitmap", "content_sha256"}
                or any(value.get(key) != item for key, item in expected.items())
                or type(value.get("row_count")) is not int
                or type(state.get("written_rows")) is not int
                or state["written_rows"] != value["row_count"]
                or type(detail.get("missing_expected_rows")) is not int
                or detail["missing_expected_rows"] != 0
                or type(detail.get("out_of_scope_rows")) is not int
                or detail["out_of_scope_rows"] != 0):
            return False
        bitmap = value["observed_bitmap"]
        if not isinstance(bitmap, str) or not re.fullmatch(
                r"[0-9a-f]{%d}" % ((len(allowed) + 3) // 4), bitmap):
            return False
        mask = int(bitmap, 16)
        if mask >> len(allowed):
            return False
        observed = {value for i, value in enumerate(allowed) if mask & (1 << i)}
        return (set(required) <= observed and len(observed) == value["row_count"]
                and value["observed_grid_sha256"] == _digest(sorted(observed))
                and isinstance(value["content_sha256"], str)
                and re.fullmatch(r"[0-9a-f]{64}", value["content_sha256"]) is not None)
    except (KeyError, TypeError, ValueError, RecursionError):
        return False


def terminal(spec, code, state):
    return state.get("status") == "no_data" or verified_complete(spec, code, state)
