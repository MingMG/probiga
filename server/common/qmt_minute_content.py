"""Bind native minute input, physical staging and published rows to one root."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext

from sqlalchemy import bindparam, text

from server.common.qmt_attestation_contract import canonical_digest

SCHEMA = "probiga.qmt-minute-canonical-content.v1"
NUMBERS = ("price", "avg_price", "change", "change_pct", "volume", "amount")
FIELDS = ("stock_code", "trade_time", "trade_date", *NUMBERS, "data_source", "source_time", "batch_id")
PROVIDER = "gj_big_qmt_inner"


def _layout(layout):
    if not isinstance(layout, dict) or set(layout) != set(NUMBERS):
        raise ValueError("minute canonical numeric layout differs")
    for precision, scale in layout.values():
        if (type(precision) is not int or type(scale) is not int
                or not 1 <= precision <= 65 or not 0 <= scale <= min(30, precision)):
            raise ValueError("minute canonical decimal layout differs")
    return layout


def load_numeric_layout(connection):
    rows = connection.execute(text("""
        SELECT COLUMN_NAME,DATA_TYPE,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION
          FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='sm_stock_minute'
    """)).mappings().all()
    columns = {row["COLUMN_NAME"]: row for row in rows}
    if not set(FIELDS) <= set(columns):
        raise ValueError("minute canonical content columns are missing")
    for field in NUMBERS:
        if columns[field]["DATA_TYPE"] != "decimal":
            raise ValueError("minute canonical numeric storage is not decimal")
    for field in ("trade_time", "source_time"):
        if columns[field]["DATA_TYPE"] != "datetime" or columns[field]["DATETIME_PRECISION"] != 0:
            raise ValueError("minute canonical timestamp storage differs")
    if columns["trade_date"]["DATA_TYPE"] != "date":
        raise ValueError("minute canonical date storage differs")
    return _layout({field: [int(columns[field]["NUMERIC_PRECISION"]),
                            int(columns[field]["NUMERIC_SCALE"])] for field in NUMBERS})


def normalized_tuple(row, *, layout, trade_date, run_id):
    """The expected tuple is computed before INSERT, from captured input."""
    _layout(layout)
    code = str(row.get("stock_code") or "")
    if not re.fullmatch(r"[0-9]{6}", code):
        raise ValueError("minute canonical stock code differs")
    stamps = {}
    for field in ("trade_time", "source_time"):
        stamp = datetime.fromisoformat(str(row.get(field)))
        if stamp.tzinfo is not None or stamp.microsecond or stamp.date().isoformat() != trade_date:
            raise ValueError("minute canonical timestamp differs")
        stamps[field] = stamp.isoformat(sep=" ", timespec="seconds")
    if (str(row.get("trade_date")) != trade_date or stamps["trade_time"] != stamps["source_time"]
            or row.get("data_source") != PROVIDER or row.get("batch_id") != run_id):
        raise ValueError("minute canonical input provenance differs")
    numbers = []
    for field in NUMBERS:
        value = row.get(field)
        if value is None and field in {"avg_price", "change", "change_pct"}:
            numbers.append(None)
            continue
        precision, scale = layout[field]
        try:
            number = Decimal(str(value))
            if isinstance(value, bool) or not number.is_finite():
                raise ValueError("nonfinite or boolean minute value")
            with localcontext() as context:
                context.prec = 80
                number = number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
                if abs(number) >= Decimal(10) ** (precision - scale):
                    raise ValueError("minute value exceeds physical precision")
            if number == 0:
                number = abs(number)
            numbers.append(format(number, f".{scale}f"))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("minute canonical numeric value differs") from exc
    return [code, stamps["trade_time"], trade_date, *numbers,
            row["data_source"], stamps["source_time"], run_id]


def input_content_rows(rows, *, layout, trade_date, run_id):
    groups = {}
    for row in rows:
        values = normalized_tuple(row, layout=layout, trade_date=trade_date, run_id=run_id)
        # MySQL JSON_ARRAY prints a space after commas. Every non-null element
        # is an exact string, so numeric JSON formatting cannot change the root.
        encoded = json.dumps(values, ensure_ascii=False, allow_nan=False).encode("utf-8")
        groups.setdefault(values[0], []).append((values[1], hashlib.sha256(encoded).hexdigest()))
    result = []
    for code, values in sorted(groups.items()):
        values.sort()
        if len({stamp for stamp, _ in values}) != len(values):
            raise ValueError("duplicate minute canonical input time")
        result.append({"stock_code": code, "row_count": len(values),
                       "row_hash": hashlib.sha256("".join(value for _, value in values).encode()).hexdigest()})
    return result


def content_proof(rows, *, layout, manifest, entities):
    _layout(layout)
    traded = sorted(row["stock_code"] for row in entities if row["expected_state"] == "TRADED")
    counts = {row["stock_code"]: row["bar_count"] for row in entities if row["expected_state"] == "TRADED"}
    ordered = sorted(({"stock_code": row["stock_code"], "row_count": int(row["row_count"]),
                       "row_hash": row["row_hash"]} for row in rows), key=lambda row: row["stock_code"])
    if (not traded or [row["stock_code"] for row in ordered] != traded
            or any(row["row_count"] != counts[row["stock_code"]]
                   or not re.fullmatch(r"[0-9a-f]{64}", str(row["row_hash"])) for row in ordered)
            or sum(row["row_count"] for row in ordered) != manifest["bar_count"]):
        raise ValueError("minute canonical content inventory differs")
    return {"schema": SCHEMA, "trade_date": manifest["trade_date"], "run_id": manifest["run_id"],
            "coverage_manifest_hash": manifest["manifest_hash"], "numeric_layout": layout,
            "row_count": manifest["bar_count"], "stock_count": len(traded),
            "stock_set_hash": canonical_digest(traded), "content_root_sha256": canonical_digest(ordered)}


def validate_content_proof(proof, *, manifest, entities):
    traded = sorted(row["stock_code"] for row in entities if row["expected_state"] == "TRADED")
    if (not isinstance(proof, dict) or proof.get("schema") != SCHEMA
            or proof.get("trade_date") != manifest["trade_date"]
            or proof.get("run_id") != manifest["run_id"]
            or proof.get("coverage_manifest_hash") != manifest["manifest_hash"]
            or proof.get("row_count") != manifest["bar_count"]
            or proof.get("stock_count") != len(traded)
            or proof.get("stock_set_hash") != canonical_digest(traded)
            or not re.fullmatch(r"[0-9a-f]{64}", str(proof.get("content_root_sha256") or ""))):
        raise ValueError("minute canonical content proof differs")
    _layout(proof.get("numeric_layout"))
    return proof


def sql_row_hash():
    return "SHA2(CAST(JSON_ARRAY(" + ",".join(f"CAST(`{field}` AS CHAR)" for field in FIELDS) + ") AS CHAR),256)"


def read_content_proof(connection, *, table, manifest, entities, layout):
    if not re.fullmatch(r"sm_stock_minute(?:_qmt_stage_[0-9]+)?", table):
        raise ValueError("minute canonical content table differs")
    if load_numeric_layout(connection) != layout:
        raise ValueError("minute canonical physical layout changed")
    codes = sorted(item["stock_code"] for item in entities)
    row_hash = sql_row_hash()
    query = text(f"""
        SELECT stock_code,COUNT(*) row_count,
               SHA2(GROUP_CONCAT({row_hash} ORDER BY trade_time SEPARATOR ''),256) row_hash,
               LENGTH(GROUP_CONCAT({row_hash} ORDER BY trade_time SEPARATOR '')) hashed_bytes
          FROM `{table}` WHERE stock_code IN :codes AND trade_time>=:day AND trade_time<:next_day
         GROUP BY stock_code ORDER BY stock_code
    """).bindparams(bindparam("codes", expanding=True))
    original = int(connection.exec_driver_sql("SELECT @@SESSION.group_concat_max_len").scalar_one())
    rows = []
    try:
        connection.exec_driver_sql("SET SESSION group_concat_max_len=32768")
        for offset in range(0, len(codes), 100):
            rows.extend(connection.execute(query, {"day": manifest["trade_date"],
                                                    "next_day": (date.fromisoformat(manifest["trade_date"]) + timedelta(days=1)).isoformat(),
                                                    "codes": codes[offset:offset + 100]}).mappings().all())
    finally:
        connection.exec_driver_sql(f"SET SESSION group_concat_max_len={original}")
    if any(row["hashed_bytes"] != row["row_count"] * 64 for row in rows):
        raise ValueError("minute canonical content hash was truncated")
    return content_proof(rows, layout=layout, manifest=manifest, entities=entities)
