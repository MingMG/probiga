"""Exact-session no-trade evidence from the existing fixed MyQuant action."""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo
from hashlib import sha256
import json
import re

from integrations.myquant.bridge import (
    UPPER_LIMIT_HISTORY_ACTION, _validate_upper_limit_result, to_gm_symbol,
)
from server.common.qmt_attestation_contract import canonical_digest


KIND = "MYQUANT_HISTORICAL_SUSPENSION"
SCHEMA = "probiga.myquant-historical-suspension.v1"
MAX_SECURITIES = 80
_CAPTURE_KEYS = frozenset({"raw_stdout", "raw_stdout_sha256", "canonical_request_json",
                           "canonical_request_sha256", "worker_sha256"})


def _response(response, *, day, cutoff):
    date.fromisoformat(day)
    if not isinstance(response, dict):
        raise ValueError("historical suspension response is unavailable")
    raw = response.get("raw_stdout")
    request = response.get("canonical_request_json")
    if (not isinstance(raw, str) or not 0 < len(raw.encode()) <= 256 * 1024
            or not isinstance(request, str) or not 0 < len(request.encode()) <= 8192
            or sha256(raw.encode()).hexdigest() != response.get("raw_stdout_sha256")
            or sha256(request.encode()).hexdigest() != response.get("canonical_request_sha256")
            or not re.fullmatch(r"[0-9a-f]{64}", str(response.get("worker_sha256") or ""))):
        raise ValueError("historical suspension transport identity differs")
    payload = json.loads(raw)
    symbols = payload.get("requested_symbols")
    if (payload.get("ok") is not True or not isinstance(symbols, list) or not 1 <= len(symbols) <= MAX_SECURITIES
            or any(not isinstance(symbol, str) or not re.fullmatch(r"(?:SHSE\.6|SZSE\.[03])[0-9]{5}", symbol) for symbol in symbols)
            or len(symbols) != len(set(symbols))
            or payload != {k: v for k, v in response.items() if k not in _CAPTURE_KEYS}
            or json.loads(request) != {"action": UPPER_LIMIT_HISTORY_ACTION,
                "symbols": symbols, "start_date": day, "end_date": day}):
        raise ValueError("historical suspension exact request differs")
    _validate_upper_limit_result(payload, requested_symbols=symbols, start_date=day, end_date=day)
    if len(payload["rows"]) != len(symbols):
        raise ValueError("historical suspension session coverage differs")
    captured = datetime.fromisoformat(payload["captured_at"])
    known = cutoff if isinstance(cutoff, datetime) else datetime.fromisoformat(cutoff)
    if (captured.tzinfo is None or known.tzinfo is None or captured > known
            or captured.astimezone(ZoneInfo("Asia/Shanghai")).date() <= date.fromisoformat(day)):
        raise ValueError("historical suspension capture exceeds knowledge cutoff")
    return {row["symbol"]: row for row in payload["rows"]}


def build_suspension_evidence(response, *, codes, day, cutoff):
    rows = _response(response, day=day, cutoff=cutoff)
    expected = {to_gm_symbol(code): code for code in codes}
    if None in expected or set(rows) != set(expected):
        raise ValueError("historical suspension security coverage differs")
    result = {}
    for symbol, code in expected.items():
        if rows[symbol]["is_suspended"] != 1:
            raise ValueError(f"historical suspension not confirmed: {day} {code}")
        core = {"kind": KIND, "schema": SCHEMA, "stock_code": code,
                "target_date": day, "response": response}
        result[code] = {**core, "evidence_sha256": canonical_digest(core)}
    return result


def validate_suspension_evidence(evidence, *, stock_code, day, cutoff):
    if (not isinstance(evidence, dict) or set(evidence) != {
            "kind", "schema", "stock_code", "target_date", "response", "evidence_sha256"}
            or evidence.get("kind") != KIND or evidence.get("schema") != SCHEMA
            or evidence.get("stock_code") != stock_code or evidence.get("target_date") != day
            or canonical_digest({k: v for k, v in evidence.items() if k != "evidence_sha256"})
               != evidence.get("evidence_sha256")):
        raise ValueError("historical suspension evidence identity differs")
    rows = _response(evidence["response"], day=day, cutoff=cutoff)
    row = rows.get(to_gm_symbol(stock_code))
    if row is None or row["is_suspended"] != 1:
        raise ValueError("historical suspension state is not confirmed")
    return evidence
