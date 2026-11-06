"""Independently corroborated public bars for exact historical daily gaps."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
import re
import subprocess
import sys

from server.common.qmt_attestation_contract import canonical_digest

SCHEMA = "probiga.historical-public-daily-bar.v1"
SINA_PROVIDER = "sina_history"
SINA_QUALITY = "SOURCE_VERIFIED"
TENCENT_PROVIDER = "tencent_qt_verified"
TENCENT_MAX_VOLUME_DIFFERENCE = 99  # Shares. Tencent's raw daily series reports lots.


def _identity(source: str, observed_at: str, row: dict) -> dict:
    payload = {"provider": source, "observed_at": observed_at, "row": row}
    return {**payload, "source_row_sha256": canonical_digest(payload)}


def _read_identity(value: dict, source: str, cutoff: datetime) -> dict:
    try:
        observed = datetime.fromisoformat(value["observed_at"])
        if (observed.tzinfo or observed > cutoff or observed.date().isoformat() < value["row"]["trade_date"]
                or value != _identity(source, value["observed_at"], value["row"])):
            raise ValueError("historical public bar source identity or time differs")
        return value["row"]
    except (KeyError, TypeError) as exc:
        raise ValueError("historical public bar source identity differs") from exc


def verify_public_bar(value: dict, *, stock_code: str, day: str, previous_day: str,
                      cutoff: datetime) -> dict:
    from server.common.qmt_datadir_daily_coverage import canonical_bar

    if (value.get("schema") != SCHEMA or not previous_day < day
            or set(value) != {"schema", "sina", "tencent", "tencent_previous"}):
        raise ValueError("historical public bar evidence contract differs")
    sina = _read_identity(value["sina"], SINA_PROVIDER, cutoff)
    tencent = _read_identity(value["tencent"], TENCENT_PROVIDER, cutoff)
    previous = _read_identity(value["tencent_previous"], TENCENT_PROVIDER, cutoff)
    if (any(row["stock_code"] != stock_code for row in (sina, tencent, previous))
            or sina["trade_date"] != day or tencent["trade_date"] != day
            or previous["trade_date"] != previous_day or canonical_bar(sina) != sina
            or any(sina[key] != tencent[key] for key in ("open", "close", "high", "low"))
            or sina["pre_close"] != previous["close"]
            or Decimal(sina["volume"]) <= 0 or Decimal(sina["amount"]) <= 0
            or not 0 <= abs(Decimal(sina["volume"]) - Decimal(tencent["volume"])) <= TENCENT_MAX_VOLUME_DIFFERENCE):
        raise ValueError("historical public bar sources disagree")
    return sina


def _tencent_row(row: dict, day: str) -> dict:
    return {"stock_code": row["stock_code"], "trade_date": day,
            **{key: format(Decimal(str(row[key])).quantize(Decimal("0.000001")), "f")
               for key in ("open", "close", "high", "low", "volume")}}


def _read_public_bars(codes: list[str], *, day: str, previous_day: str) -> dict[str, dict]:
    from server.common.qmt_datadir_daily_coverage import canonical_bar
    from tools.sync_free_stock_daily import fetch_history_day, fetch_sina_history_day

    primary, primary_receipt = fetch_sina_history_day(codes, target_date=day)
    secondary, secondary_receipt = fetch_history_day(codes, target_date=day, timeout=8)
    previous, previous_receipt = fetch_history_day(codes, target_date=previous_day, timeout=8)
    observations = []
    for frame, receipt, source, target in (
        (primary, primary_receipt, SINA_PROVIDER, day),
        (secondary, secondary_receipt, TENCENT_PROVIDER, day),
        (previous, previous_receipt, TENCENT_PROVIDER, previous_day),
    ):
        entries = frame.to_dict("records")
        if len(entries) != len(codes) or {row["stock_code"] for row in entries} != set(codes):
            raise ValueError("historical public daily coverage differs")
        observations.append({row["stock_code"]: _identity(
            source, receipt["captured_at"], canonical_bar(row) if source == SINA_PROVIDER else _tencent_row(row, target)
        ) for row in entries})
    return {code: {"schema": SCHEMA, "sina": observations[0][code], "tencent": observations[1][code],
                   "tencent_previous": observations[2][code]} for code in codes}


def _request(codes, day, previous_day) -> None:
    if (not isinstance(codes, list) or not 1 <= len(codes) <= 8 or len(set(codes)) != len(codes)
            or any(not re.fullmatch(r"\d{6}", code) for code in codes)
            or date.fromisoformat(previous_day) >= date.fromisoformat(day)):
        raise ValueError("historical public daily bar request bounds differ")


def load_public_bars(codes: list[str], *, day: str, previous_day: str, timeout_seconds: float) -> dict[str, dict]:
    _request(codes, day, previous_day)
    if not 0 < timeout_seconds <= 90:
        raise ValueError("historical public daily bar deadline differs")
    from server.common.process_env import build_child_env

    root = Path(__file__).resolve().parents[2]
    try:
        worker = subprocess.run(
            [sys.executable, "-B", "-m", __name__], cwd=root, env=build_child_env(root),
            input=json.dumps({"codes": codes, "day": day, "previous_day": previous_day}),
            capture_output=True, text=True, encoding="utf-8", timeout=timeout_seconds,
            **({"creationflags": subprocess.CREATE_NO_WINDOW} if sys.platform == "win32" else {}),
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("historical public daily source total deadline exceeded") from None
    if worker.returncode or len(worker.stdout.encode("utf-8")) > 64 * 1024:
        raise RuntimeError("historical public daily source worker failed")
    result = json.loads(worker.stdout)
    if set(result) != set(codes):
        raise ValueError("historical public daily source worker inventory differs")
    return result


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    args = json.load(sys.stdin)
    _request(**args)
    print(json.dumps(_read_public_bars(**args), ensure_ascii=False))
