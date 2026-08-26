"""Exact contracts for Eastmoney, Sina, and Xueqiu hot-rank collectors."""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)


HOT_RANK_RESULT_SCHEMA = "probiga.hot-rank-source-result.v2"
HOT_POP_EAST_TASK_TYPE = "hot_pop_east"
HOT_RANK_SINA_TASK_TYPE = "hot_rank_sina"
HOT_RANK_XQ_TASK_TYPE = "fetch_hot_rank_xq"
HOT_RANK_SOURCE_TASK_TYPES = frozenset({
    HOT_POP_EAST_TASK_TYPE,
    HOT_RANK_SINA_TASK_TYPE,
    HOT_RANK_XQ_TASK_TYPE,
})

HOT_RANK_DATASETS = {
    HOT_POP_EAST_TASK_TYPE: "eastmoney_popularity_top100",
    HOT_RANK_SINA_TASK_TYPE: "sina_attention_top100",
    HOT_RANK_XQ_TASK_TYPE: "xueqiu_hot_stock_top100",
}
HOT_RANK_CURRENT_PROVIDERS = {
    HOT_POP_EAST_TASK_TYPE: "eastmoney.getAllCurrentList",
    HOT_RANK_SINA_TASK_TYPE: "sina.Market_Center.getHQNodeData",
    HOT_RANK_XQ_TASK_TYPE: "xueqiu.hot_stock.list",
}
HOT_RANK_READY_TIMES = {
    HOT_POP_EAST_TASK_TYPE: time(17, 14),
    HOT_RANK_SINA_TASK_TYPE: time(17, 16),
    HOT_RANK_XQ_TASK_TYPE: time(17, 10),
}

CURRENT_SNAPSHOT_ONLY = "CURRENT_SNAPSHOT_ONLY"
AUTHORITATIVE_DATED_HISTORY = "AUTHORITATIVE_DATED_HISTORY"
EAST_HISTORY_PROVIDER = "eastmoney.getHisList"
EAST_HISTORY_EVIDENCE_SCHEMA = (
    "probiga.eastmoney-getHisList-response-evidence.v1"
)
SINA_ATTENTION_DATA_BLOCK_REASON = (
    "PROVIDER_ATTENTION_SEMANTICS_UNVERIFIABLE"
)
EXACT_TOP_ROWS = 100

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_A_SHARE_CODE = re.compile(r"(?:0|3|4|6|8|9)[0-9]{5}\Z")

_TEXT_FIELDS = {
    HOT_POP_EAST_TASK_TYPE: (
        "short_name", "pop_tag", "concept_tag",
    ),
    HOT_RANK_SINA_TASK_TYPE: ("short_name",),
    HOT_RANK_XQ_TASK_TYPE: (
        "short_name", "sector", "exchange",
    ),
}
_INTEGER_FIELDS = {
    HOT_POP_EAST_TASK_TYPE: ("rank_change", "his_rank"),
    HOT_RANK_SINA_TASK_TYPE: (),
    HOT_RANK_XQ_TASK_TYPE: ("followers", "increment", "diff"),
}
_NUMBER_FIELDS = {
    HOT_POP_EAST_TASK_TYPE: (
        "price", "price_change", "change_pct", "hot_value",
    ),
    HOT_RANK_SINA_TASK_TYPE: (
        "price", "price_change", "change_pct", "amount", "volume",
        "market_capital", "turnover_ratio",
    ),
    HOT_RANK_XQ_TASK_TYPE: (
        "current", "percent", "chg", "amount", "market_capital",
    ),
}
_TABLES = {
    HOT_POP_EAST_TASK_TYPE: "st_hot_pop_rank_east",
    HOT_RANK_SINA_TASK_TYPE: "st_hot_rank_sina",
    HOT_RANK_XQ_TASK_TYPE: "st_hot_rank_xq",
}
_SELECT_COLUMNS = {
    HOT_POP_EAST_TASK_TYPE: (
        "snapshot_date, `rank`, stock_code, short_name, rank_change, his_rank, "
        "price, price_change, change_pct, hot_value, pop_tag, concept_tag, "
        "etl_sync_at"
    ),
    HOT_RANK_SINA_TASK_TYPE: (
        "snapshot_date, `rank`, stock_code, short_name, price, price_change, "
        "change_pct, amount, volume, market_capital, turnover_ratio, etl_sync_at"
    ),
    HOT_RANK_XQ_TASK_TYPE: (
        "snapshot_date, `rank`, stock_code, short_name, current, percent, chg, "
        "amount, market_capital, followers, sector, exchange, increment, diff, "
        "etl_sync_at"
    ),
}


class HotRankDataBlocked(RuntimeError):
    """The provider response cannot safely be labelled with the requested date."""


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def with_receipt_id(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("receipt_id", None)
    result["receipt_id"] = canonical_hash(result)
    return result


def receipt_id_valid(payload: Mapping[str, Any]) -> bool:
    supplied = str(payload.get("receipt_id") or "").lower()
    unsigned = dict(payload)
    unsigned.pop("receipt_id", None)
    return (
        _HEX64.fullmatch(supplied) is not None
        and supplied == canonical_hash(unsigned)
    )


def shanghai_now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(_SHANGHAI)
    if current.tzinfo is not None:
        current = current.astimezone(_SHANGHAI).replace(tzinfo=None)
    return current.replace(microsecond=0)


def require_current_capture_window(
    engine,
    *,
    task_type: str,
    requested_date: str,
    now: datetime | None = None,
) -> datetime:
    """Allow a current-only response only for today's proven closed session."""

    if task_type not in HOT_RANK_READY_TIMES:
        raise ValueError("unknown hot-rank task type")
    current = shanghai_now(now)
    if requested_date != current.date().isoformat():
        raise HotRankDataBlocked("CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED")
    ready_time = HOT_RANK_READY_TIMES[task_type]
    closed_date = authoritative_closed_trade_date(
        engine,
        now=current,
        close_ready_time=ready_time,
    )
    if closed_date != requested_date:
        reason = (
            "CURRENT_SESSION_NOT_CLOSED"
            if current.time() < ready_time
            else "REQUEST_DATE_NOT_OPEN_SESSION"
        )
        raise HotRankDataBlocked(reason)
    return current


def _number(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not number.is_finite():
        return None
    normalized = format(number.normalize(), "f")
    return "0" if normalized in {"-0", "-0.0"} else normalized


def _integer(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date_text(value: Any) -> str:
    return str(value or "")[:10]


def _timestamp_text(value: Any) -> str:
    if value is None:
        return ""
    raw = (
        value.isoformat(sep=" ", timespec="seconds")
        if hasattr(value, "isoformat")
        else str(value)
    )
    return raw.replace("T", " ").split(".", 1)[0]


def canonical_rank_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    task_type: str,
    include_snapshot_date: bool = False,
) -> list[dict[str, Any]]:
    if task_type not in HOT_RANK_DATASETS:
        raise ValueError("unknown hot-rank task type")
    result: list[dict[str, Any]] = []
    for source in rows:
        row: dict[str, Any] = {
            "rank": int(source.get("rank") or 0),
            "stock_code": str(source.get("stock_code") or "").strip().zfill(6),
        }
        if include_snapshot_date:
            row["snapshot_date"] = _date_text(source.get("snapshot_date"))
        for field in _TEXT_FIELDS[task_type]:
            row[field] = str(source.get(field) or "").strip()
        for field in _INTEGER_FIELDS[task_type]:
            row[field] = _integer(source.get(field))
        for field in _NUMBER_FIELDS[task_type]:
            row[field] = _number(source.get(field))
        result.append(row)
    return sorted(result, key=lambda row: (row["rank"], row["stock_code"]))


def validate_rank_inventory(
    rows: Sequence[Mapping[str, Any]],
    *,
    task_type: str,
    target_date: str | None = None,
) -> dict[str, Any]:
    provider = canonical_rank_rows(rows, task_type=task_type)
    if len(provider) != EXACT_TOP_ROWS:
        raise RuntimeError(
            f"{task_type} exact Top100 inventory differs: rows={len(provider)}"
        )
    codes = [row["stock_code"] for row in provider]
    ranks = [int(row["rank"]) for row in provider]
    if (
        len(codes) != len(set(codes))
        or any(_A_SHARE_CODE.fullmatch(code) is None for code in codes)
        or "000000" in codes
        or ranks != list(range(1, EXACT_TOP_ROWS + 1))
        or len(ranks) != len(set(ranks))
    ):
        raise RuntimeError(f"{task_type} code/rank inventory differs")
    persisted = None
    if target_date is not None:
        persisted = canonical_rank_rows(
            rows,
            task_type=task_type,
            include_snapshot_date=True,
        )
        dates = {row["snapshot_date"] for row in persisted}
        if dates != {target_date}:
            raise RuntimeError(
                f"{task_type} persisted date differs: observed={sorted(dates)}"
            )
    return {
        "row_count": EXACT_TOP_ROWS,
        "provider_payload_sha256": canonical_hash(provider),
        "persisted_row_sha256": (
            canonical_hash(persisted) if persisted is not None else None
        ),
        "code_set_sha256": canonical_hash(sorted(codes)),
        "rank_set_sha256": canonical_hash(ranks),
    }


def validate_east_history_date_evidence(
    rows: Sequence[Mapping[str, Any]],
    *,
    target_date: str,
) -> dict[str, Any]:
    """Validate and retain the provider's exact dated response identity.

    A digest of locally relabelled ranks is not provider date evidence.  Each
    item must carry the exact ``calcTime`` and ``rank`` returned by
    ``getHisList`` plus the security code used for that request.  The compact
    canonical evidence is small enough to remain in the scheduler receipt and
    can therefore be re-bound to the persisted rank/code batch later.
    """

    canonical: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise RuntimeError("Eastmoney historical response evidence differs")
        response = row.get("provider_response")
        if not isinstance(response, Mapping):
            raise RuntimeError(
                "Eastmoney historical raw provider response is missing"
            )
        calc_time = str(response.get("calcTime") or "").strip()
        if re.fullmatch(
            r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)?",
            calc_time,
        ) is None:
            raise RuntimeError("Eastmoney historical raw calcTime differs")
        try:
            provider_rank = int(response.get("rank"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Eastmoney historical raw provider rank differs"
            ) from exc
        if row.get("rank") is not None:
            try:
                outer_rank = int(row.get("rank"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "Eastmoney historical derived/provider rank differs"
                ) from exc
            if outer_rank != provider_rank:
                raise RuntimeError(
                    "Eastmoney historical derived/provider rank differs"
                )
        src_security_code = str(
            row.get("request_src_security_code") or ""
        ).strip().upper()
        if re.fullmatch(r"(?:SH|SZ|BJ)[0-9]{6}", src_security_code) is None:
            raise RuntimeError(
                "Eastmoney historical request security identity differs"
            )
        stock_code = str(row.get("stock_code") or "").strip().zfill(6)
        if stock_code != src_security_code[2:]:
            raise RuntimeError(
                "Eastmoney historical request/response stock identity differs"
            )
        canonical.append({
            "request_src_security_code": src_security_code,
            "stock_code": stock_code,
            "provider_calc_time": calc_time,
            "provider_rank": provider_rank,
        })
    canonical.sort(
        key=lambda row: (row["provider_rank"], row["stock_code"])
    )
    if len(canonical) != EXACT_TOP_ROWS:
        raise RuntimeError(
            "Eastmoney historical date evidence is not an exact Top100"
        )
    if {
        str(row["provider_calc_time"])[:10] for row in canonical
    } != {target_date}:
        raise RuntimeError("Eastmoney historical calcTime differs from target date")
    ranks = [int(row["provider_rank"]) for row in canonical]
    codes = [row["stock_code"] for row in canonical]
    if (
        ranks != list(range(1, EXACT_TOP_ROWS + 1))
        or len(codes) != len(set(codes))
        or any(_A_SHARE_CODE.fullmatch(code) is None for code in codes)
    ):
        raise RuntimeError("Eastmoney historical date evidence identity differs")
    return {
        "provider_evidence_schema": EAST_HISTORY_EVIDENCE_SCHEMA,
        "provider_date_field": "calcTime",
        "provider_date_count": EXACT_TOP_ROWS,
        "provider_date_sha256": canonical_hash([
            {
                "calcTime": row["provider_calc_time"],
                "rank": row["provider_rank"],
                "stock_code": row["stock_code"],
            }
            for row in canonical
        ]),
        "provider_response_evidence": canonical,
        "provider_response_evidence_sha256": canonical_hash(canonical),
    }


def _east_history_receipt_evidence(
    payload: Mapping[str, Any],
    *,
    target_date: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    evidence = payload.get("provider_response_evidence")
    if not isinstance(evidence, Sequence) or isinstance(
        evidence, (str, bytes, bytearray)
    ):
        raise ValueError("Eastmoney historical raw response evidence is missing")
    try:
        reconstructed = validate_east_history_date_evidence(
            [
                {
                    "request_src_security_code": item.get(
                        "request_src_security_code"
                    ),
                    "stock_code": item.get("stock_code"),
                    "provider_response": {
                        "calcTime": item.get("provider_calc_time"),
                        "rank": item.get("provider_rank"),
                    },
                }
                if isinstance(item, Mapping)
                else {}
                for item in evidence
            ],
            target_date=target_date,
        )
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    for field in (
        "provider_evidence_schema",
        "provider_date_field",
        "provider_date_count",
        "provider_date_sha256",
        "provider_response_evidence_sha256",
    ):
        if payload.get(field) != reconstructed[field]:
            raise ValueError(
                f"Eastmoney historical receipt evidence differs: {field}"
            )
    canonical = reconstructed["provider_response_evidence"]
    if list(evidence) != canonical:
        raise ValueError("Eastmoney historical response evidence is non-canonical")
    if canonical_hash(sorted(row["stock_code"] for row in canonical)) != payload.get(
        "code_set_sha256"
    ):
        raise ValueError("Eastmoney historical evidence code set differs")
    if canonical_hash([row["provider_rank"] for row in canonical]) != payload.get(
        "rank_set_sha256"
    ):
        raise ValueError("Eastmoney historical evidence rank set differs")
    return reconstructed, canonical


def _hot_rank_batch_id(payload: Mapping[str, Any]) -> str:
    return canonical_hash({
        "schema": HOT_RANK_RESULT_SCHEMA,
        "task_type": payload.get("task_type"),
        "requested_date": payload.get("requested_date"),
        "provider": payload.get("provider"),
        "source_capability": payload.get("source_capability"),
        "batch_at": payload.get("batch_at"),
        "provider_payload_sha256": payload.get("provider_payload_sha256"),
        "persisted_row_sha256": payload.get("persisted_row_sha256"),
        "code_set_sha256": payload.get("code_set_sha256"),
        "rank_set_sha256": payload.get("rank_set_sha256"),
        "provider_date_sha256": payload.get("provider_date_sha256"),
        "provider_response_evidence_sha256": payload.get(
            "provider_response_evidence_sha256"
        ),
    })


def batch_timestamp(rows: Sequence[Mapping[str, Any]]) -> str:
    values = {_timestamp_text(row.get("etl_sync_at")) for row in rows}
    if len(values) != 1 or not next(iter(values), ""):
        raise RuntimeError("hot-rank persisted rows span multiple publish batches")
    return next(iter(values))


def build_pass_receipt(
    *,
    task_type: str,
    provider: str,
    source_capability: str,
    requested_date: str,
    started_at: datetime,
    captured_at: datetime,
    published_at: datetime,
    batch_at: str,
    inventory: Mapping[str, Any],
    date_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if task_type not in HOT_RANK_DATASETS:
        raise ValueError("unknown hot-rank task type")
    if task_type == HOT_RANK_SINA_TASK_TYPE:
        raise ValueError(
            "Sina attention semantics are unverifiable; PASS receipt prohibited"
        )
    core = {
        "schema": HOT_RANK_RESULT_SCHEMA,
        "status": "PASS",
        "task_type": task_type,
        "dataset": HOT_RANK_DATASETS[task_type],
        "provider": provider,
        "source_capability": source_capability,
        "requested_date": requested_date,
        "data_date": requested_date,
        "started_at": shanghai_now(started_at).isoformat(sep=" "),
        "captured_at": shanghai_now(captured_at).isoformat(sep=" "),
        "published_at": shanghai_now(published_at).isoformat(sep=" "),
        "batch_at": batch_at,
        **dict(inventory),
        **dict(date_evidence or {}),
    }
    core["batch_id"] = _hot_rank_batch_id(core)
    receipt = with_receipt_id(core)
    validate_receipt_shape(receipt)
    return receipt


def build_blocked_receipt(
    *,
    task_type: str,
    requested_date: str,
    started_at: datetime,
    reason: str,
) -> dict[str, Any]:
    provider = HOT_RANK_CURRENT_PROVIDERS[task_type]
    receipt = with_receipt_id({
        "schema": HOT_RANK_RESULT_SCHEMA,
        "status": "DATA_BLOCKED",
        "task_type": task_type,
        "dataset": HOT_RANK_DATASETS[task_type],
        "provider": provider,
        "source_capability": CURRENT_SNAPSHOT_ONLY,
        "requested_date": requested_date,
        "started_at": shanghai_now(started_at).isoformat(sep=" "),
        "reason": str(reason or "CURRENT_SNAPSHOT_UNAVAILABLE"),
        "row_count": 0,
    })
    validate_receipt_shape(receipt)
    return receipt


def parse_hot_rank_receipt(output: str | None) -> Mapping[str, Any] | None:
    """Extract exactly one hot-rank receipt, including nested bridge output."""

    candidates: list[Mapping[str, Any]] = []
    visited_strings: set[str] = set()

    def visit(value: Any, depth: int = 0) -> None:
        if depth > 4:
            return
        if isinstance(value, Mapping):
            if value.get("schema") == HOT_RANK_RESULT_SCHEMA:
                candidates.append(value)
            for nested in value.values():
                if isinstance(nested, (Mapping, list, tuple, str)):
                    visit(nested, depth + 1)
            return
        if isinstance(value, (list, tuple)):
            for nested in value:
                visit(nested, depth + 1)
            return
        if not isinstance(value, str):
            return
        source = value.strip()
        if not source or source in visited_strings:
            return
        visited_strings.add(source)
        for line in source.splitlines():
            candidate = line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                visit(json.loads(candidate), depth + 1)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

    visit(str(output or ""))
    unique: dict[str, Mapping[str, Any]] = {}
    for payload in candidates:
        key = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        unique[key] = payload
    return next(iter(unique.values())) if len(unique) == 1 else None


def _receipt_provider_capability_valid(payload: Mapping[str, Any]) -> bool:
    task_type = str(payload.get("task_type") or "")
    provider = str(payload.get("provider") or "")
    capability = str(payload.get("source_capability") or "")
    if task_type == HOT_POP_EAST_TASK_TYPE:
        return (provider, capability) in {
            (
                HOT_RANK_CURRENT_PROVIDERS[HOT_POP_EAST_TASK_TYPE],
                CURRENT_SNAPSHOT_ONLY,
            ),
            (EAST_HISTORY_PROVIDER, AUTHORITATIVE_DATED_HISTORY),
        }
    return (
        task_type in {HOT_RANK_SINA_TASK_TYPE, HOT_RANK_XQ_TASK_TYPE}
        and provider == HOT_RANK_CURRENT_PROVIDERS[task_type]
        and capability == CURRENT_SNAPSHOT_ONLY
    )


def validate_receipt_shape(
    payload: Mapping[str, Any],
    *,
    task_type: str | None = None,
) -> None:
    observed_task = str(payload.get("task_type") or "")
    if (
        payload.get("schema") != HOT_RANK_RESULT_SCHEMA
        or observed_task not in HOT_RANK_SOURCE_TASK_TYPES
        or (task_type is not None and observed_task != task_type)
        or payload.get("dataset") != HOT_RANK_DATASETS.get(observed_task)
        or not receipt_id_valid(payload)
        or not _receipt_provider_capability_valid(payload)
    ):
        raise ValueError("hot-rank receipt identity differs")
    status = str(payload.get("status") or "")
    if observed_task == HOT_RANK_SINA_TASK_TYPE and (
        status != "DATA_BLOCKED"
        or payload.get("reason") != SINA_ATTENTION_DATA_BLOCK_REASON
    ):
        raise ValueError(
            "Sina receipt must be the permanent provider-semantics block"
        )
    if status == "DATA_BLOCKED":
        if (
            payload.get("source_capability") != CURRENT_SNAPSHOT_ONLY
            or int(
                payload.get("row_count")
                if payload.get("row_count") is not None
                else -1
            ) != 0
            or not str(payload.get("reason") or "")
        ):
            raise ValueError("hot-rank blocked receipt differs")
        datetime.fromisoformat(str(payload.get("started_at") or ""))
        return
    if status != "PASS":
        raise ValueError("hot-rank receipt status differs")
    if (
        payload.get("requested_date") != payload.get("data_date")
        or int(payload.get("row_count") or 0) != EXACT_TOP_ROWS
        or any(
            _HEX64.fullmatch(str(payload.get(field) or "").lower()) is None
            for field in (
                "provider_payload_sha256",
                "persisted_row_sha256",
                "code_set_sha256",
                "rank_set_sha256",
                "batch_id",
            )
        )
        or payload.get("batch_id") != _hot_rank_batch_id(payload)
    ):
        raise ValueError("hot-rank PASS receipt inventory differs")
    requested_date = datetime.strptime(
        str(payload.get("requested_date") or ""),
        "%Y-%m-%d",
    ).date()
    started_at = datetime.fromisoformat(str(payload.get("started_at") or ""))
    captured_at = datetime.fromisoformat(str(payload.get("captured_at") or ""))
    published_at = datetime.fromisoformat(str(payload.get("published_at") or ""))
    batch_at = datetime.fromisoformat(str(payload.get("batch_at") or ""))
    if not (started_at <= captured_at <= published_at) or batch_at != captured_at:
        raise ValueError("hot-rank receipt timestamps differ")
    if payload.get("source_capability") == CURRENT_SNAPSHOT_ONLY:
        if requested_date != captured_at.date():
            raise ValueError("current hot-rank receipt date differs from capture date")
    else:
        if (
            payload.get("provider") != EAST_HISTORY_PROVIDER
            or requested_date >= captured_at.date()
        ):
            raise ValueError("Eastmoney historical receipt date evidence differs")
        _east_history_receipt_evidence(
            payload,
            target_date=requested_date.isoformat(),
        )


def validate_persisted_hot_rank_receipt(
    engine,
    payload: Mapping[str, Any],
    started_at: datetime,
    now: datetime,
    expected_target_date: str | None = None,
) -> dict[str, Any]:
    """Independently re-read the exact DB batch described by a PASS receipt."""

    validate_receipt_shape(payload)
    if payload.get("status") != "PASS":
        raise ValueError("cannot DB-verify a non-PASS hot-rank receipt")
    task_type = str(payload["task_type"])
    target_date = str(payload["requested_date"])
    expected = (
        str(expected_target_date)[:10]
        if expected_target_date is not None
        else None
    )
    if expected is not None and target_date != expected:
        raise RuntimeError(
            "hot-rank receipt target date differs from scheduler target"
        )
    captured_at = datetime.fromisoformat(str(payload["captured_at"]))
    published_at = datetime.fromisoformat(str(payload["published_at"]))
    scheduler_started_at = shanghai_now(started_at)
    current = shanghai_now(now)
    if captured_at < scheduler_started_at:
        raise RuntimeError("hot-rank receipt predates this scheduler run")
    if published_at > current:
        raise RuntimeError("hot-rank receipt publication is in the future")
    if payload.get("source_capability") == CURRENT_SNAPSHOT_ONLY:
        require_current_capture_window(
            engine,
            task_type=task_type,
            requested_date=target_date,
            now=captured_at,
        )

    table = _TABLES[task_type]
    columns = _SELECT_COLUMNS[task_type]
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(text(f"""
                SELECT {columns}
                  FROM `{table}`
                 WHERE snapshot_date=:snapshot_date
                 ORDER BY `rank`, stock_code
            """), {"snapshot_date": target_date}).mappings().all()
        ]
    inventory = validate_rank_inventory(
        rows,
        task_type=task_type,
        target_date=target_date,
    )
    if payload.get("source_capability") == AUTHORITATIVE_DATED_HISTORY:
        _reconstructed, evidence = _east_history_receipt_evidence(
            payload,
            target_date=target_date,
        )
        persisted_identity = [
            {
                "rank": int(row.get("rank") or 0),
                "stock_code": str(row.get("stock_code") or "").strip().zfill(6),
            }
            for row in rows
        ]
        evidence_identity = [
            {
                "rank": int(row["provider_rank"]),
                "stock_code": str(row["stock_code"]),
            }
            for row in evidence
        ]
        if persisted_identity != evidence_identity:
            raise RuntimeError(
                "persisted Eastmoney batch differs from raw getHisList evidence"
            )
        for row in rows:
            rank = int(row.get("rank") or 0)
            if (
                row.get("rank_change") is not None
                or row.get("his_rank") is not None
                or row.get("price") is not None
                or row.get("price_change") is not None
                or row.get("change_pct") is not None
                or str(row.get("pop_tag") or "") != "历史排名"
                or _number(row.get("hot_value")) != str(EXACT_TOP_ROWS + 1 - rank)
            ):
                raise RuntimeError(
                    "persisted Eastmoney historical semantics differ; "
                    "current payload relabelling prohibited"
                )
    for field in (
        "row_count",
        "provider_payload_sha256",
        "persisted_row_sha256",
        "code_set_sha256",
        "rank_set_sha256",
    ):
        if inventory[field] != payload.get(field):
            raise RuntimeError(
                f"persisted hot-rank receipt inventory differs: {field}"
            )
    if batch_timestamp(rows) != str(payload.get("batch_at") or ""):
        raise RuntimeError("persisted hot-rank receipt batch timestamp differs")
    return inventory


def basic_receipt_disposition(
    payload: Mapping[str, Any],
    *,
    task_type: str,
    return_code: int,
) -> str:
    try:
        validate_receipt_shape(payload, task_type=task_type)
    except (KeyError, TypeError, ValueError, RuntimeError, OverflowError):
        return "failed"
    status = str(payload.get("status") or "")
    if status == "DATA_BLOCKED":
        return "blocked" if int(return_code) == 2 else "failed"
    return "success" if status == "PASS" and int(return_code) == 0 else "failed"


__all__ = [
    "AUTHORITATIVE_DATED_HISTORY",
    "CURRENT_SNAPSHOT_ONLY",
    "EAST_HISTORY_PROVIDER",
    "EAST_HISTORY_EVIDENCE_SCHEMA",
    "EXACT_TOP_ROWS",
    "HOT_POP_EAST_TASK_TYPE",
    "HOT_RANK_CURRENT_PROVIDERS",
    "HOT_RANK_DATASETS",
    "HOT_RANK_READY_TIMES",
    "HOT_RANK_RESULT_SCHEMA",
    "HOT_RANK_SINA_TASK_TYPE",
    "HOT_RANK_SOURCE_TASK_TYPES",
    "HOT_RANK_XQ_TASK_TYPE",
    "SINA_ATTENTION_DATA_BLOCK_REASON",
    "HotRankDataBlocked",
    "basic_receipt_disposition",
    "batch_timestamp",
    "build_blocked_receipt",
    "build_pass_receipt",
    "canonical_hash",
    "canonical_rank_rows",
    "parse_hot_rank_receipt",
    "receipt_id_valid",
    "require_current_capture_window",
    "shanghai_now",
    "validate_east_history_date_evidence",
    "validate_persisted_hot_rank_receipt",
    "validate_receipt_shape",
    "validate_rank_inventory",
    "with_receipt_id",
]
