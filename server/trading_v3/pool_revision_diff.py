"""Compare the stock membership of two verified immutable pool revisions."""

from collections.abc import Iterable, Mapping
from datetime import date, datetime
import re
from typing import Any, Literal

from server.common.pit_facts import SHANGHAI, normalize_decision_at
from server.trading_v3.decision_truth import canonical_hash


def _revision_identity(
    identity: Mapping[str, Any], time_field: str,
) -> tuple[dict[str, Any], datetime]:
    if identity.get("complete") is not True:
        raise ValueError("pool revision membership is not verified complete")

    run_uid = identity.get("run_uid")
    result_hash = identity.get("result_hash")
    if not isinstance(run_uid, str) or not re.fullmatch(r"[0-9a-f]{32}", run_uid) or run_uid == "0" * 32:
        raise ValueError("pool revision run UID is invalid")
    if not isinstance(result_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", result_hash) or result_hash == "0" * 64:
        raise ValueError("pool revision result hash is invalid")

    raw_date = str(identity.get("data_trade_date") or "")
    try:
        trade_date = date.fromisoformat(raw_date)
        revision_time = normalize_decision_at(identity.get(time_field))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"pool revision date or {time_field} is invalid") from exc
    if trade_date.isoformat() != raw_date or revision_time.date() < trade_date:
        raise ValueError(f"pool revision {time_field} precedes its data trade date or date is invalid")

    stock_count = identity.get("stock_count")
    if type(stock_count) is not int or stock_count < 0:
        raise ValueError("pool revision stock count is invalid")
    return {
        "run_uid": run_uid,
        "result_hash": result_hash,
        "data_trade_date": raw_date,
        time_field: revision_time.replace(tzinfo=SHANGHAI).isoformat(),
        "complete": True,
        "stock_count": stock_count,
    }, revision_time


def _row_text(row: Mapping[str, Any], field: str, *, multiple: bool) -> set[str]:
    value = row.get(field)
    if value is None:
        return set()
    values = value if multiple else (value,)
    if not isinstance(values, (tuple, list)) or any(not isinstance(item, str) for item in values):
        raise ValueError(f"pool membership {field} is invalid")
    return {item.strip() for item in values if item.strip()}


def _stock_membership(
    rows: Iterable[Mapping[str, Any]], stock_count: int,
) -> dict[str, dict[str, list[str]]]:
    members: dict[str, dict[str, set[str]]] = {}
    for row in rows:
        code = row.get("stock_code")
        if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
            raise ValueError("pool membership requires a six-digit stock code")
        member = members.setdefault(code, {"strategy_keys": set(), "reasons": set(), "blocking_reasons": set()})
        member["strategy_keys"].update(
            _row_text(row, "strategy_key", multiple=False)
            | _row_text(row, "strategy_keys", multiple=True)
            | _row_text(row, "strategies", multiple=True)
        )
        member["reasons"].update(
            _row_text(row, "reason", multiple=False) | _row_text(row, "reasons", multiple=True)
        )
        member["blocking_reasons"].update(_row_text(row, "blocking_reasons", multiple=True))

    if len(members) != stock_count:
        raise ValueError("pool membership differs from the verified unique stock count")
    return {
        code: {key: sorted(values) for key, values in evidence.items()}
        for code, evidence in members.items()
    }


def build_pool_revision_diff(
    *,
    prior_identity: Mapping[str, Any],
    next_identity: Mapping[str, Any],
    prior_rows: Iterable[Mapping[str, Any]],
    next_rows: Iterable[Mapping[str, Any]],
    time_field: Literal["cutoff", "finished_at"] = "cutoff",
) -> dict[str, Any]:
    """Compare the same candidate scope from two upstream-verified pool results.

    The caller supplies verified result identities and unique stock counts for
    that scope. Rows can include multiple strategy contributions per stock.
    """
    if time_field not in {"cutoff", "finished_at"}:
        raise ValueError("pool revision time field is invalid")
    prior, prior_time = _revision_identity(prior_identity, time_field)
    next_revision, next_time = _revision_identity(next_identity, time_field)
    if prior["data_trade_date"] != next_revision["data_trade_date"]:
        raise ValueError("pool revisions must use the same data trade date")
    if prior["run_uid"] == next_revision["run_uid"] or prior_time >= next_time:
        raise ValueError(f"pool revisions require distinct runs and an increasing {time_field}")

    before = _stock_membership(prior_rows, prior["stock_count"])
    after = _stock_membership(next_rows, next_revision["stock_count"])
    added = [{"stock_code": code, **after[code]} for code in sorted(after.keys() - before.keys())]
    removed = [{"stock_code": code, **before[code]} for code in sorted(before.keys() - after.keys())]
    retained = [
        {"stock_code": code, "prior": before[code], "next": after[code]}
        for code in sorted(before.keys() & after.keys())
    ]
    payload = {
        "schema": "probiga.trading-v3.pool-revision-diff.v1",
        "prior": prior,
        "next": next_revision,
        "added": added,
        "removed": removed,
        "retained": retained,
        "counts": {"added": len(added), "removed": len(removed), "retained": len(retained)},
    }
    return {**payload, "diff_sha256": canonical_hash(payload)}
