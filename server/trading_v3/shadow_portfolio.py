from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import date
from typing import Any, Iterable, Mapping

from .domain import AlphaForecast


SHADOW_PORTFOLIO_PROTOCOL = "V3_THEME_SIGNAL_LEDGER_V2"


def _hash(*parts: object) -> str:
    return hashlib.sha256(
        "|".join(str(part) for part in parts).encode("utf-8")
    ).hexdigest()


def _score(item: AlphaForecast) -> float:
    expected = item.expected_return_net_pct
    if expected is not None:
        return float(expected)
    return float(item.raw_score or 0.0)


def _rank_key(item: AlphaForecast) -> tuple[float, float, str, str]:
    return (
        -_score(item),
        -float(item.raw_score or 0.0),
        str(item.stock_code),
        str(item.strategy_key),
    )


def _theme_signal_score(item: Mapping[str, Any]) -> float:
    expected = item.get("expected_return_net_pct")
    if expected is not None:
        return float(expected)
    return float(item.get("raw_score") or 0.0)


def _theme_signal_rank(item: Mapping[str, Any]) -> tuple[float, float, str, str]:
    return (
        -_theme_signal_score(item),
        -float(item.get("raw_score") or 0.0),
        str(item.get("stock_code") or ""),
        str(item.get("strategy_key") or ""),
    )


def build_shadow_portfolio_rows(
    forecasts: Iterable[AlphaForecast],
    *,
    run_uid: str,
    trade_date: date,
    forecast_ids: Mapping[tuple[str, str], str],
    policy: Mapping[str, Any],
    theme_signals: Iterable[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Build no-order strategy and theme portfolios from one frozen run.

    Every result key includes both stock and strategy.  A stock emitted by two
    sleeves therefore remains two distinct research observations, while a
    theme portfolio keeps at most one sleeve observation for that stock.
    """

    if not bool(policy.get("enabled", True)):
        return []
    strategy_top_k = max(
        1,
        min(100, int(policy.get("strategy_top_k", 20))),
    )
    theme_top_k = max(
        1,
        min(100, int(policy.get("theme_top_k", 10))),
    )
    maximum_theme_groups = max(
        1,
        min(5000, int(policy.get("maximum_theme_groups", 1500))),
    )
    maximum_rows = max(
        1,
        min(100000, int(policy.get("maximum_rows_per_run", 20000))),
    )
    eligible = [
        item
        for item in forecasts
        if item.raw_score is not None
        and str(item.status or "") not in {
            "DATA_BLOCKED",
            "FEATURE_QUALITY_BLOCKED",
        }
        and (str(item.stock_code), str(item.strategy_key))
        in forecast_ids
    ]

    strategy_groups: dict[str, list[AlphaForecast]] = defaultdict(list)
    theme_groups: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for item in eligible:
        strategy_groups[str(item.strategy_key)].append(item)
    for signal in theme_signals:
        stock_code = str(signal.get("stock_code") or "")
        strategy_key = str(signal.get("strategy_key") or "")
        if (
            not stock_code
            or not strategy_key
            or not signal.get("theme_signal_id")
            or (stock_code, strategy_key) not in forecast_ids
        ):
            continue
        for raw_group in signal.get("theme_cluster_keys") or ():
            theme_group = str(raw_group).strip()
            if not theme_group:
                continue
            existing = theme_groups[theme_group].get(stock_code)
            if (
                existing is None
                or _theme_signal_rank(signal) < _theme_signal_rank(existing)
            ):
                theme_groups[theme_group][stock_code] = signal

    ranked_theme_groups = sorted(
        theme_groups,
        key=lambda key: (
            min(
                (
                    _theme_signal_rank(item)
                    for item in theme_groups[key].values()
                ),
                default=(0.0, 0.0, "", ""),
            ),
            key,
        ),
    )[:maximum_theme_groups]

    rows: list[dict[str, Any]] = []

    def append_group(
        portfolio_kind: str,
        group_key: str,
        items: Iterable[AlphaForecast],
        limit: int,
    ) -> None:
        for rank_no, item in enumerate(
            sorted(items, key=_rank_key)[:limit],
            1,
        ):
            forecast_id = str(forecast_ids[
                (str(item.stock_code), str(item.strategy_key))
            ])
            strategy_result_key = _hash(
                run_uid,
                item.stock_code,
                item.strategy_key,
                item.horizon_days,
            )
            rows.append({
                "shadow_position_id": _hash(
                    run_uid,
                    portfolio_kind,
                    group_key,
                    forecast_id,
                ),
                "run_uid": run_uid,
                "trade_date": trade_date,
                "portfolio_kind": portfolio_kind,
                "group_key": group_key,
                "rank_no": rank_no,
                "source_forecast_id": forecast_id,
                "source_theme_signal_id": "",
                "strategy_result_key": strategy_result_key,
                "stock_code": str(item.stock_code),
                "short_name": str(item.stock_name),
                "strategy_key": str(item.strategy_key),
                "theme_code": str(item.theme_code or ""),
                "horizon_days": int(item.horizon_days),
                "selection_score": _score(item),
                "valid_until": item.valid_until,
                "evidence_kind": "SHADOW",
                "protocol_version": SHADOW_PORTFOLIO_PROTOCOL,
                "order_allowed": 0,
                "can_activate_model": 0,
                "result_status": "OPEN",
            })

    for strategy_key in sorted(strategy_groups):
        append_group(
            "STRATEGY",
            strategy_key,
            strategy_groups[strategy_key],
            strategy_top_k,
        )
    for theme_group in ranked_theme_groups:
        for rank_no, signal in enumerate(
            sorted(
                theme_groups[theme_group].values(),
                key=_theme_signal_rank,
            )[:theme_top_k],
            1,
        ):
            stock_code = str(signal["stock_code"])
            strategy_key = str(signal["strategy_key"])
            forecast_id = str(forecast_ids[(stock_code, strategy_key)])
            theme_feature_key = str(signal["theme_feature_key"])
            strategy_result_key = _hash(
                run_uid,
                stock_code,
                strategy_key,
                signal["horizon_days"],
                theme_feature_key,
                theme_group,
            )
            rows.append({
                "shadow_position_id": _hash(
                    run_uid,
                    "THEME",
                    theme_group,
                    signal["theme_signal_id"],
                ),
                "run_uid": run_uid,
                "trade_date": trade_date,
                "portfolio_kind": "THEME",
                "group_key": theme_group,
                "rank_no": rank_no,
                "source_forecast_id": forecast_id,
                "source_theme_signal_id": str(
                    signal["theme_signal_id"]
                ),
                "strategy_result_key": strategy_result_key,
                "stock_code": stock_code,
                "short_name": str(signal.get("short_name") or ""),
                "strategy_key": strategy_key,
                "theme_code": str(
                    signal.get("theme_name")
                    or signal.get("theme_code")
                    or ""
                ),
                "horizon_days": int(signal["horizon_days"]),
                "selection_score": _theme_signal_score(signal),
                "valid_until": signal["valid_until"],
                "evidence_kind": "SHADOW",
                "protocol_version": SHADOW_PORTFOLIO_PROTOCOL,
                "order_allowed": 0,
                "can_activate_model": 0,
                "result_status": "OPEN",
            })
    return rows[:maximum_rows]
