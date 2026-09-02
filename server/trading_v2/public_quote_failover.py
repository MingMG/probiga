"""Audited public-quote failover for paper-only intraday decisions.

Guojin Big QMT remains the primary source.  This module is only allowed to
publish a fallback snapshot when at least two independent public quote
providers agree on the same symbol and the full-market quality gates pass.
The fallback never supplies Level-1 bid/ask data and never enables real
orders.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from statistics import median
from typing import Any, Callable, Iterable, Mapping

import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.current_data import get_current_engine
from server.common.mysql_lock import mysql_named_lock


PROVIDER_ID = "PUBLIC_QUOTE_QUORUM_V1"
PORTFOLIO_PROVIDER_ID = "PUBLIC_PORTFOLIO_QUORUM_V1"
_SINA_RE = re.compile(
    r'var\s+hq_str_s_(?:sh|sz|bj)(?P<code>\d{6})="(?P<body>[^"]*)"'
)
_TENCENT_RE = re.compile(
    r'v_(?:sh|sz|bj)(?P<code>\d{6})="(?P<body>[^"]*)"'
)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _chunks(values: list[str], size: int) -> Iterable[list[str]]:
    step = max(1, int(size))
    for offset in range(0, len(values), step):
        yield values[offset : offset + step]


def _market_prefix(code: str) -> str:
    normalized = str(code).zfill(6)
    if normalized.startswith("6"):
        return "sh"
    if normalized.startswith(("4", "8", "9")):
        return "bj"
    return "sz"


def _eastmoney_secid(code: str) -> str:
    normalized = str(code).zfill(6)
    return f"{'1' if normalized.startswith('6') else '0'}.{normalized}"


def _source_time(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    raw = str(value or "").strip()
    if raw:
        for pattern, length in (
            ("%Y%m%d%H%M%S", 14),
            ("%Y-%m-%d %H:%M:%S", 19),
        ):
            try:
                return datetime.strptime(raw[:length], pattern)
            except (TypeError, ValueError):
                continue
        try:
            numeric = float(raw)
            if numeric > 1_000_000_000:
                return datetime.fromtimestamp(numeric)
        except (TypeError, ValueError, OSError, OverflowError):
            pass
    return fallback


def _get(
    url: str,
    *,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> requests.Response:
    response = requests.get(
        url,
        headers=dict(headers),
        timeout=max(1.0, float(timeout_seconds)),
    )
    response.raise_for_status()
    return response


def _fetch_sina_batch(
    codes: list[str],
    *,
    timeout_seconds: float,
) -> dict[str, dict[str, Any]]:
    requested_at = datetime.now().replace(microsecond=0)
    symbols = ",".join(
        f"s_{_market_prefix(code)}{str(code).zfill(6)}"
        for code in codes
    )
    response = _get(
        f"https://hq.sinajs.cn/list={symbols}",
        headers={
            "User-Agent": "Mozilla/5.0 ProBigA/2.0",
            "Referer": "https://finance.sina.com.cn/",
        },
        timeout_seconds=timeout_seconds,
    )
    response.encoding = "gbk"
    result: dict[str, dict[str, Any]] = {}
    for match in _SINA_RE.finditer(response.text):
        fields = match.group("body").split(",")
        if len(fields) < 6:
            continue
        price = _float(fields[1])
        change = _float(fields[2])
        pre_close = price - change
        if price <= 0 or pre_close <= 0:
            continue
        code = match.group("code")
        result[code] = {
            "stock_code": code,
            "short_name": fields[0].strip(),
            "price": price,
            "pre_close": pre_close,
            "change_pct": _float(fields[3]),
            "volume": max(0.0, _float(fields[4]) * 100.0),
            "amount": max(0.0, _float(fields[5]) * 10000.0),
            "source_time": requested_at,
            "provider": "sina",
        }
    return result


def _fetch_tencent_batch(
    codes: list[str],
    *,
    timeout_seconds: float,
) -> dict[str, dict[str, Any]]:
    received_at = datetime.now().replace(microsecond=0)
    symbols = ",".join(
        f"{_market_prefix(code)}{str(code).zfill(6)}"
        for code in codes
    )
    response = _get(
        f"https://qt.gtimg.cn/q={symbols}",
        headers={
            "User-Agent": "Mozilla/5.0 ProBigA/2.0",
            "Referer": "https://stockapp.finance.qq.com/",
        },
        timeout_seconds=timeout_seconds,
    )
    response.encoding = "gbk"
    result: dict[str, dict[str, Any]] = {}
    for match in _TENCENT_RE.finditer(response.text):
        fields = match.group("body").split("~")
        if len(fields) < 38:
            continue
        price = _float(fields[3])
        pre_close = _float(fields[4])
        if price <= 0 or pre_close <= 0:
            continue
        code = match.group("code")
        result[code] = {
            "stock_code": code,
            "short_name": fields[1].strip(),
            "price": price,
            "pre_close": pre_close,
            "change_pct": _float(fields[32]),
            "volume": max(0.0, _float(fields[36]) * 100.0),
            "amount": max(0.0, _float(fields[37]) * 10000.0),
            "source_time": _source_time(fields[30], received_at),
            "provider": "tencent",
        }
    return result


def _fetch_eastmoney_batch(
    codes: list[str],
    *,
    timeout_seconds: float,
) -> dict[str, dict[str, Any]]:
    received_at = datetime.now().replace(microsecond=0)
    secids = ",".join(_eastmoney_secid(code) for code in codes)
    response = _get(
        (
            "https://push2.eastmoney.com/api/qt/ulist.np/get"
            "?fltt=2&invt=2&fields=f12,f14,f2,f3,f4,f5,f6,f18,f124"
            f"&secids={secids}"
        ),
        headers={
            "User-Agent": "Mozilla/5.0 ProBigA/2.0",
            "Referer": "https://quote.eastmoney.com/",
        },
        timeout_seconds=timeout_seconds,
    )
    payload = response.json()
    rows = ((payload or {}).get("data") or {}).get("diff") or []
    if isinstance(rows, Mapping):
        rows = list(rows.values())
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        code = str(raw.get("f12") or "").zfill(6)
        price = _float(raw.get("f2"))
        pre_close = _float(raw.get("f18"))
        if len(code) != 6 or price <= 0 or pre_close <= 0:
            continue
        result[code] = {
            "stock_code": code,
            "short_name": str(raw.get("f14") or "").strip(),
            "price": price,
            "pre_close": pre_close,
            "change_pct": _float(raw.get("f3")),
            "volume": max(0.0, _float(raw.get("f5")) * 100.0),
            "amount": max(0.0, _float(raw.get("f6"))),
            "source_time": _source_time(raw.get("f124"), received_at),
            "provider": "eastmoney",
        }
    return result


def _fetch_eastmoney_page(
    page: int,
    *,
    timeout_seconds: float,
    page_size: int = 100,
) -> tuple[dict[str, dict[str, Any]], int]:
    received_at = datetime.now().replace(microsecond=0)
    response = requests.get(
        "https://push2.eastmoney.com/api/qt/clist/get",
        params={
            "pn": int(page),
            "pz": int(page_size),
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": (
                "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,"
                "m:0+t:81+s:2048"
            ),
            "fields": "f12,f14,f2,f3,f4,f5,f6,f18,f124",
        },
        headers={
            "User-Agent": "Mozilla/5.0 ProBigA/2.0",
            "Referer": "https://quote.eastmoney.com/",
        },
        timeout=max(1.0, float(timeout_seconds)),
    )
    response.raise_for_status()
    payload = response.json()
    data = (payload or {}).get("data") or {}
    raw_rows = data.get("diff") or []
    if isinstance(raw_rows, Mapping):
        raw_rows = list(raw_rows.values())
    result: dict[str, dict[str, Any]] = {}
    for raw in raw_rows:
        code = str(raw.get("f12") or "").zfill(6)
        price = _float(raw.get("f2"))
        pre_close = _float(raw.get("f18"))
        if len(code) != 6 or price <= 0 or pre_close <= 0:
            continue
        result[code] = {
            "stock_code": code,
            "short_name": str(raw.get("f14") or "").strip(),
            "price": price,
            "pre_close": pre_close,
            "change_pct": _float(raw.get("f3")),
            "volume": max(0.0, _float(raw.get("f5")) * 100.0),
            "amount": max(0.0, _float(raw.get("f6"))),
            "source_time": _source_time(
                raw.get("f124"),
                received_at,
            ),
            "provider": "eastmoney",
        }
    return result, int(data.get("total") or 0)


def _call_with_retries(
    callback: Callable[[], Any],
    *,
    retries: int,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(max(0, int(retries)) + 1):
        try:
            return callback()
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(0.15 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _fetch_eastmoney_market(
    expected_codes: list[str],
    *,
    config: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    timeout_seconds = float(config.get("fetch_timeout_seconds") or 8)
    retries = max(0, int(config.get("fetch_retries") or 2))
    workers = max(
        1,
        min(6, int(config.get("eastmoney_fetch_workers") or 4)),
    )
    page_size = max(
        50,
        min(100, int(config.get("eastmoney_page_size") or 100)),
    )
    started = time.monotonic()
    errors: list[str] = []
    first_rows, total = _call_with_retries(
        lambda: _fetch_eastmoney_page(
            1,
            timeout_seconds=timeout_seconds,
            page_size=page_size,
        ),
        retries=retries,
    )
    page_count = max(1, math.ceil(total / page_size))
    rows = dict(first_rows)
    if page_count > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _call_with_retries,
                    lambda page=page: _fetch_eastmoney_page(
                        page,
                        timeout_seconds=timeout_seconds,
                        page_size=page_size,
                    ),
                    retries=retries,
                ): page
                for page in range(2, page_count + 1)
            }
            for future in as_completed(futures):
                try:
                    page_rows, _ = future.result()
                    rows.update(page_rows)
                except Exception as exc:
                    errors.append(f"{type(exc).__name__}: {exc}")
    expected = {str(code).zfill(6) for code in expected_codes}
    rows = {
        code: row
        for code, row in rows.items()
        if code in expected
    }
    status = {
        "provider": "eastmoney",
        "row_count": len(rows),
        "batch_count": page_count,
        "failed_batch_count": len(errors),
        "duration_seconds": round(time.monotonic() - started, 3),
        "errors": errors[:5],
    }
    return rows, status


_PROVIDER_FETCHERS: dict[
    str,
    Callable[..., dict[str, dict[str, Any]]],
] = {
    "sina": _fetch_sina_batch,
    "tencent": _fetch_tencent_batch,
    "eastmoney": _fetch_eastmoney_batch,
}


def _fetch_provider(
    provider: str,
    codes: list[str],
    *,
    config: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if provider == "eastmoney":
        return _fetch_eastmoney_market(codes, config=config)
    fetcher = _PROVIDER_FETCHERS[provider]
    batch_size = int(config.get(f"{provider}_batch_size") or 300)
    timeout_seconds = float(config.get("fetch_timeout_seconds") or 8)
    total_workers = max(1, int(config.get("fetch_workers") or 12))
    retries = max(0, int(config.get("fetch_retries") or 2))
    per_provider_workers = max(
        1,
        min(6, total_workers // max(1, len(config.get("providers") or (provider,)))),
    )
    started = time.monotonic()
    batches = list(_chunks(codes, batch_size))
    rows: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    with ThreadPoolExecutor(
        max_workers=min(per_provider_workers, max(1, len(batches)))
    ) as executor:
        futures = {
            executor.submit(
                _call_with_retries,
                lambda batch=batch: fetcher(
                    batch,
                    timeout_seconds=timeout_seconds,
                ),
                retries=retries,
            ): batch
            for batch in batches
        }
        for future in as_completed(futures):
            try:
                rows.update(future.result())
            except Exception as exc:  # network/provider boundary
                errors.append(f"{type(exc).__name__}: {exc}")
    status = {
        "provider": provider,
        "row_count": len(rows),
        "batch_count": len(batches),
        "failed_batch_count": len(errors),
        "duration_seconds": round(time.monotonic() - started, 3),
        "errors": errors[:5],
    }
    return rows, status


def fetch_provider_quotes(
    codes: list[str],
    *,
    config: Mapping[str, Any],
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, Any]]:
    providers = [
        str(item).strip().lower()
        for item in config.get("providers", ("sina", "tencent", "eastmoney"))
        if str(item).strip().lower() in _PROVIDER_FETCHERS
    ]
    quotes: dict[str, dict[str, dict[str, Any]]] = {}
    statuses: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max(1, len(providers))) as executor:
        futures = {
            executor.submit(
                _fetch_provider,
                provider,
                codes,
                config=config,
            ): provider
            for provider in providers
        }
        for future in as_completed(futures):
            provider = futures[future]
            try:
                rows, status = future.result()
            except Exception as exc:  # defensive; provider worker is isolated
                rows = {}
                status = {
                    "provider": provider,
                    "row_count": 0,
                    "batch_count": 0,
                    "failed_batch_count": 1,
                    "duration_seconds": 0.0,
                    "errors": [f"{type(exc).__name__}: {exc}"],
                }
            quotes[provider] = rows
            statuses[provider] = status
    return quotes, statuses


def reconcile_provider_quotes(
    provider_quotes: Mapping[str, Mapping[str, Mapping[str, Any]]],
    *,
    expected_codes: Iterable[str],
    short_name_map: Mapping[str, str],
    now: datetime,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    expected = sorted(
        {
            str(code).strip().zfill(6)
            for code in expected_codes
            if str(code).strip()
        }
    )
    minimum_sources = max(
        2,
        int(config.get("minimum_sources_per_symbol") or 2),
    )
    maximum_source_age = float(
        config.get("maximum_source_age_seconds") or 30
    )
    minimum_source_time = config.get("minimum_source_time")
    if minimum_source_time and not isinstance(minimum_source_time, datetime):
        try:
            minimum_source_time = datetime.fromisoformat(
                str(minimum_source_time)[:19]
            )
        except (TypeError, ValueError):
            minimum_source_time = None
    if isinstance(minimum_source_time, datetime):
        minimum_source_time = minimum_source_time.replace(tzinfo=None)
    maximum_price_deviation = float(
        config.get("maximum_price_deviation_pct") or 0.35
    )
    maximum_change_deviation = float(
        config.get("maximum_change_deviation_pct") or 0.35
    )
    fresh_by_provider: dict[str, dict[str, Mapping[str, Any]]] = {}
    provider_latencies: dict[str, float] = {}
    for provider, rows in provider_quotes.items():
        fresh: dict[str, Mapping[str, Any]] = {}
        latencies: list[float] = []
        for code, row in rows.items():
            source_at = _source_time(row.get("source_time"), now)
            latency = max(0.0, (now - source_at).total_seconds())
            if source_at.date() != now.date():
                continue
            if source_at > now + timedelta(seconds=10):
                continue
            if minimum_source_time and source_at < minimum_source_time:
                continue
            if latency > maximum_source_age:
                continue
            normalized = str(code).zfill(6)
            fresh[normalized] = row
            latencies.append(latency)
        fresh_by_provider[str(provider)] = fresh
        provider_latencies[str(provider)] = max(latencies, default=0.0)

    rows: list[dict[str, Any]] = []
    comparable_count = 0
    rejected_disagreement = 0
    price_deviations: list[float] = []
    source_latencies: list[float] = []
    for code in expected:
        available = [
            dict(rows_by_code[code])
            for rows_by_code in fresh_by_provider.values()
            if code in rows_by_code
        ]
        if len(available) < minimum_sources:
            continue
        comparable_count += 1
        prices = [_float(item.get("price")) for item in available]
        pre_closes = [_float(item.get("pre_close")) for item in available]
        changes = [_float(item.get("change_pct")) for item in available]
        center = median(prices)
        pre_close_center = median(pre_closes)
        if center <= 0 or pre_close_center <= 0:
            continue
        price_deviation = (
            (max(prices) - min(prices)) / center * 100.0
        )
        change_deviation = max(changes) - min(changes)
        pre_close_deviation = (
            (max(pre_closes) - min(pre_closes))
            / pre_close_center
            * 100.0
        )
        if (
            price_deviation > maximum_price_deviation
            or abs(change_deviation) > maximum_change_deviation
            or pre_close_deviation > maximum_price_deviation
        ):
            rejected_disagreement += 1
            continue
        providers = sorted(
            {
                str(item.get("provider") or "").lower()
                for item in available
                if str(item.get("provider") or "")
            }
        )
        volumes = [
            _float(item.get("volume"))
            for item in available
            if _float(item.get("volume")) >= 0
        ]
        amounts = [
            _float(item.get("amount"))
            for item in available
            if _float(item.get("amount")) >= 0
        ]
        quote_latency = max(
            max(
                0.0,
                (
                    now
                    - _source_time(item.get("source_time"), now)
                ).total_seconds(),
            )
            for item in available
        )
        price_deviations.append(price_deviation)
        source_latencies.append(quote_latency)
        rows.append(
            {
                "stock_code": code,
                "short_name": (
                    next(
                        (
                            str(item.get("short_name") or "").strip()
                            for item in available
                            if str(item.get("short_name") or "").strip()
                        ),
                        "",
                    )
                    or str(short_name_map.get(code) or "")
                )[:128],
                "price": center,
                "pre_close": pre_close_center,
                "change_pct": (center / pre_close_center - 1.0)
                * 100.0,
                "volume": median(volumes) if volumes else 0.0,
                "amount": median(amounts) if amounts else 0.0,
                "source_count": len(available),
                "provider_mask": ",".join(providers),
                "price_deviation_pct": price_deviation,
                "source_latency_seconds": quote_latency,
            }
        )

    observed_count = len(rows)
    expected_count = len(expected)
    coverage = observed_count / max(expected_count, 1)
    agreement_ratio = observed_count / max(comparable_count, 1)
    active_provider_count = sum(
        len(rows_by_code) >= max(1, int(expected_count * 0.50))
        for rows_by_code in fresh_by_provider.values()
    )
    required_provider_count = max(
        2,
        int(config.get("minimum_provider_count") or 2),
    )
    minimum_observed = int(
        config.get("minimum_observed_stocks") or 5000
    )
    minimum_coverage = float(
        config.get("minimum_universe_coverage") or 0.95
    )
    minimum_agreement = float(
        config.get("minimum_agreement_ratio") or 0.98
    )
    evidence = [
        f"公共替补有效股票：{observed_count}/{expected_count}",
        f"公共替补覆盖率：{coverage:.2%}",
        f"双源以上可比股票一致率：{agreement_ratio:.2%}",
        f"有效独立数据源：{active_provider_count}/{required_provider_count}",
        (
            "最大价格偏差："
            f"{max(price_deviations, default=0.0):.4f}%/"
            f"{maximum_price_deviation:.4f}%"
        ),
        (
            "最大源延迟："
            f"{max(source_latencies, default=0.0):.1f}秒/"
            f"{maximum_source_age:.1f}秒"
        ),
    ]
    quality_status = "PASS"
    if active_provider_count < required_provider_count:
        quality_status = "BLOCK"
        evidence.append("独立公共行情源不足，禁止用替补源生成模拟买单")
    if observed_count < minimum_observed:
        quality_status = "BLOCK"
        evidence.append("公共替补有效股票数不足")
    if coverage < minimum_coverage:
        quality_status = "BLOCK"
        evidence.append("公共替补全市场覆盖率不足")
    if agreement_ratio < minimum_agreement:
        quality_status = "BLOCK"
        evidence.append("公共行情源之间分歧过大")
    if rejected_disagreement:
        evidence.append(
            f"剔除价格不一致股票：{rejected_disagreement}只"
        )
    return {
        "rows": rows,
        "expected_count": expected_count,
        "observed_count": observed_count,
        "coverage": coverage,
        "provider_count": active_provider_count,
        "minimum_sources_per_symbol": minimum_sources,
        "agreement_ratio": agreement_ratio,
        "maximum_price_deviation_pct": max(
            price_deviations,
            default=0.0,
        ),
        "maximum_source_latency_seconds": max(
            source_latencies,
            default=0.0,
        ),
        "provider_latencies": provider_latencies,
        "quality_status": quality_status,
        "evidence": evidence,
    }


def _load_universe(
    engine: Engine,
) -> tuple[list[str], dict[str, str]]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT stock_code, short_name
                FROM si_all_code
                WHERE stock_code REGEXP '^[0-9]{6}$'
                ORDER BY stock_code
                """
            )
        ).mappings().all()
    codes: list[str] = []
    names: dict[str, str] = {}
    for row in rows:
        code = str(row["stock_code"]).zfill(6)
        codes.append(code)
        names[code] = str(row.get("short_name") or "")
    return codes, names


def _load_portfolio_universe(
    engine: Engine,
) -> tuple[list[str], dict[str, str]]:
    """Load only the user watchlist for the low-latency quote lane."""

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT DISTINCT p.stock_code, a.short_name
                FROM st_user_portfolio p
                LEFT JOIN si_all_code a ON a.stock_code = p.stock_code
                WHERE p.stock_code REGEXP '^[0-9]{6}$'
                ORDER BY p.stock_code
                """
            )
        ).mappings().all()
    codes: list[str] = []
    names: dict[str, str] = {}
    for row in rows:
        code = str(row["stock_code"]).zfill(6)
        codes.append(code)
        names[code] = str(row.get("short_name") or "")
    return codes, names


def qmt_primary_health(
    primary_engine: Engine,
    *,
    now: datetime,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    provider = str(config.get("required_provider") or "GJ_BIG_QMT_INNER")
    minimum_coverage = float(
        config.get("minimum_universe_coverage") or 0.85
    )
    maximum_age = float(
        config.get("maximum_minute_age_seconds") or 15
    )
    heartbeat_max_age = float(
        config.get("maximum_heartbeat_age_seconds") or 30
    )
    snapshot_max_age = float(
        config.get("maximum_full_snapshot_age_seconds") or 75
    )
    receipt_max_age = float(
        config.get("maximum_sync_receipt_age_seconds") or 75
    )
    expected_count = 0
    try:
        with primary_engine.connect() as connection:
            expected_count = int(
                connection.execute(
                    text(
                        """
                        SELECT COUNT(DISTINCT stock_code)
                        FROM si_all_code
                        WHERE stock_code REGEXP '^[0-9]{6}$'
                        """
                    )
                ).scalar()
                or 0
            )
    except Exception as exc:
        return {
            "healthy": False,
            "reason": (
                "QMT_EXPECTED_UNIVERSE_READ_FAILED:"
                f"{type(exc).__name__}"
            ),
            "expected_count": expected_count,
            "observed_count": 0,
            "coverage": 0.0,
            "latest_at": None,
            "receipt": None,
        }
    # The QMT bridge publishes both ``sm_stock_current`` and its attestation
    # receipt into the collector/current-data plane.  Production reaches that
    # database through the guarded reverse tunnel, while ``primary_engine`` is
    # the separate business ledger.  Reading the receipt from the ledger would
    # permanently report "missing" even though the exact quote replacement
    # completed successfully.
    current_engine = get_current_engine()
    try:
        with current_engine.connect() as connection:
            receipt = connection.execute(
                text(
                    """
                    SELECT receipt_id, source_snapshot_token,
                           source_full_file_token, source_generated_at,
                           heartbeat_at, expected_count, observed_count,
                           coverage, published_at, capture_mode,
                           quality_status
                    FROM st_qmt_realtime_sync_receipt_v2
                    WHERE source_provider = :provider
                      AND capture_mode = 'LIVE_FORWARD'
                      AND quality_status = 'PASS'
                      AND heartbeat_at
                          BETWEEN :heartbeat_cutoff AND :now
                      AND source_generated_at
                          BETWEEN :snapshot_cutoff AND :now
                      AND published_at BETWEEN :receipt_cutoff AND :now
                    ORDER BY published_at DESC, created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "provider": provider.lower(),
                    "heartbeat_cutoff": (
                        now - timedelta(seconds=heartbeat_max_age)
                    ),
                    "snapshot_cutoff": (
                        now - timedelta(seconds=snapshot_max_age)
                    ),
                    "receipt_cutoff": (
                        now - timedelta(seconds=receipt_max_age)
                    ),
                    "now": now,
                },
            ).mappings().first()
    except Exception as exc:
        return {
            "healthy": False,
            "reason": (
                "QMT_END_TO_END_RECEIPT_READ_FAILED:"
                f"{type(exc).__name__}"
            ),
            "expected_count": expected_count,
            "observed_count": 0,
            "coverage": 0.0,
            "latest_at": None,
            "receipt": None,
        }
    if not receipt:
        return {
            "healthy": False,
            "reason": "QMT_END_TO_END_RECEIPT_MISSING_OR_STALE",
            "expected_count": expected_count,
            "observed_count": 0,
            "coverage": 0.0,
            "latest_at": None,
            "receipt": None,
        }
    receipt_coverage = float(receipt.get("coverage") or 0.0)
    if (
        int(receipt.get("observed_count") or 0) <= 0
        or receipt_coverage < minimum_coverage
    ):
        return {
            "healthy": False,
            "reason": "QMT_END_TO_END_RECEIPT_INCOMPLETE",
            "expected_count": expected_count,
            "observed_count": int(
                receipt.get("observed_count") or 0
            ),
            "coverage": receipt_coverage,
            "latest_at": receipt.get("published_at"),
            "receipt": dict(receipt),
        }
    try:
        with current_engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT stock_code) AS observed_count,
                           MAX(COALESCE(source_time, snapshot_at)) AS latest_at
                    FROM sm_stock_current
                    WHERE LOWER(data_source) = :provider
                      AND price > 0
                      AND COALESCE(source_time, snapshot_at)
                          BETWEEN :cutoff AND :now
                    """
                ),
                {
                    "provider": provider.lower(),
                    "cutoff": now - timedelta(seconds=maximum_age),
                    "now": now,
                },
            ).mappings().first()
    except Exception as exc:
        return {
            "healthy": False,
            "reason": f"QMT_CURRENT_READ_FAILED:{type(exc).__name__}",
            "expected_count": expected_count,
            "observed_count": 0,
            "coverage": 0.0,
            "latest_at": None,
        }
    observed_count = int((row or {}).get("observed_count") or 0)
    latest_at = (row or {}).get("latest_at")
    coverage = observed_count / max(expected_count, 1)
    healthy = bool(
        observed_count > 0
        and coverage >= minimum_coverage
        and latest_at is not None
        and receipt_coverage >= minimum_coverage
    )
    return {
        "healthy": healthy,
        "reason": (
            "QMT_END_TO_END_HEALTHY"
            if healthy
            else "QMT_PRIMARY_STALE_OR_INCOMPLETE"
        ),
        "expected_count": expected_count,
        "observed_count": observed_count,
        "coverage": coverage,
        "latest_at": latest_at,
        "receipt": dict(receipt),
    }


def _persist_result(
    engine: Engine,
    *,
    now: datetime,
    config: Mapping[str, Any],
    provider_status: Mapping[str, Any],
    result: Mapping[str, Any],
) -> str:
    # Preserve seconds. Rounding to the minute made a newly collected snapshot
    # appear 30-59 seconds older than it really was and could immediately trip
    # the 45-second failover freshness gate.
    quote_at = now.replace(microsecond=0)
    source_provider = str(
        config.get("source_provider") or PROVIDER_ID
    ).upper()
    batch_id = hashlib.sha256(
        (
            f"{source_provider}|{quote_at.isoformat()}|"
            f"{result['expected_count']}|{result['observed_count']}"
        ).encode("utf-8")
    ).hexdigest()[:32]
    received_at = datetime.now().replace(microsecond=0)
    provider_json = json.dumps(
        provider_status,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    evidence_json = json.dumps(
        result.get("evidence") or [],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    with engine.begin() as connection:
        if result["quality_status"] == "PASS":
            statement = text(
                """
                INSERT INTO st_public_quote_current_v2 (
                    stock_code, batch_id, trade_date, quote_at,
                    short_name, price, pre_close, change_pct,
                    volume, amount, source_provider, source_count,
                    provider_mask, price_deviation_pct, received_at,
                    quality_status, evidence_json, created_at, updated_at
                ) VALUES (
                    :stock_code, :batch_id, :trade_date, :quote_at,
                    :short_name, :price, :pre_close, :change_pct,
                    :volume, :amount, :source_provider, :source_count,
                    :provider_mask, :price_deviation_pct, :received_at,
                    'PASS', :evidence_json, :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                    batch_id=VALUES(batch_id),
                    trade_date=VALUES(trade_date),
                    quote_at=VALUES(quote_at),
                    short_name=VALUES(short_name),
                    price=VALUES(price),
                    pre_close=VALUES(pre_close),
                    change_pct=VALUES(change_pct),
                    volume=VALUES(volume),
                    amount=VALUES(amount),
                    source_provider=VALUES(source_provider),
                    source_count=VALUES(source_count),
                    provider_mask=VALUES(provider_mask),
                    price_deviation_pct=VALUES(price_deviation_pct),
                    received_at=VALUES(received_at),
                    quality_status='PASS',
                    evidence_json=VALUES(evidence_json),
                    updated_at=VALUES(updated_at)
                """
            )
            payloads = [
                {
                    **row,
                    "batch_id": batch_id,
                    "trade_date": quote_at.date(),
                    "quote_at": quote_at,
                    "source_provider": source_provider,
                    "received_at": received_at,
                    "evidence_json": json.dumps(
                        {
                            "provider_mask": row["provider_mask"],
                            "source_count": row["source_count"],
                            "price_deviation_pct": (
                                row["price_deviation_pct"]
                            ),
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    "created_at": received_at,
                    "updated_at": received_at,
                }
                for row in result["rows"]
            ]
            for offset in range(0, len(payloads), 500):
                connection.execute(
                    statement,
                    payloads[offset : offset + 500],
                )
        connection.execute(
            text(
                """
                INSERT INTO st_public_quote_receipt_v2 (
                    batch_id, trade_date, quote_at, received_at,
                    expected_count, observed_count, coverage,
                    provider_count, minimum_sources_per_symbol,
                    agreement_ratio, source_provider,
                    maximum_price_deviation_pct,
                    maximum_source_latency_seconds, quality_status,
                    provider_status_json, evidence_json, created_at
                ) VALUES (
                    :batch_id, :trade_date, :quote_at, :received_at,
                    :expected_count, :observed_count, :coverage,
                    :provider_count, :minimum_sources_per_symbol,
                    :agreement_ratio, :source_provider,
                    :maximum_price_deviation_pct,
                    :maximum_source_latency_seconds, :quality_status,
                    :provider_status_json, :evidence_json, :created_at
                )
                ON DUPLICATE KEY UPDATE
                    received_at=VALUES(received_at),
                    expected_count=VALUES(expected_count),
                    observed_count=VALUES(observed_count),
                    coverage=VALUES(coverage),
                    provider_count=VALUES(provider_count),
                    minimum_sources_per_symbol=
                        VALUES(minimum_sources_per_symbol),
                    agreement_ratio=VALUES(agreement_ratio),
                    maximum_price_deviation_pct=
                        VALUES(maximum_price_deviation_pct),
                    maximum_source_latency_seconds=
                        VALUES(maximum_source_latency_seconds),
                    quality_status=VALUES(quality_status),
                    provider_status_json=VALUES(provider_status_json),
                    evidence_json=VALUES(evidence_json)
                """
            ),
            {
                "batch_id": batch_id,
                "trade_date": quote_at.date(),
                "quote_at": quote_at,
                "received_at": received_at,
                "expected_count": result["expected_count"],
                "observed_count": result["observed_count"],
                "coverage": result["coverage"],
                "provider_count": result["provider_count"],
                "minimum_sources_per_symbol": result[
                    "minimum_sources_per_symbol"
                ],
                "agreement_ratio": result["agreement_ratio"],
                "source_provider": source_provider,
                "maximum_price_deviation_pct": result[
                    "maximum_price_deviation_pct"
                ],
                "maximum_source_latency_seconds": result[
                    "maximum_source_latency_seconds"
                ],
                "quality_status": result["quality_status"],
                "provider_status_json": provider_json,
                "evidence_json": evidence_json,
                "created_at": received_at,
            },
        )
    return batch_id


def _persist_portfolio_result(
    engine: Engine,
    *,
    now: datetime,
    result: Mapping[str, Any],
) -> str:
    """Atomically publish a passed dual-source watchlist snapshot."""

    quote_at = now.replace(microsecond=0)
    batch_id = hashlib.sha256(
        (
            f"{PORTFOLIO_PROVIDER_ID}|{quote_at.isoformat()}|"
            f"{result['expected_count']}|{result['observed_count']}"
        ).encode("utf-8")
    ).hexdigest()[:32]
    received_at = datetime.now().replace(microsecond=0)
    if result["quality_status"] != "PASS":
        return batch_id
    statement = text(
        """
        INSERT INTO st_portfolio_public_quote_v1 (
            stock_code, batch_id, trade_date, quote_at, short_name,
            price, pre_close, change_pct, volume, amount,
            source_provider, source_count, provider_mask,
            price_deviation_pct, received_at, quality_status,
            created_at, updated_at
        ) VALUES (
            :stock_code, :batch_id, :trade_date, :quote_at, :short_name,
            :price, :pre_close, :change_pct, :volume, :amount,
            :source_provider, :source_count, :provider_mask,
            :price_deviation_pct, :received_at, 'PASS',
            :created_at, :updated_at
        )
        ON DUPLICATE KEY UPDATE
            batch_id=VALUES(batch_id),
            trade_date=VALUES(trade_date),
            quote_at=VALUES(quote_at),
            short_name=VALUES(short_name),
            price=VALUES(price),
            pre_close=VALUES(pre_close),
            change_pct=VALUES(change_pct),
            volume=VALUES(volume),
            amount=VALUES(amount),
            source_provider=VALUES(source_provider),
            source_count=VALUES(source_count),
            provider_mask=VALUES(provider_mask),
            price_deviation_pct=VALUES(price_deviation_pct),
            received_at=VALUES(received_at),
            quality_status='PASS',
            updated_at=VALUES(updated_at)
        """
    )
    payloads = [
        {
            **row,
            "batch_id": batch_id,
            "trade_date": quote_at.date(),
            "quote_at": quote_at,
            "source_provider": PORTFOLIO_PROVIDER_ID,
            "received_at": received_at,
            "created_at": received_at,
            "updated_at": received_at,
        }
        for row in result["rows"]
    ]
    with engine.begin() as connection:
        connection.execute(statement, payloads)
    return batch_id


def collect_portfolio_quote_refresh(
    engine: Engine,
    *,
    now: datetime,
    config: Mapping[str, Any],
    force: bool = False,
    lock_timeout_seconds: int = 0,
) -> dict[str, Any]:
    """Collect a small independent Sina/Tencent snapshot for the watchlist."""

    hhmm = now.hour * 100 + now.minute
    if not force and not (
        now.weekday() < 5
        and ((925 <= hhmm <= 1135) or (1255 <= hhmm <= 1505))
    ):
        return {"status": "skipped", "reason": "OUTSIDE_TRADING_SESSION"}
    try:
        with mysql_named_lock(
            engine,
            "probiga:portfolio_quote_refresh",
            timeout_seconds=max(0, int(lock_timeout_seconds)),
        ):
            codes, names = _load_portfolio_universe(engine)
            if not codes:
                return {"status": "success", "reason": "PORTFOLIO_EMPTY"}
            portfolio_config = dict(config)
            portfolio_config.update(
                {
                    "source_provider": PORTFOLIO_PROVIDER_ID,
                    "providers": ["sina", "tencent"],
                    "minimum_provider_count": 2,
                    "minimum_sources_per_symbol": 2,
                    "minimum_observed_stocks": max(
                        1,
                        math.ceil(len(codes) * 0.90),
                    ),
                    "minimum_universe_coverage": 0.90,
                    "minimum_agreement_ratio": 0.98,
                    "maximum_source_age_seconds": max(
                        45,
                        int(config.get("maximum_source_age_seconds") or 0),
                    ),
                }
            )
            close_start = now.replace(
                hour=15,
                minute=0,
                second=0,
                microsecond=0,
            )
            if force and now >= close_start:
                # The providers keep publishing today's final quote after the
                # close.  Accept that 15:00 evidence without relaxing the
                # same-day or two-source gates, and reject pre-close snapshots.
                portfolio_config["minimum_source_time"] = close_start
                portfolio_config["maximum_source_age_seconds"] = max(
                    int(portfolio_config["maximum_source_age_seconds"]),
                    int((now - close_start).total_seconds()) + 120,
                )
            provider_quotes, provider_status = fetch_provider_quotes(
                codes,
                config=portfolio_config,
            )
            collected_at = datetime.now().replace(microsecond=0)
            result = reconcile_provider_quotes(
                provider_quotes,
                expected_codes=codes,
                short_name_map=names,
                now=collected_at,
                config=portfolio_config,
            )
            batch_id = _persist_portfolio_result(
                engine,
                now=collected_at,
                result=result,
            )
    except TimeoutError:
        return {
            "status": "already_running",
            "reason": "PORTFOLIO_QUOTE_REFRESH_ALREADY_RUNNING",
        }
    return {
        "status": (
            "success"
            if result["quality_status"] == "PASS"
            else "blocked"
        ),
        "reason": (
            "PORTFOLIO_QUOTE_QUORUM_READY"
            if result["quality_status"] == "PASS"
            else "PORTFOLIO_QUOTE_QUORUM_QUALITY_BLOCK"
        ),
        "batch_id": batch_id,
        "quote_at": collected_at.isoformat(sep=" "),
        "quality_status": result["quality_status"],
        "expected_count": result["expected_count"],
        "observed_count": result["observed_count"],
        "coverage": result["coverage"],
        "provider_count": result["provider_count"],
        "agreement_ratio": result["agreement_ratio"],
        "provider_status": provider_status,
        "evidence": result["evidence"],
    }


def collect_public_quote_failover(
    engine: Engine,
    *,
    now: datetime,
    config: Mapping[str, Any],
    force: bool = False,
    lock_timeout_seconds: int = 0,
) -> dict[str, Any]:
    if not bool(config.get("enabled", True)):
        return {"status": "skipped", "reason": "FAILOVER_DISABLED"}
    hhmm = now.hour * 100 + now.minute
    if not force and not (
        now.weekday() < 5
        and ((925 <= hhmm <= 1135) or (1255 <= hhmm <= 1505))
    ):
        return {
            "status": "skipped",
            "reason": "OUTSIDE_TRADING_SESSION",
        }
    primary_quality = {
        "required_provider": "GJ_BIG_QMT_INNER",
        "minimum_universe_coverage": 0.85,
        "maximum_minute_age_seconds": 15,
    }
    if not force:
        primary = qmt_primary_health(
            engine,
            now=now,
            config=primary_quality,
        )
        if primary["healthy"]:
            return {
                "status": "primary_healthy",
                "reason": "QMT_PRIMARY_HEALTHY",
                "primary": primary,
            }
    try:
        with mysql_named_lock(
            engine,
            "probiga:public_quote_failover",
            timeout_seconds=max(0, int(lock_timeout_seconds)),
        ):
            lock_now = datetime.now().replace(microsecond=0)
            if not force:
                refreshed_primary = qmt_primary_health(
                    engine,
                    now=lock_now,
                    config=primary_quality,
                )
                if refreshed_primary["healthy"]:
                    return {
                        "status": "primary_healthy",
                        "reason": "QMT_PRIMARY_RECOVERED",
                        "primary": refreshed_primary,
                    }
                receipt, _ = load_latest_public_quote_snapshot(
                    engine,
                    now=lock_now,
                    config=config,
                )
                if receipt:
                    return {
                        "status": "existing_fresh",
                        "reason": "PUBLIC_QUOTE_QUORUM_ALREADY_READY",
                        "batch_id": receipt["batch_id"],
                        "quote_at": str(receipt["quote_at"]),
                    }
            codes, names = _load_universe(engine)
            provider_quotes, provider_status = fetch_provider_quotes(
                codes,
                config=config,
            )
            collected_at = datetime.now().replace(microsecond=0)
            result = reconcile_provider_quotes(
                provider_quotes,
                expected_codes=codes,
                short_name_map=names,
                now=collected_at,
                config=config,
            )
            batch_id = _persist_result(
                engine,
                now=collected_at,
                config=config,
                provider_status=provider_status,
                result=result,
            )
    except TimeoutError:
        return {
            "status": "already_running",
            "reason": "FAILOVER_COLLECTION_ALREADY_RUNNING",
        }
    return {
        "status": (
            "success"
            if result["quality_status"] == "PASS"
            else "blocked"
        ),
        "reason": (
            "PUBLIC_QUOTE_QUORUM_READY"
            if result["quality_status"] == "PASS"
            else "PUBLIC_QUOTE_QUORUM_QUALITY_BLOCK"
        ),
        "batch_id": batch_id,
        "quote_at": collected_at.isoformat(sep=" "),
        "quality_status": result["quality_status"],
        "expected_count": result["expected_count"],
        "observed_count": result["observed_count"],
        "coverage": result["coverage"],
        "provider_count": result["provider_count"],
        "agreement_ratio": result["agreement_ratio"],
        "maximum_price_deviation_pct": result[
            "maximum_price_deviation_pct"
        ],
        "maximum_source_latency_seconds": result[
            "maximum_source_latency_seconds"
        ],
        "provider_status": provider_status,
        "evidence": result["evidence"],
    }


def load_latest_public_quote_snapshot(
    engine: Engine,
    *,
    now: datetime,
    config: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, dict[str, Any]]]:
    provider = str(
        config.get("source_provider") or PROVIDER_ID
    ).upper()
    maximum_age = float(
        config.get("maximum_snapshot_age_seconds") or 45
    )
    with engine.connect() as connection:
        receipt = connection.execute(
            text(
                """
                SELECT *
                FROM st_public_quote_receipt_v2
                WHERE trade_date = :trade_date
                  AND quote_at BETWEEN :cutoff AND :now
                  AND source_provider = :source_provider
                  AND quality_status = 'PASS'
                ORDER BY quote_at DESC, received_at DESC
                LIMIT 1
                """
            ),
            {
                "trade_date": now.date(),
                "cutoff": now - timedelta(seconds=maximum_age),
                "now": now,
                "source_provider": provider,
            },
        ).mappings().first()
        if not receipt:
            return None, {}
        rows = connection.execute(
            text(
                """
                SELECT stock_code, short_name, price, pre_close,
                       change_pct, volume, amount, quote_at,
                       source_provider, source_count, provider_mask,
                       price_deviation_pct
                FROM st_public_quote_current_v2
                WHERE batch_id = :batch_id
                  AND quality_status = 'PASS'
                """
            ),
            {"batch_id": receipt["batch_id"]},
        ).mappings().all()
    quotes = {
        str(row["stock_code"]).zfill(6): {
            **dict(row),
            "stock_code": str(row["stock_code"]).zfill(6),
            "price": _float(row["price"]),
            "pre_close": _float(row["pre_close"]),
            "return_pct": _float(row["change_pct"]),
            "observed_at": row["quote_at"],
            "data_source": str(row["source_provider"]).lower(),
        }
        for row in rows
        if _float(row["price"]) > 0 and _float(row["pre_close"]) > 0
    }
    return dict(receipt), quotes
