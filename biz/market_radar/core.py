# -*- coding: utf-8 -*-
"""Full-market intraday anomaly radar built on ordinary QMT quotes.

This module intentionally does not call VIP/L2 periods.  QMT's standard
``get_full_tick`` snapshot supplies the latest price, cumulative amount and
five best bid/ask levels.  The radar turns those fields into transparent
proxies for money intensity, price acceleration, five-level pressure and
sector breadth.
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median
from typing import Any, Iterable

from sqlalchemy import text

from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt import bridge
from server.common.config import get_market_radar_runtime_config, get_wecom_webhook
from server.common.kline_data import get_kline_engine

logger = logging.getLogger("market_radar")


STOCK_TABLE = "sm_market_radar_stock"
SECTOR_TABLE = "sm_market_radar_sector"
EVENT_TABLE = "sm_market_radar_event"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _text(value: Any) -> str:
    return str(value or "").strip()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def _first(row: dict[str, Any], primary: str, fallback: str) -> Any:
    value = row.get(primary)
    return value if value is not None else row.get(fallback)


def _clamp(value: float, low: float = -100.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _robust_z(values: Iterable[float]) -> list[float]:
    """Return cross-sectional robust z-scores without scipy/pandas."""
    numbers = [float(value) for value in values]
    if not numbers:
        return []
    centre = median(numbers)
    deviations = [abs(value - centre) for value in numbers]
    mad = median(deviations)
    scale = 1.4826 * mad
    if scale < 1e-12:
        mean = sum(numbers) / len(numbers)
        variance = sum((value - mean) ** 2 for value in numbers) / max(1, len(numbers))
        scale = math.sqrt(variance)
    if scale < 1e-12:
        return [0.0] * len(numbers)
    return [(value - centre) / scale for value in numbers]


def _array(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if hasattr(value, "tolist"):
        try:
            value = value.tolist()
        except Exception:
            return []
    if not isinstance(value, (list, tuple)):
        return []
    return [_float(item) for item in value[:5]]


def _qmt_timestamp(value: Any, fallback: datetime) -> datetime:
    raw = _text(value)
    for fmt in ("%Y%m%d %H:%M:%S.%f", "%Y%m%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return fallback


def _dt_string(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _read_rows(engine, sql: str, params: dict[str, Any] | None = None, **_: Any) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(row) for row in result.mappings().all()]


def market_phase(now: datetime | None = None) -> str:
    current = now or datetime.now()
    if current.weekday() >= 5:
        return "closed"
    hhmm = current.hour * 100 + current.minute
    if 900 <= hhmm < 915:
        return "pre_open"
    if 915 <= hhmm < 925:
        return "call_auction"
    if 925 <= hhmm < 1130:
        return "morning"
    if 1130 <= hhmm < 1300:
        return "midday"
    if 1300 <= hhmm <= 1505:
        return "afternoon"
    return "closed"


def ensure_radar_tables(engine) -> None:
    """Create the small latest-state/event tables used by the radar."""
    ddl = (
        f"""
        CREATE TABLE IF NOT EXISTS {STOCK_TABLE} (
            stock_code VARCHAR(16) NOT NULL,
            short_name VARCHAR(128) NULL,
            snapshot_at DATETIME NOT NULL,
            qmt_timetag VARCHAR(40) NULL,
            price DOUBLE NULL,
            pre_close DOUBLE NULL,
            change_pct DOUBLE NULL,
            volume DOUBLE NULL,
            amount DOUBLE NULL,
            amount_delta DOUBLE NULL,
            price_speed DOUBLE NULL,
            five_bid_value DOUBLE NULL,
            five_ask_value DOUBLE NULL,
            five_pressure DOUBLE NULL,
            amount_score DOUBLE NULL,
            price_score DOUBLE NULL,
            pressure_score DOUBLE NULL,
            score DOUBLE NULL,
            direction VARCHAR(16) NULL,
            stale TINYINT NOT NULL DEFAULT 0,
            signal_tags TEXT NULL,
            bid_price_json TEXT NULL,
            bid_vol_json TEXT NULL,
            ask_price_json TEXT NULL,
            ask_vol_json TEXT NULL,
            data_source VARCHAR(64) NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (stock_code),
            KEY idx_radar_stock_score (score),
            KEY idx_radar_stock_snapshot (snapshot_at)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {SECTOR_TABLE} (
            sector_code VARCHAR(160) NOT NULL,
            sector_name VARCHAR(192) NOT NULL,
            sector_type VARCHAR(32) NOT NULL,
            snapshot_at DATETIME NOT NULL,
            member_count INT NOT NULL DEFAULT 0,
            positive_count INT NOT NULL DEFAULT 0,
            negative_count INT NOT NULL DEFAULT 0,
            breadth_pct DOUBLE NULL,
            avg_change_pct DOUBLE NULL,
            amount_delta DOUBLE NULL,
            score DOUBLE NULL,
            direction VARCHAR(16) NULL,
            dragon_json TEXT NULL,
            core_json TEXT NULL,
            follower_json TEXT NULL,
            data_source VARCHAR(64) NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (sector_code),
            KEY idx_radar_sector_score (score),
            KEY idx_radar_sector_snapshot (snapshot_at)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {EVENT_TABLE} (
            event_id BIGINT NOT NULL AUTO_INCREMENT,
            event_key VARCHAR(160) NOT NULL,
            event_type VARCHAR(32) NOT NULL,
            direction VARCHAR(16) NOT NULL,
            sector_code VARCHAR(160) NULL,
            sector_name VARCHAR(192) NULL,
            stock_code VARCHAR(16) NULL,
            snapshot_at DATETIME NOT NULL,
            score DOUBLE NULL,
            detail_json TEXT NULL,
            data_source VARCHAR(64) NOT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (event_id),
            KEY idx_radar_event_time (snapshot_at),
            KEY idx_radar_event_key (event_key)
        )
        """,
    )
    with engine.begin() as conn:
        for statement in ddl:
            conn.execute(text(statement))
        # The project still has installations using older utf8mb4 index
        # limits.  These small alters also upgrade tables created by an
        # interrupted first run without requiring a destructive migration.
        for table in (STOCK_TABLE, SECTOR_TABLE, EVENT_TABLE):
            try:
                conn.execute(text(f"ALTER TABLE {table} MODIFY data_source VARCHAR(64) NOT NULL"))
            except Exception:
                logger.debug("radar table data_source alter skipped: %s", table, exc_info=True)


def _upsert_sql(table: str, columns: list[str], key: str) -> str:
    values = ", ".join(f":{column}" for column in columns)
    updates = ", ".join(f"{column}=VALUES({column})" for column in columns if column != key)
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values}) ON DUPLICATE KEY UPDATE {updates}"


class MarketRadarEngine:
    """Collect, score and persist one full-market radar state."""

    def __init__(self, engine, config: dict[str, int | bool] | None = None):
        self.engine = engine
        self.config = config or get_market_radar_runtime_config()
        self._previous: dict[str, dict[str, float | datetime]] = {}
        self._universe: list[str] = []
        self._names: dict[str, str] = {}
        self._sectors: dict[str, dict[str, Any]] = {}
        self._metadata_loaded_at = 0.0
        self._last_events: dict[str, datetime] = {}
        self._lock = threading.Lock()
        self.last_result: dict[str, Any] | None = None

    def _load_universe(self) -> list[str]:
        try:
            rows = _read_rows(
                self.engine,
                "SELECT stock_code, short_name FROM si_all_code",
                context="market_radar_universe",
            )
        except Exception as exc:
            logger.warning("load radar stock universe failed: %s", exc)
            rows = []
        names: dict[str, str] = {}
        codes: list[str] = []
        for row in rows:
            code = _text(row.get("stock_code")).zfill(6)
            if not code.isdigit() or len(code) != 6 or not to_qmt_symbol(code):
                continue
            codes.append(code)
            names.setdefault(code, _text(row.get("short_name")))

        if not codes:
            try:
                fallback = bridge.sector_members("沪深A股", timeout=int(self.config["qmt_timeout"]))
                for row in fallback.to_dict(orient="records") if fallback is not None else []:
                    code = _text(row.get("stock_code")).zfill(6)
                    if code.isdigit() and len(code) == 6 and to_qmt_symbol(code):
                        codes.append(code)
            except Exception as exc:
                logger.warning("fallback QMT A-share universe failed: %s", exc)

        deduped = list(dict.fromkeys(codes))
        limit = int(self.config.get("stock_limit") or 0)
        if limit > 0:
            deduped = deduped[:limit]
        self._names = names
        self._universe = deduped
        return deduped

    def _load_sector_metadata(self, *, force: bool = False) -> None:
        refresh_seconds = int(self.config.get("metadata_refresh_seconds") or 900)
        if self._sectors and not force and time.time() - self._metadata_loaded_at < refresh_seconds:
            return
        codes = set(self._universe)
        sectors: dict[str, dict[str, Any]] = {}

        # Prefer the validated QMT point-in-time catalogue.  These codes are
        # shared with Trading V2 so an intraday radar observation can confirm
        # or veto the exact theme that created a conditional paper order.
        try:
            qmt_engine = get_kline_engine()
            with qmt_engine.connect() as connection:
                industry_date = connection.execute(
                    text(
                        """
                        SELECT MAX(snapshot_date)
                        FROM qmt_industry_member_snapshot
                        WHERE quality_status = 'QMT_VALIDATED'
                        """
                    )
                ).scalar()
                concept_date = connection.execute(
                    text(
                        """
                        SELECT MAX(snapshot_date)
                        FROM qmt_concept_member_snapshot
                        WHERE quality_status = 'QMT_VALIDATED'
                        """
                    )
                ).scalar()
                industry_rows = (
                    connection.execute(
                        text(
                            """
                            SELECT industry_code AS sector_key,
                                   industry_name AS sector_name,
                                   stock_code, short_name
                            FROM qmt_industry_member_snapshot
                            WHERE snapshot_date = :snapshot_date
                              AND quality_status = 'QMT_VALIDATED'
                            """
                        ),
                        {"snapshot_date": industry_date},
                    ).mappings().all()
                    if industry_date
                    else []
                )
                concept_rows = (
                    connection.execute(
                        text(
                            """
                            SELECT concept_code AS sector_key,
                                   concept_name AS sector_name,
                                   stock_code, short_name
                            FROM qmt_concept_member_snapshot
                            WHERE snapshot_date = :snapshot_date
                              AND quality_status = 'QMT_VALIDATED'
                            """
                        ),
                        {"snapshot_date": concept_date},
                    ).mappings().all()
                    if concept_date
                    else []
                )
            for sector_type, prefix, rows in (
                ("industry", "INDUSTRY", industry_rows),
                ("concept", "CONCEPT", concept_rows),
            ):
                for row in rows:
                    code = _text(row.get("stock_code")).zfill(6)
                    raw_sector_code = _text(row.get("sector_key"))
                    name = _text(row.get("sector_name")) or raw_sector_code
                    if code not in codes or not raw_sector_code:
                        continue
                    sector_code = f"{prefix}:{raw_sector_code}"
                    item = sectors.setdefault(
                        sector_code,
                        {
                            "sector_code": sector_code,
                            "sector_name": name,
                            "sector_type": sector_type,
                            "members": set(),
                        },
                    )
                    item["members"].add(code)
                    if row.get("short_name") and not self._names.get(code):
                        self._names[code] = _text(row.get("short_name"))
        except Exception as exc:
            logger.warning("load validated QMT radar memberships failed: %s", exc)

        minimum = int(self.config.get("min_sector_members") or 5)
        if sectors:
            self._sectors = {
                key: value
                for key, value in sectors.items()
                if len(value["members"]) >= minimum
            }
            self._metadata_loaded_at = time.time()
            logger.info(
                "market radar QMT metadata loaded sectors=%s stocks=%s",
                len(self._sectors),
                len(codes),
            )
            return

        try:
            industry_rows = _read_rows(
                self.engine,
                """
                SELECT stock_code, industry_name
                FROM si_industry_sw
                WHERE industry_name IS NOT NULL AND industry_name <> ''
                  AND (industry_type = '申万一级' OR industry_type LIKE '%一级%')
                """,
                context="market_radar_industry_members",
            )
        except Exception:
            industry_rows = []
        for row in industry_rows:
            code = _text(row.get("stock_code")).zfill(6)
            name = _text(row.get("industry_name"))
            if code not in codes or not name:
                continue
            sector_code = f"SW1:{name}"
            item = sectors.setdefault(
                sector_code,
                {"sector_code": sector_code, "sector_name": name, "sector_type": "industry", "members": set()},
            )
            item["members"].add(code)

        try:
            concept_rows = _read_rows(
                self.engine,
                """
                SELECT c.concept_code, c.stock_code, n.name
                FROM si_concept_constituent_east c
                LEFT JOIN si_concept_code_east n ON n.concept_code = c.concept_code
                WHERE c.concept_code IS NOT NULL AND c.concept_code <> ''
                """,
                context="market_radar_concept_members",
            )
        except Exception:
            concept_rows = []
        for row in concept_rows:
            code = _text(row.get("stock_code")).zfill(6)
            concept_code = _text(row.get("concept_code"))
            name = _text(row.get("name")) or concept_code
            if code not in codes or not concept_code:
                continue
            sector_code = f"CONCEPT:{concept_code}"
            item = sectors.setdefault(
                sector_code,
                {"sector_code": sector_code, "sector_name": name, "sector_type": "concept", "members": set()},
            )
            item["members"].add(code)

        minimum = int(self.config.get("min_sector_members") or 5)
        self._sectors = {key: value for key, value in sectors.items() if len(value["members"]) >= minimum}
        self._metadata_loaded_at = time.time()
        logger.info("market radar metadata loaded sectors=%s stocks=%s", len(self._sectors), len(codes))

    def _normalize_quote(self, row: dict[str, Any], now: datetime) -> dict[str, Any] | None:
        code = _text(row.get("stock_code")).zfill(6)
        if not code or len(code) != 6 or not code.isdigit():
            return None
        price = _float(row.get("price") or row.get("lastPrice"))
        pre_close = _float(row.get("pre_close") or row.get("last_close") or row.get("lastClose"))
        if price <= 0 and pre_close <= 0:
            return None
        if price <= 0:
            price = pre_close
        change_pct = row.get("change_pct")
        if change_pct is None and pre_close > 0:
            change_pct = (price / pre_close - 1.0) * 100.0
        ask_price = _array(_first(row, "ask_price", "askPrice"))
        ask_vol = _array(_first(row, "ask_vol", "askVol"))
        bid_price = _array(_first(row, "bid_price", "bidPrice"))
        bid_vol = _array(_first(row, "bid_vol", "bidVol"))
        weights = [1.0, 0.8, 0.6, 0.4, 0.2]
        bid_value = sum(price_i * vol_i * weights[idx] for idx, (price_i, vol_i) in enumerate(zip(bid_price, bid_vol)))
        ask_value = sum(price_i * vol_i * weights[idx] for idx, (price_i, vol_i) in enumerate(zip(ask_price, ask_vol)))
        pressure = ((bid_value - ask_value) / (bid_value + ask_value) * 100.0) if bid_value + ask_value > 0 else 0.0
        snapshot_dt = _qmt_timestamp(row.get("snapshot_at") or row.get("timetag"), now)
        previous = self._previous.get(code)
        amount = _float(row.get("amount"))
        price_speed = 0.0
        amount_delta = amount
        if previous:
            previous_dt = previous.get("snapshot_at")
            elapsed = (snapshot_dt - previous_dt).total_seconds() if isinstance(previous_dt, datetime) else 0
            previous_amount = _float(previous.get("amount"))
            previous_price = _float(previous.get("price"))
            if 0 <= elapsed <= 180:
                amount_delta = amount - previous_amount if amount >= previous_amount else amount
                price_speed = price - previous_price
        stale = int((now - snapshot_dt).total_seconds() > 180)
        return {
            "stock_code": code,
            "short_name": self._names.get(code, _text(row.get("short_name"))),
            "snapshot_at": _dt_string(snapshot_dt),
            "snapshot_dt": snapshot_dt,
            "qmt_timetag": _text(row.get("timetag") or row.get("snapshot_at")),
            "price": price,
            "pre_close": pre_close,
            "change_pct": _float(change_pct),
            "volume": _float(row.get("volume")),
            "amount": amount,
            "amount_delta": max(0.0, amount_delta),
            "price_speed": price_speed,
            "five_bid_value": bid_value,
            "five_ask_value": ask_value,
            "five_pressure": _clamp(pressure),
            "bid_price": bid_price,
            "bid_vol": bid_vol,
            "ask_price": ask_price,
            "ask_vol": ask_vol,
            "stale": stale,
            "data_source": "qmt_full_tick_5level",
        }

    def _score_stocks(self, quotes: list[dict[str, Any]]) -> None:
        if not quotes:
            return
        money_z = _robust_z([math.log1p(max(0.0, item["amount_delta"])) for item in quotes])
        price_z = _robust_z([item["change_pct"] for item in quotes])
        speed_z = _robust_z([item["price_speed"] for item in quotes])
        for idx, item in enumerate(quotes):
            item["amount_score"] = _clamp(money_z[idx] / 3.0 * 100.0)
            item["price_score"] = _clamp(price_z[idx] / 3.0 * 100.0)
            item["pressure_score"] = _clamp(item["five_pressure"])
            speed_score = _clamp(speed_z[idx] / 3.0 * 100.0)
            score = (
                0.35 * item["amount_score"]
                + 0.35 * item["price_score"]
                + 0.20 * item["pressure_score"]
                + 0.10 * speed_score
            )
            item["score"] = round(_clamp(score), 2)
            item["direction"] = "UP" if item["score"] >= 20 else "DOWN" if item["score"] <= -20 else "NEUTRAL"
            tags: list[str] = []
            if abs(item["amount_score"]) >= 55:
                tags.append("成交额异常")
            if abs(item["price_score"]) >= 55:
                tags.append("价格偏离")
            if abs(item["pressure_score"]) >= 35:
                tags.append("五档买压" if item["pressure_score"] > 0 else "五档卖压")
            if item["stale"]:
                tags.append("快照滞后")
            item["signal_tags"] = tags

    @staticmethod
    def _role_row(item: dict[str, Any], role: str, rank: int) -> dict[str, Any]:
        return {
            "stock_code": item["stock_code"],
            "short_name": item.get("short_name", ""),
            "score": round(_float(item.get("score")), 2),
            "change_pct": round(_float(item.get("change_pct")), 2),
            "amount_delta": round(_float(item.get("amount_delta")), 2),
            "five_pressure": round(_float(item.get("five_pressure")), 2),
            "rank": rank,
            "role": role,
        }

    def _build_sectors(self, quotes: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
        by_code = {item["stock_code"]: item for item in quotes}
        result: list[dict[str, Any]] = []
        for sector in self._sectors.values():
            members = [by_code[code] for code in sector["members"] if code in by_code]
            if len(members) < int(self.config.get("min_sector_members") or 5):
                continue
            positive = sorted((item for item in members if item["score"] > 0), key=lambda x: x["score"], reverse=True)
            negative = sorted((item for item in members if item["score"] < 0), key=lambda x: x["score"])
            avg_change = sum(item["change_pct"] for item in members) / len(members)
            breadth = (len(positive) - len(negative)) / len(members) * 100.0
            avg_score = sum(item["score"] for item in members) / len(members)
            score = _clamp(0.70 * avg_score + 0.30 * breadth)
            direction = "UP" if score >= 20 and breadth >= 10 else "DOWN" if score <= -20 and breadth <= -10 else "NEUTRAL"
            active = sorted(members, key=lambda x: x["amount_delta"], reverse=True)
            core_candidates = [item for item in active if item["direction"] == direction]
            core = core_candidates[0] if core_candidates else (active[0] if active else None)
            leaders_source = positive if direction == "UP" else negative if direction == "DOWN" else sorted(members, key=lambda x: abs(x["score"]), reverse=True)
            dragons = [self._role_row(item, f"龙{idx + 1}", idx + 1) for idx, item in enumerate(leaders_source[:3])]
            dragon_codes = {item["stock_code"] for item in leaders_source[:3]}
            followers_source = [item for item in leaders_source[3:] if item["stock_code"] not in dragon_codes][:8]
            followers = [self._role_row(item, "跟涨" if direction == "UP" else "跟跌", idx + 4) for idx, item in enumerate(followers_source)]
            core_row = self._role_row(core, "板块中军", 0) if core else None
            result.append(
                {
                    "sector_code": sector["sector_code"],
                    "sector_name": sector["sector_name"],
                    "sector_type": sector["sector_type"],
                    "snapshot_at": _dt_string(now),
                    "member_count": len(members),
                    "positive_count": len(positive),
                    "negative_count": len(negative),
                    "breadth_pct": round(breadth, 2),
                    "avg_change_pct": round(avg_change, 2),
                    "amount_delta": round(sum(item["amount_delta"] for item in members), 2),
                    "score": round(score, 2),
                    "direction": direction,
                    "dragons": dragons,
                    "core": core_row,
                    "followers": followers,
                    "data_source": "qmt_full_tick_5level+local_membership",
                }
            )
        result.sort(key=lambda item: abs(item["score"]), reverse=True)
        return result

    def _event_candidates(self, quotes: list[dict[str, Any]], sectors: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for item in sorted(quotes, key=lambda x: abs(x["score"]), reverse=True)[:30]:
            if abs(item["score"]) < 50 or item["stale"]:
                continue
            direction = item["direction"]
            key = f"stock:{item['stock_code']}:{direction}"
            events.append(
                {
                    "event_key": key,
                    "event_type": "stock_anomaly",
                    "direction": direction,
                    "stock_code": item["stock_code"],
                    "sector_code": None,
                    "sector_name": None,
                    "score": item["score"],
                    "detail": {"stock": self._role_row(item, "异动个股", 0), "tags": item["signal_tags"]},
                }
            )
        for item in sectors[:30]:
            if abs(item["score"]) < 30 or item["direction"] == "NEUTRAL":
                continue
            key = f"sector:{item['sector_code']}:{item['direction']}"
            events.append(
                {
                    "event_key": key,
                    "event_type": "sector_anomaly",
                    "direction": item["direction"],
                    "stock_code": None,
                    "sector_code": item["sector_code"],
                    "sector_name": item["sector_name"],
                    "score": item["score"],
                    "detail": {
                        "sector": {
                            "sector_name": item["sector_name"],
                            "score": item["score"],
                            "breadth_pct": item["breadth_pct"],
                            "member_count": item["member_count"],
                        },
                        "dragons": item["dragons"],
                        "core": item["core"],
                        "followers": item["followers"],
                    },
                }
            )
        cooldown = int(self.config.get("event_cooldown_seconds") or 60)
        new_events: list[dict[str, Any]] = []
        for item in events:
            previous = self._last_events.get(item["event_key"])
            if previous and (now - previous).total_seconds() < cooldown:
                continue
            self._last_events[item["event_key"]] = now
            item["snapshot_at"] = _dt_string(now)
            item["data_source"] = "qmt_full_tick_5level"
            new_events.append(item)
        return new_events

    def _persist(self, quotes: list[dict[str, Any]], sectors: list[dict[str, Any]], events: list[dict[str, Any]], now: datetime) -> None:
        stock_columns = [
            "stock_code", "short_name", "snapshot_at", "qmt_timetag", "price", "pre_close", "change_pct",
            "volume", "amount", "amount_delta", "price_speed", "five_bid_value", "five_ask_value", "five_pressure",
            "amount_score", "price_score", "pressure_score", "score", "direction", "stale", "signal_tags",
            "bid_price_json", "bid_vol_json", "ask_price_json", "ask_vol_json", "data_source", "updated_at",
        ]
        stock_rows = []
        for item in quotes:
            stock_rows.append(
                {
                    "stock_code": item["stock_code"], "short_name": item["short_name"], "snapshot_at": item["snapshot_at"],
                    "qmt_timetag": item["qmt_timetag"], "price": item["price"], "pre_close": item["pre_close"],
                    "change_pct": item["change_pct"], "volume": item["volume"], "amount": item["amount"],
                    "amount_delta": item["amount_delta"], "price_speed": item["price_speed"], "five_bid_value": item["five_bid_value"],
                    "five_ask_value": item["five_ask_value"], "five_pressure": item["five_pressure"], "amount_score": item["amount_score"],
                    "price_score": item["price_score"], "pressure_score": item["pressure_score"], "score": item["score"],
                    "direction": item["direction"], "stale": item["stale"], "signal_tags": _json(item["signal_tags"]),
                    "bid_price_json": _json(item["bid_price"]), "bid_vol_json": _json(item["bid_vol"]),
                    "ask_price_json": _json(item["ask_price"]), "ask_vol_json": _json(item["ask_vol"]),
                    "data_source": item["data_source"], "updated_at": now,
                }
            )
        sector_columns = [
            "sector_code", "sector_name", "sector_type", "snapshot_at", "member_count", "positive_count", "negative_count",
            "breadth_pct", "avg_change_pct", "amount_delta", "score", "direction", "dragon_json", "core_json", "follower_json",
            "data_source", "updated_at",
        ]
        sector_rows = [
            {
                "sector_code": item["sector_code"], "sector_name": item["sector_name"], "sector_type": item["sector_type"],
                "snapshot_at": item["snapshot_at"], "member_count": item["member_count"], "positive_count": item["positive_count"],
                "negative_count": item["negative_count"], "breadth_pct": item["breadth_pct"], "avg_change_pct": item["avg_change_pct"],
                "amount_delta": item["amount_delta"], "score": item["score"], "direction": item["direction"],
                "dragon_json": _json(item["dragons"]), "core_json": _json(item["core"]), "follower_json": _json(item["followers"]),
                "data_source": item["data_source"], "updated_at": now,
            }
            for item in sectors[: int(self.config.get("sector_limit") or 500)]
        ]
        with self.engine.begin() as conn:
            stock_sql = text(_upsert_sql(STOCK_TABLE, stock_columns, "stock_code"))
            for offset in range(0, len(stock_rows), 1000):
                conn.execute(stock_sql, stock_rows[offset : offset + 1000])
            sector_sql = text(_upsert_sql(SECTOR_TABLE, sector_columns, "sector_code"))
            for offset in range(0, len(sector_rows), 500):
                conn.execute(sector_sql, sector_rows[offset : offset + 500])
            if events:
                event_sql = text(
                    f"""INSERT INTO {EVENT_TABLE}
                    (event_key, event_type, direction, sector_code, sector_name, stock_code, snapshot_at, score, detail_json, data_source, created_at)
                    VALUES (:event_key, :event_type, :direction, :sector_code, :sector_name, :stock_code, :snapshot_at, :score, :detail_json, :data_source, :created_at)"""
                )
                conn.execute(
                    event_sql,
                    [
                        {**item, "detail_json": _json(item["detail"]), "created_at": now}
                        for item in events
                    ],
                )

    def _maybe_alert(self, events: list[dict[str, Any]], phase: str) -> None:
        if not events or not bool(self.config.get("alert_enabled")):
            return
        try:
            webhook = get_wecom_webhook(required=False)
            if not webhook:
                return
            from integrations.wecom.webhook import send_markdown

            lines = [f"**异动雷达｜{phase}**"]
            for event in events[:10]:
                target = event.get("sector_name") or event.get("stock_code") or "市场"
                lines.append(f"> {event['direction']} {target}｜评分 {event['score']:.1f}")
            send_markdown(webhook, "\n".join(lines))
        except Exception as exc:
            logger.warning("market radar alert failed: %s", exc)

    def scan_once(self) -> dict[str, Any]:
        with self._lock:
            now = datetime.now()
            phase = market_phase(now)
            ensure_radar_tables(self.engine)
            codes = self._load_universe()
            self._load_sector_metadata()
            if not codes:
                result = {
                    "status": "degraded", "error": "stock_universe_empty", "phase": phase,
                    "data_source": "qmt_full_tick_5level", "l2_available": False,
                }
                self.last_result = result
                return result
            raw = bridge.current(
                codes,
                batch_size=int(self.config.get("batch_size") or 500),
                timeout=int(self.config.get("qmt_timeout") or 120),
            )
            records = raw.to_dict(orient="records") if raw is not None and not raw.empty else []
            quotes = [item for row in records if (item := self._normalize_quote(row, now)) is not None]
            self._score_stocks(quotes)
            sectors = self._build_sectors(quotes, now)
            events = self._event_candidates(quotes, sectors, now) if phase != "closed" else []
            self._persist(quotes, sectors, events, now)
            for item in quotes:
                self._previous[item["stock_code"]] = {
                    "price": item["price"], "amount": item["amount"], "snapshot_at": item["snapshot_dt"],
                }
            result = {
                "status": "ok", "phase": phase, "snapshot_at": _dt_string(now), "quote_rows": len(quotes),
                "sector_rows": len(sectors), "event_rows": len(events), "data_source": "qmt_full_tick_5level",
                "l2_available": False,
                "method": "成交额增量/横截面异常 + 涨跌强度 + QMT五档压力 + 板块宽度",
                "events": [
                    {"event_type": event["event_type"], "direction": event["direction"], "stock_code": event["stock_code"],
                     "sector_name": event["sector_name"], "score": event["score"], "detail": event["detail"]}
                    for event in events
                ],
                "top_up_sectors": [item for item in sectors if item["direction"] == "UP"][:10],
                "top_down_sectors": [item for item in sectors if item["direction"] == "DOWN"][:10],
                "top_up_stocks": sorted((item for item in quotes if item["direction"] == "UP"), key=lambda x: x["score"], reverse=True)[:20],
                "top_down_stocks": sorted((item for item in quotes if item["direction"] == "DOWN"), key=lambda x: x["score"])[:20],
            }
            self.last_result = result
            self._maybe_alert(events, phase)
            return result


_SHARED_ENGINE: MarketRadarEngine | None = None
_SHARED_LOCK = threading.Lock()


def get_shared_radar_engine(engine=None) -> MarketRadarEngine:
    global _SHARED_ENGINE
    if _SHARED_ENGINE is None:
        with _SHARED_LOCK:
            if _SHARED_ENGINE is None:
                if engine is None:
                    raise ValueError("MarketRadarEngine requires a configured SQLAlchemy engine")
                _SHARED_ENGINE = MarketRadarEngine(engine)
    return _SHARED_ENGINE
