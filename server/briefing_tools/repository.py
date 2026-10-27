# -*- coding: utf-8 -*-
"""Bounded, read-only queries backing the A-share briefing tools."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import re
from typing import Any, Callable, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.api.routers._engine import get_engine as get_api_engine
from server.common.batch_db import quote_identifier
from server.common.config import get_briefing_tools_config
from server.common.minute_data import get_minute_engine, get_minute_stock_table, minute_source_info
from server.briefing_tools.contracts import ToolResult


_VALID_TIME = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_VALID_STOCK = re.compile(r"^\d{6}$")
_SECTOR_LIMIT_EXPRESSION = """
CASE
  WHEN k.short_name LIKE '%ST%' THEN 0.05
  WHEN k.stock_code REGEXP '^(300|301|688|689)' THEN 0.20
  WHEN k.stock_code REGEXP '^(4|8|920)' THEN 0.30
  ELSE 0.10
END
"""


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _rows(result) -> list[dict[str, Any]]:
    return [
        {str(key): _jsonable(value) for key, value in row.items()}
        for row in result.mappings().all()
    ]


def _status(rows: list[dict[str, Any]], missing: tuple[str, ...]) -> str:
    if not rows:
        return "NO_DATA"
    return "PARTIAL" if missing else "AVAILABLE"


def _bounded(value: int, *, maximum: int = 200) -> int:
    return max(1, min(int(maximum), int(value)))


def _parse_window(value: str) -> tuple[str, str]:
    parts = [item.strip() for item in str(value or "").split("-", 1)]
    if (
        len(parts) != 2
        or _VALID_TIME.fullmatch(parts[0]) is None
        or _VALID_TIME.fullmatch(parts[1]) is None
        or parts[0] >= parts[1]
    ):
        raise ValueError("time_window must use ascending HH:MM-HH:MM values")
    return f"{parts[0]}:00", f"{parts[1]}:00"


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _stock_minute_expressions() -> tuple[str, str, str]:
    info = minute_source_info()
    if info["kind"] == "ohlc":
        return (
            "close",
            """
            CASE
              WHEN pre_close IS NOT NULL AND pre_close > 0
              THEN (close - pre_close) / pre_close * 100
              ELSE NULL
            END
            """,
            "close IS NOT NULL AND close > 0",
        )
    return (
        "price",
        "change_pct",
        "price IS NOT NULL AND price > 0",
    )


class BriefingDataReader:
    """Execute the only SQL permitted by the briefing tool process."""

    def __init__(
        self,
        *,
        api_engine_factory: Callable[[], Engine] = get_api_engine,
        minute_engine_factory: Callable[[], Engine] = get_minute_engine,
    ) -> None:
        self._api_engine_factory = api_engine_factory
        self._minute_engine_factory = minute_engine_factory
        config = get_briefing_tools_config()
        self._timeout_ms = min(
            60_000,
            max(1_000, int(config["timeout_seconds"]) * 1_000),
        )

    def _read(
        self,
        factory: Callable[[], Engine],
        sql: str,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        engine = factory()
        with engine.connect() as connection:
            if engine.dialect.name == "mysql":
                connection.execute(
                    text(
                        "SET SESSION MAX_EXECUTION_TIME = "
                        f"{int(self._timeout_ms)}"
                    )
                )
            return _rows(connection.execute(text(sql), dict(params or {})))

    def _stock_names(self, codes: list[str]) -> dict[str, str]:
        if not codes:
            return {}
        placeholders = ", ".join(f":code_{index}" for index in range(len(codes)))
        rows = self._read(
            self._api_engine_factory,
            f"""
            SELECT stock_code, short_name
            FROM si_all_code
            WHERE stock_code IN ({placeholders})
            LIMIT 200
            """,
            {
                f"code_{index}": code
                for index, code in enumerate(codes[:200])
            },
        )
        return {
            str(row.get("stock_code") or "").zfill(6): str(
                row.get("short_name") or row.get("stock_code") or ""
            )
            for row in rows
        }

    def _resolve_sector(self, sector: str) -> dict[str, str]:
        value = str(sector or "").strip()
        if not value:
            raise ValueError("sector must not be empty")
        rows = self._read(
            self._api_engine_factory,
            """
            SELECT concept_code, name
            FROM si_concept_code_east
            WHERE concept_code = :value OR name = :value
            ORDER BY concept_code = :value DESC, name
            LIMIT 1
            """,
            {"value": value},
        )
        if not rows:
            raise ValueError("sector was not found in the verified catalog")
        return {
            "concept_code": str(rows[0].get("concept_code") or ""),
            "name": str(rows[0].get("name") or rows[0].get("concept_code") or ""),
        }

    def index_snapshot(self, trade_date: date) -> ToolResult:
        target = trade_date.isoformat()
        rows = self._read(
            self._api_engine_factory,
            """
            SELECT
                k.index_code,
                COALESCE(NULLIF(i.name, ''), k.index_code) AS index_name,
                k.trade_date,
                k.open,
                k.close,
                k.high,
                k.low,
                k.change,
                k.change_pct,
                k.amount
            FROM sm_index_kline k
            LEFT JOIN si_all_index_code i
              ON i.index_code = k.index_code
            WHERE k.trade_date = :trade_date
              AND k.k_type = 1
              AND k.index_code IN (
                '000001', '399001', '399006', '000688', '899050'
              )
            ORDER BY FIELD(
              k.index_code,
              '000001', '399001', '399006', '000688', '899050'
            )
            LIMIT 200
            """,
            {"trade_date": target},
        )
        return ToolResult(
            data=rows,
            as_of=target,
            data_status=_status(rows, ()),
            meta={"source": "sm_index_kline", "max_rows": 200},
        )

    def sector_rank(self, trade_date: date, top_n: int) -> ToolResult:
        target = trade_date.isoformat()
        limit = _bounded(top_n)
        rows = self._read(
            self._api_engine_factory,
            f"""
            WITH sector_kline AS (
                SELECT index_code, change_pct, amount
                FROM sm_concept_east_kline
                WHERE trade_date = :trade_date
                  AND k_type = 1
            ),
            sector_names AS (
                SELECT concept_code, MAX(name) AS concept_name
                FROM si_concept_code_east
                GROUP BY concept_code
            ),
            top_sectors AS (
                SELECT index_code, change_pct, amount
                FROM sector_kline
                ORDER BY change_pct DESC, amount DESC
                LIMIT :limit
            ),
            latest_flow AS (
                SELECT f.index_code, f.main_net_inflow, f.snapshot_at
                FROM sm_concept_capital_flow_east f
                INNER JOIN (
                    SELECT index_code, MAX(snapshot_at) AS snapshot_at
                    FROM sm_concept_capital_flow_east
                    WHERE days_type = 1
                      AND snapshot_at >= :trade_date
                      AND snapshot_at < DATE_ADD(:trade_date, INTERVAL 1 DAY)
                    GROUP BY index_code
                ) newest
                  ON newest.index_code = f.index_code
                 AND newest.snapshot_at = f.snapshot_at
                WHERE f.days_type = 1
            ),
            member_limits AS (
                SELECT
                    c.concept_code,
                    SUM(
                      CASE
                        WHEN k.pre_close > 0
                         AND k.close >= ROUND(
                           k.pre_close * (1 + ({_SECTOR_LIMIT_EXPRESSION})),
                           2
                         )
                        THEN 1 ELSE 0
                      END
                    ) AS limit_up_count
                FROM (
                    SELECT DISTINCT concept_code, stock_code
                    FROM si_concept_constituent_east
                ) c
                INNER JOIN sm_stock_kline k
                  ON k.stock_code = c.stock_code
                 AND k.trade_date = :trade_date
                 AND k.k_type = 1
                 AND k.adjust_type = 0
                WHERE c.concept_code IN (
                    SELECT index_code FROM top_sectors
                )
                GROUP BY c.concept_code
            )
            SELECT
                s.index_code AS sector_code,
                COALESCE(n.concept_name, s.index_code) AS sector_name,
                :trade_date AS trade_date,
                s.change_pct,
                s.amount,
                f.main_net_inflow,
                f.snapshot_at AS flow_as_of,
                COALESCE(l.limit_up_count, 0) AS limit_up_count
            FROM top_sectors s
            LEFT JOIN sector_names n
              ON n.concept_code = s.index_code
            LEFT JOIN latest_flow f
              ON f.index_code = s.index_code
            LEFT JOIN member_limits l
              ON l.concept_code = s.index_code
            ORDER BY s.change_pct DESC, s.amount DESC
            """,
            {"trade_date": target, "limit": limit},
        )
        missing = tuple(
            field
            for field in ("main_net_inflow", "amount")
            if any(row.get(field) is None for row in rows)
        )
        return ToolResult(
            data=rows,
            as_of=target,
            data_status=_status(rows, missing),
            missing_fields=missing,
            meta={
                "source": (
                    "sm_concept_east_kline+sm_concept_capital_flow_east+"
                    "si_concept_constituent_east+sm_stock_kline"
                ),
                "flow_scope": "same-date latest days_type=1 snapshot",
                "limit_status_basis": "pre_close_board_rule",
            },
        )

    def stock_rank(
        self,
        trade_date: date,
        sector_name: str,
        top_n: int,
    ) -> ToolResult:
        target = trade_date.isoformat()
        limit = _bounded(top_n)
        rows = self._read(
            self._api_engine_factory,
            f"""
            WITH selected_sectors AS (
                SELECT DISTINCT concept_code
                FROM si_concept_code_east
                WHERE name = :sector OR concept_code = :sector
            ),
            members AS (
                SELECT DISTINCT c.stock_code
                FROM si_concept_constituent_east c
                INNER JOIN selected_sectors s
                  ON s.concept_code = c.concept_code
            ),
            bars AS (
                SELECT
                    k.stock_code,
                    k.short_name,
                    k.trade_date,
                    k.close,
                    k.pre_close,
                    k.change_pct,
                    k.volume,
                    k.amount,
                    k.turnover_ratio,
                    ROW_NUMBER() OVER (
                      PARTITION BY k.stock_code
                      ORDER BY k.trade_date DESC
                    ) AS rn,
                    CASE
                      WHEN k.pre_close > 0
                       AND k.close >= ROUND(
                         k.pre_close * (1 + ({_SECTOR_LIMIT_EXPRESSION})),
                         2
                       )
                      THEN 1 ELSE 0
                    END AS is_limit_up
                FROM sm_stock_kline k
                INNER JOIN members m
                  ON m.stock_code = k.stock_code
                WHERE k.k_type = 1
                  AND k.adjust_type = 0
                  AND k.trade_date <= :trade_date
                  AND k.trade_date >= DATE_SUB(:trade_date, INTERVAL 90 DAY)
            ),
            target AS (
                SELECT *
                FROM bars
                WHERE rn = 1 AND trade_date = :trade_date
            ),
            average_volume AS (
                SELECT stock_code, AVG(volume) AS avg_volume_20
                FROM bars
                WHERE rn BETWEEN 2 AND 21
                GROUP BY stock_code
            ),
            first_break AS (
                SELECT stock_code, MIN(CASE WHEN is_limit_up = 0 THEN rn END) AS break_rn
                FROM bars
                WHERE rn BETWEEN 1 AND 10
                GROUP BY stock_code
            ),
            board_streak AS (
                SELECT
                    b.stock_code,
                    SUM(
                      CASE
                        WHEN b.is_limit_up = 1
                         AND b.rn < COALESCE(f.break_rn, 999)
                        THEN 1 ELSE 0
                      END
                    ) AS consecutive_boards
                FROM bars b
                LEFT JOIN first_break f
                  ON f.stock_code = b.stock_code
                WHERE b.rn BETWEEN 1 AND 10
                GROUP BY b.stock_code
            ),
            stock_flow AS (
                SELECT stock_code, main_net_inflow
                FROM sm_stock_capital_flow_daily
                WHERE trade_date = :trade_date
            )
            SELECT
                t.stock_code,
                COALESCE(NULLIF(t.short_name, ''), t.stock_code) AS short_name,
                CASE
                  WHEN t.stock_code LIKE '6%' THEN 'SH'
                  WHEN t.stock_code LIKE '0%' OR t.stock_code LIKE '3%' THEN 'SZ'
                  WHEN t.stock_code REGEXP '^(4|8|920)' THEN 'BJ'
                  ELSE 'UNKNOWN'
                END AS exchange,
                t.trade_date,
                t.close,
                t.change_pct,
                t.amount,
                t.turnover_ratio,
                ROUND(
                  t.volume / NULLIF(a.avg_volume_20, 0),
                  2
                ) AS volume_ratio_20d,
                f.main_net_inflow,
                t.is_limit_up,
                CASE WHEN t.is_limit_up = 1
                  THEN COALESCE(s.consecutive_boards, 1)
                  ELSE 0
                END AS consecutive_boards,
                'pre_close_board_rule' AS limit_status_basis
            FROM target t
            LEFT JOIN average_volume a
              ON a.stock_code = t.stock_code
            LEFT JOIN board_streak s
              ON s.stock_code = t.stock_code
            LEFT JOIN stock_flow f
              ON f.stock_code = t.stock_code
            WHERE COALESCE(t.short_name, '') NOT LIKE '%ST%'
            ORDER BY t.is_limit_up DESC, t.change_pct DESC, f.main_net_inflow DESC
            LIMIT :limit
            """,
            {"trade_date": target, "sector": sector_name, "limit": limit},
        )
        missing = tuple(
            field
            for field in ("volume_ratio_20d", "main_net_inflow", "turnover_ratio")
            if any(row.get(field) is None for row in rows)
        )
        return ToolResult(
            data=rows,
            as_of=target,
            data_status=_status(rows, missing),
            missing_fields=missing,
            meta={
                "source": (
                    "si_concept_constituent_east+sm_stock_kline+"
                    "sm_stock_capital_flow_daily"
                ),
                "sector": sector_name,
                "volume_ratio_definition": "volume / previous 20 trading-day average",
                "limit_status_basis": "pre_close_board_rule",
            },
        )

    def watchlist(self, trade_date: date) -> ToolResult:
        target = trade_date.isoformat()
        rows = self._read(
            self._api_engine_factory,
            """
            SELECT
                p.stock_code,
                COALESCE(NULLIF(p.short_name, ''), s.short_name, p.stock_code)
                  AS short_name,
                p.shares,
                CASE WHEN p.shares > 0 THEN 1 ELSE 0 END AS is_holding,
                p.sort_order,
                p.position_date,
                p.add_date,
                s.price,
                s.change_pct,
                s.amount,
                s.main_net_inflow,
                s.trade_date AS quote_date
            FROM st_user_portfolio p
            LEFT JOIN sm_stock_snapshot s
              ON s.stock_code = p.stock_code
             AND s.trade_date = :trade_date
            ORDER BY p.sort_order IS NULL, p.sort_order, p.stock_code
            LIMIT 200
            """,
            {"trade_date": target},
        )
        missing = tuple(
            field
            for field in ("price", "change_pct", "amount", "main_net_inflow")
            if any(row.get(field) is None for row in rows)
        )
        return ToolResult(
            data=rows,
            as_of=target,
            data_status=_status(rows, missing),
            missing_fields=missing,
            meta={
                "source": "st_user_portfolio+sm_stock_snapshot",
                "holding_definition": "shares > 0",
            },
        )

    def announcements(
        self,
        trade_date: date,
        limit: int,
    ) -> ToolResult:
        target = trade_date.isoformat()
        rows = self._read(
            self._api_engine_factory,
            """
            SELECT
                n.stock_code,
                COALESCE(NULLIF(c.short_name, ''), n.stock_code) AS short_name,
                n.title,
                n.column_name AS event_type,
                n.notice_date,
                n.display_time,
                n.source_time,
                n.detail_url,
                n.quality_status,
                n.permission_status
            FROM si_notice_eastmoney n
            LEFT JOIN si_all_code c
              ON c.stock_code = n.stock_code
            WHERE n.notice_date = :trade_date
              AND n.association_validated = 1
            ORDER BY n.source_time DESC, n.display_time DESC, n.stock_code
            LIMIT :limit
            """,
            {"trade_date": target, "limit": _bounded(limit)},
        )
        return ToolResult(
            data=rows,
            as_of=target,
            data_status=_status(rows, ()),
            meta={
                "source": "si_notice_eastmoney",
                "association_validated": 1,
            },
        )

    def market_breadth(self, trade_date: date) -> ToolResult:
        target = trade_date.isoformat()
        rows = self._read(
            self._api_engine_factory,
            """
            SELECT
                review_date AS trade_date,
                up_count,
                down_count,
                flat_count,
                volume_today AS total_amount,
                limit_up_count,
                limit_down_count,
                touch_limit_up,
                broken_board_count,
                seal_rate,
                broken_rate,
                max_boards,
                breadth_status,
                sentiment_cycle
            FROM st_daily_review_pro
            WHERE review_date = :trade_date
            LIMIT 1
            """,
            {"trade_date": target},
        )
        if rows:
            missing = tuple(
                field
                for field in (
                    "up_count",
                    "down_count",
                    "flat_count",
                    "total_amount",
                    "limit_up_count",
                    "limit_down_count",
                    "max_boards",
                )
                if rows[0].get(field) is None
            )
            return ToolResult(
                data=rows,
                as_of=target,
                data_status="PARTIAL" if missing else "AVAILABLE",
                missing_fields=missing,
                meta={"source": "st_daily_review_pro"},
            )
        fallback = self._read(
            self._api_engine_factory,
            """
            SELECT
                trade_date,
                up_cnt AS up_count,
                down_cnt AS down_count,
                sideline_cnt AS flat_count,
                total_amount,
                NULL AS limit_up_count,
                NULL AS limit_down_count,
                NULL AS touch_limit_up,
                NULL AS broken_board_count,
                NULL AS seal_rate,
                NULL AS broken_rate,
                NULL AS max_boards,
                quality_status,
                NULL AS sentiment_cycle
            FROM sm_market_overview_daily
            WHERE trade_date = :trade_date
            LIMIT 1
            """,
            {"trade_date": target},
        )
        return ToolResult(
            data=fallback,
            as_of=target,
            data_status="PARTIAL" if fallback else "NO_DATA",
            missing_fields=(
                "limit_up_count",
                "limit_down_count",
                "touch_limit_up",
                "broken_board_count",
                "seal_rate",
                "broken_rate",
                "max_boards",
            )
            if fallback
            else (),
            meta={
                "source": "sm_market_overview_daily",
                "daily_review_available": False,
            },
        )

    def intraday_snapshot(
        self,
        trade_date: date,
        time_from: str,
        time_to: str,
        scope: str,
        limit: int,
    ) -> ToolResult:
        for value in (time_from, time_to):
            if _VALID_TIME.fullmatch(value) is None:
                raise ValueError("time values must use HH:MM")
        if time_from >= time_to:
            raise ValueError("time_from must be earlier than time_to")
        normalized = str(scope or "market").strip()
        limit = _bounded(limit)
        if normalized == "market":
            table = quote_identifier(get_minute_stock_table())
            price_expr, change_expr, valid_expr = _stock_minute_expressions()
            rows = self._read(
                self._minute_engine_factory,
                f"""
                WITH ranked AS (
                    SELECT
                        stock_code,
                        trade_time,
                        {price_expr} AS price,
                        {change_expr} AS change_pct,
                        volume,
                        amount,
                        ROW_NUMBER() OVER (
                          PARTITION BY stock_code
                          ORDER BY trade_time DESC
                        ) AS rn
                    FROM {table}
                    WHERE trade_date = :trade_date
                      AND TIME(trade_time) >= :time_from
                      AND TIME(trade_time) <= :time_to
                      AND {valid_expr}
                )
                SELECT stock_code, trade_time, price, change_pct, volume, amount
                FROM ranked
                WHERE rn = 1
                ORDER BY amount DESC
                LIMIT :limit
                """,
                {
                    "trade_date": trade_date.isoformat(),
                    "time_from": f"{time_from}:00",
                    "time_to": f"{time_to}:00",
                    "limit": limit,
                },
            )
            names = self._stock_names(
                [str(row.get("stock_code") or "") for row in rows]
            )
            for row in rows:
                code = str(row.get("stock_code") or "").zfill(6)
                row["short_name"] = names.get(code, code)
                row["scope_type"] = "stock"
                row["scope_code"] = code
            return ToolResult(
                data=rows,
                as_of=f"{trade_date.isoformat()} {time_to}:00",
                data_status=_status(rows, ()),
                meta={
                    "scope": "market",
                    "source": minute_source_info()["table"],
                    "snapshot_rule": "latest available row per stock at time_to",
                },
            )

        if normalized.startswith("stock:"):
            code = normalized.split(":", 1)[1].strip()
            if _VALID_STOCK.fullmatch(code) is None:
                raise ValueError("stock scope requires a six-digit code")
            table = quote_identifier(get_minute_stock_table())
            price_expr, change_expr, valid_expr = _stock_minute_expressions()
            rows = self._read(
                self._minute_engine_factory,
                f"""
                SELECT
                    stock_code,
                    trade_time,
                    {price_expr} AS price,
                    {change_expr} AS change_pct,
                    volume,
                    amount
                FROM {table}
                WHERE trade_date = :trade_date
                  AND stock_code = :stock_code
                  AND TIME(trade_time) >= :time_from
                  AND TIME(trade_time) <= :time_to
                  AND {valid_expr}
                ORDER BY trade_time
                LIMIT :limit
                """,
                {
                    "trade_date": trade_date.isoformat(),
                    "stock_code": code,
                    "time_from": f"{time_from}:00",
                    "time_to": f"{time_to}:00",
                    "limit": limit,
                },
            )
            name = self._stock_names([code]).get(code, code)
            for row in rows:
                row["short_name"] = name
                row["scope_type"] = "stock"
                row["scope_code"] = code
            return ToolResult(
                data=rows,
                as_of=f"{trade_date.isoformat()} {time_to}:00",
                data_status=_status(rows, ()),
                meta={
                    "scope": f"stock:{code}",
                    "source": minute_source_info()["table"],
                },
            )

        if normalized.startswith("sector:"):
            sector = normalized.split(":", 1)[1].strip()
            resolved = self._resolve_sector(sector)
            rows = self._read(
                self._minute_engine_factory,
                """
                SELECT
                    index_code,
                    trade_time,
                    price,
                    change_pct,
                    volume,
                    amount
                FROM sm_concept_east_minute
                WHERE trade_date = :trade_date
                  AND index_code = :index_code
                  AND TIME(trade_time) >= :time_from
                  AND TIME(trade_time) <= :time_to
                ORDER BY trade_time
                LIMIT :limit
                """,
                {
                    "trade_date": trade_date.isoformat(),
                    "index_code": resolved["concept_code"],
                    "time_from": f"{time_from}:00",
                    "time_to": f"{time_to}:00",
                    "limit": limit,
                },
            )
            for row in rows:
                row["scope_type"] = "sector"
                row["scope_code"] = resolved["concept_code"]
                row["short_name"] = resolved["name"]
            return ToolResult(
                data=rows,
                as_of=f"{trade_date.isoformat()} {time_to}:00",
                data_status=_status(rows, ()),
                meta={
                    "scope": f"sector:{resolved['concept_code']}",
                    "sector_name": resolved["name"],
                    "source": "sm_concept_east_minute",
                },
            )

        raise ValueError("scope must be market, stock:<code>, or sector:<name|code>")

    def limit_up_events(
        self,
        trade_date: date,
        stock_code: str,
        time_from: str,
        time_to: str,
        limit: int,
    ) -> ToolResult:
        for value in (time_from, time_to):
            if _VALID_TIME.fullmatch(value) is None:
                raise ValueError("time values must use HH:MM")
        if time_from >= time_to:
            raise ValueError("time_from must be earlier than time_to")
        code = str(stock_code or "").strip()
        if code and _VALID_STOCK.fullmatch(code) is None:
            raise ValueError("stock_code must contain six digits")
        params: dict[str, Any] = {
            "trade_date": trade_date.isoformat(),
            "time_from": f"{time_from}:00",
            "time_to": f"{time_to}:00",
            "limit": _bounded(limit),
        }
        code_filter = ""
        if code:
            code_filter = "AND stock_code = :stock_code"
            params["stock_code"] = code
        rows = self._read(
            self._api_engine_factory,
            f"""
            SELECT
                trade_date,
                stock_code,
                short_name,
                event_type,
                event_at,
                price,
                pre_close,
                upper_limit,
                limit_status_basis,
                bid1_price,
                bid1_volume_raw,
                previous_bid1_volume_raw,
                ask1_price,
                ask1_volume_raw,
                source_provider,
                source_snapshot_at
            FROM sm_limit_up_event
            WHERE trade_date = :trade_date
              AND TIME(event_at) >= :time_from
              AND TIME(event_at) <= :time_to
              {code_filter}
            ORDER BY event_at, stock_code, event_type
            LIMIT :limit
            """,
            params,
        )
        return ToolResult(
            data=rows,
            as_of=f"{trade_date.isoformat()} {time_to}:00",
            data_status=_status(rows, ()),
            meta={
                "source": "sm_limit_up_event",
                "limit_status_basis": "pre_close_board_rule",
                "raw_level_unit": "QMT_TRANSPORT_RAW",
            },
        )

    def _sector_window_summary(
        self,
        trade_date: date,
        time_from: str,
        time_to: str,
        concept_code: str,
    ) -> dict[str, Any] | None:
        rows = self._read(
            self._minute_engine_factory,
            """
            WITH ranked AS (
                SELECT
                    index_code,
                    trade_time,
                    change_pct,
                    FIRST_VALUE(change_pct) OVER (
                      PARTITION BY index_code ORDER BY trade_time
                    ) AS baseline_change_pct,
                    ROW_NUMBER() OVER (
                      PARTITION BY index_code ORDER BY trade_time
                    ) AS first_rn,
                    ROW_NUMBER() OVER (
                      PARTITION BY index_code ORDER BY trade_time DESC
                    ) AS last_rn
                FROM sm_concept_east_minute
                WHERE trade_date = :trade_date
                  AND index_code = :concept_code
                  AND TIME(trade_time) >= :time_from
                  AND TIME(trade_time) <= :time_to
            )
            SELECT
                index_code,
                MAX(CASE WHEN first_rn = 1 THEN trade_time END) AS first_at,
                MAX(CASE WHEN last_rn = 1 THEN trade_time END) AS last_at,
                MAX(CASE WHEN first_rn = 1 THEN change_pct END) AS first_change_pct,
                MAX(CASE WHEN last_rn = 1 THEN change_pct END) AS last_change_pct,
                MIN(
                  CASE
                    WHEN change_pct >= baseline_change_pct + :trigger_pp
                    THEN trade_time
                  END
                ) AS movement_started_at
            FROM ranked
            GROUP BY index_code
            LIMIT 1
            """,
            {
                "trade_date": trade_date.isoformat(),
                "concept_code": concept_code,
                "time_from": time_from,
                "time_to": time_to,
                "trigger_pp": 1.0,
            },
        )
        return rows[0] if rows else None

    def _member_window_summary(
        self,
        trade_date: date,
        time_from: str,
        time_to: str,
        codes: list[str],
    ) -> list[dict[str, Any]]:
        if not codes:
            return []
        table = quote_identifier(get_minute_stock_table())
        _, change_expr, valid_expr = _stock_minute_expressions()
        placeholders = ", ".join(f":code_{index}" for index in range(len(codes)))
        params: dict[str, Any] = {
            "trade_date": trade_date.isoformat(),
            "time_from": time_from,
            "time_to": time_to,
            "trigger_pp": 1.0,
            **{f"code_{index}": code for index, code in enumerate(codes)},
        }
        return self._read(
            self._minute_engine_factory,
            f"""
            WITH ranked AS (
                SELECT
                    stock_code,
                    trade_time,
                    {change_expr} AS change_pct,
                    FIRST_VALUE({change_expr}) OVER (
                      PARTITION BY stock_code ORDER BY trade_time
                    ) AS baseline_change_pct,
                    ROW_NUMBER() OVER (
                      PARTITION BY stock_code ORDER BY trade_time
                    ) AS first_rn,
                    ROW_NUMBER() OVER (
                      PARTITION BY stock_code ORDER BY trade_time DESC
                    ) AS last_rn
                FROM {table}
                WHERE trade_date = :trade_date
                  AND stock_code IN ({placeholders})
                  AND TIME(trade_time) >= :time_from
                  AND TIME(trade_time) <= :time_to
                  AND {valid_expr}
            )
            SELECT
                stock_code,
                MAX(CASE WHEN first_rn = 1 THEN trade_time END) AS first_at,
                MAX(CASE WHEN last_rn = 1 THEN trade_time END) AS last_at,
                MAX(CASE WHEN first_rn = 1 THEN change_pct END) AS first_change_pct,
                MAX(CASE WHEN last_rn = 1 THEN change_pct END) AS last_change_pct,
                MIN(
                  CASE
                    WHEN change_pct >= baseline_change_pct + :trigger_pp
                    THEN trade_time
                  END
                ) AS movement_started_at
            FROM ranked
            GROUP BY stock_code
            ORDER BY last_change_pct DESC
            LIMIT 200
            """,
            params,
        )

    def sector_linkage(
        self,
        trade_date: date,
        time_window: str,
        sector: str,
        limit: int,
    ) -> ToolResult:
        time_from, time_to = _parse_window(time_window)
        resolved: dict[str, str]
        if sector:
            resolved = self._resolve_sector(sector)
        else:
            candidates = self._read(
                self._minute_engine_factory,
                """
                WITH ranked AS (
                    SELECT
                        index_code,
                        change_pct,
                        ROW_NUMBER() OVER (
                          PARTITION BY index_code ORDER BY trade_time
                        ) AS first_rn,
                        ROW_NUMBER() OVER (
                          PARTITION BY index_code ORDER BY trade_time DESC
                        ) AS last_rn
                    FROM sm_concept_east_minute
                    WHERE trade_date = :trade_date
                      AND TIME(trade_time) >= :time_from
                      AND TIME(trade_time) <= :time_to
                )
                SELECT
                    index_code AS concept_code,
                    MAX(CASE WHEN first_rn = 1 THEN change_pct END) AS first_change_pct,
                    MAX(CASE WHEN last_rn = 1 THEN change_pct END) AS last_change_pct
                FROM ranked
                GROUP BY index_code
                ORDER BY ABS(
                  MAX(CASE WHEN last_rn = 1 THEN change_pct END)
                  - MAX(CASE WHEN first_rn = 1 THEN change_pct END)
                ) DESC
                LIMIT 20
                """,
                {
                    "trade_date": trade_date.isoformat(),
                    "time_from": time_from,
                    "time_to": time_to,
                },
            )
            if not candidates:
                return ToolResult(
                    data=[],
                    as_of=f"{trade_date.isoformat()} {time_to}",
                    data_status="NO_DATA",
                    meta={"source": "sm_concept_east_minute"},
                )
            top_code = str(candidates[0].get("concept_code") or "")
            names = self._read(
                self._api_engine_factory,
                """
                SELECT concept_code, MAX(name) AS name
                FROM si_concept_code_east
                WHERE concept_code = :concept_code
                GROUP BY concept_code
                LIMIT 1
                """,
                {"concept_code": top_code},
            )
            resolved = {
                "concept_code": top_code,
                "name": str(names[0].get("name") or top_code) if names else top_code,
            }

        member_rows = self._read(
            self._api_engine_factory,
            """
            SELECT DISTINCT stock_code
            FROM si_concept_constituent_east
            WHERE concept_code = :concept_code
            ORDER BY stock_code
            LIMIT 200
            """,
            {"concept_code": resolved["concept_code"]},
        )
        codes = [str(row.get("stock_code") or "") for row in member_rows]
        names = self._stock_names(codes)
        sector_summary = self._sector_window_summary(
            trade_date,
            time_from,
            time_to,
            resolved["concept_code"],
        )
        member_summaries = self._member_window_summary(
            trade_date,
            time_from,
            time_to,
            codes,
        )
        sector_start = _parse_datetime(
            (sector_summary or {}).get("movement_started_at")
        )
        sector_delta = None
        if sector_summary:
            try:
                sector_delta = float(sector_summary["last_change_pct"]) - float(
                    sector_summary["first_change_pct"]
                )
            except (TypeError, ValueError):
                sector_delta = None

        movement_rows: list[dict[str, Any]] = []
        for row in member_summaries:
            code = str(row.get("stock_code") or "").zfill(6)
            try:
                delta = float(row["last_change_pct"]) - float(
                    row["first_change_pct"]
                )
            except (TypeError, ValueError):
                continue
            started = _parse_datetime(row.get("movement_started_at"))
            time_delta = (
                int((started - sector_start).total_seconds())
                if started is not None and sector_start is not None
                else None
            )
            movement_rows.append(
                {
                    **row,
                    "stock_code": code,
                    "short_name": names.get(code, code),
                    "change_delta_pp": round(delta, 4),
                    "time_delta_vs_sector_seconds": time_delta,
                }
            )
        direction = "UP" if (sector_delta or 0) > 0 else "DOWN"
        followers = [
            row
            for row in movement_rows
            if (
                (direction == "UP" and row["change_delta_pp"] >= 1.0)
                or (direction == "DOWN" and row["change_delta_pp"] <= -1.0)
            )
        ]
        event = {
            "sector_code": resolved["concept_code"],
            "sector_name": resolved["name"],
            "window": time_window,
            "sector_first_at": (sector_summary or {}).get("first_at"),
            "sector_last_at": (sector_summary or {}).get("last_at"),
            "sector_change_delta_pp": (
                round(float(sector_delta), 4)
                if sector_delta is not None
                else None
            ),
            "sector_movement_started_at": (
                sector_summary or {}
            ).get("movement_started_at"),
            "direction": direction,
            "member_count": len(codes),
            "movement_member_count": len(followers),
            "linkage_status": (
                "SUBSTANTIAL_MOVE"
                if sector_delta is not None
                and abs(sector_delta) >= 1.0
                and len(followers) >= 2
                else "UNCONFIRMED"
            ),
            "members": followers[: _bounded(limit)],
        }
        missing_fields: list[str] = []
        if sector_summary is None:
            missing_fields.append("sector_window_summary")
        if len(codes) >= 200:
            missing_fields.append("members_beyond_200")
        if not member_summaries:
            missing_fields.append("member_window_summary")
        return ToolResult(
            data=[event],
            as_of=f"{trade_date.isoformat()} {time_to}",
            data_status="PARTIAL" if missing_fields else "AVAILABLE",
            missing_fields=tuple(missing_fields),
            meta={
                "source": (
                    "sm_concept_east_minute+"
                    f"{minute_source_info()['table']}+"
                    "si_concept_constituent_east"
                ),
                "movement_threshold_pp": 1.0,
                "membership_limit": 200,
                "causality": (
                    "time_delta_vs_sector_seconds is evidence only; "
                    "the caller must preserve its sign and magnitude"
                ),
            },
        )

    def market_breadth_intraday(
        self,
        trade_date: date,
        time_from: str,
        time_to: str,
        interval_minutes: int,
    ) -> ToolResult:
        for value in (time_from, time_to):
            if _VALID_TIME.fullmatch(value) is None:
                raise ValueError("time values must use HH:MM")
        if time_from >= time_to:
            raise ValueError("time_from must be earlier than time_to")
        if interval_minutes not in {1, 5, 15, 30, 60}:
            raise ValueError("interval_minutes must be one of 1, 5, 15, 30, 60")
        table = quote_identifier(get_minute_stock_table())
        _, change_expr, valid_expr = _stock_minute_expressions()
        rows = self._read(
            self._minute_engine_factory,
            f"""
            WITH base AS (
                SELECT
                    stock_code,
                    trade_time,
                    {change_expr} AS change_pct,
                    amount,
                    FLOOR(
                      TIME_TO_SEC(TIME(trade_time)) / 60 / :interval_minutes
                    ) * :interval_minutes AS bucket_minute,
                    ROW_NUMBER() OVER (
                      PARTITION BY
                        stock_code,
                        FLOOR(
                          TIME_TO_SEC(TIME(trade_time)) / 60 / :interval_minutes
                        )
                      ORDER BY trade_time DESC
                    ) AS rn
                FROM {table}
                WHERE trade_date = :trade_date
                  AND TIME(trade_time) >= :time_from
                  AND TIME(trade_time) <= :time_to
                  AND {valid_expr}
            ),
            latest AS (
                SELECT *
                FROM base
                WHERE rn = 1
            )
            SELECT
                SEC_TO_TIME(bucket_minute * 60) AS bucket,
                COUNT(*) AS stock_count,
                SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_count,
                SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_count,
                SUM(CASE WHEN change_pct = 0 THEN 1 ELSE 0 END) AS flat_count,
                SUM(
                  CASE
                    WHEN stock_code REGEXP '^(300|301|688|689)'
                     AND change_pct >= 19.8
                    THEN 1
                    WHEN stock_code REGEXP '^(4|8|920)'
                     AND change_pct >= 29.8
                    THEN 1
                    WHEN stock_code NOT REGEXP '^(300|301|688|689|4|8|920)'
                     AND change_pct >= 9.8
                    THEN 1 ELSE 0
                  END
                ) AS limit_up_count_approx,
                SUM(
                  CASE
                    WHEN stock_code REGEXP '^(300|301|688|689)'
                     AND change_pct <= -19.8
                    THEN 1
                    WHEN stock_code REGEXP '^(4|8|920)'
                     AND change_pct <= -29.8
                    THEN 1
                    WHEN stock_code NOT REGEXP '^(300|301|688|689|4|8|920)'
                     AND change_pct <= -9.8
                    THEN 1 ELSE 0
                  END
                ) AS limit_down_count_approx,
                COALESCE(SUM(amount), 0) AS total_amount
            FROM latest
            GROUP BY bucket_minute
            ORDER BY bucket_minute
            LIMIT 200
            """,
            {
                "trade_date": trade_date.isoformat(),
                "time_from": f"{time_from}:00",
                "time_to": f"{time_to}:00",
                "interval_minutes": interval_minutes,
            },
        )
        event_rows = self._read(
            self._api_engine_factory,
            """
            SELECT
                SEC_TO_TIME(
                  FLOOR(
                    TIME_TO_SEC(TIME(event_at)) / 60 / :interval_minutes
                  ) * :interval_minutes * 60
                ) AS bucket,
                COUNT(
                  DISTINCT CASE
                    WHEN event_type = 'TOUCH' THEN stock_code
                  END
                ) AS touched_stock_count,
                COUNT(
                  DISTINCT CASE
                    WHEN event_type = 'BREAK' THEN stock_code
                  END
                ) AS broken_stock_count
            FROM sm_limit_up_event
            WHERE trade_date = :trade_date
              AND TIME(event_at) >= :time_from
              AND TIME(event_at) <= :time_to
            GROUP BY bucket
            ORDER BY bucket
            LIMIT 200
            """,
            {
                "trade_date": trade_date.isoformat(),
                "time_from": f"{time_from}:00",
                "time_to": f"{time_to}:00",
                "interval_minutes": interval_minutes,
            },
        )
        event_by_bucket = {
            str(row.get("bucket") or ""): row
            for row in event_rows
        }
        for row in rows:
            event = event_by_bucket.get(str(row.get("bucket") or ""), {})
            touched = int(event.get("touched_stock_count") or 0)
            broken = int(event.get("broken_stock_count") or 0)
            row["touched_stock_count"] = touched
            row["broken_stock_count"] = broken
            row["broken_rate"] = (
                round(broken / touched * 100.0, 4)
                if touched
                else None
            )
            row["limit_status_basis"] = (
                "code_board_threshold_non_st_approx"
            )
        return ToolResult(
            data=rows,
            as_of=f"{trade_date.isoformat()} {time_to}:00",
            data_status=(
                "PARTIAL"
                if rows
                else "NO_DATA"
            ),
            missing_fields=(
                "exact_st_limit_adjustment",
                "exact_limit_price",
            )
            if rows
            else (),
            meta={
                "source": (
                    f"{minute_source_info()['table']}+"
                    "sm_limit_up_event"
                ),
                "interval_minutes": interval_minutes,
                "limit_up_count_approx": True,
                "broken_rate_definition": (
                    "distinct broken stocks in bucket / "
                    "distinct touched stocks in bucket"
                ),
            },
        )


__all__ = ["BriefingDataReader"]
