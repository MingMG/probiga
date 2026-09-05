"""Read-only V2 repository used by API handlers.

All writes live in explicit worker/migration commands. Functions in this module
execute SELECT statements only.
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


V2_TABLES: tuple[str, ...] = (
    "st_trade_account_v2",
    "st_strategy_version_v2",
    "st_data_snapshot_v2",
    "st_decision_run_v2",
    "st_strategy_signal_v2",
    "st_portfolio_plan_v2",
    "st_trade_intent_v2",
    "st_risk_decision_v2",
    "st_order_v2",
    "st_fill_v2",
    "st_position_lot_v2",
    "st_cash_ledger_v2",
    "st_equity_daily_v2",
    "st_reconciliation_v2",
    "st_trade_event_v2",
    "st_quote_event_v2",
    "st_execution_capability_v2",
    "st_fee_profile_v2",
    "st_instrument_rule_v2",
    "st_backtest_run_v2",
    "st_backtest_trade_v2",
    "st_strategy_health_daily_v2",
    "st_fault_drill_v2",
    "st_worker_heartbeat_v2",
    "si_etf_code",
    "sm_etf_kline",
    "st_etf_forward_strategy",
    "st_etf_forward_observation",
    "st_intraday_market_state_v2",
    "st_intraday_activation_v2",
    "st_qmt_minute_sync_receipt_v2",
    "st_qmt_realtime_sync_receipt_v2",
    "st_intraday_watch_quote_v2",
    "st_public_quote_current_v2",
    "st_public_quote_receipt_v2",
)


JSON_COLUMNS = {
    "manifest_json",
    "validation_json",
    "source_manifest_json",
    "blocked_capabilities_json",
    "raw_features_json",
    "evidence_json",
    "positions_json",
    "rejected_candidates_json",
    "theme_exposure_json",
    "checks_json",
    "event_payload_json",
    "request_json",
    "result_json",
    "other_fee_json",
    "target_json",
    "context_json",
    "config_json",
}


def _normalize(value: Any) -> Any:
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _decode_row(row: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in row.items():
        if key in JSON_COLUMNS and isinstance(value, str):
            try:
                output[key.removesuffix("_json")] = json.loads(value)
            except json.JSONDecodeError:
                output[key.removesuffix("_json")] = None
        else:
            output[key] = _normalize(value)
    return output


class TradingV2ReadRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def _all(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(text(sql), params or {}).mappings().all()
        return [_decode_row(dict(row)) for row in rows]

    def _one(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        rows = self._all(sql, params)
        return rows[0] if rows else None

    def _security_name_map(self, codes: list[str]) -> dict[str, str]:
        normalized = sorted(
            {
                str(code or "").strip().split(".", 1)[0].zfill(6)
                for code in codes
                if str(code or "").strip()
            }
        )
        if not normalized:
            return {}
        params = {
            f"security_code_{index}": code
            for index, code in enumerate(normalized)
        }
        placeholders = ",".join(f":{key}" for key in params)
        rows = self._all(
            f"""
            SELECT stock_code AS security_code, short_name
            FROM si_all_code
            WHERE stock_code IN ({placeholders})
            """,
            params,
        )
        rows.extend(
            self._all(
                f"""
                SELECT etf_code AS security_code, short_name
                FROM si_etf_code
                WHERE etf_code IN ({placeholders})
                """,
                params,
            )
        )
        names: dict[str, str] = {}
        for row in rows:
            code = str(row.get("security_code") or "").split(".", 1)[0].zfill(6)
            short_name = str(row.get("short_name") or "").strip()
            if code and short_name:
                names[code] = short_name
        return names

    def _enrich_security_rows(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        names = self._security_name_map(
            [
                str(row.get("stock_code") or row.get("etf_code") or "")
                for row in rows
            ]
        )
        output: list[dict[str, Any]] = []
        for source in rows:
            row = dict(source)
            code = str(
                row.get("stock_code") or row.get("etf_code") or ""
            ).split(".", 1)[0].zfill(6)
            if code and not row.get("short_name"):
                row["short_name"] = names.get(code) or code
            output.append(row)
        return output

    def _enrich_plan(
        self,
        plan: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not plan:
            return plan
        output = dict(plan)
        positions = list(output.get("positions") or [])
        rejected = list(output.get("rejected_candidates") or [])
        names = self._security_name_map(
            [
                str(row.get("stock_code") or "")
                for row in positions + rejected
            ]
        )
        for rows in (positions, rejected):
            for row in rows:
                code = str(row.get("stock_code") or "").split(".", 1)[0].zfill(6)
                if code and not row.get("short_name"):
                    row["short_name"] = names.get(code) or code
        output["positions"] = positions
        output["rejected_candidates"] = rejected
        return output

    def table_readiness(self) -> dict[str, bool]:
        names = self._all(
            """
            SELECT TABLE_NAME AS table_name
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME IN (
                'st_trade_account_v2','st_strategy_version_v2',
                'st_data_snapshot_v2','st_decision_run_v2',
                'st_strategy_signal_v2','st_portfolio_plan_v2',
                'st_trade_intent_v2','st_risk_decision_v2',
                'st_order_v2','st_fill_v2','st_position_lot_v2',
                'st_cash_ledger_v2','st_equity_daily_v2',
                'st_reconciliation_v2','st_trade_event_v2',
                'st_quote_event_v2','st_execution_capability_v2',
                'st_fee_profile_v2','st_instrument_rule_v2',
                'st_backtest_run_v2','st_backtest_trade_v2',
                'st_strategy_health_daily_v2','st_fault_drill_v2',
                'st_worker_heartbeat_v2',
                'si_etf_code','sm_etf_kline',
                'st_etf_forward_strategy',
                'st_etf_forward_observation',
                'st_intraday_market_state_v2',
                'st_intraday_activation_v2',
                'st_qmt_minute_sync_receipt_v2',
                'st_qmt_realtime_sync_receipt_v2',
                'st_intraday_watch_quote_v2',
                'st_public_quote_current_v2',
                'st_public_quote_receipt_v2'
              )
            """
        )
        existing = {str(row["table_name"]) for row in names}
        return {name: name in existing for name in V2_TABLES}

    def execution_capability(self, capability_code: str) -> dict[str, Any] | None:
        return self._one(
            """
            SELECT * FROM st_execution_capability_v2
            WHERE capability_code = :capability_code
            """,
            {"capability_code": capability_code},
        )
    def fee_profile_confirmation(
        self,
        fee_profile_version: str,
    ) -> dict[str, Any] | None:
        if not fee_profile_version:
            return None
        return self._one(
            """
            SELECT
                fee_profile_version,
                COUNT(*) AS profile_count,
                SUM(
                    CASE WHEN confirmation_status = 'CONFIRMED'
                         THEN 1 ELSE 0 END
                ) AS confirmed_profile_count,
                COUNT(
                    DISTINCT CASE
                        WHEN confirmation_status = 'CONFIRMED'
                         AND security_type IN ('A_SHARE','ETF')
                        THEN security_type
                    END
                ) AS confirmed_required_type_count
            FROM st_fee_profile_v2
            WHERE fee_profile_version = :fee_profile_version
            GROUP BY fee_profile_version
            """,
            {"fee_profile_version": fee_profile_version},
        )
    def latest_snapshot(self) -> dict[str, Any] | None:
        return self._one(
            """
            SELECT * FROM st_data_snapshot_v2
            ORDER BY decision_at DESC, created_at DESC, snapshot_id DESC
            LIMIT 1
            """
        )

    def latest_regime(self) -> dict[str, Any] | None:
        return self._one(
            """
            SELECT run_uid, trade_date, decision_at, snapshot_id,
                   market_regime, market_regime_version, status,
                   result_hash, code_commit_sha, config_version
            FROM st_decision_run_v2
            WHERE status IN ('COMPLETED','BLOCKED')
            ORDER BY decision_at DESC, started_at DESC, run_uid DESC
            LIMIT 1
            """
        )

    def strategies(self) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT * FROM st_strategy_version_v2
            ORDER BY strategy_id, version
            """
        )

    def decision_run(self, run_uid: str) -> dict[str, Any] | None:
        run = self._one(
            "SELECT * FROM st_decision_run_v2 WHERE run_uid = :run_uid",
            {"run_uid": run_uid},
        )
        if not run:
            return None
        run["signals"] = self._enrich_security_rows(
            self._all(
                """
                SELECT * FROM st_strategy_signal_v2
                WHERE run_uid = :run_uid
                ORDER BY competition_status, stock_code, strategy_version
                """,
                {"run_uid": run_uid},
            )
        )
        run["plans"] = [
            self._enrich_plan(plan)
            for plan in self._all(
                """
                SELECT * FROM st_portfolio_plan_v2
                WHERE run_uid = :run_uid
                ORDER BY account_id
                """,
                {"run_uid": run_uid},
            )
        ]
        return run

    def decision_runs(
        self,
        *,
        trade_date: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """List persisted decision batches without recalculating history."""
        where = "AND r.trade_date = :trade_date" if trade_date else ""
        return self._all(
            f"""
            SELECT
                r.run_uid,
                r.trade_date,
                r.decision_at,
                r.started_at,
                r.finished_at,
                r.status,
                r.market_regime,
                r.market_regime_version,
                r.snapshot_id,
                r.result_hash,
                r.code_commit_sha,
                r.config_version,
                (
                    SELECT COUNT(*)
                    FROM st_strategy_signal_v2 s
                    WHERE s.run_uid = r.run_uid
                ) AS signal_count
            FROM st_decision_run_v2 r
            WHERE r.status IN ('COMPLETED','BLOCKED')
              {where}
            ORDER BY
                r.trade_date DESC,
                r.decision_at DESC,
                r.started_at DESC,
                r.run_uid DESC
            LIMIT :limit
            """,
            {
                "trade_date": trade_date,
                "limit": int(limit),
            },
        )

    def candidates(self, *, run_uid: str = "", limit: int = 200) -> list[dict[str, Any]]:
        if not run_uid:
            row = self._one(
                """
                SELECT run_uid FROM st_decision_run_v2
                WHERE status IN ('COMPLETED','BLOCKED')
                ORDER BY decision_at DESC, started_at DESC, run_uid DESC
                LIMIT 1
                """
            )
            run_uid = str((row or {}).get("run_uid") or "")
        if not run_uid:
            return []
        return self._enrich_security_rows(
            self._all(
                """
                SELECT * FROM st_strategy_signal_v2
                WHERE run_uid = :run_uid
                ORDER BY
                    CASE competition_status
                        WHEN 'ELIGIBLE' THEN 0
                        WHEN 'PAPER_TRIAL_ELIGIBLE' THEN 1
                        WHEN 'RESEARCH_ONLY' THEN 2
                        ELSE 3
                    END,
                    expected_return_lower_bound DESC,
                    risk_reward_ratio DESC,
                    raw_score DESC,
                    stock_code ASC,
                    strategy_version ASC
                LIMIT :limit
                """,
                {"run_uid": run_uid, "limit": int(limit)},
            )
        )

    def intraday_summary(
        self,
        *,
        account_id: str,
        limit: int = 200,
    ) -> dict[str, Any]:
        state = self._one(
            """
            SELECT *
            FROM st_intraday_market_state_v2
            ORDER BY observed_at DESC, created_at DESC
            LIMIT 1
            """
        )
        if not state:
            return {
                "status": "waiting_first_tick",
                "market_state": None,
                "current_realtime_state": {
                    "status": "UNAVAILABLE",
                    "actionable": False,
                    "reason": "尚未收到当前交易时段的实时快照",
                    "snapshot": None,
                },
                "latest_historical_snapshot": None,
                "decisions": [],
                "automatic_real_order_submission": False,
            }
        now = datetime.now()
        observed_at = state.get("observed_at")
        try:
            observed_dt = datetime.fromisoformat(
                str(observed_at or "").replace(" ", "T")
            )
        except ValueError:
            observed_dt = None
        snapshot_age_seconds = (
            max(0.0, (now - observed_dt).total_seconds())
            if observed_dt is not None
            else None
        )
        hhmm = now.hour * 100 + now.minute
        session_open = bool(
            now.weekday() < 5
            and (
                931 <= hhmm <= 1130
                or 1301 <= hhmm <= 1500
            )
        )
        snapshot_stale = bool(
            snapshot_age_seconds is None
            or snapshot_age_seconds > 120
        )
        realtime_live = bool(session_open and not snapshot_stale)
        if not realtime_live:
            state["actionable"] = False
            state["snapshot_status"] = (
                "STALE_DURING_SESSION"
                if session_open
                else "HISTORICAL_SNAPSHOT"
            )
            state["snapshot_age_seconds"] = (
                round(snapshot_age_seconds, 3)
                if snapshot_age_seconds is not None
                else None
            )
            if session_open:
                state["quality_status"] = "BLOCK"
                state["evidence"] = [
                    "最新行情快照超过2分钟，结果已过期，只允许查看，禁止创建新买单",
                    *(state.get("evidence") or []),
                ]
        historical_state = state
        if realtime_live:
            historical_state = self._one(
                """
                SELECT *
                FROM st_intraday_market_state_v2
                WHERE observed_at < :observed_at
                ORDER BY observed_at DESC, created_at DESC
                LIMIT 1
                """,
                {"observed_at": observed_at},
            )
        decisions = self._enrich_security_rows(
            self._all(
                """
                SELECT *
                FROM st_intraday_activation_v2
                WHERE account_id = :account_id
                  AND state_id = :state_id
                ORDER BY
                    CASE status
                        WHEN 'ORDER_CREATED' THEN 0
                        WHEN 'ACTIVATABLE' THEN 1
                        WHEN 'RISK_REJECTED' THEN 2
                        ELSE 3
                    END,
                    raw_score DESC,
                    relative_strength_pct DESC,
                    stock_code
                LIMIT :limit
                """,
                {
                    "account_id": account_id,
                    "state_id": state["state_id"],
                    "limit": int(limit),
                },
            )
        )
        return {
            "status": (
                "historical"
                if not session_open
                else
                "actionable"
                if bool(state.get("actionable"))
                else "blocked"
                if state.get("quality_status") == "BLOCK"
                else "observing"
            ),
            "market_state": state,
            "current_realtime_state": {
                "status": (
                    "LIVE"
                    if realtime_live
                    else "STALE"
                    if session_open
                    else "MARKET_CLOSED"
                ),
                "actionable": bool(
                    realtime_live and state.get("actionable")
                ),
                "observed_at": (
                    state.get("observed_at")
                    if realtime_live
                    else None
                ),
                "snapshot_age_seconds": snapshot_age_seconds,
                "reason": (
                    "当前实时状态有效"
                    if realtime_live
                    else "结果已过期，当前没有可执行的实时状态"
                    if session_open
                    else "当前已收盘，仅展示最新历史快照"
                ),
                "snapshot": state if realtime_live else None,
            },
            "latest_historical_snapshot": historical_state,
            "decisions": decisions,
            "decision_count": len(decisions),
            "order_created_count": sum(
                item.get("status") == "ORDER_CREATED"
                for item in decisions
            ),
            "automatic_real_order_submission": False,
        }

    def account(self, account_id: str) -> dict[str, Any] | None:
        account = self._one(
            "SELECT * FROM st_trade_account_v2 WHERE account_id = :account_id",
            {"account_id": account_id},
        )
        if not account:
            return None
        account["latest_equity"] = self._one(
            """
            SELECT * FROM st_equity_daily_v2
            WHERE account_id = :account_id
            ORDER BY trade_date DESC LIMIT 1
            """,
            {"account_id": account_id},
        )
        account["latest_reconciliation"] = self._one(
            """
            SELECT * FROM st_reconciliation_v2
            WHERE account_id = :account_id
            ORDER BY trade_date DESC, version DESC LIMIT 1
            """,
            {"account_id": account_id},
        )
        return account

    def current_plan(self, account_id: str) -> dict[str, Any] | None:
        return self._enrich_plan(
            self._one(
                """
                SELECT p.*
                FROM st_portfolio_plan_v2 p
                INNER JOIN st_decision_run_v2 r ON r.run_uid = p.run_uid
                WHERE p.account_id = :account_id
                ORDER BY r.decision_at DESC, r.started_at DESC,
                         p.plan_version DESC
                LIMIT 1
                """,
                {"account_id": account_id},
            )
        )

    def positions(self, account_id: str) -> list[dict[str, Any]]:
        return self._enrich_security_rows(
            self._all(
                """
                SELECT * FROM st_position_lot_v2
                WHERE account_id = :account_id AND remaining_quantity > 0
                ORDER BY stock_code, opened_trade_date, lot_id
                """,
                {"account_id": account_id},
            )
        )

    def orders(self, account_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return self._enrich_security_rows(
            self._all(
                """
                SELECT * FROM st_order_v2
                WHERE account_id = :account_id
                ORDER BY created_at DESC, order_id DESC LIMIT :limit
                """,
                {"account_id": account_id, "limit": int(limit)},
            )
        )

    def fills(self, account_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return self._enrich_security_rows(
            self._all(
                """
                SELECT f.*, q.source_provider AS execution_price_source,
                       q.quote_at, q.received_at AS quote_received_at
                FROM st_fill_v2 f
                LEFT JOIN st_quote_event_v2 q
                  ON q.quote_event_id = f.quote_event_id
                WHERE f.account_id = :account_id
                ORDER BY f.filled_at DESC, f.fill_id DESC LIMIT :limit
                """,
                {"account_id": account_id, "limit": int(limit)},
            )
        )

    def cash_ledger(self, account_id: str, limit: int = 500) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT * FROM st_cash_ledger_v2
            WHERE account_id = :account_id
            ORDER BY occurred_at DESC, cash_event_id DESC LIMIT :limit
            """,
            {"account_id": account_id, "limit": int(limit)},
        )

    def reconciliations(self, account_id: str, limit: int = 100) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT * FROM st_reconciliation_v2
            WHERE account_id = :account_id
            ORDER BY trade_date DESC, version DESC LIMIT :limit
            """,
            {"account_id": account_id, "limit": int(limit)},
        )

    def daily_reports(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT e.*, r.status AS reconciliation_status,
                   r.cash_difference, r.equity_difference,
                   r.position_difference, r.order_difference,
                   r.fill_difference
            FROM st_equity_daily_v2 e
            LEFT JOIN st_reconciliation_v2 r
              ON r.account_id = e.account_id
             AND r.trade_date = e.trade_date
             AND r.version = (
                SELECT MAX(r2.version) FROM st_reconciliation_v2 r2
                WHERE r2.account_id = e.account_id
                  AND r2.trade_date = e.trade_date
             )
            ORDER BY e.trade_date DESC, e.account_id LIMIT :limit
            """,
            {"limit": int(limit)},
        )

    def job(self, job_id: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM st_job_v2 WHERE job_id = :job_id",
            {"job_id": job_id},
        )

    def backtest(self, backtest_uid: str) -> dict[str, Any] | None:
        return self._one(
            """
            SELECT b.*, v.strategy_id
            FROM st_backtest_run_v2 b
            LEFT JOIN st_strategy_version_v2 v
              ON BINARY v.version = BINARY b.strategy_version
             AND BINARY v.config_hash = BINARY b.config_hash
            WHERE b.backtest_uid = :backtest_uid
            """,
            {"backtest_uid": backtest_uid},
        )

    def backtests(self, limit: int = 30) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT b.backtest_uid, v.strategy_id, b.strategy_version,
                   b.start_date, b.end_date, b.random_seed, b.status,
                   b.gate_status, b.error_code, b.error_message,
                   b.request_hash, b.data_snapshot_hash,
                   b.code_commit_sha, b.config_hash, b.protocol_version,
                   b.result_json, b.result_hash, b.started_at, b.finished_at
            FROM st_backtest_run_v2 b
            LEFT JOIN st_strategy_version_v2 v
              ON BINARY v.version = BINARY b.strategy_version
             AND BINARY v.config_hash = BINARY b.config_hash
            ORDER BY b.started_at DESC, b.backtest_uid DESC
            LIMIT :limit
            """,
            {"limit": max(1, min(int(limit), 100))},
        )

    def tomorrow_action(self, account_id: str) -> dict[str, Any]:
        regime = self.latest_regime() or {}
        plan = self.current_plan(account_id) or {}
        plan_run_uid = str(plan.get("run_uid") or "")
        if plan_run_uid and plan_run_uid != str(regime.get("run_uid") or ""):
            regime = self._one(
                """
                SELECT run_uid, trade_date, decision_at, snapshot_id,
                       market_regime, market_regime_version, status,
                       result_hash, code_commit_sha, config_version
                FROM st_decision_run_v2
                WHERE run_uid = :run_uid
                LIMIT 1
                """,
                {"run_uid": plan_run_uid},
            ) or regime
        source_date = str(
            regime.get("trade_date")
            or ""
        )[:10]
        execution_date = ""
        if source_date:
            row = self._one(
                """
                SELECT MIN(trade_date) AS trade_date
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date > :source_date
                """,
                {"source_date": source_date},
            )
            execution_date = str((row or {}).get("trade_date") or "")[:10]
        positions = list(plan.get("positions") or [])
        pending_positions: list[dict[str, Any]] = []
        if execution_date:
            pending_positions = self._all(
                """
                SELECT o.order_id, o.stock_code,
                       COALESCE(a.short_name, '') AS short_name,
                       GREATEST(0, o.quantity - o.filled_quantity)
                           AS target_quantity,
                       i.target_weight, i.strategy_version, i.theme_code,
                       o.status AS order_status, o.limit_price,
                       i.initial_stop, o.earliest_at, o.expires_at
                FROM st_order_v2 o
                JOIN st_trade_intent_v2 i
                  ON i.intent_id = o.intent_id
                LEFT JOIN si_all_code a
                  ON BINARY a.stock_code = BINARY o.stock_code
                WHERE o.account_id = :account_id
                  AND o.status IN (
                    'CREATED','APPROVED','RISK_APPROVED','QUEUED',
                    'SUBMITTED','PARTIALLY_FILLED','WAITING'
                  )
                  AND o.quantity > o.filled_quantity
                  AND DATE(o.earliest_at) = :execution_date
                ORDER BY o.created_at, o.order_id
                """,
                {
                    "account_id": account_id,
                    "execution_date": execution_date,
                },
            )
        existing_order_ids = {
            str(item.get("order_id") or "")
            for item in positions
            if str(item.get("order_id") or "")
        }
        existing_codes = {
            str(item.get("stock_code") or "")
            for item in positions
            if str(item.get("stock_code") or "")
        }
        plan_equity: Decimal | None = None
        try:
            target_cash = Decimal(str(plan.get("target_cash") or ""))
            risk_weight = Decimal(
                str(plan.get("target_risk_asset_weight") or "")
            )
            if target_cash >= 0 and Decimal("0") <= risk_weight < Decimal("1"):
                plan_equity = target_cash / (Decimal("1") - risk_weight)
        except (InvalidOperation, ValueError, ZeroDivisionError):
            plan_equity = None
        for item in pending_positions:
            order_id = str(item.get("order_id") or "")
            stock_code = str(item.get("stock_code") or "")
            if (
                (order_id and order_id in existing_order_ids)
                or (stock_code and stock_code in existing_codes)
            ):
                continue
            item["execution_note"] = (
                "模拟单已通过风控，等待开盘后的价格与流动性检查"
            )
            if plan_equity and plan_equity > 0:
                try:
                    pending_value = Decimal(
                        str(item.get("limit_price") or "0")
                    ) * Decimal(str(item.get("target_quantity") or "0"))
                    item["target_weight"] = str(
                        (pending_value / plan_equity).quantize(
                            Decimal("0.00000001")
                        )
                    )
                except (InvalidOperation, ValueError):
                    pass
            positions.append(item)
            if order_id:
                existing_order_ids.add(order_id)
            if stock_code:
                existing_codes.add(stock_code)
        watch = self.candidates(
            run_uid=str(regime.get("run_uid") or ""),
            limit=250,
        )
        watch.sort(
            key=lambda item: (
                -float(item.get("raw_score") or 0),
                str(item.get("stock_code") or ""),
                str(item.get("strategy_version") or ""),
            )
        )
        return {
            "source_trade_date": source_date,
            "execution_trade_date": execution_date,
            "market_regime": regime.get("market_regime") or "DATA_BLOCKED",
            "run_uid": regime.get("run_uid") or "",
            "run_status": regime.get("status") or "",
            "action": "BUY" if positions else "NO_BUY",
            "positions": positions,
            "target_cash": plan.get("target_cash"),
            "target_risk_asset_weight": plan.get(
                "target_risk_asset_weight"
            ),
            "worst_case_loss": plan.get("worst_case_loss"),
            "rejected_candidate_count": len(
                plan.get("rejected_candidates") or []
            ),
            "pending_order_count": len(pending_positions),
            "watch_candidates": watch[:10],
        }

    def etf_forward_summary(self, limit: int = 100) -> dict[str, Any]:
        limit = max(1, min(500, int(limit)))
        strategies = self._all(
            """
            SELECT strategy_version, config_hash, frozen_at,
                   forward_start_date, mode, status, config_json,
                   registered_at
            FROM st_etf_forward_strategy
            ORDER BY registered_at DESC
            """
        )
        observations = self._all(
            """
            SELECT strategy_version, config_hash, data_date, observed_at,
                   data_source, input_hash, signal_type, execution_date,
                   target_json, context_json, created_at
            FROM st_etf_forward_observation
            ORDER BY data_date DESC, id DESC
            LIMIT :limit
            """,
            {"limit": limit},
        )
        data = self._one(
            """
            SELECT COUNT(*) AS row_count,
                   COUNT(DISTINCT etf_code) AS symbol_count,
                   MAX(trade_date) AS latest_trade_date,
                   SUM(
                       CASE
                           WHEN validation_status = 'passed'
                            AND quality_status = 'validated'
                           THEN 1 ELSE 0
                       END
                   ) AS validated_rows
            FROM sm_etf_kline
            WHERE adjust_type = 0 AND k_type = 1
            """
        ) or {}
        task = self._one(
            """
            SELECT id, task_name, task_type, script_path, script_args,
                   cron_time, enabled, last_run_status, last_run_at,
                   last_triggered_at, last_run_duration, last_run_output
            FROM st_scheduled_tasks
            WHERE task_type = 'etf_forward_daily'
            LIMIT 1
            """
        )
        security_names = {
            str(row.get("etf_code") or ""): str(row.get("short_name") or "")
            for row in self._all(
                """
                SELECT etf_code, short_name
                FROM si_etf_code
                ORDER BY etf_code
                """
            )
            if row.get("etf_code")
        }
        return {
            "status": (
                "collecting"
                if observations
                else "waiting_first_forward_close"
                if strategies
                else "not_registered"
            ),
            "strategies": strategies,
            "observations": observations,
            "observation_count": len(observations),
            "data": data,
            "task": task,
            "security_names": security_names,
            "backfill": "prohibited",
            "automatic_order_submission": False,
        }

    def operations_summary(self) -> dict[str, Any]:
        tasks = self._all(
            """
            SELECT id, task_name, task_type, cron_time, interval_minutes,
                   enabled, last_run_status, last_run_at,
                   last_triggered_at, last_run_duration, last_run_output
            FROM st_scheduled_tasks
            WHERE task_type IN (
                'etf_forward_daily',
                'trading_v2_premarket_decision',
                'trading_v2_close_decision',
                'trading_v2_paper_tick',
                'trading_v2_reconciliation',
                'trading_v2_job_worker',
                'trading_v2_level1_validation',
                'trading_v2_strategy_health',
                'trading_v2_intraday_activation',
                'qmt_membership_snapshot'
            )
            ORDER BY sort_order, id
            """
        )
        backtests = self._all(
            """
            SELECT backtest_uid, strategy_version, start_date, end_date,
                   random_seed, status, gate_status, error_code,
                   error_message, started_at, finished_at
            FROM st_backtest_run_v2
            ORDER BY started_at DESC, backtest_uid DESC
            LIMIT 30
            """
        )
        jobs = self._all(
            """
            SELECT job_id, job_type, status, result_ref, error_code,
                   error_message, requested_at, started_at, finished_at
            FROM st_job_v2
            ORDER BY requested_at DESC, job_id DESC
            LIMIT 30
            """
        )
        guards = self._all(
            """
            SELECT TRIGGER_NAME AS trigger_name,
                   EVENT_MANIPULATION AS event_manipulation,
                   ACTION_TIMING AS action_timing
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA = DATABASE()
              AND EVENT_OBJECT_TABLE = 'st_trade_account_v2'
              AND TRIGGER_NAME IN (
                  'trg_trade_account_v2_real_disabled_bi',
                  'trg_trade_account_v2_real_disabled_bu'
              )
            ORDER BY TRIGGER_NAME
            """
        )
        return {
            "tasks": tasks,
            "backtests": backtests,
            "jobs": jobs,
            "workers": self.worker_heartbeats(),
            "real_trading_guards": guards,
            "running_backtest_count": sum(
                1
                for item in backtests
                if str(item.get("status")) == "RUNNING"
            ),
        }

    def worker_heartbeats(self) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT * FROM st_worker_heartbeat_v2
            ORDER BY worker_name
            """
        )
