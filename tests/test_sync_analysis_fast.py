# -*- coding: utf-8 -*-
import json
import math
import os
import unittest
from contextlib import ExitStack
from datetime import date, datetime, timedelta
from types import SimpleNamespace

from biz.analysis.sync_analysis_fast import (
    add_strategy_signals,
    attach_price_crosscheck,
    backfill_recommendation_reviews,
    build_evidence_chain,
    build_failure_tags,
    _build_text_fields,
    build_chan_structure,
    classify_intraday_behavior,
    classify_macro_indicator_context,
    build_event_impact,
    build_minute_chan_structure,
    build_recommendation_rows,
    build_rule_flags,
    build_research_theme_features,
    build_strategy_trade_plan,
    build_technical_evidence,
    _build_chase_risk_features_from_rows,
    calibrate_strategy_thresholds,
    choose_recommend_status,
    classify_event_fulfillment,
    classify_etf_flow_context,
    classify_macro_policy_context,
    classify_market_style_context,
    classify_north_flow_context,
    classify_retail_sentiment_context,
    classify_valuation_style,
    clamp_score,
    classify_notice_title,
    compute_market_breadth_features,
    detect_classic_top_bottom_structure,
    detect_kline_pattern,
    derive_position_risk_level,
    derive_investment_rating,
    enrich_recommendation_minute_chan,
    evaluate_business_purity,
    evaluate_fundamental_quality,
    evaluate_industry_prosperity,
    evaluate_institutional_profile,
    evaluate_investor_interaction_profile,
    evaluate_peg_valuation,
    evaluate_liquidity_profile,
    evaluate_order_book_depth,
    evaluate_size_liquidity_profile,
    evaluate_unlock_pressure,
    evaluate_volume_temperature_profile,
    estimate_trade_probabilities,
    estimate_latest_close_percentile,
    estimate_volume_profile_levels,
    load_chip_capital_features,
    load_etf_flow_context,
    load_investor_interaction_features,
    load_retail_sentiment_context,
    load_order_book_features,
    load_sector_rotation_features,
    load_stock_north_holding_features,
    linear_score,
    parse_dividend_cash_per_share,
    publish_strategy_runtime_params,
    repair_missing_qmt_kline_for_trade_date,
    runtime_threshold,
    select_primary_strategy,
    set_active_runtime_params,
)
import pandas as pd
from unittest.mock import patch


def _chase_path(
    surge_count: int,
    follow_returns: list[float] | None = None,
    *,
    stock_code: str = "600001",
    start: str = "2026-06-01",
) -> list[dict]:
    returns = [10.0] * surge_count + list(follow_returns or [])
    days = pd.bdate_range(start, periods=len(returns))
    previous = 10.0
    rows: list[dict] = []
    for trade_day, return_pct in zip(days, returns):
        close = previous * (1.0 + return_pct / 100.0)
        is_surge = return_pct >= 9.5
        rows.append({
            "stock_code": stock_code,
            "trade_date": trade_day.strftime("%Y-%m-%d"),
            "open": previous,
            "high": close if is_surge else max(previous, close),
            "low": min(previous, close),
            "close": close,
            "volume": 1_000_000,
            "amount": 100_000_000,
            "change_pct": return_pct,
            "turnover_ratio": 3.0,
            "pre_close": previous,
            "received_at": trade_day.strftime("%Y-%m-%d 16:00:00"),
        })
        previous = close
    return rows


def test_kline_features_default_to_memory_bounded_streaming_batches():
    from biz.analysis import sync_analysis_fast as module

    code_df = pd.DataFrame({"stock_code": ["000001", "600000"]})
    names = pd.DataFrame({"stock_code": ["000001", "600000"], "short_name": ["A", "B"]})
    history = pd.DataFrame({"stock_code": ["000001", "600000"]})
    latest = pd.DataFrame({"stock_code": ["000001", "600000"]})

    with patch.dict(os.environ, {"PROBIGA_KLINE_FEATURE_STREAM_BATCHES": "1"}, clear=False), \
         patch("biz.analysis.sync_analysis_fast._recent_dates", return_value=["2026-07-17"]), \
         patch("biz.analysis.sync_analysis_fast._query_engine", return_value=object()), \
         patch("biz.analysis.sync_analysis_fast._mysql_force_index_hint", return_value=""), \
         patch("biz.analysis.sync_analysis_fast._read_frame", side_effect=[code_df, names, history]) as read_mock, \
         patch("biz.analysis.sync_analysis_fast._build_latest_kline_features_from_rows", return_value=latest) as build_mock:
        os.environ.pop("PROBIGA_KLINE_FEATURE_SQL_MODE", None)
        result = module.load_kline_features(object(), "2026-07-17")

    assert len(result) == 2
    build_mock.assert_called_once()
    rendered_sql = "\n".join(str(call.args[0]) for call in read_mock.call_args_list)
    assert "GROUP BY stock_code" not in rendered_sql


class SyncAnalysisFastTest(unittest.TestCase):
    def test_clamp_score_handles_invalid_values(self):
        self.assertEqual(clamp_score(120), 100.0)
        self.assertEqual(clamp_score(-5), 0.0)
        self.assertEqual(clamp_score(None), 50.0)

    def test_finance_period_is_unavailable_until_notice_and_acquisition_cutoff(self):
        from sqlalchemy import create_engine, text
        from biz.analysis import sync_analysis_fast as module

        engine = create_engine("sqlite+pysqlite:///:memory:")
        fixed = [
            "basic_eps", "net_asset_ps", "oper_cf_ps", "total_rev_yoy_gr",
            "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr", "roe_wtd",
            "gross_margin", "net_margin", "curr_ratio", "cash_flow_ratio",
            "asset_liab_ratio",
        ]
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE si_stock_finance ("
                "stock_code TEXT, report_date TEXT, notice_date TEXT, "
                + ", ".join(f"{column} REAL" for column in fixed)
                + ", received_at TEXT, etl_sync_at TEXT)"
            ))
            conn.execute(text(
                "INSERT INTO si_stock_finance ("
                "stock_code, report_date, notice_date, roe_wtd, gross_margin, "
                "net_margin, asset_liab_ratio, etl_sync_at"
                ") VALUES ("
                "'600001', '2026-03-31', '2026-05-21', 12, 30, 8, 40, "
                "'2026-05-30 18:00:00')"
            ))
        columns = {
            "stock_code", "report_date", "notice_date", "received_at",
            "etl_sync_at", *fixed,
        }
        try:
            with patch("biz.analysis.sync_analysis_fast._table_columns", return_value=columns):
                before_notice = module.load_finance(
                    engine,
                    "2026-06-30",
                    as_of_at="2026-05-20T10:00:00+08:00",
                )
                before_acquisition = module.load_finance(
                    engine,
                    "2026-06-30",
                    as_of_at="2026-05-22T10:00:00+08:00",
                )
                known = module.load_finance(
                    engine,
                    "2026-06-30",
                    as_of_at="2026-05-31T10:00:00+08:00",
                )
        finally:
            engine.dispose()

        self.assertTrue(before_notice.empty)
        self.assertTrue(before_acquisition.empty)
        self.assertEqual(known["stock_code"].tolist(), ["600001"])
        self.assertEqual(known.iloc[0]["finance_pit_status"], "KNOWN_AT_CUTOFF")

    def test_exact_cutoff_excludes_later_price_and_order_book_snapshots(self):
        from sqlalchemy import create_engine, text
        from biz.analysis import sync_analysis_fast as module

        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE sm_stock_current ("
                "stock_code TEXT, price REAL, snapshot_at TEXT, "
                "received_at TEXT, etl_sync_at TEXT)"
            ))
            conn.execute(text(
                "INSERT INTO sm_stock_current VALUES "
                "('600001', 10.0, '2026-08-04 09:30:00', "
                " '2026-08-04 09:30:01', '2026-08-04 09:30:02'),"
                "('600001', 11.5, '2026-08-04 14:30:00', "
                " '2026-08-04 14:30:01', '2026-08-04 14:30:02')"
            ))
            conn.execute(text(
                "CREATE TABLE sm_stock_five_level ("
                "stock_code TEXT, snapshot_at TEXT, b1 REAL, bv1 REAL, "
                "s1 REAL, sv1 REAL, received_at TEXT, etl_sync_at TEXT)"
            ))
            conn.execute(text(
                "INSERT INTO sm_stock_five_level VALUES "
                "('600001', '2026-08-04 09:30:00', 9.9, 100, 10.1, 100, "
                " '2026-08-04 09:30:01', '2026-08-04 09:30:02'),"
                "('600001', '2026-08-04 14:30:00', 11.4, 200, 11.6, 200, "
                " '2026-08-04 14:30:01', '2026-08-04 14:30:02')"
            ))
        table_columns = {
            "sm_stock_current": {
                "stock_code", "price", "snapshot_at", "received_at", "etl_sync_at",
            },
            "sm_stock_five_level": {
                "stock_code", "snapshot_at", "b1", "bv1", "s1", "sv1",
                "received_at", "etl_sync_at",
            },
        }
        try:
            with patch(
                "biz.analysis.sync_analysis_fast._table_exists",
                side_effect=lambda _engine, table: table in table_columns,
            ), patch(
                "biz.analysis.sync_analysis_fast._table_columns",
                side_effect=lambda _engine, table: table_columns.get(table, set()),
            ):
                prices = module.load_price_validation_features(
                    engine,
                    "2026-08-04",
                    as_of_at="2026-08-04T10:00:00+08:00",
                )
                book = module.load_order_book_features(
                    engine,
                    "2026-08-04",
                    as_of_at="2026-08-04T10:00:00+08:00",
                )
        finally:
            engine.dispose()

        self.assertEqual(float(prices.iloc[0]["current_price"]), 10.0)
        self.assertTrue(str(prices.iloc[0]["current_snapshot_at"]).startswith("2026-08-04 09:30"))
        self.assertTrue(str(book.iloc[0]["order_book_snapshot_at"]).startswith("2026-08-04 09:30"))

    def test_empty_or_stale_event_source_forces_new_buy_data_blocked(self):
        from biz.analysis import sync_analysis_fast as module

        columns = {
            "si_notice_eastmoney": {
                "notice_date", "etl_sync_at", "association_validated",
            },
            "st_news_flash": {"publish_time", "etl_sync_at"},
        }
        empty_watermark = pd.DataFrame([{
            "row_count": 0,
            "latest_acquired_at": None,
        }])
        with patch(
            "biz.analysis.sync_analysis_fast._table_exists", return_value=True
        ), patch(
            "biz.analysis.sync_analysis_fast._table_columns",
            side_effect=lambda _engine, table: columns[table],
        ), patch(
            "biz.analysis.sync_analysis_fast._read_frame",
            return_value=empty_watermark,
        ):
            health = module.load_event_source_health(
                object(),
                "2026-08-04",
                as_of_at="2026-08-04T10:00:00+08:00",
            )

        gated = module._apply_event_source_health_gate(pd.DataFrame([{
            "stock_code": "600001",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
            "chase_risk_reason": "price path verified",
            "chase_risk_evidence_json": "{}",
        }]), health)
        status, _ = choose_recommend_status(
            stock_code="600001",
            short_name="sample",
            ai_score=99,
            short_term_score=99,
            long_term_score=99,
            event_risk_level="LOW",
            amount=100_000_000,
            change_pct=1.0,
            min_score=62,
            chase_risk_status=gated.iloc[0]["chase_risk_status"],
            ordinary_buy_eligible=gated.iloc[0]["ordinary_buy_eligible"],
            chase_risk_reason=gated.iloc[0]["chase_risk_reason"],
        )

        self.assertEqual(health["event_source_status"], "DATA_BLOCKED")
        self.assertEqual(gated.iloc[0]["chase_risk_status"], "DATA_BLOCKED")
        self.assertFalse(gated.iloc[0]["ordinary_buy_eligible"])
        self.assertEqual(status, "BLOCK")

    def test_single_stock_exact_cutoff_gate_is_read_only_and_fail_closed_on_event_health(self):
        from biz.analysis import sync_analysis_fast as module

        daily = pd.DataFrame([{
            "stock_code": "600001", "short_name": "sample",
            "trade_date": "2026-08-04", "open": 10.0, "high": 10.2,
            "low": 9.9, "close": 10.1, "volume": 1_000_000,
            "amount": 10_000_000, "change_pct": 1.0,
            "turnover_ratio": 2.0, "pre_close": 10.0,
            "received_at": "2026-08-04 09:40:00",
            "etl_sync_at": "2026-08-04 09:41:00",
        }])
        healthy = {
            "event_source_status": "HEALTHY",
            "event_source_reason": "fresh",
            "event_sources": {},
        }
        blocked = {
            "event_source_status": "DATA_BLOCKED",
            "event_source_reason": "news=MISSING",
            "event_sources": {"news": {"status": "MISSING"}},
        }
        common_patches = [
            patch("biz.analysis.sync_analysis_fast._read_frame", return_value=daily),
            patch(
                "biz.analysis.sync_analysis_fast._read_intraday_quote_for_stock_at_cutoff",
                return_value=(pd.DataFrame(), ""),
            ),
        ]
        with ExitStack() as stack:
            for patcher in common_patches:
                stack.enter_context(patcher)
            stack.enter_context(patch(
                "biz.analysis.sync_analysis_fast.load_event_source_health",
                return_value=healthy,
            ))
            allowed = module.evaluate_stock_buy_gate_at_cutoff(
                object(),
                "600001",
                "2026-08-04",
                "2026-08-04T10:00:00+08:00",
            )
        with ExitStack() as stack:
            stack.enter_context(patch(
                "biz.analysis.sync_analysis_fast._read_frame", return_value=daily
            ))
            stack.enter_context(patch(
                "biz.analysis.sync_analysis_fast._read_intraday_quote_for_stock_at_cutoff",
                return_value=(pd.DataFrame(), ""),
            ))
            stack.enter_context(patch(
                "biz.analysis.sync_analysis_fast.load_event_source_health",
                return_value=blocked,
            ))
            denied = module.evaluate_stock_buy_gate_at_cutoff(
                object(),
                "600001",
                "2026-08-04",
                "2026-08-04T10:00:00+08:00",
            )

        self.assertEqual(allowed["status"], "ALLOW")
        self.assertTrue(allowed["eligible"])
        self.assertEqual(len(allowed["context_hash"]), 64)
        self.assertGreater(allowed["valid_until"], allowed["evaluated_at"])
        self.assertEqual(denied["status"], "DATA_BLOCKED")
        self.assertFalse(denied["eligible"])
        self.assertIn("news=MISSING", denied["reason"])

    def test_buy_gate_hash_binds_evidence_not_query_cutoff(self):
        from biz.analysis import sync_analysis_fast as module

        daily = pd.DataFrame([{
            "stock_code": "600001", "short_name": "sample",
            "trade_date": "2026-08-04", "open": 10.0, "high": 10.2,
            "low": 9.9, "close": 10.1, "volume": 1_000_000,
            "amount": 10_000_000, "change_pct": 1.0,
            "turnover_ratio": 2.0, "pre_close": 10.0,
            "received_at": "2026-08-04 09:40:00",
            "etl_sync_at": "2026-08-04 09:41:00",
        }])
        revised = daily.copy()
        revised.loc[0, "close"] = 10.15
        revised.loc[0, "etl_sync_at"] = "2026-08-04 10:01:30"

        def health(cutoff, age):
            return {
                "event_source_status": "HEALTHY",
                "event_source_cutoff": cutoff,
                "event_source_reason": f"fresh at age={age}",
                "event_sources": {
                    "news": {
                        "status": "HEALTHY",
                        "latest_acquired_at": "2026-08-04T09:39:00+08:00",
                        "age_minutes": age,
                        "reason": f"fresh for {age} minutes",
                    },
                    "notice": {
                        "status": "HEALTHY",
                        "latest_acquired_at": "2026-08-04T09:38:00+08:00",
                        "age_minutes": age + 1,
                        "reason": f"fresh for {age + 1} minutes",
                    },
                },
            }

        with patch.object(
            module,
            "_read_stock_daily_rows_at_cutoff",
            side_effect=[(daily, ""), (daily, ""), (revised, "")],
        ), patch.object(
            module,
            "_read_intraday_quote_for_stock_at_cutoff",
            return_value=(pd.DataFrame(), ""),
        ), patch.object(
            module,
            "load_event_source_health",
            side_effect=[
                health("2026-08-04T10:00:00+08:00", 21),
                health("2026-08-04T10:01:00+08:00", 22),
                health("2026-08-04T10:02:00+08:00", 23),
            ],
        ):
            first = module.evaluate_stock_buy_gate_at_cutoff(
                object(), "600001", "2026-08-04", "2026-08-04T10:00:00+08:00"
            )
            repeated = module.evaluate_stock_buy_gate_at_cutoff(
                object(), "600001", "2026-08-04", "2026-08-04T10:01:00+08:00"
            )
            changed = module.evaluate_stock_buy_gate_at_cutoff(
                object(), "600001", "2026-08-04", "2026-08-04T10:02:00+08:00"
            )

        self.assertEqual(first["context_hash"], first["evidence_hash"])
        self.assertEqual(first["evidence_hash"], repeated["evidence_hash"])
        self.assertNotEqual(first["evidence_hash"], changed["evidence_hash"])
        self.assertNotEqual(first["evaluated_at"], repeated["evaluated_at"])

    def test_evidence_hash_normalizes_mixed_daily_naive_and_intraday_aware_times(self):
        from biz.analysis import sync_analysis_fast as module

        mixed = pd.DataFrame([
            {
                "stock_code": "603221",
                "trade_date": date(2026, 8, 3),
                "close": 10.0,
                "received_at": pd.Timestamp("2026-08-03 15:01:00"),
                "etl_sync_at": "2026-08-03 15:02:00",
                "snapshot_at": None,
            },
            {
                "stock_code": "603221",
                "trade_date": pd.Timestamp("2026-08-04"),
                "close": 10.2,
                "received_at": pd.Timestamp("2026-08-04T10:00:00+08:00"),
                "etl_sync_at": pd.Timestamp("2026-08-04T10:00:00+08:00"),
                "snapshot_at": pd.Timestamp("2026-08-04T10:00:00+08:00"),
            },
        ])
        uniformly_aware = mixed.copy()
        for column in ("received_at", "etl_sync_at", "snapshot_at"):
            uniformly_aware[column] = uniformly_aware[column].map(
                lambda value: (
                    value
                    if value is None or pd.isna(value)
                    else (
                        pd.Timestamp(value).tz_localize("Asia/Shanghai")
                        if pd.Timestamp(value).tzinfo is None
                        else pd.Timestamp(value).tz_convert("Asia/Shanghai")
                    )
                )
            )

        mixed_hash = module._frame_evidence_hash(mixed)
        aware_hash = module._frame_evidence_hash(uniformly_aware)

        self.assertEqual(len(mixed_hash), 64)
        self.assertEqual(mixed_hash, aware_hash)

    def test_holding_exit_monitor_covers_non_candidate_and_fails_closed(self):
        from biz.analysis import sync_analysis_fast as module

        daily = pd.DataFrame([
            {
                "stock_code": "600001", "short_name": "sample",
                "trade_date": f"2026-07-{day:02d}", "open": 10.0,
                "high": 10.2, "low": 9.8, "close": 10.0,
                "volume": 1_000_000, "amount": 10_000_000,
                "change_pct": 0.0, "turnover_ratio": 2.0, "pre_close": 10.0,
                "received_at": f"2026-07-{day:02d} 15:05:00",
                "etl_sync_at": f"2026-07-{day:02d} 15:06:00",
            }
            for day in range(16, 26)
        ])
        analysis = {
            "stock_code": "600001",
            "analysis_date": "2026-07-25",
            "event_risk_level": "LOW",
            "updated_at": "2026-07-25 16:00:00",
        }
        healthy = {
            "event_source_status": "HEALTHY",
            "event_source_reason": "fresh",
            "event_sources": {},
        }
        missing = {
            "event_source_status": "DATA_BLOCKED",
            "event_source_reason": "news=MISSING",
            "event_sources": {"news": {"status": "MISSING"}},
        }

        def run(health):
            with patch.object(
                module,
                "_read_latest_decision_output_at_cutoff",
                side_effect=[(analysis, ""), ({}, "no cutoff-eligible recommendation")],
            ), patch.object(
                module,
                "_read_stock_daily_rows_at_cutoff",
                return_value=(daily, ""),
            ), patch.object(
                module,
                "_read_intraday_quote_for_stock_at_cutoff",
                return_value=(pd.DataFrame(), ""),
            ), patch.object(
                module, "load_event_source_health", return_value=health,
            ), patch.object(
                module, "load_notice_features", return_value=pd.DataFrame(),
            ), patch.object(
                module, "load_news_features", return_value=pd.DataFrame(),
            ):
                return module.evaluate_stock_holding_exit_at_cutoff(
                    object(), "600001", "2026-07-25", "2026-07-25T16:30:00+08:00"
                )

        allowed_hold = run(healthy)
        wait = run(missing)

        self.assertEqual(allowed_hold["exit_intent"], "HOLD")
        self.assertIn("recommendation_error", allowed_hold["evidence"])
        self.assertEqual(wait["exit_intent"], "WAIT_DATA")
        self.assertIn("news=MISSING", wait["reason"])
        self.assertEqual(len(wait["context_hash"]), 64)

    def test_holding_explicit_stop_is_not_suppressed_by_missing_event_source(self):
        from biz.analysis import sync_analysis_fast as module

        daily = pd.DataFrame([{
            "stock_code": "600001", "short_name": "sample",
            "trade_date": "2026-08-04", "open": 8.9, "high": 9.0,
            "low": 8.7, "close": 8.8, "volume": 1_000_000,
            "amount": 8_800_000, "change_pct": -4.0,
            "turnover_ratio": 2.0, "pre_close": 9.17,
            "received_at": "2026-08-04 15:01:00",
            "etl_sync_at": "2026-08-04 15:02:00",
        }])
        analysis = {
            "stock_code": "600001", "analysis_date": "2026-08-04",
            "event_risk_level": "LOW", "updated_at": "2026-08-04 15:03:00",
        }
        recommendation = {
            "stock_code": "600001", "pick_date": "2026-08-04",
            "signal_status": "WATCH", "stop_loss_price": 9.0,
            "updated_at": "2026-08-04 15:03:00",
        }
        with patch.object(
            module,
            "_read_latest_decision_output_at_cutoff",
            side_effect=[(analysis, ""), (recommendation, "")],
        ), patch.object(
            module, "_read_stock_daily_rows_at_cutoff", return_value=(daily, ""),
        ), patch.object(
            module,
            "_read_intraday_quote_for_stock_at_cutoff",
            return_value=(pd.DataFrame(), ""),
        ), patch.object(
            module,
            "load_event_source_health",
            return_value={
                "event_source_status": "DATA_BLOCKED",
                "event_source_reason": "news=MISSING",
                "event_sources": {},
            },
        ), patch.object(
            module, "load_notice_features", return_value=pd.DataFrame(),
        ), patch.object(
            module, "load_news_features", return_value=pd.DataFrame(),
        ):
            result = module.evaluate_stock_holding_exit_at_cutoff(
                object(), "600001", "2026-08-04", "2026-08-04T15:10:00+08:00"
            )

        self.assertEqual(result["exit_intent"], "SELL")
        self.assertIn("breached stop loss", result["reason"])

    def test_disabled_factor_inventory_is_exposed_in_evidence_chain(self):
        from biz.analysis import sync_analysis_fast as module

        row = {
            "stock_code": "600001",
            "disabled_factor_inventory": json.dumps(
                list(module.LEGACY_PIT_DISABLED_FACTOR_INVENTORY),
                ensure_ascii=False,
            ),
            "disabled_factor_reason": module.LEGACY_PIT_DISABLED_FACTOR_REASON,
        }
        chain = module.build_evidence_chain(row, trade_date="2026-08-04")
        disabled = next(
            item for item in chain
            if item.get("module") == "disabled_factor_inventory"
        )

        self.assertEqual(disabled["status"], "NEUTRALIZED")
        self.assertEqual(
            set(disabled["disabled_factor_inventory"]),
            set(module.LEGACY_PIT_DISABLED_FACTOR_INVENTORY),
        )
        self.assertIn("acquisition-time", disabled["reason"])

    def test_prepare_cutoff_neutralizes_every_unversioned_optional_factor(self):
        from biz.analysis import sync_analysis_fast as module

        kline = pd.DataFrame([{
            "stock_code": "600001",
            "close": 10.0,
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
            "chase_risk_reason": "verified",
            "chase_risk_evidence_json": "{}",
        }])
        empty = pd.DataFrame({"stock_code": []})
        disabled_names = (
            "load_dividend_features",
            "load_research_theme_features",
            "load_business_purity_features",
            "load_institutional_features",
            "load_industry_prosperity_features",
            "load_investor_interaction_features",
            "load_confidence_features",
            "load_recommendation_history",
            "load_failure_features",
            "load_chip_capital_features",
            "load_stock_north_holding_features",
            "load_size_liquidity_features",
            "load_market_margin_features",
            "load_market_style_context",
            "load_market_north_flow_features",
            "load_etf_flow_context",
            "load_retail_sentiment_context",
            "load_macro_indicator_context",
            "load_event_relation_rules",
            "enrich_recommendation_minute_chan",
        )

        def fake_scores(**kwargs):
            for key in (
                "confidence", "rec_history", "failures", "chip_context",
                "size_context", "dividend_context", "research_context",
                "north_stock_context", "institutional_context",
                "prosperity_context", "business_context", "interaction_context",
            ):
                self.assertTrue(kwargs[key].empty, key)
            self.assertEqual(kwargs["event_relation_rules"], [])
            for key in (
                "market_margin_balance", "market_style", "north_net_1d",
                "etf_net_1d", "retail_sentiment_score", "macro_indicator_score",
            ):
                self.assertNotIn(key, kwargs["market_context"])
            return pd.DataFrame([{
                "stock_code": "600001",
                "decision_value": float(kwargs["kline"].iloc[0]["close"]),
            }])

        def fake_text_fields(frame, **_kwargs):
            out = frame.copy()
            out["evidence_chain_json"] = out.apply(
                lambda row: json.dumps(
                    module.build_evidence_chain(row.to_dict(), "2026-08-04"),
                    ensure_ascii=False,
                ),
                axis=1,
            )
            return out

        def fake_rows(frame, _trade_date, **_kwargs):
            row = frame.iloc[0]
            return [{
                "stock_code": row["stock_code"],
                "decision_value": row["decision_value"],
                "evidence_chain_json": row.get("evidence_chain_json", "[]"),
            }]

        with ExitStack() as stack:
            disabled_mocks = {
                name: stack.enter_context(patch.object(
                    module,
                    name,
                    side_effect=AssertionError(f"future-only source called: {name}"),
                ))
                for name in disabled_names
            }
            stack.enter_context(patch.object(module, "load_kline_features", return_value=kline))
            stack.enter_context(patch.object(
                module,
                "load_event_source_health",
                return_value={
                    "event_source_status": "HEALTHY",
                    "event_source_reason": "fresh",
                    "event_sources": {},
                },
            ))
            stack.enter_context(patch.object(module, "_assert_chase_risk_coverage"))
            stack.enter_context(patch.object(module, "load_finance", return_value=empty))
            stack.enter_context(patch.object(
                module, "load_flow_features", return_value=(empty, "2026-08-04")
            ))
            stack.enter_context(patch.object(
                module, "load_hot_rank", return_value=(empty, "2026-08-04")
            ))
            stack.enter_context(patch.object(module, "load_notice_features", return_value=empty))
            stack.enter_context(patch.object(module, "load_news_features", return_value=empty))
            stack.enter_context(patch.object(module, "load_sector_rotation_features", return_value=empty))
            stack.enter_context(patch.object(module, "load_price_validation_features", return_value=empty))
            stack.enter_context(patch.object(module, "load_order_book_features", return_value=empty))
            stack.enter_context(patch.object(module, "compute_market_mood", return_value=50.0))
            stack.enter_context(patch.object(
                module,
                "compute_market_breadth_features",
                return_value={"market_extreme_status": "NEUTRAL"},
            ))
            stack.enter_context(patch.object(module, "load_macro_policy_context", return_value={}))
            stack.enter_context(patch.object(
                module, "load_latest_external_market_context", return_value={}
            ))
            stack.enter_context(patch.object(module, "compute_scores", side_effect=fake_scores))
            stack.enter_context(patch.object(
                module, "_build_text_fields", side_effect=fake_text_fields
            ))
            stack.enter_context(patch.object(
                module, "build_analysis_rows", side_effect=fake_rows
            ))
            stack.enter_context(patch.object(
                module, "build_recommendation_rows", side_effect=fake_rows
            ))

            first = module._prepare_batch_outputs(
                object(),
                "2026-08-04",
                62.0,
                80,
                news_cutoff_time="2026-08-04T10:00:00+08:00",
                use_intraday_current=True,
            )
            second = module._prepare_batch_outputs(
                object(),
                "2026-08-04",
                62.0,
                80,
                news_cutoff_time="2026-08-04T10:01:00+08:00",
                use_intraday_current=True,
            )

        self.assertEqual(first[0], second[0])
        self.assertEqual(first[1], second[1])
        for name, mock in disabled_mocks.items():
            self.assertFalse(mock.called, name)
        evidence = json.loads(first[1][0]["evidence_chain_json"])
        self.assertTrue(any(
            item.get("module") == "disabled_factor_inventory"
            for item in evidence
        ))

    def test_unversioned_runtime_params_are_neutralized_to_code_defaults(self):
        from biz.analysis import sync_analysis_fast as module

        events = []
        with patch.object(
            module,
            "load_strategy_runtime_params",
            side_effect=AssertionError("mutable runtime params must not be read"),
        ) as loader:
            module._activate_runtime_params(
                object(), "2026-08-04", progress_callback=events.append
            )

        loader.assert_not_called()
        self.assertEqual(module.ACTIVE_RUNTIME_PARAMS, module.DEFAULT_RUNTIME_PARAMS)
        self.assertEqual(events[-1]["stage"], "runtime_params_neutralized")

    def test_model_version_column_is_widened_before_writes(self):
        from biz.analysis import sync_analysis_fast as module

        class DummyConn:
            def __init__(self):
                self.statements = []

            def execute(self, statement, _params=None):
                self.statements.append(str(statement))

        class DummyBegin:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyEngine:
            def __init__(self):
                self.conn = DummyConn()

            def begin(self):
                return DummyBegin(self.conn)

        engine = DummyEngine()
        with patch.object(module, "_table_columns", return_value={"model_version"}), \
             patch.object(module, "_character_column_length", return_value=20):
            module._ensure_analysis_columns(engine)

        self.assertLessEqual(len(module.MODEL_VERSION), module.MODEL_VERSION_COLUMN_LENGTH)
        self.assertTrue(any(
            "MODIFY COLUMN `model_version` VARCHAR(64)" in statement
            for statement in engine.conn.statements
        ))

    def test_linear_score_maps_range(self):
        self.assertEqual(linear_score(5, 0, 10), 50.0)
        self.assertEqual(linear_score(20, 0, 10), 100.0)
        self.assertEqual(linear_score(None, 0, 10), 50.0)

    def test_peg_valuation_uses_industry_specific_ranges(self):
        growth = evaluate_peg_valuation(
            pe_ttm=24,
            pb_ratio=3.0,
            growth_pct=30,
            industry_name="AI软件",
        )
        self.assertEqual(growth["valuation_style"], "growth")
        self.assertEqual(growth["peg_ratio"], 0.8)
        self.assertGreaterEqual(growth["valuation_score"], 70)

        stable = evaluate_peg_valuation(
            pe_ttm=18,
            pb_ratio=2.0,
            growth_pct=20,
            industry_name="食品饮料",
        )
        self.assertEqual(stable["valuation_style"], "stable")
        self.assertEqual(stable["peg_ratio"], 0.9)
        self.assertIn("合理", stable["valuation_reason"])

    def test_cyclical_valuation_does_not_force_peg(self):
        result = evaluate_peg_valuation(
            pe_ttm=8,
            pb_ratio=1.2,
            growth_pct=35,
            industry_name="煤炭化工",
        )
        self.assertEqual(classify_valuation_style("煤炭化工"), "cyclical")
        self.assertIsNone(result["peg_ratio"])
        self.assertIn("PEG不适用", result["valuation_reason"])

    def test_market_style_context_detects_bull_growth_and_bear_defense(self):
        bull = classify_market_style_context({
            "000300": {"close": 4200, "ma20": 4000, "ma60": 3800, "pct_20": 4.0},
            "399006": {"close": 2500, "ma20": 2350, "ma60": 2250, "pct_20": 9.0},
        })
        self.assertEqual(bull["market_regime"], "BULL")
        self.assertEqual(bull["style_bias"], "growth")
        self.assertTrue(bull["style_growth_allowed"])

        bear = classify_market_style_context({
            "000300": {"close": 3400, "ma20": 3600, "ma60": 3900, "pct_20": -6.0},
            "399006": {"close": 1800, "ma20": 1900, "ma60": 2100, "pct_20": -8.0},
        })
        self.assertEqual(bear["market_regime"], "BEAR")
        self.assertEqual(bear["style_bias"], "defensive")
        self.assertFalse(bear["style_growth_allowed"])

    def test_north_flow_context_classifies_three_day_pressure(self):
        outflow = classify_north_flow_context([
            {"trade_date": "2026-06-25", "net_tgt": -1_200_000_000},
            {"trade_date": "2026-06-26", "net_tgt": -1_500_000_000},
            {"trade_date": "2026-06-29", "net_tgt": -1_100_000_000},
        ])
        self.assertEqual(outflow["north_flow_status"], "OUTFLOW")
        self.assertLess(outflow["north_net_3d"], -3_000_000_000)

        inflow = classify_north_flow_context([
            {"trade_date": "2026-06-25", "net_tgt": 500_000_000},
            {"trade_date": "2026-06-26", "net_tgt": 1_600_000_000},
            {"trade_date": "2026-06-29", "net_tgt": 1_200_000_000},
        ])
        self.assertEqual(inflow["north_flow_status"], "INFLOW")
        self.assertGreater(inflow["north_net_3d"], 3_000_000_000)

    def test_macro_policy_context_classifies_support_and_risk(self):
        risk = classify_macro_policy_context([
            {"publish_time": "2026-06-29 10:00:00", "title": "地缘冲突升级叠加制裁风险", "content": "人民币贬值压力升温"},
            {"publish_time": "2026-06-29 11:00:00", "title": "监管趋严与补贴退坡影响行业", "content": "产能过剩"},
        ])
        self.assertEqual(risk["macro_policy_status"], "RISK")
        self.assertGreater(risk["macro_policy_risk_count"], 3)

        support = classify_macro_policy_context([
            {"publish_time": "2026-06-29 10:00:00", "title": "央行降准释放流动性", "content": "政策支持扩内需"},
            {"publish_time": "2026-06-29 11:00:00", "title": "设备更新和以旧换新推进", "content": "财政刺激"},
        ])
        self.assertEqual(support["macro_policy_status"], "SUPPORT")
        self.assertGreater(support["macro_policy_score"], 50)

    def test_macro_indicator_context_classifies_hard_data_pressure_and_support(self):
        risk = classify_macro_indicator_context([
            {
                "indicator_name": "PMI",
                "period_date": "2026-06-30",
                "value": 48.5,
                "expected_value": 49.8,
            },
            {
                "indicator_name": "USD/CNY",
                "period_date": "2026-06-30",
                "value": 7.42,
                "previous_value": 7.30,
            },
        ])
        self.assertEqual(risk["macro_indicator_status"], "RISK")
        self.assertGreaterEqual(risk["macro_indicator_risk_count"], 2)

        support = classify_macro_indicator_context([
            {"indicator_name": "PMI", "period_date": "2026-06-30", "value": 51.2},
            {"indicator_name": "GDP", "period_date": "2026-06-30", "value": 5.2},
            {"indicator_name": "CPI", "period_date": "2026-06-30", "value": 1.4},
        ])
        self.assertEqual(support["macro_indicator_status"], "SUPPORT")
        self.assertGreater(support["macro_indicator_score"], 50)
        self.assertEqual(support["macro_cycle"], "RECOVERY")

    def test_etf_flow_context_classifies_market_risk_appetite(self):
        outflow = classify_etf_flow_context([
            {"trade_date": "2026-06-26", "net_amount": -1_100_000_000},
            {"trade_date": "2026-06-29", "net_amount": -1_200_000_000},
            {"trade_date": "2026-06-30", "net_amount": -1_300_000_000},
        ])
        self.assertEqual(outflow["etf_flow_status"], "OUTFLOW")
        self.assertLess(outflow["etf_net_3d"], -3_000_000_000)

        inflow = classify_etf_flow_context([
            {"trade_date": "2026-06-26", "net_amount": 1_100_000_000},
            {"trade_date": "2026-06-29", "net_amount": 1_200_000_000},
            {"trade_date": "2026-06-30", "net_amount": 1_300_000_000},
        ])
        self.assertEqual(inflow["etf_flow_status"], "INFLOW")
        self.assertGreater(inflow["etf_flow_score"], 50)

    def test_retail_sentiment_context_classifies_extremes(self):
        bullish = classify_retail_sentiment_context([
            {"trade_date": "2026-06-30", "bullish_pct": 82.0, "bearish_pct": 12.0, "sample_size": 1200},
        ])
        self.assertEqual(bullish["retail_sentiment_status"], "EXTREME_BULLISH")
        self.assertLess(bullish["retail_sentiment_score"], 50)

        bearish = classify_retail_sentiment_context([
            {"trade_date": "2026-06-30", "bullish_pct": 0.18, "bearish_pct": 0.78, "sample_size": 900},
        ])
        self.assertEqual(bearish["retail_sentiment_status"], "EXTREME_BEARISH")
        self.assertGreater(bearish["retail_sentiment_score"], 50)

    def test_new_structured_profile_evaluators_classify_risk_and_support(self):
        pure = evaluate_business_purity({
            "business_scope": "AI software platform and industrial cloud software services",
            "industry_name": "software",
            "research_theme_name": "AI software",
            "research_theme_role": "platform",
        })
        self.assertEqual(pure["business_purity_status"], "PASS")
        self.assertGreaterEqual(pure["business_purity_match_count"], 2)

        impure = evaluate_business_purity({
            "business_scope": "commodity trading, real estate leasing and consulting agency",
            "industry_name": "semiconductor",
            "research_theme_name": "AI chip",
        })
        self.assertEqual(impure["business_purity_status"], "RISK")

        weak_prosperity = evaluate_industry_prosperity({
            "industry_price_change_30d": -8.0,
            "capacity_utilization": 0.52,
            "order_contract_to_revenue_pct": 0.0,
        })
        self.assertEqual(weak_prosperity["industry_prosperity_status"], "RISK")
        self.assertIn("industry_prosperity_weak", weak_prosperity["industry_prosperity_flags"])

        strong_institution = evaluate_institutional_profile({
            "fund_hold_ratio": 0.06,
            "qfii_hold_ratio": 0.02,
            "institution_hold_ratio": 0.08,
            "rating_upgrade_count_90d": 3,
            "target_price_upside_pct": 28.0,
            "survey_count_90d": 5,
        })
        self.assertEqual(strong_institution["institutional_status"], "PASS")

        weak_institution = evaluate_institutional_profile({
            "rating_downgrade_count_90d": 2,
            "target_price": 10.0,
            "target_price_upside_pct": -8.0,
        })
        self.assertEqual(weak_institution["institutional_status"], "RISK")

        interaction = evaluate_investor_interaction_profile({
            "investor_interaction_count_180d": 6,
            "investor_interaction_support_count": 3,
            "latest_investor_interaction": "new order and customer certification",
        })
        self.assertEqual(interaction["investor_interaction_status"], "PASS")

        risky_interaction = evaluate_investor_interaction_profile({
            "investor_interaction_count_180d": 4,
            "investor_interaction_risk_count": 3,
            "latest_investor_interaction": "delay and margin pressure",
        })
        self.assertEqual(risky_interaction["investor_interaction_status"], "RISK")

    def test_liquidity_profile_applies_bear_market_thresholds(self):
        good = evaluate_liquidity_profile({
            "amount": 900_000_000,
            "amount_ma20": 900_000_000,
            "turnover_ratio": 8.0,
            "market_regime": "BEAR",
        })
        self.assertEqual(good["liquidity_status"], "PASS")

        thin = evaluate_liquidity_profile({
            "amount": 80_000_000,
            "amount_ma20": 90_000_000,
            "turnover_ratio": 3.0,
            "market_regime": "RANGE",
        })
        self.assertEqual(thin["liquidity_status"], "BLOCK")
        self.assertIn("liquidity_hard_floor", thin["liquidity_flags"])

    def test_size_liquidity_profile_applies_float_market_cap_floor(self):
        good = evaluate_size_liquidity_profile({
            "float_market_cap": 8_000_000_000,
            "market_cap": 12_000_000_000,
        })
        self.assertEqual(good["size_liquidity_status"], "PASS")

        small = evaluate_size_liquidity_profile({
            "float_market_cap": 3_000_000_000,
            "market_cap": 10_000_000_000,
        })
        self.assertEqual(small["size_liquidity_status"], "WATCH")
        self.assertIn("float_market_cap_low", small["size_liquidity_flags"])

    def test_volume_temperature_detects_blowoff_and_moderate_volume(self):
        moderate = evaluate_volume_temperature_profile({
            "amount_ratio_20": 1.5,
            "turnover_ratio": 9.0,
            "change_pct": 2.0,
        })
        self.assertEqual(moderate["volume_temperature_status"], "PASS")

        blowoff = evaluate_volume_temperature_profile({
            "amount_ratio_20": 3.5,
            "turnover_ratio": 18.0,
            "change_pct": 4.0,
            "pct_5": 18.0,
        })
        self.assertEqual(blowoff["volume_temperature_status"], "RISK")
        self.assertIn("blowoff_volume_risk", blowoff["volume_temperature_flags"])

    def test_fundamental_quality_applies_style_thresholds(self):
        good = evaluate_fundamental_quality({
            "industry_name": "AI软件",
            "valuation_style": "growth",
            "market_regime": "BULL",
            "basic_eps": 0.8,
            "roe_wtd": 14.0,
            "gross_margin": 38.0,
            "net_margin": 12.0,
            "asset_liab_ratio": 38.0,
            "total_rev_yoy_gr": 28.0,
            "total_rev_qoq_gr": 12.0,
            "net_profit_yoy_gr": 35.0,
            "non_gaap_net_profit_yoy_gr": 32.0,
            "net_profit_qoq_gr": 18.0,
            "roe_non_gaap_wtd": 15.0,
            "roa_wtd": 6.0,
            "quick_ratio": 1.3,
        })
        self.assertEqual(good["fundamental_quality_status"], "PASS")

        weak = evaluate_fundamental_quality({
            "industry_name": "AI软件",
            "valuation_style": "growth",
            "market_regime": "BEAR",
            "basic_eps": -0.1,
            "roe_wtd": 5.0,
            "gross_margin": 18.0,
            "net_margin": -8.0,
            "asset_liab_ratio": 68.0,
            "total_rev_yoy_gr": -3.0,
            "net_profit_yoy_gr": -25.0,
            "total_rev_qoq_gr": -8.0,
            "net_profit_qoq_gr": -28.0,
            "roa_wtd": 1.2,
            "quick_ratio": 0.6,
        })
        self.assertEqual(weak["fundamental_quality_status"], "BLOCK")
        self.assertIn("fundamental_loss", weak["fundamental_quality_flags"])
        self.assertIn("debt_ratio_over_cap", weak["fundamental_quality_flags"])
        self.assertIn("qoq_performance_drop", weak["fundamental_quality_flags"])
        self.assertIn("quick_ratio_low", weak["fundamental_quality_flags"])

    def test_fundamental_quality_uses_qoq_roa_and_quick_ratio_gates(self):
        result = evaluate_fundamental_quality({
            "industry_name": "消费电子",
            "valuation_style": "stable",
            "market_regime": "RANGE",
            "basic_eps": 0.2,
            "roe_wtd": 7.0,
            "roe_non_gaap_wtd": 7.5,
            "roa_wtd": 1.4,
            "gross_margin": 24.0,
            "net_margin": 3.0,
            "asset_liab_ratio": 45.0,
            "quick_ratio": 0.55,
            "total_rev_yoy_gr": 12.0,
            "net_profit_yoy_gr": 15.0,
            "non_gaap_net_profit_yoy_gr": 14.0,
            "total_rev_qoq_gr": -6.0,
            "net_profit_qoq_gr": -24.0,
        })
        self.assertEqual(result["fundamental_quality_status"], "WATCH")
        self.assertIn("qoq_performance_drop", result["fundamental_quality_flags"])
        self.assertIn("roa_below_threshold", result["fundamental_quality_flags"])
        self.assertIn("quick_ratio_low", result["fundamental_quality_flags"])
        self.assertIn("profit QoQ -24.0%", result["fundamental_quality_reason"])

        flags = result["fundamental_quality_flags"]
        status, reason = choose_recommend_status(
            "600123", "样本股份", 78, 70, 72, "LOW", 200_000_000, 1.0, 60,
            data_quality_score=90, data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("环比", reason)
        self.assertIn("fundamental_weak", build_failure_tags({"data_quality_flags": flags}))

    def test_fundamental_quality_flags_financial_landmines(self):
        result = evaluate_fundamental_quality({
            "industry_name": "高端制造",
            "valuation_style": "stable",
            "market_regime": "RANGE",
            "basic_eps": 0.5,
            "roe_wtd": 12.0,
            "gross_margin": 30.0,
            "net_margin": 8.0,
            "asset_liab_ratio": 40.0,
            "quick_ratio": 1.1,
            "total_rev_yoy_gr": 18.0,
            "net_profit_yoy_gr": 22.0,
            "total_rev_qoq_gr": 5.0,
            "net_profit_qoq_gr": 8.0,
            "roic": 9.0,
            "acct_recv_to_rev": 38.0,
            "prepayment_yoy_gr": 62.0,
            "related_transaction_to_rev": 24.0,
        })

        self.assertEqual(result["fundamental_quality_status"], "WATCH")
        self.assertIn("roic_below_threshold", result["fundamental_quality_flags"])
        self.assertIn("receivable_ratio_high", result["fundamental_quality_flags"])
        self.assertIn("prepayment_growth_high", result["fundamental_quality_flags"])
        self.assertIn("related_transaction_ratio_high", result["fundamental_quality_flags"])
        status, reason = choose_recommend_status(
            "600123", "样本股份", 78, 70, 72, "LOW", 200_000_000, 1.0, 60,
            data_quality_score=90, data_quality_flags=result["fundamental_quality_flags"],
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("财务雷区", reason)
        self.assertIn("fundamental_weak", build_failure_tags({"data_quality_flags": result["fundamental_quality_flags"]}))

    def test_parse_dividend_cash_per_share(self):
        self.assertEqual(parse_dividend_cash_per_share("10派3.5元"), 0.35)
        self.assertEqual(parse_dividend_cash_per_share("每10股派发现金红利2.8元"), 0.28)
        self.assertEqual(parse_dividend_cash_per_share("每股派息0.42元"), 0.42)
        self.assertEqual(parse_dividend_cash_per_share("10转4股"), 0.0)

    def test_build_research_theme_features_maps_theme_pool(self):
        features = build_research_theme_features([{
            "id": "ai_compute",
            "name": "AI算力",
            "trend": "最强产业线",
            "score": 88,
            "evidence_level": "财报兑现强",
            "verification": "看订单和利润率",
            "risk": "估值高",
            "stocks": [
                {"code": "300308", "role": "光模块", "tier": "核心验证"},
                {"code": "002463", "role": "AI PCB", "tier": "弹性跟踪"},
            ],
        }])

        row = features[features["stock_code"] == "300308"].iloc[0]
        self.assertEqual(row["research_theme_name"], "AI算力")
        self.assertGreaterEqual(row["research_theme_score"], 90)
        self.assertEqual(row["research_theme_tier"], "核心验证")

    def test_sector_rotation_features_classifies_leadership_tier(self):
        kline = pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2026-06-28", "change_pct": 1.0, "amount": 500_000_000},
            {"stock_code": "000001", "trade_date": "2026-06-29", "change_pct": 2.0, "amount": 600_000_000},
            {"stock_code": "000001", "trade_date": "2026-06-30", "change_pct": 3.0, "amount": 700_000_000},
            {"stock_code": "000002", "trade_date": "2026-06-28", "change_pct": 0.5, "amount": 220_000_000},
            {"stock_code": "000002", "trade_date": "2026-06-29", "change_pct": 0.8, "amount": 230_000_000},
            {"stock_code": "000002", "trade_date": "2026-06-30", "change_pct": 1.0, "amount": 240_000_000},
            {"stock_code": "000003", "trade_date": "2026-06-28", "change_pct": -1.0, "amount": 80_000_000},
            {"stock_code": "000003", "trade_date": "2026-06-29", "change_pct": -0.5, "amount": 70_000_000},
            {"stock_code": "000003", "trade_date": "2026-06-30", "change_pct": 0.1, "amount": 60_000_000},
        ])
        codes = pd.DataFrame([
            {"stock_code": "000001", "industry_name": "AI"},
            {"stock_code": "000002", "industry_name": "AI"},
            {"stock_code": "000003", "industry_name": "AI"},
        ])
        flow = pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2026-06-28", "main_net_inflow": 80_000_000},
            {"stock_code": "000001", "trade_date": "2026-06-29", "main_net_inflow": 90_000_000},
            {"stock_code": "000001", "trade_date": "2026-06-30", "main_net_inflow": 100_000_000},
            {"stock_code": "000002", "trade_date": "2026-06-28", "main_net_inflow": 20_000_000},
            {"stock_code": "000002", "trade_date": "2026-06-29", "main_net_inflow": 25_000_000},
            {"stock_code": "000002", "trade_date": "2026-06-30", "main_net_inflow": 30_000_000},
            {"stock_code": "000003", "trade_date": "2026-06-28", "main_net_inflow": -10_000_000},
            {"stock_code": "000003", "trade_date": "2026-06-29", "main_net_inflow": -8_000_000},
            {"stock_code": "000003", "trade_date": "2026-06-30", "main_net_inflow": -5_000_000},
        ])
        with patch("biz.analysis.sync_analysis_fast._table_exists", return_value=True), \
             patch("biz.analysis.sync_analysis_fast._table_columns", side_effect=lambda _engine, table: {
                 "sm_stock_kline": {"received_at", "etl_sync_at"},
                 "si_industry_sw": {"etl_sync_at"},
                 "sm_stock_capital_flow_daily": {"received_at", "etl_sync_at"},
             }.get(table, set())), \
             patch("biz.analysis.sync_analysis_fast._recent_dates", return_value=["2026-06-30", "2026-06-29", "2026-06-28"]), \
             patch("biz.analysis.sync_analysis_fast._read_frame", side_effect=[kline, codes, flow]):
            out = load_sector_rotation_features(object(), "2026-06-30")

        leader = out[out["stock_code"] == "000001"].iloc[0]
        follower = out[out["stock_code"] == "000003"].iloc[0]
        self.assertEqual(leader["sector_leadership_tier"], "leader")
        self.assertGreaterEqual(leader["sector_leadership_score"], 85)
        self.assertGreaterEqual(leader["theme_continuity_score_10"], 6.0)
        self.assertIn(leader["theme_continuity_level"], {"MEDIUM", "HIGH"})
        self.assertIn("题材延续性", leader["theme_continuity_reason"])
        self.assertEqual(follower["sector_leadership_tier"], "follower")

    def test_notice_title_classification(self):
        result = classify_notice_title("关于公司被立案调查及股份回购计划的公告")
        self.assertGreater(result["critical"], 0)
        self.assertGreater(result["positive"], 0)
        policy = classify_notice_title("行业集采限产及补贴退坡政策影响提示")
        self.assertGreaterEqual(policy["negative"], 3)
        growth = classify_notice_title("2026年半年度业绩快报 净利润大幅增长并实现预盈")
        self.assertGreaterEqual(growth["positive"], 3)
        impairment = classify_notice_title("关于计提大额商誉减值及应收账款坏账准备的公告")
        self.assertGreaterEqual(impairment["negative"], 3)

    def test_recommend_gate_blocks_st_name(self):
        status, reason = choose_recommend_status(
            stock_code="000001",
            short_name="*ST测试",
            ai_score=90,
            short_term_score=80,
            long_term_score=80,
            event_risk_level="LOW",
            amount=100_000_000,
            change_pct=2,
            min_score=60,
        )
        self.assertEqual(status, "BLOCK")
        self.assertIn("ST", reason)

    def test_recommend_gate_blocks_688_prefix(self):
        status, reason = choose_recommend_status(
            stock_code="688001",
            short_name="Alpha",
            ai_score=90,
            short_term_score=80,
            long_term_score=80,
            event_risk_level="LOW",
            amount=100_000_000,
            change_pct=2,
            min_score=60,
        )

        self.assertEqual(status, "BLOCK")
        self.assertIn("688", reason)

    def test_rule_flags_detect_cash_flow_downtrend_and_outflow(self):
        flags = build_rule_flags({
            "stock_code": "688001",
            "close": 8.0,
            "ma5": 8.5,
            "ma10": 9.0,
            "ma20": 10.0,
            "ma60": 11.0,
            "oper_cf_ps": -0.1,
            "cash_flow_ratio": -2.0,
            "main_outflow_days_3d": 3,
        })

        self.assertIn("excluded_688", flags)
        self.assertIn("downtrend_clock", flags)
        self.assertIn("negative_oper_cash_flow", flags)
        self.assertIn("negative_cash_flow_ratio", flags)
        self.assertIn("main_outflow_3d", flags)

    def test_rule_flags_detect_ten_day_main_outflow(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "main_outflow_days_10d": 7,
            "main_net_inflow_10d": -220_000_000,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("main_outflow_10d", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="Alpha",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("10日", reason)
        self.assertIn("capital_outflow", build_failure_tags({"data_quality_flags": flags}))

    def test_order_book_depth_flags_thin_or_imbalanced_depth(self):
        thin = evaluate_order_book_depth({
            "bid5_amount": 18_000_000,
            "ask5_amount": 12_000_000,
        })
        self.assertEqual(thin["order_book_status"], "WATCH")
        self.assertIn("order_book_depth_low", thin["order_book_flags"])

        flags = build_rule_flags({
            "stock_code": "300001",
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
            "bid5_amount": 15_000_000,
            "ask5_amount": 120_000_000,
        })
        self.assertIn("order_book_imbalance", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="Alpha",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("盘口", reason)
        self.assertIn("liquidity_risk", build_failure_tags({"data_quality_flags": flags}))

    def test_load_order_book_features_computes_five_level_depth(self):
        rows = pd.DataFrame([{
            "stock_code": "1",
            "snapshot_at": "2026-06-30 10:00:00",
            "b1": 10.0, "bv1": 10_000, "b2": 9.9, "bv2": 8_000,
            "b3": 9.8, "bv3": 6_000, "b4": 9.7, "bv4": 4_000, "b5": 9.6, "bv5": 2_000,
            "s1": 10.1, "sv1": 9_000, "s2": 10.2, "sv2": 7_000,
            "s3": 10.3, "sv3": 5_000, "s4": 10.4, "sv4": 3_000, "s5": 10.5, "sv5": 1_000,
        }])
        cols = set(rows.columns)
        with patch("biz.analysis.sync_analysis_fast._table_exists", return_value=True), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value=cols), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=rows):
            out = load_order_book_features(object(), "2026-06-30")

        self.assertEqual(out.iloc[0]["stock_code"], "000001")
        self.assertGreater(out.iloc[0]["bid5_amount"], 29_000_000)
        self.assertGreater(out.iloc[0]["ask5_amount"], 25_000_000)
        self.assertAlmostEqual(out.iloc[0]["order_book_depth_amount"], out.iloc[0]["bid5_amount"] + out.iloc[0]["ask5_amount"])

    def test_relative_industry_valuation_flags_overpriced_stock(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "valuation_score": 38,
            "pe_industry_multiple": 1.9,
        })
        self.assertIn("industry_relative_overvalued", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=78,
            short_term_score=72,
            long_term_score=70,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=2,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("行业", reason)

    def test_relative_industry_ps_valuation_flags_overpriced_stock(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "valuation_score": 38,
            "ps_industry_multiple": 2.7,
        })
        self.assertIn("industry_relative_ps_overvalued", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="Alpha",
            ai_score=78,
            short_term_score=72,
            long_term_score=70,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=2,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("PS", reason)
        self.assertIn("valuation_expensive", build_failure_tags({"data_quality_flags": flags}))

    def test_valuation_history_percentile_flags_crowded_valuation(self):
        rows = [
            {"trade_date": (datetime(2026, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d"), "close": float(i + 1)}
            for i in range(60)
        ]
        result = estimate_latest_close_percentile(rows, lookback=60, min_count=20)
        self.assertEqual(result["close_percentile_250d"], 100.0)
        self.assertEqual(result["close_history_count"], 60)

        flags = build_rule_flags({
            "stock_code": "300001",
            "valuation_score": 40,
            "valuation_history_percentile_250d": 92.0,
        })
        self.assertIn("valuation_history_percentile_high", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="Alpha",
            ai_score=78,
            short_term_score=72,
            long_term_score=70,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=2,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("250", reason)
        self.assertIn("valuation_expensive", build_failure_tags({"data_quality_flags": flags}))

    def test_bear_market_pauses_non_defensive_growth_stock(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "industry_name": "AI软件",
            "valuation_style": "growth",
            "market_regime": "BEAR",
        })
        self.assertIn("bear_market_growth_pause", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=180_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("沪深300", reason)

    def test_institutional_lhb_outflow_suspends_candidate(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "lhb_inst_count_20d": 2,
            "lhb_inst_net_amount_20d": -150_000_000,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("institutional_lhb_outflow", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="娴嬭瘯鑲′唤",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("机构", reason)
        self.assertIn("institutional_outflow", build_failure_tags({"data_quality_flags": flags}))

    def test_margin_deleveraging_suspends_candidate(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "margin_balance_delta_3d": -80_000_000,
            "margin_contracting_days_3d": 3,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("margin_deleveraging_3d", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("两融", reason)
        self.assertIn("margin_deleveraging", build_failure_tags({"data_quality_flags": flags}))

    def test_build_text_fields_parses_data_quality_flags(self):
        df = pd.DataFrame([{
            "stock_code": "300001",
            "short_name": "测试股份",
            "trade_date": "2026-07-03",
            "close": 10.0,
            "open": 9.8,
            "high": 10.2,
            "low": 9.7,
            "amount": 600_000_000,
            "change_pct": 1.5,
            "technical_score": 70,
            "capital_score": 70,
            "fundamental_score": 70,
            "growth_score": 70,
            "valuation_score": 70,
            "sentiment_score": 70,
            "event_score": 60,
            "event_risk_score": 20,
            "short_term_score": 76,
            "long_term_score": 72,
            "ai_score": 74,
            "recommend_status": "SUSPENDED",
            "event_risk_level": "LOW",
            "data_quality_flags": json.dumps(["margin_deleveraging_3d"]),
            "margin_balance_delta_3d": -120_000_000,
        }])

        out = _build_text_fields(df, flow_date="2026-07-03", trade_date="2026-07-03")

        self.assertIn("margin_deleveraging", json.loads(out.loc[0, "failure_tags_json"]))
        self.assertTrue(any("1.20" in item for item in json.loads(out.loc[0, "risks"])))

    def test_unlock_pressure_uses_size_thresholds(self):
        minor = evaluate_unlock_pressure({
            "lifting_count_30d": 1,
            "lifting_amount_30d": 80_000_000,
            "lifting_max_ratio_30d": 3.0,
            "reduction_count_90d": 1,
            "reduction_max_ratio_90d": 1.2,
            "reduction_amount_90d": 60_000_000,
            "effective_market_cap": 10_000_000_000,
        })
        major = evaluate_unlock_pressure({
            "lifting_count_30d": 1,
            "lifting_amount_30d": 80_000_000,
            "lifting_max_ratio_30d": 12.0,
            "effective_market_cap": 10_000_000_000,
        })

        self.assertEqual(minor["unlock_status"], "WATCH")
        self.assertIn("minor_unlock_watch", minor["unlock_flags"])
        self.assertEqual(major["unlock_status"], "BLOCK")
        self.assertIn("unlock_risk", major["unlock_flags"])

        minor_flags = build_rule_flags({
            "stock_code": "300001",
            "lifting_count_30d": 1,
            "lifting_amount_30d": 80_000_000,
            "lifting_max_ratio_30d": 3.0,
            "effective_market_cap": 10_000_000_000,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("minor_unlock_watch", minor_flags)
        self.assertNotIn("unlock_risk", minor_flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=minor_flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("小额解禁", reason)
        self.assertIn("unlock_watch", build_failure_tags({"data_quality_flags": minor_flags}))

    def test_pledge_and_goodwill_risks_suspend_candidate(self):
        pledge_flags = build_rule_flags({
            "stock_code": "300001",
            "pledge_ratio": 55.0,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        goodwill_flags = build_rule_flags({
            "stock_code": "300001",
            "goodwill_to_net_asset_pct": 32.0,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })

        self.assertIn("pledge_ratio_high", pledge_flags)
        self.assertIn("goodwill_ratio_high", goodwill_flags)
        self.assertEqual(derive_position_risk_level({"data_quality_flags": pledge_flags}, "CONFIRM"), "HIGH")
        self.assertIn("pledge_risk", build_failure_tags({"data_quality_flags": pledge_flags}))
        self.assertIn("goodwill_risk", build_failure_tags({"data_quality_flags": goodwill_flags}))

        pledge_status, pledge_reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=pledge_flags,
        )
        goodwill_status, goodwill_reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=goodwill_flags,
        )

        self.assertEqual(pledge_status, "SUSPENDED")
        self.assertIn("质押", pledge_reason)
        self.assertEqual(goodwill_status, "SUSPENDED")
        self.assertIn("商誉", goodwill_reason)

    def test_shareholder_reduction_ratio_suspends_candidate(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "reduction_max_ratio_90d": 2.4,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })

        self.assertIn("shareholder_reduction_high", flags)
        self.assertEqual(derive_position_risk_level({"data_quality_flags": flags}, "CONFIRM"), "HIGH")
        self.assertIn("shareholder_reduction", build_failure_tags({"data_quality_flags": flags}))
        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("减持", reason)

    def test_load_chip_capital_features_aggregates_margin_trend(self):
        margin_rows = pd.DataFrame([
            {
                "stock_code": "300001",
                "margin_trade_date": "2026-06-26",
                "margin_balance": 500_000_000,
                "financing_balance": 450_000_000,
                "margin_balance_delta": -30_000_000,
                "financing_buy_amount": 10_000_000,
            },
            {
                "stock_code": "300001",
                "margin_trade_date": "2026-06-29",
                "margin_balance": 460_000_000,
                "financing_balance": 420_000_000,
                "margin_balance_delta": -25_000_000,
                "financing_buy_amount": 15_000_000,
            },
            {
                "stock_code": "300001",
                "margin_trade_date": "2026-06-30",
                "margin_balance": 420_000_000,
                "financing_balance": 390_000_000,
                "margin_balance_delta": -25_000_000,
                "financing_buy_amount": 20_000_000,
            },
        ])

        with patch("biz.analysis.sync_analysis_fast._table_exists", side_effect=lambda _engine, table: table == "st_securities_margin"), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={
                 "stock_code", "trade_date", "rzrqye", "rzye", "rzrqyecz", "rzmre",
             }), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=margin_rows):
            df = load_chip_capital_features(object(), "2026-06-30")

        row = df[df["stock_code"] == "300001"].iloc[0]
        self.assertEqual(row["margin_balance_delta_3d"], -80_000_000)
        self.assertEqual(row["financing_buy_amount_3d"], 45_000_000)
        self.assertEqual(row["margin_contracting_days_3d"], 3)

    def test_load_chip_capital_features_reads_optional_pledge_ratio(self):
        pledge_rows = pd.DataFrame([{
            "stock_code": "300001",
            "pledge_report_date": "2026-06-30",
            "pledge_ratio": 0.55,
        }])

        with patch("biz.analysis.sync_analysis_fast._table_exists", side_effect=lambda _engine, table: table == "st_stock_pledge"), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={
                 "stock_code", "report_date", "pledge_ratio",
             }), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=pledge_rows):
            df = load_chip_capital_features(object(), "2026-06-30")

        row = df[df["stock_code"] == "300001"].iloc[0]
        self.assertAlmostEqual(row["pledge_ratio"], 55.0)

    def test_load_chip_capital_features_reads_optional_reduction_ratio(self):
        reduction_rows = pd.DataFrame([{
            "stock_code": "300001",
            "reduction_count_90d": 1,
            "reduction_max_ratio_90d": 0.024,
            "reduction_amount_90d": 120_000_000,
            "reduction_latest_date": "2026-06-30",
        }])

        with patch("biz.analysis.sync_analysis_fast._table_exists", side_effect=lambda _engine, table: table == "st_stock_holder_reduction"), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={
                 "stock_code", "announcement_date", "reduction_ratio", "reduction_amount",
             }), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=reduction_rows):
            df = load_chip_capital_features(object(), "2026-06-30")

        row = df[df["stock_code"] == "300001"].iloc[0]
        self.assertAlmostEqual(row["reduction_max_ratio_90d"], 2.4)
        self.assertEqual(row["reduction_amount_90d"], 120_000_000)

    def test_load_stock_north_holding_features_reads_optional_table(self):
        north_rows = pd.DataFrame([
            {
                "stock_code": "300001",
                "north_stock_trade_date": "2026-06-25",
                "north_holding_ratio": 0.015,
                "north_holding_market_value": 150_000_000,
                "north_holding_shares": 12_000_000,
                "north_net_buy_amount": 10_000_000,
            },
            {
                "stock_code": "300001",
                "north_stock_trade_date": "2026-06-26",
                "north_holding_ratio": 0.013,
                "north_holding_market_value": 130_000_000,
                "north_holding_shares": 11_000_000,
                "north_net_buy_amount": -60_000_000,
            },
            {
                "stock_code": "300001",
                "north_stock_trade_date": "2026-06-29",
                "north_holding_ratio": 0.012,
                "north_holding_market_value": 120_000_000,
                "north_holding_shares": 10_000_000,
                "north_net_buy_amount": -70_000_000,
            },
            {
                "stock_code": "300001",
                "north_stock_trade_date": "2026-06-30",
                "north_holding_ratio": 0.011,
                "north_holding_market_value": 110_000_000,
                "north_holding_shares": 9_000_000,
                "north_net_buy_amount": -80_000_000,
            },
        ])

        with patch("biz.analysis.sync_analysis_fast._table_exists", side_effect=lambda _engine, table: table == "st_stock_north_holding"), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={
                 "stock_code", "trade_date", "holding_ratio", "market_value", "holding_shares", "net_buy_amount",
             }), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=north_rows):
            df = load_stock_north_holding_features(object(), "2026-06-30")

        row = df[df["stock_code"] == "300001"].iloc[0]
        self.assertAlmostEqual(row["north_holding_ratio"], 1.1)
        self.assertAlmostEqual(row["north_holding_ratio_delta_3d"], -0.4)
        self.assertEqual(row["north_stock_status"], "RISK")
        self.assertLess(row["north_net_buy_amount_3d"], -50_000_000)

    def test_load_etf_flow_context_reads_optional_table(self):
        etf_rows = pd.DataFrame([
            {"trade_date": "2026-06-26", "net_amount": -1_200_000_000},
            {"trade_date": "2026-06-29", "net_amount": -1_100_000_000},
            {"trade_date": "2026-06-30", "net_amount": -1_000_000_000},
        ])

        with patch("biz.analysis.sync_analysis_fast._table_exists", side_effect=lambda _engine, table: table == "st_etf_flow_daily"), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={"trade_date", "net_amount"}), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=etf_rows):
            out = load_etf_flow_context(object(), "2026-06-30")

        self.assertEqual(out["etf_flow_status"], "OUTFLOW")
        self.assertEqual(out["etf_net_3d"], -3_300_000_000)

    def test_load_retail_sentiment_context_reads_optional_table(self):
        sentiment_rows = pd.DataFrame([{
            "trade_date": "2026-06-30",
            "bullish_pct": 81.0,
            "bearish_pct": 11.0,
            "sample_size": 1500,
        }])

        with patch("biz.analysis.sync_analysis_fast._table_exists", side_effect=lambda _engine, table: table == "st_retail_sentiment"), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={
                 "trade_date", "bullish_pct", "bearish_pct", "sample_size",
             }), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=sentiment_rows):
            out = load_retail_sentiment_context(object(), "2026-06-30")

        self.assertEqual(out["retail_sentiment_status"], "EXTREME_BULLISH")
        self.assertEqual(out["retail_bullish_pct"], 81.0)

    def test_load_investor_interaction_features_reads_optional_table(self):
        interaction_rows = pd.DataFrame([{
            "stock_code": "300001",
            "investor_interaction_count_180d": 5,
            "investor_interaction_support_count": 0,
            "investor_interaction_risk_count": 3,
            "latest_investor_interaction_date": "2026-06-30",
            "latest_investor_interaction": "delay and margin pressure",
        }])

        with patch("biz.analysis.sync_analysis_fast._table_exists", side_effect=lambda _engine, table: table == "st_investor_interaction"), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={"stock_code", "publish_date", "question", "answer"}), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=interaction_rows):
            df = load_investor_interaction_features(object(), "2026-06-30")

        row = df[df["stock_code"] == "300001"].iloc[0]
        self.assertEqual(row["investor_interaction_status"], "RISK")
        self.assertGreaterEqual(row["investor_interaction_risk_count"], 3)

    def test_market_relative_weak_suspends_candidate(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "pct_20": -6.0,
            "relative_hs300_20": -12.0,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("market_relative_weak", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="娴嬭瘯鑲′唤",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("沪深300", reason)
        self.assertIn("relative_weak", build_failure_tags({"data_quality_flags": flags}))

    def test_macro_policy_pressure_suspends_candidate(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "market_regime": "BEAR",
            "macro_policy_status": "RISK",
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("macro_policy_pressure", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="娴嬭瘯鑲′唤",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("宏观", reason)
        self.assertIn("macro_policy_pressure", build_failure_tags({"data_quality_flags": flags}))

    def test_new_context_risks_suspend_candidates_and_create_failure_tags(self):
        base = {
            "stock_code": "300001",
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        }
        cases = [
            (
                "macro_indicator_pressure",
                {"market_regime": "BEAR", "macro_indicator_status": "RISK"},
                "macro_indicator_pressure",
            ),
            (
                "etf_flow_pressure",
                {"market_regime": "BEAR", "etf_net_3d": -3_500_000_000},
                "etf_flow_pressure",
            ),
            (
                "north_stock_outflow",
                {"north_holding_ratio": 1.5, "north_holding_ratio_delta_3d": -0.35, "north_net_buy_amount_3d": -60_000_000},
                "north_stock_weak",
            ),
            (
                "north_stock_underweight",
                {"north_holding_ratio": 0.6, "north_net_buy_amount_3d": 0},
                "north_stock_weak",
            ),
            (
                "institutional_profile_weak",
                {"institutional_status": "RISK"},
                "institutional_profile_weak",
            ),
            (
                "investor_interaction_risk",
                {"investor_interaction_status": "RISK"},
                "investor_interaction_risk",
            ),
            (
                "retail_institution_contrarian_risk",
                {
                    "retail_sentiment_status": "EXTREME_BULLISH",
                    "institutional_status": "RISK",
                    "north_flow_status": "OUTFLOW",
                },
                "retail_institution_contrarian_risk",
            ),
            (
                "theme_continuity_low",
                {"theme_continuity_score_10": 4.8},
                "theme_continuity_low",
            ),
            (
                "business_purity_low",
                {"business_purity_status": "RISK", "business_purity_score": 25.0},
                "business_purity_low",
            ),
            (
                "industry_prosperity_weak",
                {"industry_price_change_30d": -8.0, "capacity_utilization": 0.52},
                "industry_prosperity_weak",
            ),
            (
                "classic_top_breakdown",
                {"classic_pattern_direction": "bearish", "classic_pattern_status": "CONFIRMED"},
                "classic_pattern_risk",
            ),
        ]
        for expected_flag, payload, expected_tag in cases:
            with self.subTest(expected_flag=expected_flag):
                flags = build_rule_flags({**base, **payload})
                self.assertIn(expected_flag, flags)
                status, reason = choose_recommend_status(
                    stock_code="300001",
                    short_name="Alpha",
                    ai_score=82,
                    short_term_score=76,
                    long_term_score=72,
                    event_risk_level="LOW",
                    amount=600_000_000,
                    change_pct=1.5,
                    min_score=60,
                    data_quality_flags=[expected_flag],
                )
                self.assertEqual(status, "SUSPENDED")
                self.assertTrue(reason)
                self.assertIn(expected_tag, build_failure_tags({"data_quality_flags": flags}))

    def test_blowoff_volume_suspends_candidate(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "amount_ratio_20": 3.4,
            "turnover_ratio": 18.0,
            "change_pct": 3.0,
            "pct_5": 18.0,
            "amount": 900_000_000,
            "amount_ma20": 900_000_000,
            "float_market_cap": 8_000_000_000,
        })
        self.assertIn("blowoff_volume_risk", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="Alpha",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=900_000_000,
            change_pct=3.0,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("量能", reason)
        self.assertIn("volume_overheat", build_failure_tags({"data_quality_flags": flags}))

    def test_float_market_cap_low_suspends_candidate(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "float_market_cap": 3_000_000_000,
            "market_cap": 12_000_000_000,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("float_market_cap_low", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="Alpha",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("50亿", reason)
        self.assertIn("float_market_cap_low", build_failure_tags({"data_quality_flags": flags}))

    def test_fundamental_loss_blocks_recommendation(self):
        flags = build_rule_flags({
            "stock_code": "300001",
            "industry_name": "软件服务",
            "valuation_style": "growth",
            "basic_eps": -0.2,
            "net_margin": -6.0,
            "total_rev_yoy_gr": -4.0,
            "net_profit_yoy_gr": -30.0,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("fundamental_loss", flags)

        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=76,
            long_term_score=72,
            event_risk_level="LOW",
            amount=600_000_000,
            change_pct=1.5,
            min_score=60,
            data_quality_flags=flags,
        )
        self.assertEqual(status, "BLOCK")
        self.assertIn("业绩", reason)

    def test_recommend_gate_allows_clean_candidate(self):
        status, _ = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=70,
            short_term_score=68,
            long_term_score=66,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
        )
        self.assertEqual(status, "ALLOW")

    def test_recommend_gate_suspends_missing_core_data(self):
        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="娴嬭瘯鑲′唤",
            ai_score=78,
            short_term_score=72,
            long_term_score=66,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=58,
            data_quality_flags=["missing_flow"],
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertTrue(reason)

    def test_recommend_gate_suspends_stale_flow_when_score_not_strong_enough(self):
        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="娴嬭瘯鑲′唤",
            ai_score=65,
            short_term_score=68,
            long_term_score=66,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=88,
            data_quality_flags=["stale_flow"],
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertTrue(reason)

    def test_primary_strategy_prefers_highest_qualified_score(self):
        strategy = select_primary_strategy({
            "ultra_short_score": 82,
            "short_term_score": 74,
            "swing_score": 66,
        })
        self.assertEqual(strategy, "ultra_short")

    def test_trade_probability_estimate_outputs_upside_and_downside(self):
        probs = estimate_trade_probabilities({
            "final_trade_score": 78.0,
            "risk_reward_ratio": 3.6,
            "data_quality_score": 90.0,
            "volatility_20": 4.2,
            "position_risk_level": "LOW",
            "event_risk_level": "LOW",
            "recommend_status": "ALLOW",
        })

        self.assertGreater(probs["upside_probability_pct"], probs["downside_probability_pct"])
        self.assertEqual(probs["probability_model"], "rule_heuristic_v1")
        self.assertAlmostEqual(
            probs["upside_probability_pct"] + probs["downside_probability_pct"] + probs["sideways_probability_pct"],
            100.0,
            places=1,
        )

    def test_technical_evidence_builds_trend_and_ema_items(self):
        evidence = build_technical_evidence({
            "close": 12.0,
            "ma5": 11.5,
            "ma10": 11.0,
            "ma20": 10.5,
            "ma60": 10.0,
            "ma120": 9.5,
            "ma250": 9.0,
            "ema12": 11.8,
            "ema26": 11.2,
            "deduction_price_20": 10.2,
            "deduction_price_60": 10.4,
            "deduction_date_20": "2026-05-29",
            "deduction_date_60": "2026-03-31",
            "dist_ma20": 14.3,
            "pct_20": 18.0,
            "amount_ratio_20": 1.4,
            "bbi": 11.1,
            "bias6": 4.5,
            "bias12": 7.2,
            "bias24": 9.1,
            "macd_dif": 0.42,
            "macd_dea": 0.31,
            "macd_hist": 0.22,
            "kdj_k": 72.0,
            "kdj_d": 64.0,
            "kdj_j": 88.0,
            "rsi6": 62.0,
            "rsi12": 55.0,
            "rsi24": 52.0,
            "boll_upper": 12.8,
            "boll_mid": 11.4,
            "boll_lower": 10.0,
            "boll_width_pct": 24.5,
            "mtm10_pct": 6.4,
            "lwr9": 16.0,
            "pdi14": 31.0,
            "mdi14": 14.0,
            "adx14": 24.0,
            "volume_profile_peak_price": 11.6,
            "volume_profile_support_price": 11.4,
            "volume_profile_resistance_price": 12.9,
            "volume_profile_peak_density": 0.28,
            "kline_pattern": "bullish_engulfing",
            "kline_pattern_direction": "bullish",
            "kline_pattern_strength": 72.0,
            "classic_pattern": "double_bottom",
            "classic_pattern_direction": "bullish",
            "classic_pattern_status": "CONFIRMED",
            "classic_pattern_strength": 82.0,
            "classic_pattern_neckline": 12.4,
            "classic_pattern_support": 10.2,
            "classic_pattern_resistance": 12.8,
            "classic_pattern_reason": "double bottom neckline confirmed",
            "kline_pattern_reason": "最新阳线实体吞没前一日阴线实体",
        })

        self.assertEqual(evidence["direction"], "bullish")
        self.assertEqual(evidence["ema_sma_divergence"]["status"], "SYNC_BULLISH")
        ma_rows = {item["name"]: item for item in evidence["moving_average_table"]}
        self.assertIn("EMA12", ma_rows)
        self.assertIn("SMA20", ma_rows)
        self.assertIn("SMA60", ma_rows)
        self.assertEqual(ma_rows["SMA20"]["direction"], "向上")
        self.assertEqual(ma_rows["SMA20"]["price_position"], "上方")
        self.assertIn("④", ma_rows["SMA20"]["stage"])
        projection = {item["period"]: item for item in evidence["deduction_projection"]}
        self.assertEqual(projection[20]["deduction_date"], "2026-05-29")
        self.assertAlmostEqual(projection[20]["diff"], 1.8)
        self.assertIn("未来3日", projection[20]["forecast_3d"])
        kinds = {item["kind"] for item in evidence["items"]}
        self.assertIn("trend_clock", kinds)
        self.assertIn("moving_average_table", kinds)
        self.assertIn("ema_momentum", kinds)
        self.assertIn("bbi", kinds)
        self.assertIn("bias", kinds)
        self.assertIn("macd", kinds)
        self.assertIn("kdj", kinds)
        self.assertIn("rsi", kinds)
        self.assertIn("boll", kinds)
        self.assertIn("kline_pattern", kinds)
        self.assertIn("classic_pattern", kinds)
        self.assertIn("mtm", kinds)
        self.assertIn("lwr", kinds)
        self.assertIn("dmi", kinds)
        self.assertIn("volume_profile", kinds)

    def test_volume_profile_levels_find_amount_weighted_zones(self):
        rows = []
        for idx in range(20):
            price = 10.0 + (idx % 3) * 0.05
            rows.append({
                "trade_date": f"2026-05-{idx + 1:02d}",
                "high": price + 0.08,
                "low": price - 0.08,
                "close": price,
                "amount": 300_000_000,
            })
        for idx in range(8):
            price = 12.8 + (idx % 2) * 0.04
            rows.append({
                "trade_date": f"2026-06-{idx + 1:02d}",
                "high": price + 0.08,
                "low": price - 0.08,
                "close": price,
                "amount": 180_000_000,
            })
        rows.append({
            "trade_date": "2026-06-30",
            "high": 12.15,
            "low": 11.95,
            "close": 12.0,
            "amount": 80_000_000,
        })

        levels = estimate_volume_profile_levels(rows, bins=8)

        self.assertLess(levels["volume_profile_support_price"], 12.0)
        self.assertGreater(levels["volume_profile_resistance_price"], 12.0)
        self.assertGreater(levels["volume_profile_peak_density"], 0)

    def test_market_breadth_detects_extreme_width(self):
        hot = pd.DataFrame({
            "close": [12, 11, 10, 9],
            "ma20": [10, 10, 9, 8],
            "change_pct": [2.0, 1.0, 9.8, -1.0],
        })
        cold = pd.DataFrame({
            "close": [8, 9, 7, 6],
            "ma20": [10, 10, 8, 7],
            "change_pct": [-2.0, -1.0, -9.8, 0.5],
        })

        self.assertEqual(compute_market_breadth_features(hot)["market_extreme_status"], "OVERHEAT")
        self.assertEqual(compute_market_breadth_features(cold)["market_extreme_status"], "OVERSOLD")

    def test_detect_kline_pattern_identifies_reversal_shapes(self):
        bullish = detect_kline_pattern([
            {"trade_date": "2026-06-01", "open": 10.2, "high": 10.4, "low": 9.7, "close": 9.8},
            {"trade_date": "2026-06-02", "open": 9.7, "high": 10.6, "low": 9.6, "close": 10.5},
        ])
        bearish = detect_kline_pattern([
            {"trade_date": "2026-06-01", "open": 10.0, "high": 11.3, "low": 9.9, "close": 11.1},
            {"trade_date": "2026-06-02", "open": 11.2, "high": 11.4, "low": 10.9, "close": 11.1},
            {"trade_date": "2026-06-03", "open": 11.0, "high": 11.1, "low": 10.1, "close": 10.3},
        ])

        self.assertEqual(bullish["kline_pattern"], "bullish_engulfing")
        self.assertEqual(bullish["kline_pattern_direction"], "bullish")
        self.assertEqual(bearish["kline_pattern"], "evening_star")
        self.assertEqual(bearish["kline_pattern_direction"], "bearish")

    def test_detect_classic_pattern_identifies_confirmed_double_top(self):
        closes = [
            9.5, 10.0, 10.8, 11.4, 12.0, 11.5, 10.7, 9.8, 9.4,
            10.2, 11.0, 11.7, 12.05, 11.4, 10.4, 9.8, 9.2, 8.8,
            8.7, 8.6,
        ]
        records = []
        for idx, close in enumerate(closes):
            high = close + 0.2
            if idx == 4:
                high = 12.3
            if idx == 12:
                high = 12.2
            records.append({
                "trade_date": f"2026-06-{idx + 1:02d}",
                "high": high,
                "low": close - 0.2,
                "close": close,
            })

        out = detect_classic_top_bottom_structure(records)

        self.assertEqual(out["classic_pattern"], "double_top")
        self.assertEqual(out["classic_pattern_direction"], "bearish")
        self.assertEqual(out["classic_pattern_status"], "CONFIRMED")
        self.assertGreater(out["classic_pattern_strength"], 80.0)
        self.assertIn("classic_pattern_wave_high", out)

    def test_detect_classic_pattern_neutral_reports_current_wave(self):
        records = []
        for idx in range(24):
            close = 10.0 + idx * 0.08
            records.append({
                "trade_date": f"2026-06-{idx + 1:02d}",
                "high": close + 0.12,
                "low": close - 0.10,
                "close": close,
            })

        out = detect_classic_top_bottom_structure(records)

        self.assertEqual(out["classic_pattern"], "none")
        self.assertEqual(out["classic_pattern_wave_direction"], "UP")
        self.assertGreater(out["classic_pattern_wave_pct"], 0)
        self.assertIn("当前无明确顶底结构", out["classic_pattern_reason"])

    def test_rule_flags_detect_chip_unlock_and_market_overheat(self):
        flags = build_rule_flags({
            "stock_code": "000001",
            "close": 12.0,
            "ma5": 11.8,
            "ma10": 11.5,
            "ma20": 10.0,
            "ma60": 9.0,
            "pct_5": 22.0,
            "holder_num_ratio": 12.0,
            "lifting_count_30d": 1,
            "lifting_max_ratio_30d": 12.0,
            "mine_clearance_score": 76,
            "market_extreme_status": "OVERHEAT",
            "dist_ma20": 18.0,
            "kline_pattern_direction": "bearish",
        })

        self.assertIn("weekly_overheat", flags)
        self.assertIn("holder_spread", flags)
        self.assertIn("unlock_risk", flags)
        self.assertIn("mine_clearance_risk", flags)
        self.assertIn("market_extreme_overheat", flags)
        self.assertIn("bearish_kline_pattern", flags)

    def test_recommend_gate_blocks_unlock_and_suspends_weekly_overheat(self):
        blocked, blocked_reason = choose_recommend_status(
            stock_code="000001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=78,
            long_term_score=72,
            event_risk_level="LOW",
            amount=300_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=90,
            data_quality_flags=["unlock_risk"],
        )
        suspended, suspended_reason = choose_recommend_status(
            stock_code="000001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=78,
            long_term_score=72,
            event_risk_level="LOW",
            amount=300_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=90,
            data_quality_flags=["weekly_overheat"],
        )

        self.assertEqual(blocked, "BLOCK")
        self.assertIn("解禁", blocked_reason)
        self.assertEqual(suspended, "SUSPENDED")
        self.assertIn("近一周", suspended_reason)

    def test_recommend_gate_suspends_bearish_kline_pattern(self):
        status, reason = choose_recommend_status(
            stock_code="000001",
            short_name="测试股份",
            ai_score=82,
            short_term_score=78,
            long_term_score=72,
            event_risk_level="LOW",
            amount=300_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=90,
            data_quality_flags=["bearish_kline_pattern"],
        )

        self.assertEqual(status, "SUSPENDED")
        self.assertIn("K线", reason)

    def test_price_crosscheck_flags_mismatch_and_single_source(self):
        df = pd.DataFrame([
            {"stock_code": "000001", "close": 10.0, "current_price": 10.05, "current_snapshot_at": "2026-06-30 15:00:00"},
            {"stock_code": "000002", "close": 10.0, "current_price": 10.8, "current_snapshot_at": "2026-06-30 15:00:00"},
            {"stock_code": "000003", "close": 10.0, "snapshot_price": 10.0, "snapshot_close": 10.0, "snapshot_trade_date": "2026-06-30"},
        ])

        out = attach_price_crosscheck(df, "2026-06-30")
        statuses = dict(zip(out["stock_code"], out["price_check_status"]))

        self.assertEqual(statuses["000001"], "PASS")
        self.assertEqual(statuses["000002"], "FAIL")
        self.assertEqual(statuses["000003"], "SINGLE_SOURCE")

    def test_runtime_price_tolerance_can_be_published_into_crosscheck(self):
        try:
            set_active_runtime_params({"price_crosscheck_tolerance_pct": 2.0})
            df = pd.DataFrame([{
                "stock_code": "000001",
                "close": 10.0,
                "current_price": 10.15,
                "current_snapshot_at": "2026-06-30 15:00:00",
            }])

            out = attach_price_crosscheck(df, "2026-06-30")

            self.assertEqual(runtime_threshold("price_crosscheck_tolerance_pct"), 2.0)
            self.assertEqual(out.iloc[0]["price_check_status"], "PASS")
        finally:
            set_active_runtime_params({})

    def test_chan_structure_detects_center_break_and_signal(self):
        records = [
            {"trade_date": "2026-06-01", "high": 10.0, "low": 9.0, "close": 9.5, "macd_hist": 0.1},
            {"trade_date": "2026-06-02", "high": 12.0, "low": 9.5, "close": 11.5, "macd_hist": 0.3},
            {"trade_date": "2026-06-03", "high": 11.0, "low": 8.0, "close": 8.5, "macd_hist": -0.2},
            {"trade_date": "2026-06-04", "high": 13.0, "low": 9.0, "close": 12.5, "macd_hist": 0.25},
            {"trade_date": "2026-06-05", "high": 12.0, "low": 8.5, "close": 9.0, "macd_hist": -0.15},
            {"trade_date": "2026-06-08", "high": 13.5, "low": 9.2, "close": 13.2, "macd_hist": 0.2},
            {"trade_date": "2026-06-09", "high": 12.8, "low": 9.4, "close": 10.0, "macd_hist": -0.1},
            {"trade_date": "2026-06-10", "high": 14.8, "low": 10.2, "close": 14.2, "macd_hist": 0.18},
            {"trade_date": "2026-06-11", "high": 15.0, "low": 13.5, "close": 14.6, "macd_hist": 0.16},
        ]

        out = build_chan_structure(records)

        self.assertEqual(out["chan_signal"], "third_buy")
        self.assertIn(out["chan_pivot_status"], {"UP_BREAK", "CENTER_FORMED"})
        self.assertIsNotNone(out["chan_support_price"])
        self.assertIsNotNone(out["chan_resistance_price"])
        self.assertIsNotNone(out["chan_invalidation_price"])
        self.assertTrue(out["chan_summary"])

    def test_chan_structure_detects_second_buy_and_invalidation(self):
        records = [
            {"trade_date": "2026-06-01", "high": 10.0, "low": 9.0, "close": 9.5, "macd_hist": 0.1},
            {"trade_date": "2026-06-02", "high": 12.0, "low": 9.5, "close": 11.5, "macd_hist": 0.2},
            {"trade_date": "2026-06-03", "high": 11.0, "low": 8.0, "close": 8.5, "macd_hist": -0.2},
            {"trade_date": "2026-06-04", "high": 13.0, "low": 9.0, "close": 12.5, "macd_hist": 0.25},
            {"trade_date": "2026-06-05", "high": 12.0, "low": 8.5, "close": 9.0, "macd_hist": -0.15},
            {"trade_date": "2026-06-08", "high": 13.5, "low": 9.2, "close": 13.0, "macd_hist": 0.35},
            {"trade_date": "2026-06-09", "high": 12.8, "low": 9.4, "close": 10.0, "macd_hist": -0.1},
            {"trade_date": "2026-06-10", "high": 13.0, "low": 10.0, "close": 10.5, "macd_hist": 0.05},
        ]

        out = build_chan_structure(records)

        self.assertEqual(out["chan_signal"], "second_buy_watch")
        self.assertEqual(out["chan_pivot_status"], "CENTER_FORMED")
        self.assertAlmostEqual(out["chan_invalidation_price"], out["chan_support_price"])
        self.assertIn("失效", out["chan_summary"])

    def _minute_rows(self, count=240):
        start = datetime(2026, 6, 30, 9, 30)
        rows = []
        for idx in range(count):
            ts = start + timedelta(minutes=idx)
            price = 10.0 + math.sin(idx / 8.0) * 0.35 + idx * 0.004
            rows.append({
                "trade_date": "2026-06-30",
                "trade_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "close": price,
                "high": price + 0.05,
                "low": price - 0.05,
            })
        return rows

    def test_minute_chan_structure_aggregates_1m_rows(self):
        out = build_minute_chan_structure(self._minute_rows(), frame_minutes=30)

        self.assertEqual(out["frame"], "30m")
        self.assertEqual(out["bar_count"], 8)
        self.assertIn("chan_signal", out)

    def test_classify_intraday_behavior_detects_accumulation(self):
        rows = []
        start = datetime(2026, 6, 30, 9, 30)
        for idx in range(120):
            ts = start + timedelta(minutes=idx)
            price = 10.0 - idx * 0.001
            rows.append({
                "trade_date": "2026-06-30",
                "trade_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "close": price,
                "high": price + 0.02,
                "low": price - 0.02,
            })
        start_pm = datetime(2026, 6, 30, 13, 0)
        for idx in range(120):
            ts = start_pm + timedelta(minutes=idx)
            price = 9.9 + idx * 0.012
            rows.append({
                "trade_date": "2026-06-30",
                "trade_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "close": price,
                "high": price + 0.02,
                "low": price - 0.02,
            })

        out = classify_intraday_behavior(rows)

        self.assertEqual(out["pattern"], "accumulation")
        self.assertEqual(out["direction"], "bullish")

    def test_enrich_recommendation_minute_chan_appends_evidence_chain(self):
        rows = [{
            "stock_code": "000001",
            "technical_evidence_json": "{\"items\": []}",
            "evidence_chain_json": "[]",
        }]

        with patch("biz.analysis.sync_analysis_fast.get_stock_minute_prices", return_value=self._minute_rows(300)), \
             patch("biz.analysis.sync_analysis_fast.minute_source_info", return_value={"table": "sm_stock_minute", "kind": "legacy"}):
            out = enrich_recommendation_minute_chan(rows, "2026-06-30")

        chain = json.loads(out[0]["evidence_chain_json"])
        modules = {item["module"] for item in chain}
        self.assertIn("minute_chan_30m", modules)
        self.assertIn("minute_chan_60m", modules)
        self.assertIn("intraday_behavior", modules)

    def test_event_impact_classifies_benefit_and_damage(self):
        impact = build_event_impact({
            "stock_code": "000001",
            "short_name": "测试股份",
            "industry_name": "软件服务",
            "positive_titles": ["关于签订重大合同的公告"],
            "risk_titles": ["关于股东减持计划的公告"],
        })

        self.assertEqual(impact["beneficiaries"][0]["target"], "测试股份")
        self.assertEqual(impact["damaged"][0]["target"], "测试股份")
        self.assertTrue(impact["alternative_scope"])
        self.assertEqual(impact["event_fulfillment_status"], "RISK_EVENT")

    def test_event_fulfillment_marks_priced_in_positive_news(self):
        fulfillment = classify_event_fulfillment({
            "positive_titles": ["关于签订重大合同的公告"],
            "risk_titles": [],
            "pct_5": 16.0,
            "change_pct": 4.0,
            "dist_ma20": 14.0,
        })
        self.assertEqual(fulfillment["event_fulfillment_status"], "PRICED_IN")

        flags = build_rule_flags({
            "stock_code": "300001",
            "positive_titles": ["关于签订重大合同的公告"],
            "pct_5": 16.0,
            "change_pct": 4.0,
            "dist_ma20": 14.0,
            "amount": 600_000_000,
            "amount_ma20": 600_000_000,
            "turnover_ratio": 8.0,
        })
        self.assertIn("positive_event_priced_in", flags)
        self.assertIn("event_priced_in", build_failure_tags({"data_quality_flags": flags}))

    def test_event_impact_applies_industry_relation_rules(self):
        impact = build_event_impact({
            "stock_code": "000001",
            "short_name": "Alpha",
            "industry_name": "software",
            "positive_titles": ["AI order signed"],
            "risk_titles": [],
            "event_relation_rules": [{
                "trigger_keyword": "AI",
                "source_scope": "industry",
                "source_key": "software",
                "target_name": "AI chip suppliers",
                "impact_type": "beneficiary",
                "reason": "upstream demand may expand",
            }],
        })

        self.assertIn("AI chip suppliers", {item["target"] for item in impact["beneficiaries"]})
        self.assertEqual(impact["confidence"], "rules_with_relation_graph")

    def test_evidence_chain_includes_sector_and_trade_plan(self):
        chain = build_evidence_chain({
            "trade_date": "2026-06-30",
            "data_quality_score": 88,
            "final_trade_score": 78.0,
            "flow_trade_date": "2026-06-30",
            "hot_trade_date": "2026-06-30",
            "sector_gate_status": "PASS",
            "sector_gate_reason": "3日主力净流入6.00亿，宽度55.0%，轮动70.0",
            "sector_rotation_score": 70.0,
            "sector_width_pct": 55.0,
            "sector_flow_3d": 600_000_000,
            "theme_continuity_score_10": 8.1,
            "theme_continuity_level": "HIGH",
            "theme_continuity_reason": "题材延续性8.1/10",
            "sector_leadership_score": 91.0,
            "sector_leadership_tier": "leader",
            "sector_amount_rank": 1,
            "capital_score": 75,
            "main_net_inflow": 120_000_000,
            "main_outflow_days_3d": 0,
            "roe_non_gaap_wtd": 12.0,
            "roa_wtd": 4.0,
            "roic": 16.0,
            "quick_ratio": 1.1,
            "total_rev_qoq_gr": 8.0,
            "net_profit_qoq_gr": 12.0,
            "acct_recv_to_rev": 18.0,
            "prepayment_yoy_gr": 8.0,
            "related_transaction_to_rev": 6.0,
            "ps_ratio": 12.5,
            "industry_ps_median": 5.0,
            "ps_industry_multiple": 2.5,
            "valuation_history_percentile_250d": 88.0,
            "pe_percentile_250d": 88.0,
            "pb_percentile_250d": 88.0,
            "close_history_count": 250,
            "margin_balance_delta_3d": 60_000_000,
            "financing_buy_amount_3d": 90_000_000,
            "margin_expanding_days_3d": 2,
            "pledge_ratio": 18.0,
            "lifting_count_30d": 1,
            "lifting_amount_30d": 80_000_000,
            "lifting_max_ratio_30d": 3.0,
            "reduction_count_90d": 1,
            "reduction_max_ratio_90d": 1.2,
            "reduction_amount_90d": 60_000_000,
            "effective_market_cap": 10_000_000_000,
            "goodwill": 1_200_000_000,
            "net_assets": 5_000_000_000,
            "goodwill_to_net_asset_pct": 24.0,
            "bid5_amount": 120_000_000,
            "ask5_amount": 110_000_000,
            "order_book_depth_amount": 230_000_000,
            "bid_ask_imbalance": 1.09,
            "order_book_status": "PASS",
            "macro_indicator_status": "RISK",
            "macro_indicator_score": 38.0,
            "macro_indicator_risk_count": 2.0,
            "macro_indicator_support_count": 0.0,
            "macro_indicator_latest_name": "PMI",
            "macro_indicator_latest_period": "2026-06-30",
            "macro_indicator_reason": "macro hard data support=0, risk=2",
            "macro_cycle": "RECESSION",
            "macro_cycle_reason": "cycle=RECESSION",
            "etf_flow_status": "OUTFLOW",
            "etf_flow_trade_date": "2026-06-30",
            "etf_net_1d": -1_000_000_000,
            "etf_net_3d": -3_300_000_000,
            "etf_net_5d": -4_000_000_000,
            "etf_flow_score": 30.0,
            "etf_flow_reason": "ETF flow 3d=-33.00e8",
            "retail_sentiment_status": "EXTREME_BULLISH",
            "retail_sentiment_trade_date": "2026-06-30",
            "retail_bullish_pct": 82.0,
            "retail_bearish_pct": 12.0,
            "retail_sentiment_sample_size": 1200,
            "retail_sentiment_score": 36.0,
            "retail_sentiment_reason": "retail bullish=82.0%, bearish=12.0%",
            "north_stock_status": "RISK",
            "north_stock_trade_date": "2026-06-30",
            "north_holding_ratio": 0.8,
            "north_holding_ratio_delta_3d": -0.4,
            "north_holding_ratio_delta_5d": -0.6,
            "north_holding_market_value": 100_000_000,
            "north_net_buy_amount_3d": -80_000_000,
            "north_net_buy_amount_5d": -120_000_000,
            "institutional_status": "RISK",
            "institutional_score": 30.0,
            "institutional_reason": "rating_down=2",
            "fund_hold_ratio": 1.5,
            "qfii_hold_ratio": 0.2,
            "rqfii_hold_ratio": 0.1,
            "social_security_hold_ratio": 0.3,
            "private_fund_hold_ratio": 0.4,
            "institution_hold_ratio": 2.0,
            "rating_upgrade_count_90d": 0,
            "rating_downgrade_count_90d": 2,
            "target_price": 10.0,
            "target_price_upside_pct": -8.0,
            "survey_count_90d": 0,
            "broker_gold_count_90d": 1,
            "investor_interaction_status": "RISK",
            "investor_interaction_score": 32.0,
            "investor_interaction_reason": "interactions=4, support=0, risk=3",
            "investor_interaction_count_180d": 4,
            "investor_interaction_support_count": 0,
            "investor_interaction_risk_count": 3,
            "latest_investor_interaction_date": "2026-06-30",
            "latest_investor_interaction": "delay and margin pressure",
            "business_purity_status": "RISK",
            "business_purity_score": 25.0,
            "business_purity_match_count": 0.0,
            "business_purity_reason": "business matches=0, weak_terms=3",
            "industry_prosperity_status": "RISK",
            "industry_prosperity_score": 28.0,
            "industry_prosperity_reason": "price30d=-8.0%, utilization=52.0%, contract/revenue=0.0%",
            "industry_price_change_30d": -8.0,
            "capacity_utilization": 0.52,
            "order_contract_amount_180d": 0,
            "order_contract_to_revenue_pct": 0.0,
            "event_risk_level": "LOW",
            "notice_negative": 0,
            "notice_critical": 0,
            "close": 12.0,
            "ma5": 11.5,
            "ma10": 11.0,
            "ma20": 10.5,
            "entry_price_low": 11.8,
            "entry_price_high": 12.1,
            "stop_loss_price": 11.3,
            "risk_reward_ratio": 3.2,
            "position_risk_level": "LOW",
            "volatility_20": 4.0,
            "signal_status": "CONFIRM",
        }, trade_date="2026-06-30")

        modules = {item["module"] for item in chain}
        self.assertIn("sector", modules)
        self.assertIn("trade_plan", modules)
        self.assertIn("price_crosscheck", modules)
        self.assertIn("chan", modules)
        self.assertIn("market_breadth", modules)
        self.assertIn("market_style", modules)
        self.assertIn("macro_policy", modules)
        self.assertIn("macro_indicator", modules)
        self.assertIn("relative_strength", modules)
        self.assertIn("north_flow", modules)
        self.assertIn("etf_flow", modules)
        self.assertIn("retail_sentiment", modules)
        self.assertIn("north_stock", modules)
        self.assertIn("research_theme", modules)
        self.assertIn("institutional_profile", modules)
        self.assertIn("investor_interaction", modules)
        self.assertIn("business_purity", modules)
        self.assertIn("industry_prosperity", modules)
        self.assertIn("volume_temperature", modules)
        self.assertIn("liquidity", modules)
        self.assertIn("size_liquidity", modules)
        self.assertIn("chip_capital", modules)
        self.assertIn("valuation", modules)
        self.assertIn("fundamental_quality", modules)
        self.assertIn("dividend", modules)
        self.assertIn("probability", modules)
        sector = next(item for item in chain if item["module"] == "sector")
        self.assertEqual(sector["value"]["leadership_tier"], "leader")
        self.assertEqual(sector["value"]["leadership_score"], 91.0)
        self.assertEqual(sector["value"]["theme_continuity_score_10"], 8.1)
        self.assertEqual(sector["value"]["theme_continuity_level"], "HIGH")
        valuation = next(item for item in chain if item["module"] == "valuation")
        self.assertEqual(valuation["value"]["ps_ratio"], 12.5)
        self.assertEqual(valuation["value"]["industry_ps_median"], 5.0)
        self.assertEqual(valuation["value"]["ps_industry_multiple"], 2.5)
        self.assertEqual(valuation["value"]["valuation_history_percentile_250d"], 88.0)
        self.assertEqual(valuation["value"]["close_history_count"], 250)
        liquidity = next(item for item in chain if item["module"] == "liquidity")
        self.assertEqual(liquidity["value"]["order_book_depth_amount"], 230_000_000)
        self.assertEqual(liquidity["value"]["bid_ask_imbalance"], 1.09)
        macro_indicator = next(item for item in chain if item["module"] == "macro_indicator")
        self.assertEqual(macro_indicator["status"], "RISK")
        self.assertEqual(macro_indicator["value"]["risk_count"], 2.0)
        self.assertEqual(macro_indicator["value"]["macro_cycle"], "RECESSION")
        etf_flow = next(item for item in chain if item["module"] == "etf_flow")
        self.assertEqual(etf_flow["status"], "OUTFLOW")
        self.assertEqual(etf_flow["value"]["net_3d"], -3_300_000_000)
        retail_sentiment = next(item for item in chain if item["module"] == "retail_sentiment")
        self.assertEqual(retail_sentiment["status"], "EXTREME_BULLISH")
        self.assertEqual(retail_sentiment["value"]["bullish_pct"], 82.0)
        probability = next(item for item in chain if item["module"] == "probability")
        self.assertIn("upside_probability_pct", probability["value"])
        self.assertIn("downside_probability_pct", probability["value"])
        north_stock = next(item for item in chain if item["module"] == "north_stock")
        self.assertEqual(north_stock["status"], "RISK")
        self.assertEqual(north_stock["value"]["holding_ratio_delta_3d"], -0.4)
        institutional = next(item for item in chain if item["module"] == "institutional_profile")
        self.assertEqual(institutional["status"], "RISK")
        self.assertEqual(institutional["value"]["rating_downgrade_count_90d"], 2.0)
        self.assertEqual(institutional["value"]["rqfii_hold_ratio"], 0.1)
        self.assertEqual(institutional["value"]["broker_gold_count_90d"], 1.0)
        interaction = next(item for item in chain if item["module"] == "investor_interaction")
        self.assertEqual(interaction["status"], "RISK")
        self.assertEqual(interaction["value"]["risk_count"], 3.0)
        business = next(item for item in chain if item["module"] == "business_purity")
        self.assertEqual(business["status"], "RISK")
        self.assertEqual(business["value"]["score"], 25.0)
        prosperity = next(item for item in chain if item["module"] == "industry_prosperity")
        self.assertEqual(prosperity["status"], "RISK")
        self.assertEqual(prosperity["value"]["price_change_30d"], -8.0)
        fundamental = next(item for item in chain if item["module"] == "fundamental_quality")
        self.assertEqual(fundamental["value"]["quick_ratio"], 1.1)
        self.assertEqual(fundamental["value"]["net_profit_qoq_gr"], 12.0)
        self.assertEqual(fundamental["value"]["roic"], 16.0)
        self.assertEqual(fundamental["value"]["acct_recv_to_rev"], 18.0)
        chip = next(item for item in chain if item["module"] == "chip_capital")
        self.assertEqual(chip["value"]["margin_balance_delta_3d"], 60_000_000)
        self.assertEqual(chip["value"]["financing_buy_amount_3d"], 90_000_000)
        self.assertEqual(chip["value"]["pledge_ratio"], 18.0)
        self.assertEqual(chip["value"]["reduction_max_ratio_90d"], 1.2)
        self.assertEqual(chip["value"]["unlock_status"], "WATCH")
        self.assertEqual(chip["value"]["lifting_max_ratio_30d"], 3.0)
        self.assertEqual(fundamental["value"]["goodwill_to_net_asset_pct"], 24.0)

    def test_trade_plan_outputs_price_band_and_risk_levels(self):
        plan = build_strategy_trade_plan({
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.6,
            "ma60": 9.3,
            "amount": 120_000_000,
            "change_pct": 2.0,
            "market_mood_score": 55,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 80,
            "entry_score": 72,
            "final_trade_score": 77.6,
            "expected_return_pct": 14,
            "heat_overload_score": 70,
            "confidence_score": 80,
            "failure_penalty_score": 100,
            "ultra_short_score": 80,
            "short_term_score": 72,
            "swing_score": 68,
        }, "ultra_short")

        self.assertEqual(plan["signal_status"], "CONFIRM")
        self.assertLess(plan["entry_price_low"], plan["entry_price_high"])
        self.assertLess(plan["stop_loss_price"], plan["entry_price_low"])
        self.assertGreater(plan["take_profit_1"], plan["entry_price_high"])
        self.assertGreater(plan["take_profit_2"], plan["take_profit_1"])
        self.assertGreaterEqual(plan["risk_reward_ratio"], 3.0)
        self.assertEqual(plan["position_risk_level"], "LOW")
        self.assertEqual(plan["position_cap_pct"], 12.0)

    def test_trade_plan_keeps_stop_and_targets_outside_a_wide_entry_band(self):
        plan = build_strategy_trade_plan({
            "close": 6.0,
            "ma5": 5.8,
            "ma10": 5.7,
            "ma20": 5.0,
            "ma60": 4.8,
            "amount": 120_000_000,
            "change_pct": 1.0,
            "market_mood_score": 55,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 80,
            "entry_score": 72,
            "final_trade_score": 77.6,
            "expected_return_pct": 14,
            "heat_overload_score": 70,
            "confidence_score": 80,
            "failure_penalty_score": 100,
            "swing_score": 80,
        }, "swing")

        self.assertLess(plan["stop_loss_price"], plan["entry_price_low"])
        self.assertGreater(plan["take_profit_1"], plan["entry_price_high"])
        self.assertGreater(plan["take_profit_2"], plan["take_profit_1"])

    def test_trade_plan_blocks_small_expected_return(self):
        plan = build_strategy_trade_plan({
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.6,
            "ma60": 9.3,
            "amount": 120_000_000,
            "change_pct": 1.0,
            "market_mood_score": 55,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 90,
            "entry_score": 80,
            "final_trade_score": 87,
            "expected_return_pct": 3.5,
            "heat_overload_score": 70,
            "confidence_score": 80,
            "failure_penalty_score": 100,
            "ultra_short_score": 82,
        }, "ultra_short")

        self.assertEqual(plan["signal_status"], "BLOCK")
        self.assertEqual(plan["position_weight"], 0.0)

    def test_trade_plan_caps_high_risk_position(self):
        plan = build_strategy_trade_plan({
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.6,
            "ma60": 9.3,
            "amount": 220_000_000,
            "change_pct": 1.0,
            "market_mood_score": 70,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "data_quality_flags": ["margin_deleveraging_3d"],
            "quality_score": 92,
            "entry_score": 82,
            "final_trade_score": 96,
            "expected_return_pct": 32.0,
            "heat_overload_score": 70,
            "confidence_score": 88,
            "failure_penalty_score": 100,
            "swing_score": 96,
        }, "swing")

        self.assertEqual(derive_position_risk_level({"data_quality_flags": ["margin_deleveraging_3d"]}, "CONFIRM"), "HIGH")
        self.assertEqual(plan["signal_status"], "CONFIRM")
        self.assertEqual(plan["position_risk_level"], "HIGH")
        self.assertEqual(plan["position_cap_pct"], 10.0)
        self.assertLessEqual(plan["position_weight"], 10.0)
        self.assertTrue(any("single-stock cap <= 10.0%" in item for item in json.loads(plan["sell_rules_json"])))

    def test_trade_plan_blocks_low_risk_reward(self):
        plan = build_strategy_trade_plan({
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.6,
            "ma60": 9.3,
            "amount": 120_000_000,
            "change_pct": 1.0,
            "market_mood_score": 55,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 90,
            "entry_score": 80,
            "final_trade_score": 87,
            "expected_return_pct": 6.0,
            "heat_overload_score": 70,
            "confidence_score": 80,
            "failure_penalty_score": 100,
            "ultra_short_score": 82,
        }, "ultra_short")

        self.assertEqual(plan["signal_status"], "BLOCK")
        self.assertLess(plan["risk_reward_ratio"], 3.0)
        self.assertIn("risk/reward", plan["signal_reason"])

    def test_trade_plan_blocks_failed_sector_gate(self):
        plan = build_strategy_trade_plan({
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.6,
            "ma60": 9.3,
            "amount": 120_000_000,
            "change_pct": 1.0,
            "market_mood_score": 55,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "sector_gate_status": "BLOCK",
            "quality_score": 90,
            "entry_score": 80,
            "final_trade_score": 87,
            "expected_return_pct": 14.0,
            "heat_overload_score": 70,
            "confidence_score": 80,
            "failure_penalty_score": 100,
            "ultra_short_score": 82,
        }, "ultra_short")

        self.assertEqual(plan["signal_status"], "BLOCK")
        self.assertIn("sector gate", plan["signal_reason"])

    def test_investment_rating_uses_stock_txt_five_level_vocab(self):
        buy_rating, buy_reason = derive_investment_rating({
            "signal_status": "CONFIRM",
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "final_trade_score": 82,
            "entry_score": 70,
            "expected_return_pct": 14,
            "risk_reward_ratio": 3.4,
        })
        sell_rating, sell_reason = derive_investment_rating({
            "signal_status": "BLOCK",
            "recommend_status": "BLOCK",
            "event_risk_level": "CRITICAL",
            "final_trade_score": 30,
        })

        self.assertEqual(buy_rating, "买入")
        self.assertIn("跑赢", buy_reason)
        self.assertEqual(sell_rating, "卖出")
        self.assertIn("回避", sell_reason)

    def test_main_wave_buy_ready_ignores_fixed_expected_return_cap(self):
        row = {
            "close": 20.0,
            "ma5": 19.5,
            "ma10": 18.8,
            "ma20": 17.2,
            "ma60": 16.5,
            "amount": 300_000_000,
            "change_pct": 4.0,
            "market_mood_score": 60,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 66,
            "entry_score": 62,
            "final_trade_score": 65,
            "expected_return_pct": 1.0,
            "heat_overload_score": 70,
            "confidence_score": 70,
            "failure_penalty_score": 100,
            "main_wave_score": 78,
            "trend_hold_score": 72,
            "main_wave_signal": "BUY_READY",
            "main_wave_reason": "主升浪放量突破",
            "trend_stop_price": 16.68,
        }

        plan = build_strategy_trade_plan(row, "main_wave")

        self.assertEqual(plan["signal_status"], "BUY_READY")
        self.assertIn("main-wave", plan["signal_reason"])

    def test_main_wave_extended_move_triggers_sell_alert(self):
        df = pd.DataFrame([{
            "stock_code": "603629",
            "ai_score": 80,
            "long_term_score": 70,
            "short_term_score": 82,
            "technical_score": 90,
            "capital_score": 88,
            "sentiment_score": 70,
            "event_score": 80,
            "risk_score": 75,
            "close": 211.94,
            "ma5": 205,
            "ma10": 183.26,
            "ma20": 162.51,
            "ma60": 95,
            "high_20": 216.88,
            "high_60": 216.88,
            "low_60": 51.3,
            "amount": 1_000_000_000,
            "amount_ma5": 850_000_000,
            "amount_ma20": 830_000_000,
            "change_pct": 4.92,
            "turnover_ratio": 8.0,
            "volatility_20": 6.0,
            "pct_20": 120,
            "dist_ma20": 30.4,
            "from_low_60": 313.1,
            "amount_ratio_5": 1.18,
            "amount_ratio_20": 1.2,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "market_mood_score": 70,
        }])

        out = add_strategy_signals(df)
        row = out.iloc[0]

        self.assertGreaterEqual(row["main_wave_score"], 70)
        self.assertEqual(row["main_wave_signal"], "REDUCE")
        self.assertIn("高位扩张", row["main_wave_reason"])

    def test_recommendation_rows_exclude_sell_alerts(self):
        df = pd.DataFrame([
            {
                "stock_code": "600001",
                "short_name": "高位风险",
                "ai_score": 90,
                "long_term_score": 82,
                "short_term_score": 88,
                "quality_score": 90,
                "final_trade_score": 92,
                "entry_score": 80,
                "capital_score": 82,
                "main_wave_score": 96,
                "trend_hold_score": 30,
                "main_wave_signal": "SELL_ALERT",
                "main_wave_reason": "高位跌破MA10",
                "signal_status": "SELL_ALERT",
                "signal_reason": "高位跌破MA10",
                "recommend_status": "ALLOW",
                "recommend_reason": "基础评分通过",
                "event_risk_level": "LOW",
            },
            {
                "stock_code": "600002",
                "short_name": "低位观察",
                "ai_score": 66,
                "long_term_score": 70,
                "short_term_score": 65,
                "quality_score": 66,
                "final_trade_score": 68,
                "entry_score": 58,
                "capital_score": 68,
                "main_wave_score": 72,
                "trend_hold_score": 62,
                "main_wave_signal": "WATCH",
                "main_wave_reason": "等待突破确认",
                "signal_status": "WATCH",
                "signal_reason": "等待突破确认",
                "recommend_status": "ALLOW",
                "recommend_reason": "基础评分通过",
                "event_risk_level": "LOW",
            },
            {
                "stock_code": "688001",
                "short_name": "STAR",
                "ai_score": 99,
                "long_term_score": 90,
                "short_term_score": 90,
                "quality_score": 99,
                "final_trade_score": 99,
                "entry_score": 90,
                "capital_score": 90,
                "main_wave_score": 90,
                "trend_hold_score": 90,
                "main_wave_signal": "WATCH",
                "main_wave_reason": "watch",
                "signal_status": "WATCH",
                "signal_reason": "watch",
                "recommend_status": "ALLOW",
                "recommend_reason": "clean",
                "event_risk_level": "LOW",
            },
            {
                "stock_code": "600003",
                "short_name": "弱板块",
                "ai_score": 98,
                "long_term_score": 90,
                "short_term_score": 90,
                "quality_score": 98,
                "final_trade_score": 98,
                "entry_score": 90,
                "capital_score": 90,
                "main_wave_score": 88,
                "trend_hold_score": 80,
                "main_wave_signal": "WATCH",
                "main_wave_reason": "watch",
                "signal_status": "WATCH",
                "signal_reason": "watch",
                "recommend_status": "ALLOW",
                "recommend_reason": "clean",
                "event_risk_level": "LOW",
                "sector_gate_status": "BLOCK",
            },
        ])
        df["chase_risk_status"] = "ALLOW"
        df["ordinary_buy_eligible"] = True

        rows = build_recommendation_rows(df, "2026-06-30", top_n=80, min_score=62)

        self.assertEqual([row["stock_code"] for row in rows], ["600002"])
        self.assertEqual(rows[0]["signal_status"], "WATCH")

    def test_chase_risk_gate_blocks_nine_surge_sessions_followed_by_zero_capacity(self):
        trade_dates = [
            "2026-07-21", "2026-07-22", "2026-07-23", "2026-07-24",
            "2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30",
            "2026-07-31",
        ]
        rows = []
        previous = 9.57
        for trade_day in trade_dates:
            close = round(previous * 1.10, 2)
            rows.append({
                "stock_code": "603221",
                "trade_date": trade_day,
                "open": previous,
                "high": close,
                "low": previous,
                "close": close,
                "volume": 1_000_000,
                "amount": 100_000_000,
                "change_pct": (close / previous - 1.0) * 100.0,
                "pre_close": previous,
                "received_at": f"{trade_day} 16:00:00",
            })
            previous = close
        rows.extend([
            {
                "stock_code": "603221", "trade_date": "2026-08-03",
                "open": previous, "high": previous, "low": previous,
                "close": previous, "volume": 0, "amount": 0,
                "change_pct": 0, "pre_close": 0,
                "received_at": "2026-08-03 16:00:00",
            },
            {
                "stock_code": "603221", "trade_date": "2026-08-04",
                "open": previous, "high": previous, "low": previous,
                "close": previous, "volume": 0, "amount": 0,
                "change_pct": 0, "pre_close": 0,
                "received_at": "2026-08-04 16:00:00",
            },
        ])

        risk = _build_chase_risk_features_from_rows(pd.DataFrame(rows), "2026-08-04").iloc[0]

        self.assertEqual(risk["surge_streak_lower_bound"], 9)
        self.assertTrue(pd.isna(risk["exact_limit_up_streak"]))
        self.assertEqual(risk["limit_rule_status"], "PROXY_ONLY")
        self.assertEqual(risk["trailing_untradeable_sessions"], 2)
        self.assertEqual(risk["chase_risk_status"], "EXECUTION_BLOCKED")
        self.assertFalse(risk["ordinary_buy_eligible"])

        future = rows + [{
            "stock_code": "603221", "trade_date": "2026-08-05",
            "open": previous, "high": round(previous * 1.10, 2),
            "low": previous, "close": round(previous * 1.10, 2),
            "volume": 1_000_000, "amount": 100_000_000,
            "change_pct": 10.0, "pre_close": previous,
            "received_at": "2026-08-05 16:00:00",
        }]
        earlier_cutoff = _build_chase_risk_features_from_rows(
            pd.DataFrame(future), "2026-08-04"
        ).iloc[0]
        self.assertEqual(earlier_cutoff["surge_streak_lower_bound"], 9)
        self.assertEqual(earlier_cutoff["chase_risk_status"], "EXECUTION_BLOCKED")

    def test_nine_surge_sessions_then_one_percent_remains_in_cooldown_watch(self):
        rows = _chase_path(9, [1.0])
        cutoff = f"{rows[-1]['trade_date']}T17:00:00+08:00"

        risk = _build_chase_risk_features_from_rows(rows, cutoff).iloc[0]

        self.assertEqual(risk["surge_streak_lower_bound"], 0)
        self.assertEqual(risk["recent_max_surge_streak"], 9)
        self.assertEqual(risk["latest_danger_surge_streak"], 9)
        self.assertEqual(risk["sessions_since_extreme_surge"], 1)
        self.assertFalse(risk["rebase_confirmed"])
        self.assertEqual(risk["chase_risk_status"], "WATCH")
        self.assertFalse(risk["ordinary_buy_eligible"])

    def test_extreme_episode_allows_only_after_cooldown_and_pullback_rebase(self):
        rows = _chase_path(9, [-3.0, -3.0, -3.0, -3.0, -3.0])
        cutoff = f"{rows[-1]['trade_date']}T17:00:00+08:00"

        risk = _build_chase_risk_features_from_rows(rows, cutoff).iloc[0]

        self.assertEqual(risk["latest_danger_surge_streak"], 9)
        self.assertEqual(risk["sessions_since_extreme_surge"], 5)
        self.assertLessEqual(risk["drawdown_from_recent_peak_pct"], -10.0)
        self.assertTrue(risk["rebase_confirmed"])
        self.assertEqual(risk["chase_risk_status"], "ALLOW")
        self.assertTrue(risk["ordinary_buy_eligible"])

    def test_latest_danger_episode_controls_cooldown_not_old_window_maximum(self):
        rows = _chase_path(
            9,
            [-3.0, -3.0, -3.0, -3.0, -3.0, 10.0, 10.0, 10.0, 1.0],
        )
        cutoff = f"{rows[-1]['trade_date']}T17:00:00+08:00"

        risk = _build_chase_risk_features_from_rows(rows, cutoff).iloc[0]

        self.assertEqual(risk["recent_max_surge_streak"], 9)
        self.assertEqual(risk["latest_danger_surge_streak"], 3)
        self.assertEqual(risk["sessions_since_extreme_surge"], 1)
        self.assertFalse(risk["rebase_confirmed"])
        self.assertEqual(risk["chase_risk_status"], "WATCH")

    def test_future_daily_revision_cannot_change_point_in_time_chase_state(self):
        rows = _chase_path(4)
        cutoff = f"{rows[-1]['trade_date']}T16:00:00+08:00"
        prefix = _build_chase_risk_features_from_rows(rows, cutoff).iloc[0]
        future_revision = dict(rows[-1])
        future_revision.update({
            "close": future_revision["pre_close"] * 1.01,
            "high": future_revision["pre_close"] * 1.02,
            "change_pct": 1.0,
            "received_at": f"{rows[-1]['trade_date']} 18:00:00",
        })

        with_future = _build_chase_risk_features_from_rows(
            rows + [future_revision], cutoff
        ).iloc[0]

        self.assertEqual(prefix["surge_streak_lower_bound"], 4)
        self.assertEqual(with_future["surge_streak_lower_bound"], 4)
        self.assertEqual(with_future["chase_risk_status"], prefix["chase_risk_status"])
        evidence = json.loads(with_future["chase_risk_evidence_json"])
        self.assertEqual(evidence["knowledge_cutoff"], cutoff)

    def test_chase_cutoff_requires_aware_exact_time_and_acquisition_evidence(self):
        rows = _chase_path(1)
        without_evidence = [{
            key: value for key, value in rows[0].items() if key != "received_at"
        }]

        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            _build_chase_risk_features_from_rows(
                rows, f"{rows[-1]['trade_date']} 16:30:00"
            )
        blocked = _build_chase_risk_features_from_rows(
            without_evidence, rows[-1]["trade_date"]
        ).iloc[0]

        self.assertEqual(blocked["chase_risk_status"], "DATA_BLOCKED")
        self.assertIn("acquisition evidence", blocked["chase_risk_reason"])

    def test_recommendation_rows_fail_closed_on_chase_risk(self):
        base = {
            "short_name": "candidate",
            "ai_score": 95,
            "long_term_score": 90,
            "short_term_score": 95,
            "quality_score": 95,
            "final_trade_score": 96,
            "entry_score": 90,
            "capital_score": 90,
            "main_wave_score": 96,
            "main_wave_signal": "BUY_READY",
            "signal_status": "CONFIRM",
            "signal_reason": "score confirmed",
            "recommend_status": "ALLOW",
            "recommend_reason": "quality confirmed",
            "event_risk_level": "LOW",
            "sector_gate_status": "ALLOW",
        }
        blocked = dict(base, stock_code="603221", chase_risk_status="EXECUTION_BLOCKED", ordinary_buy_eligible=False)
        allowed = dict(base, stock_code="600001", chase_risk_status="ALLOW", ordinary_buy_eligible=True)

        rows = build_recommendation_rows(
            pd.DataFrame([blocked, allowed]), "2026-08-04", top_n=80, min_score=62
        )

        self.assertEqual([row["stock_code"] for row in rows], ["600001"])

    def test_recommend_status_fails_closed_when_chase_gate_is_unknown(self):
        common = {
            "stock_code": "600001",
            "short_name": "sample",
            "ai_score": 99,
            "short_term_score": 99,
            "long_term_score": 99,
            "event_risk_level": "LOW",
            "amount": 300_000_000,
            "change_pct": 2,
            "min_score": 62,
        }
        cases = [
            ("UNKNOWN", True),
            (float("nan"), True),
            ("ALLOW", float("nan")),
            ("ALLOW", 1),
        ]

        for chase_status, eligible in cases:
            with self.subTest(chase_status=chase_status, eligible=eligible):
                status, _ = choose_recommend_status(
                    **common,
                    chase_risk_status=chase_status,
                    ordinary_buy_eligible=eligible,
                )
                self.assertEqual(status, "BLOCK")

    def test_chase_coverage_rejects_status_eligibility_mismatch(self):
        from biz.analysis import sync_analysis_fast as module

        valid = pd.DataFrame([{
            "stock_code": "600001",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
            "chase_risk_reason": "verified",
            "chase_risk_evidence_json": "{}",
        }])
        module._assert_chase_risk_coverage(valid, "2026-08-04")

        invalid_rows = [
            {**valid.iloc[0].to_dict(), "ordinary_buy_eligible": 1},
            {
                **valid.iloc[0].to_dict(),
                "chase_risk_status": "DATA_BLOCKED",
                "ordinary_buy_eligible": True,
            },
        ]
        for invalid in invalid_rows:
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(RuntimeError, "coverage is incomplete"):
                    module._assert_chase_risk_coverage(
                        pd.DataFrame([invalid]), "2026-08-04"
                    )

    def test_trade_plan_applies_chase_gate_before_high_alpha_score(self):
        plan = build_strategy_trade_plan({
            "stock_code": "603221",
            "trade_date": "2026-08-04",
            "close": 22.54,
            "ma5": 21.0,
            "ma10": 19.0,
            "ma20": 17.0,
            "amount": 0,
            "change_pct": 0,
            "market_mood_score": 80,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 99,
            "entry_score": 99,
            "final_trade_score": 99,
            "expected_return_pct": 30,
            "main_wave_score": 99,
            "trend_hold_score": 99,
            "main_wave_signal": "BUY_READY",
            "sector_gate_status": "ALLOW",
            "chase_risk_status": "EXECUTION_BLOCKED",
            "chase_risk_reason": "nine-session surge followed by two zero-capacity rows",
        }, "main_wave")

        self.assertEqual(plan["signal_status"], "BLOCK")
        self.assertIn("zero-capacity", plan["signal_reason"])

    def test_trade_plan_fails_closed_for_unknown_or_nan_chase_gate(self):
        base = {
            "stock_code": "600001",
            "trade_date": "2026-08-04",
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.5,
            "amount": 100_000_000,
            "change_pct": 1.0,
            "market_mood_score": 80,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 99,
            "entry_score": 99,
            "final_trade_score": 99,
            "expected_return_pct": 20,
            "sector_gate_status": "ALLOW",
        }
        cases = [
            {"chase_risk_status": "UNKNOWN", "ordinary_buy_eligible": True},
            {"chase_risk_status": float("nan"), "ordinary_buy_eligible": True},
            {"chase_risk_status": "ALLOW", "ordinary_buy_eligible": float("nan")},
            {
                "chase_risk_status": pd.NA,
                "ordinary_buy_eligible": True,
                "chase_risk_reason": pd.NA,
            },
        ]

        for values in cases:
            with self.subTest(values=values):
                plan = build_strategy_trade_plan(
                    {**base, **values},
                    "short_term",
                )
                self.assertEqual(plan["signal_status"], "BLOCK")

    def test_one_price_limit_up_with_small_nonzero_volume_has_no_buy_capacity(self):
        rows = pd.DataFrame([
            {
                "stock_code": "600001",
                "trade_date": "2026-08-03",
                "open": 9.90,
                "high": 10.10,
                "low": 9.80,
                "close": 10.00,
                "volume": 1_000_000,
                "amount": 10_000_000,
                "change_pct": 1.01,
                "pre_close": 9.90,
                "received_at": "2026-08-03 16:00:00",
            },
            {
                "stock_code": "600001",
                "trade_date": "2026-08-04",
                "open": 11.00,
                "high": 11.00,
                "low": 11.00,
                "close": 11.00,
                "volume": 1,
                "amount": 11,
                "change_pct": 10.00,
                "pre_close": 10.00,
                "received_at": "2026-08-04 16:00:00",
            },
        ])

        risk = _build_chase_risk_features_from_rows(rows, "2026-08-04").iloc[0]

        self.assertTrue(risk["one_price_limit_up_proxy"])
        self.assertEqual(risk["capacity_state"], "KNOWN_NO_CAPACITY")
        self.assertEqual(risk["chase_risk_status"], "EXECUTION_BLOCKED")
        self.assertFalse(risk["ordinary_buy_eligible"])
        self.assertEqual(str(risk["latest_tradable_date"]), "2026-08-04")

    def test_duplicate_revision_requires_complete_acquisition_timestamps(self):
        rows = pd.DataFrame([
            {
                "stock_code": "600001",
                "trade_date": "2026-08-04",
                "open": 10.00,
                "high": 10.20,
                "low": 9.90,
                "close": 10.10,
                "volume": 1_000_000,
                "amount": 10_000_000,
                "change_pct": 1.00,
                "pre_close": 10.00,
                "received_at": "2026-08-04 15:01:00",
            },
            {
                "stock_code": "600001",
                "trade_date": "2026-08-04",
                "open": 10.00,
                "high": 10.30,
                "low": 9.90,
                "close": 10.20,
                "volume": 1_100_000,
                "amount": 11_000_000,
                "change_pct": 2.00,
                "pre_close": 10.00,
                "received_at": None,
            },
        ])

        ambiguous = _build_chase_risk_features_from_rows(rows, "2026-08-04").iloc[0]
        complete = rows.copy()
        complete.loc[1, "received_at"] = "2026-08-04 15:02:00"
        resolved = _build_chase_risk_features_from_rows(
            complete, "2026-08-04"
        ).iloc[0]

        self.assertEqual(ambiguous["chase_risk_status"], "DATA_BLOCKED")
        self.assertIn("duplicate daily revisions", ambiguous["chase_risk_reason"])
        self.assertEqual(resolved["chase_risk_status"], "ALLOW")
        self.assertTrue(resolved["ordinary_buy_eligible"])

    def test_intraday_quote_sources_enforce_as_of_cutoff(self):
        from sqlalchemy import create_engine, text
        from biz.analysis import sync_analysis_fast as module

        schema = """
            CREATE TABLE {table_name} (
                stock_code TEXT,
                short_name TEXT,
                price REAL,
                `change` REAL,
                change_pct REAL,
                volume REAL,
                amount REAL,
                snapshot_at TEXT
            )
        """
        insert_sql = """
            INSERT INTO {table_name} (
                stock_code, short_name, price, `change`, change_pct,
                volume, amount, snapshot_at
            ) VALUES (
                :stock_code, :short_name, :price, :change, :change_pct,
                :volume, :amount, :snapshot_at
            )
        """
        snapshots = [
            {
                "stock_code": "600001", "short_name": "sample",
                "price": 10.0, "change": 0.1, "change_pct": 1.0,
                "volume": 100.0, "amount": 1_000.0,
                "snapshot_at": "2026-08-04 10:00:00",
            },
            {
                "stock_code": "600001", "short_name": "sample",
                "price": 11.0, "change": 1.1, "change_pct": 11.0,
                "volume": 200.0, "amount": 2_000.0,
                "snapshot_at": "2026-08-04 14:00:00",
            },
        ]

        for table_name in ("sm_stock_current", "sm_rt_quote_snapshot"):
            with self.subTest(table_name=table_name):
                engine = create_engine("sqlite+pysqlite:///:memory:")
                with engine.begin() as conn:
                    conn.execute(text(schema.format(table_name=table_name)))
                    conn.execute(
                        text(insert_sql.format(table_name=table_name)), snapshots
                    )
                try:
                    with patch(
                        "biz.analysis.sync_analysis_fast._table_exists",
                        side_effect=lambda _engine, name: name == table_name,
                    ):
                        quotes, source = module._read_intraday_current_quotes(
                            engine,
                            "2026-08-04",
                            as_of_at="2026-08-04 11:00:00",
                        )
                finally:
                    engine.dispose()

                self.assertEqual(source, table_name)
                self.assertEqual(len(quotes), 1)
                self.assertEqual(float(quotes.iloc[0]["price"]), 10.0)
                self.assertLessEqual(
                    str(quotes.iloc[0]["snapshot_at"]),
                    "2026-08-04 11:00:00",
                )

    def test_intraday_path_low_then_limit_price_is_not_one_price_board(self):
        from biz.analysis import sync_analysis_fast as module

        quotes = pd.DataFrame([{
            "stock_code": "600001",
            "short_name": "sample",
            "price": 11.0,
            "change": 1.0,
            "change_pct": 10.0,
            "volume": 1_000_000,
            "amount": 10_000_000,
            "snapshot_at": "2026-08-04 10:30:00",
        }])
        paths = pd.DataFrame([
            {
                "stock_code": "600001", "observed_at": "2026-08-04 09:30:00",
                "acquired_at": "2026-08-04 09:30:01", "close": 10.0,
                "path_source": "snapshot", "path_kind": "snapshot",
            },
            {
                "stock_code": "600001", "observed_at": "2026-08-04 09:45:00",
                "acquired_at": "2026-08-04 09:45:01", "close": 9.8,
                "path_source": "snapshot", "path_kind": "snapshot",
            },
            {
                "stock_code": "600001", "observed_at": "2026-08-04 10:30:00",
                "acquired_at": "2026-08-04 10:30:00", "close": 11.0,
                "path_source": "snapshot", "path_kind": "snapshot",
            },
        ])
        current = module._aggregate_intraday_current_bars(
            quotes,
            paths,
            "2026-08-04",
            as_of_at="2026-08-04T10:30:00+08:00",
        )
        risk = _build_chase_risk_features_from_rows(
            current, "2026-08-04T10:30:00+08:00"
        ).iloc[0]

        self.assertEqual(float(current.iloc[0]["open"]), 10.0)
        self.assertEqual(float(current.iloc[0]["low"]), 9.8)
        self.assertTrue(pd.isna(current.iloc[0]["turnover_ratio"]))
        self.assertFalse(risk["one_price_limit_up_proxy"])
        self.assertEqual(risk["capacity_state"], "KNOWN_CAPACITY")

    def test_intraday_missing_path_fails_closed_instead_of_fabricating_ohlc(self):
        from biz.analysis import sync_analysis_fast as module

        quotes = pd.DataFrame([{
            "stock_code": "600001", "short_name": "sample", "price": 11.0,
            "change": 1.0, "change_pct": 10.0, "volume": 1_000_000,
            "amount": 10_000_000, "snapshot_at": "2026-08-04 10:30:00",
        }])
        current = module._aggregate_intraday_current_bars(
            quotes,
            pd.DataFrame(),
            "2026-08-04",
            as_of_at="2026-08-04T10:30:00+08:00",
        )
        risk = _build_chase_risk_features_from_rows(
            current, "2026-08-04T10:30:00+08:00"
        ).iloc[0]

        self.assertTrue(pd.isna(current.iloc[0]["open"]))
        self.assertTrue(pd.isna(current.iloc[0]["high"]))
        self.assertTrue(pd.isna(current.iloc[0]["low"]))
        self.assertEqual(current.iloc[0]["intraday_ohlc_status"], "DATA_BLOCKED")
        self.assertEqual(risk["chase_risk_status"], "DATA_BLOCKED")
        self.assertIn("intraday OHLC path", risk["chase_risk_reason"])

    def test_intraday_history_sql_requires_knowledge_time_cutoff(self):
        from biz.analysis import sync_analysis_fast as module

        quotes = pd.DataFrame([{
            "stock_code": "600001",
            "short_name": "sample",
            "price": 10.5,
            "change": 0.5,
            "change_pct": 5.0,
            "volume": 1_000_000,
            "amount": 10_000_000,
            "snapshot_at": "2026-08-04 10:00:00",
        }])
        names = pd.DataFrame({"stock_code": ["600001"], "short_name": ["sample"]})
        history = pd.DataFrame([{
            "stock_code": "600001",
            "short_name": "sample",
            "trade_date": "2026-08-03",
            "open": 9.8,
            "high": 10.1,
            "low": 9.7,
            "close": 10.0,
            "volume": 1_000_000,
            "amount": 10_000_000,
            "change_pct": 2.0,
            "turnover_ratio": 1.0,
            "pre_close": 9.8,
            "received_at": "2026-08-03 16:00:00",
            "etl_sync_at": "2026-08-03 16:01:00",
        }])
        latest = pd.DataFrame({"stock_code": ["600001"]})

        with patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-04", "2026-08-03"],
        ), patch(
            "biz.analysis.sync_analysis_fast._expected_intraday_universe_size",
            return_value=1,
        ), patch(
            "biz.analysis.sync_analysis_fast._read_intraday_current_quotes",
            return_value=(quotes, "sm_stock_current"),
        ), patch(
            "biz.analysis.sync_analysis_fast._read_intraday_path_rows",
            return_value=pd.DataFrame(),
        ), patch(
            "biz.analysis.sync_analysis_fast._read_frame",
            side_effect=[names, history],
        ) as read_mock, patch(
            "biz.analysis.sync_analysis_fast._build_latest_kline_features_from_rows",
            return_value=latest,
        ):
            result = module._load_intraday_current_kline_features(
                object(),
                "2026-08-04",
                as_of_at="2026-08-04 10:30:00",
            )

        self.assertEqual(result["stock_code"].tolist(), ["600001"])
        history_call = read_mock.call_args_list[1]
        rendered_sql = str(history_call.args[0])
        params = history_call.kwargs["params"]
        self.assertIn("trade_date < :trade_date", rendered_sql)
        self.assertIn("END <= :as_of_at", rendered_sql)
        self.assertIn("received_at, etl_sync_at", rendered_sql)
        self.assertEqual(params["as_of_at"], "2026-08-04 10:30:00.000000")

    def test_chase_fields_survive_row_build_and_sql_binding(self):
        from biz.analysis import sync_analysis_fast as module

        source = pd.DataFrame([{
            "stock_code": "600001",
            "short_name": "sample",
            "ai_score": 95.0,
            "long_term_score": 90.0,
            "short_term_score": 95.0,
            "quality_score": 95.0,
            "final_trade_score": 96.0,
            "entry_score": 90.0,
            "capital_score": 90.0,
            "main_wave_score": 96.0,
            "main_wave_signal": "BUY_READY",
            "signal_status": "CONFIRM",
            "signal_reason": "confirmed",
            "recommend_status": "ALLOW",
            "recommend_reason": "quality confirmed",
            "event_risk_level": "LOW",
            "sector_gate_status": "ALLOW",
            "chase_policy_version": "EXTREME_EXTENSION_POLICY_V1",
            "surge_streak_lower_bound": 0,
            "exact_limit_up_streak": None,
            "trailing_untradeable_sessions": 0,
            "latest_tradable_date": "2026-08-04",
            "limit_rule_status": "PROXY_ONLY",
            "capacity_state": "KNOWN_CAPACITY",
            "one_price_limit_up_proxy": False,
            "extreme_extension_flag": False,
            "ordinary_buy_eligible": True,
            "chase_risk_status": "ALLOW",
            "chase_risk_reason": "conservative surge streak lower bound is 0",
            "chase_risk_evidence_json": "{\"surge_streak_lower_bound\":0}",
        }])
        analysis_rows = module.build_analysis_rows(source, "2026-08-04")
        rec_rows = module.build_recommendation_rows(
            source, "2026-08-04", top_n=1, min_score=62
        )

        self.assertEqual(analysis_rows[0]["chase_risk_status"], "ALLOW")
        self.assertEqual(rec_rows[0]["chase_risk_status"], "ALLOW")
        self.assertEqual(
            rec_rows[0]["chase_risk_evidence_json"],
            "{\"surge_streak_lower_bound\":0}",
        )

        class DummyConn:
            def __init__(self):
                self.calls = []

            def execute(self, statement, params=None):
                self.calls.append((statement, params or {}))

        class DummyBegin:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyEngine:
            def __init__(self):
                self.conn = DummyConn()

            def begin(self):
                return DummyBegin(self.conn)

        engine = DummyEngine()
        with patch("biz.analysis.sync_analysis_fast._ensure_output_schema"):
            module.save_outputs(
                engine,
                analysis_rows,
                rec_rows,
                "2026-08-04",
            )

        inserts = [
            (statement, params)
            for statement, params in engine.conn.calls
            if str(statement).lstrip().startswith("INSERT INTO")
        ]
        self.assertEqual(len(inserts), 2)
        for statement, params in inserts:
            self.assertEqual(set(statement._bindparams), set(params))
            self.assertEqual(params["chase_risk_status"], "ALLOW")
            self.assertTrue(params["ordinary_buy_eligible"])

    def test_backfill_recommendation_reviews_updates_returns_and_failure_samples(self):
        class DummyConn:
            def __init__(self):
                self.calls = []

            def execute(self, statement, params=None):
                self.calls.append((str(statement), params or {}))

        class DummyBegin:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyEngine:
            def __init__(self):
                self.conn = DummyConn()

            def begin(self):
                return DummyBegin(self.conn)

        recs = pd.DataFrame([{
            "stock_code": "600001",
            "short_name": "样本",
            "pick_date": "2026-06-01",
            "primary_strategy": "short_term",
            "strategy_profile": "short_term",
            "failure_tags_json": "[]",
        }])
        kline = pd.DataFrame([
            {"stock_code": "600001", "trade_date": "2026-06-01", "close": 10.0},
            {"stock_code": "600001", "trade_date": "2026-06-02", "close": 10.5},
            {"stock_code": "600001", "trade_date": "2026-06-03", "close": 9.8},
            {"stock_code": "600001", "trade_date": "2026-06-04", "close": 9.6},
            {"stock_code": "600001", "trade_date": "2026-06-05", "close": 9.4},
            {"stock_code": "600001", "trade_date": "2026-06-08", "close": 9.3},
            {"stock_code": "600001", "trade_date": "2026-06-09", "close": 9.2},
            {"stock_code": "600001", "trade_date": "2026-06-10", "close": 9.1},
            {"stock_code": "600001", "trade_date": "2026-06-11", "close": 9.0},
            {"stock_code": "600001", "trade_date": "2026-06-12", "close": 8.9},
            {"stock_code": "600001", "trade_date": "2026-06-15", "close": 8.8},
        ])
        engine = DummyEngine()

        with patch("biz.analysis.sync_analysis_fast._ensure_recommended_columns"), \
             patch("biz.analysis.sync_analysis_fast._ensure_learning_tables"), \
             patch("biz.analysis.sync_analysis_fast._read_frame", side_effect=[recs, kline]):
            stats = backfill_recommendation_reviews(engine, "2026-06-15")

        self.assertEqual(stats["updated"], 1)
        self.assertEqual(stats["failure_samples"], 1)
        update_params = next(params for sql, params in engine.conn.calls if "UPDATE st_recommended_stocks" in sql)
        self.assertEqual(update_params["review_1d_pct"], 5.0)
        self.assertEqual(update_params["review_10d_pct"], -12.0)
        self.assertIn("review_loss_10d", update_params["failure_tags_json"])

    def test_calibrate_strategy_thresholds_writes_scope_suggestions(self):
        class DummyConn:
            def __init__(self):
                self.calls = []

            def execute(self, statement, params=None):
                self.calls.append((str(statement), params or {}))

        class DummyBegin:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyEngine:
            def __init__(self):
                self.conn = DummyConn()

            def begin(self):
                return DummyBegin(self.conn)

        recs = pd.DataFrame([
            {
                "stock_code": f"600{i:03d}",
                "pick_date": "2026-06-01",
                "strategy_key": "short_term",
                "sector_key": "PASS",
                "risk_reward_ratio": 3.5,
                "review_5d_pct": -2.0,
                "review_10d_pct": -3.0,
            }
            for i in range(12)
        ])
        engine = DummyEngine()

        with patch("biz.analysis.sync_analysis_fast._ensure_learning_tables"), \
             patch("biz.analysis.sync_analysis_fast._table_exists", return_value=True), \
             patch("biz.analysis.sync_analysis_fast._table_columns", return_value={
                 "primary_strategy", "strategy_profile", "sector_gate_status", "risk_reward_ratio",
                 "review_5d_pct", "review_10d_pct",
             }), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=recs):
            stats = calibrate_strategy_thresholds(engine, "2026-06-30")

        self.assertEqual(stats["samples"], 12)
        self.assertGreaterEqual(stats["calibrations"], 4)
        inserts = [params for sql, params in engine.conn.calls if "INSERT INTO st_strategy_threshold_calibration" in sql]
        self.assertTrue(any("收紧" in params["suggestion"] for params in inserts))

    def test_publish_strategy_runtime_params_publishes_stable_tightening(self):
        class DummyConn:
            def __init__(self):
                self.calls = []

            def execute(self, statement, params=None):
                self.calls.append((str(statement), params or {}))

        class DummyBegin:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyEngine:
            def __init__(self):
                self.conn = DummyConn()

            def begin(self):
                return DummyBegin(self.conn)

        calibrations = pd.DataFrame([
            {
                "calibration_date": f"2026-06-{day:02d}",
                "sample_count": 42,
                "avg_return_5d": -1.5,
                "win_rate_5d": 38.0,
                "suggestion": "建议收紧：提高最低盈亏比/板块闸门",
            }
            for day in (30, 29, 28)
        ])
        engine = DummyEngine()

        with patch("biz.analysis.sync_analysis_fast._ensure_learning_tables"), \
             patch("biz.analysis.sync_analysis_fast._table_exists", return_value=True), \
             patch("biz.analysis.sync_analysis_fast._read_frame", return_value=calibrations), \
             patch("biz.analysis.sync_analysis_fast.load_strategy_runtime_params", return_value={
                 "min_risk_reward": 3.0,
                 "min_sector_flow_amount_3d": 500_000_000.0,
                 "min_sector_rotation_score": 50.0,
                 "price_crosscheck_tolerance_pct": 1.0,
             }):
            stats = publish_strategy_runtime_params(engine, "2026-06-30")

        self.assertEqual(stats["direction"], "tighten")
        self.assertEqual(stats["published"], 3)
        inserts = [params for sql, params in engine.conn.calls if "INSERT INTO st_strategy_runtime_params" in sql]
        self.assertTrue(any(params["param_key"] == "min_risk_reward" and params["param_value"] == 3.25 for params in inserts))

    def test_run_batch_emits_progress_events(self):
        progress_events = []
        empty_df = pd.DataFrame()

        patches = [
            patch("biz.analysis.sync_analysis_fast._ensure_output_schema"),
            patch("biz.analysis.sync_analysis_fast._activate_runtime_params"),
            patch("biz.analysis.sync_analysis_fast.load_kline_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast._assert_chase_risk_coverage"),
            patch("biz.analysis.sync_analysis_fast.load_finance", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_dividend_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_research_theme_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_business_purity_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_institutional_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_industry_prosperity_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_investor_interaction_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_flow_features", return_value=(empty_df, "2026-06-13")),
            patch("biz.analysis.sync_analysis_fast.load_hot_rank", return_value=(empty_df, "2026-06-13")),
            patch("biz.analysis.sync_analysis_fast.load_notice_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_news_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_event_relation_rules", return_value=[]),
            patch("biz.analysis.sync_analysis_fast.load_confidence_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_recommendation_history", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_failure_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_chip_capital_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_stock_north_holding_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_size_liquidity_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_order_book_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_sector_rotation_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.load_price_validation_features", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.compute_market_mood", return_value=55.0),
            patch("biz.analysis.sync_analysis_fast.compute_market_breadth_features", return_value={"market_extreme_status": "NEUTRAL"}),
            patch("biz.analysis.sync_analysis_fast.load_market_margin_features", return_value={}),
            patch("biz.analysis.sync_analysis_fast.load_market_style_context", return_value={}),
            patch("biz.analysis.sync_analysis_fast.load_market_north_flow_features", return_value={}),
            patch("biz.analysis.sync_analysis_fast.load_etf_flow_context", return_value={}),
            patch("biz.analysis.sync_analysis_fast.load_retail_sentiment_context", return_value={}),
            patch("biz.analysis.sync_analysis_fast.load_macro_policy_context", return_value={}),
            patch("biz.analysis.sync_analysis_fast.load_macro_indicator_context", return_value={}),
            patch("biz.analysis.sync_analysis_fast.compute_scores", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast._build_text_fields", return_value=empty_df),
            patch("biz.analysis.sync_analysis_fast.build_analysis_rows", return_value=[]),
            patch("biz.analysis.sync_analysis_fast.build_recommendation_rows", return_value=[]),
            patch("biz.analysis.sync_analysis_fast.enrich_recommendation_minute_chan", return_value=[]),
            patch("biz.analysis.sync_analysis_fast.save_outputs"),
            patch("biz.analysis.sync_analysis_fast.backfill_recommendation_reviews", return_value={"checked": 0, "updated": 0, "failure_samples": 0}),
            patch("biz.analysis.sync_analysis_fast.calibrate_strategy_thresholds", return_value={"samples": 0, "calibrations": 0}),
            patch("biz.analysis.sync_analysis_fast._publish_runtime_params_after_calibration"),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            from biz.analysis.sync_analysis_fast import run_batch

            stats = run_batch(
                engine=object(),
                trade_date="2026-06-13",
                progress_callback=progress_events.append,
            )

        self.assertEqual(stats.trade_date, "2026-06-13")
        self.assertTrue(progress_events)
        stages = [event["stage"] for event in progress_events]
        self.assertIn("load_kline", stages)
        self.assertEqual(progress_events[-1]["stage"], "done")
        self.assertEqual(progress_events[-1]["analysis_count"], 0)

    def test_repair_missing_qmt_kline_uses_shared_child_env(self):
        progress_events = []
        completed = SimpleNamespace(returncode=0, stdout="done", stderr="")

        with patch(
            "biz.analysis.sync_analysis_fast.build_child_env",
            return_value={"PYTHONPATH": "repo"},
        ) as child_env, patch(
            "biz.analysis.sync_analysis_fast.subprocess.run",
            return_value=completed,
        ) as run:
            repair_missing_qmt_kline_for_trade_date(
                "2026-06-26",
                progress_callback=progress_events.append,
                timeout_seconds=300,
            )

        child_env.assert_called_once()
        env = run.call_args.kwargs["env"]
        self.assertEqual(env["PYTHONPATH"], "repo")
        self.assertEqual(env["SM_STOCK_KLINE_SOURCE"], "qmt")
        self.assertEqual(env["SM_MAX_STOCKS"], "0")
        self.assertEqual(env["PYTHONIOENCODING"], "utf-8")
        self.assertIn("--kline-start", run.call_args.args[0])
        self.assertIn("repair_kline_done", [event["stage"] for event in progress_events])

    def test_strict_run_repairs_missing_qmt_kline_before_analysis(self):
        progress_events = []

        with patch("biz.analysis.sync_analysis_fast.previous_trade_date", return_value="2026-06-26"), \
             patch(
                 "biz.analysis.sync_analysis_fast.assert_trade_date_ready",
                 side_effect=[
                     RuntimeError("K-line latest date is 2026-06-25, earlier than required 2026-06-26"),
                     {
                         "trade_date": "2026-06-26",
                         "latest_kline_date": "2026-06-26",
                         "kline_count": 4200,
                         "expected_count": 5000,
                         "min_coverage": 0.8,
                     },
                 ],
             ) as ready_mock, \
             patch("biz.analysis.sync_analysis_fast.repair_missing_qmt_kline_for_trade_date") as repair_mock, \
             patch("biz.analysis.sync_analysis_fast._ensure_output_schema"), \
             patch("biz.analysis.sync_analysis_fast._activate_runtime_params"), \
             patch("biz.analysis.sync_analysis_fast._prepare_batch_outputs", return_value=([], [], 55.0, "2026-06-26", "2026-06-26")), \
             patch("biz.analysis.sync_analysis_fast.save_outputs"), \
             patch("biz.analysis.sync_analysis_fast.backfill_recommendation_reviews", return_value={"checked": 0, "updated": 0, "failure_samples": 0}), \
             patch("biz.analysis.sync_analysis_fast.calibrate_strategy_thresholds", return_value={"samples": 0, "calibrations": 0}), \
             patch("biz.analysis.sync_analysis_fast._publish_runtime_params_after_calibration"):
            from biz.analysis.sync_analysis_fast import run_batch

            stats = run_batch(
                engine=object(),
                trade_date="2026-06-26",
                strict_prev_trade_day=True,
                execution_time="2026-06-28 08:30:00",
                auto_repair_missing_kline=True,
                progress_callback=progress_events.append,
            )

        self.assertEqual(stats.trade_date, "2026-06-26")
        self.assertEqual(ready_mock.call_count, 2)
        repair_mock.assert_called_once_with("2026-06-26", progress_callback=progress_events.append)
        stages = [event["stage"] for event in progress_events]
        self.assertIn("strict_date_missing", stages)
        self.assertIn("strict_date_ready", stages)

    def test_run_batch_passes_intraday_current_to_prepare_outputs(self):
        with patch("biz.analysis.sync_analysis_fast._ensure_output_schema"), \
             patch("biz.analysis.sync_analysis_fast._activate_runtime_params"), \
             patch("biz.analysis.sync_analysis_fast._prepare_batch_outputs", return_value=([], [], 55.0, "2026-07-08", "2026-07-08")) as prepare_mock, \
             patch("biz.analysis.sync_analysis_fast.save_outputs"), \
             patch("biz.analysis.sync_analysis_fast.backfill_recommendation_reviews", return_value={"checked": 0, "updated": 0, "failure_samples": 0}), \
             patch("biz.analysis.sync_analysis_fast.calibrate_strategy_thresholds", return_value={"samples": 0, "calibrations": 0}), \
             patch("biz.analysis.sync_analysis_fast._publish_runtime_params_after_calibration"):
            from biz.analysis.sync_analysis_fast import run_batch

            stats = run_batch(
                engine=object(),
                trade_date="2026-07-08",
                execution_time="2026-07-08 10:20:00",
                use_intraday_current=True,
            )

        self.assertEqual(stats.trade_date, "2026-07-08")
        self.assertTrue(prepare_mock.call_args.kwargs["use_intraday_current"])
        self.assertEqual(prepare_mock.call_args.kwargs["news_cutoff_time"], "2026-07-08 10:20:00")


if __name__ == "__main__":
    unittest.main()
