# -*- coding: utf-8 -*-
import unittest
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from biz.analysis.sync_analysis_fast import (
    KlineFeatureDataBlocked,
    _load_canonical_chase_risk_evidence,
    _execute_batches,
    _load_kline_rolling_state,
    _load_sector_industry_memberships,
    _complete_membership_proof_scope,
    _recent_dates,
    _refresh_exact_upper_limit_execution_evidence,
    _write_kline_rolling_state,
    add_strategy_signals,
    apply_canonical_execution_eligibility,
    build_data_quality,
    build_analysis_rows,
    build_recommendation_rows,
    build_strategy_trade_plan,
    choose_recommend_status,
    clamp_score,
    classify_notice_title,
    linear_score,
    load_flow_features,
    load_confidence_features,
    load_failure_features,
    load_hot_rank,
    load_kline_features,
    load_recommendation_history,
    load_sector_rotation_features,
    select_primary_strategy,
    validate_exact_daily_flow_coverage,
)
import pandas as pd
from sqlalchemy import create_engine, text
from unittest.mock import patch

from server.common.daily_stock_universe import DailyStockUniverse
from server.common.analysis_pool_receipt import (
    TURNOVER_DIRECT_FORMULA,
    build_turnover_evidence,
    build_upper_limit_evidence,
    is_executable_recommendation,
    validate_turnover_evidence,
)
from server.common.chase_risk_policy import (
    CanonicalChaseBar,
    ChaseRiskPolicy,
    assess_chase_risk as assess_production_chase_risk,
)
from server.trading_v4.domain import AsOfDataset, AsOfRecord, QualityStatus
from server.trading_v4.factors.chase_risk import (
    ChaseRiskPolicy as FrozenV4ChaseRiskPolicy,
    assess_chase_risk as assess_frozen_v4_chase_risk,
)


class SyncAnalysisFastTest(unittest.TestCase):
    def test_execute_batches_uses_bounded_multi_values_round_trips(self):
        class RecordingConnection:
            def __init__(self):
                self.calls = []

            def execute(self, statement, parameters):
                self.calls.append((str(statement), dict(parameters)))

        connection = RecordingConnection()
        rows = [
            {"stock_code": f"{index:06d}", "score": index}
            for index in range(161)
        ]
        _execute_batches(
            connection,
            """
                INSERT INTO sample (stock_code, score)
                VALUES (:stock_code, :score)
                ON DUPLICATE KEY UPDATE score=VALUES(score)
            """,
            rows,
        )

        assert len(connection.calls) == 3
        assert [len(parameters) for _, parameters in connection.calls] == [
            160, 160, 2,
        ]
        first_sql, first_parameters = connection.calls[0]
        assert first_sql.count("(:stock_code_") == 80
        assert "score=VALUES(score)" in first_sql
        assert first_parameters["stock_code_0"] == "000000"
        assert first_parameters["stock_code_79"] == "000079"

    def test_kline_rolling_state_is_hash_bound_and_recoverable(self):
        frame = pd.DataFrame([
            {
                "stock_code": "000001",
                "trade_date": "2026-08-31",
                "close": 10.5,
            }
        ])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rolling.json.gz"
            receipt = _write_kline_rolling_state(
                path,
                frame=frame,
                trade_date="2026-08-31",
                window_start="2026-05-01",
                decision_known_at=datetime(2026, 8, 31, 22, 10),
            )
            loaded = _load_kline_rolling_state(path)
            self.assertIsNotNone(loaded)
            header, restored = loaded
            self.assertEqual(header["bars_sha256"], receipt["bars_sha256"])
            self.assertEqual(restored["stock_code"].tolist(), [1])

            path.write_bytes(path.read_bytes()[:-1] + b"x")
            self.assertIsNone(_load_kline_rolling_state(path))

    def test_recent_dates_excludes_partitions_unknown_at_decision_cutoff(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as connection:
            connection.execute(text(
                "CREATE TABLE facts (trade_date DATE, received_at DATETIME)"
            ))
            connection.execute(
                text(
                    "INSERT INTO facts (trade_date, received_at) VALUES "
                    "('2026-08-25', '2026-08-25 18:00:00'), "
                    "('2026-08-26', '2026-08-27 08:00:00')"
                )
            )

        self.assertEqual(
            _recent_dates(
                engine,
                "facts",
                "trade_date",
                "2026-08-26",
                2,
                known_at_column="received_at",
                decision_known_at=datetime(2026, 8, 26, 18, 50),
            ),
            ["2026-08-25"],
        )

    def test_kline_features_use_indexed_date_chunks_and_reuse_bars(self):
        dates = [
            value.date().isoformat()
            for value in pd.date_range("2026-08-10", periods=12, freq="B")
        ]
        target = dates[-1]
        source = pd.DataFrame([
            {
                "stock_code": "1",
                "short_name": "测试股",
                "trade_date": trade_day,
                "open": 10 + index / 10,
                "high": 10.2 + index / 10,
                "low": 9.8 + index / 10,
                "close": 10.1 + index / 10,
                "volume": 1000 + index,
                "amount": 10000 + index,
                "change_pct": 1,
                "turnover_ratio": 2,
                "pre_close": 10 + index / 10,
            }
            for index, trade_day in enumerate(dates)
        ])
        statements = []
        chunk_params = []

        def read_sql(statement, _engine, params):
            rendered = str(statement)
            statements.append(rendered)
            if "chunk_start_date" not in params:
                raise RuntimeError("grouped aggregate unavailable")
            chunk_params.append(dict(params))
            return source[
                source["trade_date"].between(
                    params["chunk_start_date"],
                    params["chunk_end_date"],
                )
            ].copy()

        class MySqlEngine:
            class Dialect:
                name = "mysql"

            dialect = Dialect()

        progress = []
        attached = {}

        def attach(frame, *_args, **kwargs):
            attached["bars"] = kwargs.get("preloaded_bars")
            return frame

        with patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=list(reversed(dates)),
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            side_effect=read_sql,
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "_attach_canonical_chase_risk_evidence",
            side_effect=attach,
        ), patch.dict(
            "os.environ",
            {"PROBIGA_KLINE_FEATURE_CHUNK_DAYS": "5"},
        ):
            result = load_kline_features(
                MySqlEngine(),
                target,
                progress_callback=progress.append,
            )

        self.assertEqual(result["stock_code"].tolist(), ["000001"])
        self.assertEqual(len(chunk_params), 3)
        self.assertEqual(len(attached["bars"]), len(dates))
        self.assertTrue(all("FORCE INDEX (idx_date_ktype)" in sql for sql in statements))
        self.assertNotIn("ORDER BY k.stock_code", "\n".join(statements))
        self.assertEqual(progress[-1]["stage"], "load_kline_done")
        self.assertEqual(
            progress[-1]["kline_feature_mode"],
            "INDEXED_DATE_CHUNKS",
        )

    def test_kline_feature_stage_timeout_is_explicit_data_block(self):
        class MySqlEngine:
            class Dialect:
                name = "mysql"

            dialect = Dialect()

        with patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-31"],
        ), patch(
            "biz.analysis.sync_analysis_fast.time.monotonic",
            side_effect=[0.0, 31.0],
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
        ) as read_sql, patch.dict(
            "os.environ",
            {"PROBIGA_KLINE_FEATURE_STAGE_TIMEOUT_SECONDS": "30"},
        ):
            with self.assertRaises(KlineFeatureDataBlocked) as captured:
                load_kline_features(MySqlEngine(), "2026-08-31")

        self.assertEqual(
            captured.exception.reason_code,
            "KLINE_FEATURE_STAGE_TIMEOUT",
        )
        self.assertIn("DATA_BLOCKED:", str(captured.exception))
        read_sql.assert_not_called()

    def test_canonical_execution_eligibility_requires_complete_evidence(self):
        cases = ["ALLOW", "BLOCK", "DATA_BLOCKED", None, "UNKNOWN"]
        result = apply_canonical_execution_eligibility(
            pd.DataFrame({"chase_evidence_status": cases})
        )
        self.assertEqual(
            result["chase_risk_status"].tolist(),
            ["ALLOW", "BLOCK", "DATA_BLOCKED", "DATA_BLOCKED", "DATA_BLOCKED"],
        )
        self.assertEqual(
            result["ordinary_buy_eligible"].tolist(),
            [1, 0, 0, 0, 0],
        )

    def test_chase_evidence_blocks_unproven_turnover_and_future_bars(self):
        days = pd.date_range("2026-07-29", periods=21, freq="D")

        def rows_for(code, closes):
            rows = []
            previous = closes[0]
            for index, (day, close) in enumerate(zip(days, closes)):
                pre_close = previous if index else close
                rows.append({
                    "stock_code": code,
                    "trade_date": day.date(),
                    "open": pre_close,
                    "high": max(pre_close, close),
                    "low": min(pre_close, close),
                    "close": close,
                    "volume": 1000,
                    "amount": 10000,
                    "pre_close": pre_close,
                    "turnover_ratio": 2,
                    "data_source": "gj_big_qmt_inner",
                    "batch_id": f"batch-{index}",
                    "data_version": f"version-{index}",
                    "quality_status": "QMT_ATTESTED",
                    "permission_status": "SUPPORTED",
                    "received_at": day,
                })
                previous = close
            return rows

        low_price = [0.22] * 18 + [0.23, 0.24, 0.25]
        safe = [10.0] * 21
        bad = [8.0] * 21
        source = pd.DataFrame(
            rows_for("000001", low_price)
            + rows_for("000002", safe)
            + rows_for("000003", bad)
            + rows_for("000004", safe)
            + rows_for("000005", safe)
            + rows_for("000006", safe)
        )
        source.loc[
            (source["stock_code"] == "000003")
            & (source["trade_date"] == days[-1].date()),
            "pre_close",
        ] = 0
        source.loc[
            source["trade_date"].eq(days[-1].date())
            & source["stock_code"].ne("000005"),
            "turnover_ratio",
        ] = None
        source.loc[
            (source["stock_code"] == "000006")
            & (source["trade_date"] == days[-2].date()),
            "received_at",
        ] = days[-1].replace(hour=13)
        with patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            side_effect=[source],
        ), patch(
            "biz.analysis.sync_analysis_fast.load_verified_turnover_evidence",
            return_value={},
        ):
            result = _load_canonical_chase_risk_evidence(
                object(),
                start_date=days[0].date().isoformat(),
                trade_date=days[-1].date().isoformat(),
                decision_known_at=days[-1].replace(hour=12),
            ).set_index("stock_code")

        self.assertEqual(result.loc["000001", "chase_evidence_status"], "DATA_BLOCKED")
        self.assertEqual(result.loc["000002", "chase_evidence_status"], "DATA_BLOCKED")
        self.assertEqual(result.loc["000003", "chase_evidence_status"], "DATA_BLOCKED")
        self.assertEqual(result.loc["000004", "chase_evidence_status"], "DATA_BLOCKED")
        self.assertEqual(result.loc["000005", "chase_evidence_status"], "DATA_BLOCKED")
        self.assertEqual(result.loc["000006", "chase_evidence_status"], "DATA_BLOCKED")
        proof = validate_turnover_evidence(
            result.loc["000002", "turnover_evidence_json"]
        )
        self.assertEqual(proof["status"], "DATA_BLOCKED")
        self.assertEqual(proof["source_table"], "st_market_field_capture_row")
        self.assertEqual(
            proof["decision_known_at"],
            days[-1].replace(hour=12).isoformat(sep=" "),
        )
        raw_proof = validate_turnover_evidence(
            result.loc["000005", "turnover_evidence_json"]
        )
        self.assertEqual(raw_proof["status"], "DATA_BLOCKED")
        self.assertEqual(
            raw_proof["decision_known_at"],
            days[-1].replace(hour=12).isoformat(sep=" "),
        )

    def test_verified_upper_and_turnover_use_frozen_v4_and_allow_safe_bar(self):
        days = pd.date_range("2026-07-24", periods=21, freq="B")
        source = pd.DataFrame([
            {
                "stock_code": "000001",
                "trade_date": day.date(),
                "open": 10,
                "high": 10.1,
                "low": 9.9,
                "close": 10,
                "volume": 1000,
                "amount": 10000,
                "pre_close": 10,
                "turnover_ratio": None,
                "data_source": "gj_big_qmt_inner",
                "batch_id": f"batch-{index}",
                "data_version": f"version-{index}",
                "quality_status": "QMT_ATTESTED",
                "permission_status": "SUPPORTED",
                "received_at": day.replace(hour=17),
            }
            for index, day in enumerate(days)
        ])
        target = days[-1].date().isoformat()
        decision = days[-1].replace(hour=18, minute=50).to_pydatetime()
        turnover_proof = build_turnover_evidence({
            "status": "PASS",
            "stock_code": "000001",
            "trade_date": target,
            "decision_known_at": decision.isoformat(sep=" "),
            "source_table": "st_market_field_capture_row",
            "formula": TURNOVER_DIRECT_FORMULA,
            "volume": "1000",
            "turnover_ratio": "2.00",
            "provider": "eastmoney.push2his.kline",
            "transport_contract": "HTTPS_TLS_VERIFIED_PINNED_RESOLVE_V1",
            "resolved_endpoint": "push2his.eastmoney.com:443:61.129.129.48",
            "source_field": "f61",
            "unit": "PERCENT",
            "source_trade_date": target,
            "captured_at": days[-1].replace(hour=18).isoformat(sep=" "),
            "provider_http_date": days[-1].replace(hour=10).strftime(
                "%a, %d %b %Y %H:%M:%S GMT"
            ),
            "snapshot_run_id": "1" * 32,
            "collector_build_sha": "a" * 40,
            "collector_binary_sha256": "b" * 64,
            "authority_proof_kind": "QMT_DAILY_MARKET_TRUTH",
            "authority_proof_identity": "qmt-daily-truth-run-1",
            "authority_proof_sha256": "c" * 64,
            "authority_set_sha256": "d" * 64,
            "raw_payload_sha256": "2" * 64,
            "snapshot_row_sha256": "3" * 64,
            "snapshot_semantic_sha256": "4" * 64,
            "source_open": "10.00", "source_high": "10.10",
            "source_low": "9.90", "source_close": "10.00",
            "source_volume_shares": "1000",
            "qmt_open": "10.00", "qmt_high": "10.10",
            "qmt_low": "9.90", "qmt_close": "10.00",
            "qmt_volume_shares": "1000",
            "qmt_received_at": days[-1].replace(hour=17).isoformat(sep=" "),
            "qmt_data_source": "gj_big_qmt_inner",
            "qmt_batch_id": "batch-20", "qmt_data_version": "version-20",
            "qmt_quality_status": "QMT_ATTESTED",
            "qmt_permission_status": "SUPPORTED",
        })
        upper_proof = build_upper_limit_evidence({
            "status": "PASS", "stock_code": "000001",
            "trade_date": target,
            "window_start_date": days[0].date().isoformat(),
            "window_end_date": target,
            "decision_known_at": decision.isoformat(sep=" "),
            "captured_at": days[-1].replace(hour=18).isoformat(sep=" "),
            "source_table": "st_market_field_capture_row",
            "capture_kind": "DAILY_UPPER_LIMIT_HISTORY",
            "provider": "myquant.gm.get_history_instruments",
            "source_field": "upper_limit", "unit": "PRICE_CNY",
            "transport_contract": "MYQUANT_GM_SDK_FIXED_ACTION_V1",
            "entitlement_status": "SUPPORTED", "timezone": "Asia/Shanghai",
            "expected_stock_count": 1, "expected_date_count": 21,
            "snapshot_run_id": "5" * 32,
            "subject_identity": f"{target}:fixture",
            "subject_sha256": "6" * 64, "code_set_sha256": "7" * 64,
            "trade_dates_sha256": "8" * 64,
            "calendar_batch_id": "calendar-batch-1",
            "calendar_manifest_sha256": "f" * 64,
            "calendar_session_set_sha256": "0" * 64,
            "expected_keyset_sha256": "9" * 64,
            "snapshot_semantic_sha256": "a" * 64,
            "stock_rows_sha256": "b" * 64,
            "provider_response_sha256": "c" * 64,
            "canonical_request_sha256": "d" * 64,
            "worker_sha256": "e" * 64,
            "sdk_version": "3.0.114", "python_version": "3.6.8",
        })
        with patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=source,
        ), patch(
            "biz.analysis.sync_analysis_fast.load_verified_turnover_evidence",
            return_value={
                "000001": {
                    "turnover_ratio": Decimal("2.00"),
                    "turnover_evidence_json": turnover_proof,
                }
            },
        ):
            result = _load_canonical_chase_risk_evidence(
                object(),
                start_date=days[0].date().isoformat(),
                trade_date=target,
                decision_known_at=decision,
                upper_limit_evidence={
                    "000001": {
                        "upper_limits": {
                            day.date().isoformat(): Decimal("11.00")
                            for day in days
                        },
                        "upper_limit_evidence_json": upper_proof,
                    }
                },
            ).iloc[0]

        self.assertEqual(result["chase_evidence_status"], "ALLOW")
        self.assertEqual(result["chase_effective_streak"], 0)
        self.assertEqual(result["chase_no_capacity"], 0)

    def test_top80_wiring_refreshes_exact_upper_map_and_empty_map_stays_blocked(self):
        codes = [f"{number:06d}" for number in range(1, 81)]
        scored = pd.DataFrame({
            "stock_code": codes,
            "chase_evidence_status": ["DATA_BLOCKED"] * 80,
        })
        preliminary = [
            {
                "stock_code": code,
                "chase_bar_window_root_sha256": f"{index + 1:064x}",
            }
            for index, code in enumerate(codes)
        ]
        upper_map = {code: {"proof": code} for code in codes}
        refreshed = pd.DataFrame({
            "stock_code": codes,
            "turnover_ratio_effective": [2.0] * 80,
            "chase_evidence_status": ["ALLOW"] * 80,
            "chase_bar_window_root_sha256": [
                f"{index + 1:064x}" for index in range(80)
            ],
        })
        with patch(
            "biz.analysis.sync_analysis_fast.build_recommendation_rows",
            return_value=preliminary,
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "build_preliminary_upper_subject_receipt",
            return_value={"receipt_sha256": "f" * 64},
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "validate_preliminary_upper_subject_receipt",
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "load_latest_verified_upper_limit_evidence",
            return_value=upper_map,
        ) as loader, patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-21", "2026-07-24"],
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "_load_canonical_chase_risk_evidence",
            return_value=refreshed,
        ) as chase_loader, patch(
            "biz.analysis.sync_analysis_fast._build_text_fields",
            side_effect=lambda frame, **_kwargs: frame,
        ):
            result = _refresh_exact_upper_limit_execution_evidence(
                engine=object(), scored=scored,
                trade_date="2026-08-21",
                decision_at="2026-08-27 18:50:00",
                top_n=80, min_score=62.0, flow_date="2026-08-21",
                publisher_build_sha="b" * 40,
            )

        self.assertEqual(result["chase_risk_status"].tolist(), ["ALLOW"] * 80)
        self.assertEqual(result["ordinary_buy_eligible"].tolist(), [1] * 80)
        self.assertEqual(
            loader.call_args.kwargs["stock_codes"], codes
        )
        self.assertEqual(
            loader.call_args.kwargs["preliminary_receipt_sha256"],
            "f" * 64,
        )
        self.assertIs(
            chase_loader.call_args.kwargs["upper_limit_evidence"], upper_map
        )

        drifted = refreshed.copy()
        drifted.loc[0, "chase_bar_window_root_sha256"] = "f" * 64
        with patch(
            "biz.analysis.sync_analysis_fast.build_recommendation_rows",
            return_value=preliminary,
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "build_preliminary_upper_subject_receipt",
            return_value={"receipt_sha256": "f" * 64},
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "validate_preliminary_upper_subject_receipt",
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "load_latest_verified_upper_limit_evidence",
            return_value=upper_map,
        ), patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-21", "2026-07-24"],
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "_load_canonical_chase_risk_evidence",
            return_value=drifted,
        ):
            with self.assertRaisesRegex(
                RuntimeError, "chase bar evidence changed"
            ):
                _refresh_exact_upper_limit_execution_evidence(
                    engine=object(), scored=scored,
                    trade_date="2026-08-21",
                    decision_at="2026-08-27 18:50:00",
                    top_n=80, min_score=62.0, flow_date="2026-08-21",
                    publisher_build_sha="b" * 40,
                )

        with patch(
            "biz.analysis.sync_analysis_fast.build_recommendation_rows",
            return_value=preliminary,
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "build_preliminary_upper_subject_receipt",
            return_value={"receipt_sha256": "f" * 64},
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "validate_preliminary_upper_subject_receipt",
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "load_latest_verified_upper_limit_evidence",
            return_value={},
        ), patch(
            "biz.analysis.sync_analysis_fast."
            "_load_canonical_chase_risk_evidence",
        ) as blocked_chase:
            unchanged = _refresh_exact_upper_limit_execution_evidence(
                engine=object(), scored=scored,
                trade_date="2026-08-21",
                decision_at="2026-08-27 18:50:00",
                top_n=80, min_score=62.0, flow_date="2026-08-21",
                publisher_build_sha="b" * 40,
            )
        self.assertIs(unchanged, scored)
        blocked_chase.assert_not_called()

    def test_projection_threshold_values_match_frozen_v4_policy(self):
        self.assertEqual(
            ChaseRiskPolicy().__dict__,
            FrozenV4ChaseRiskPolicy().__dict__,
        )

    def test_production_chase_assessment_matches_frozen_v4(self):
        instrument = "000001"
        start = datetime(2026, 7, 24, 15, 0, tzinfo=timezone.utc)

        for limit_sessions in (set(), {15, 16, 17}):
            with self.subTest(limit_sessions=sorted(limit_sessions)):
                frozen_records = []
                production_bars = []
                previous = Decimal("10")
                for index in range(21):
                    event_time = start + timedelta(days=index)
                    is_limit = index in limit_sessions
                    close = (
                        previous * Decimal("1.10")
                        if is_limit
                        else previous
                    )
                    open_price = previous
                    high = close if is_limit else close + Decimal("0.10")
                    low = min(open_price, close) - Decimal("0.10")
                    upper_limit = (
                        close if is_limit else previous * Decimal("1.10")
                    )
                    knowledge_time = event_time + timedelta(minutes=10)
                    payload = {
                        "instrument": instrument,
                        "trade_date": event_time.date().isoformat(),
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "close": close,
                        "previous_close": previous,
                        "volume": Decimal("1000000"),
                        "amount": Decimal("1000000") * close,
                        "upper_limit": upper_limit,
                        "turnover_pct": Decimal("10"),
                        "is_suspended": False,
                    }
                    record_id = f"bar-{index:03d}"
                    frozen_records.append(AsOfRecord(
                        record_id=record_id,
                        source="canonical-parity-fixture",
                        event_time=event_time,
                        knowledge_time=knowledge_time,
                        ingested_at=knowledge_time,
                        received_at=knowledge_time,
                        payload=payload,
                        quality_status=QualityStatus.PASS,
                    ))
                    production_bars.append(CanonicalChaseBar(
                        record_id=record_id,
                        instrument=instrument,
                        session=event_time.date(),
                        knowledge_time=knowledge_time,
                        open=open_price,
                        high=high,
                        low=low,
                        close=close,
                        previous_close=previous,
                        volume=Decimal("1000000"),
                        amount=Decimal("1000000") * close,
                        upper_limit=upper_limit,
                        turnover_pct=Decimal("10"),
                    ))
                    previous = close

                cutoff = frozen_records[-1].knowledge_time
                frozen = assess_frozen_v4_chase_risk(
                    AsOfDataset(
                        dataset_name="canonical-parity-fixture",
                        as_of=cutoff,
                        records=tuple(frozen_records),
                        quality_status=QualityStatus.PASS,
                    ),
                    instrument=instrument,
                    cutoff=cutoff,
                )
                production = assess_production_chase_risk(
                    tuple(production_bars),
                    instrument=instrument,
                    cutoff=cutoff,
                )
                for field_name in (
                    "bar_count",
                    "surge_streak",
                    "limit_streak",
                    "peak_streak",
                    "recent_peak_streak",
                    "sessions_since_peak",
                    "drawdown_from_peak_pct",
                    "cooldown_active",
                    "zero_volume",
                    "one_price_limit_up",
                    "has_verified_capacity",
                    "no_capacity",
                    "return_1d_pct",
                    "return_5d_pct",
                    "return_20d_pct",
                    "ma5",
                    "ma20",
                    "atr14",
                    "ma5_extension_pct",
                    "ma20_extension_pct",
                    "atr14_pct",
                    "ma5_extension_atr",
                    "ma20_extension_atr",
                    "gap_pct",
                    "crowding_detected",
                    "extreme_extension",
                    "ordinary_buy_eligible",
                    "missing_fields",
                    "reason_codes",
                ):
                    self.assertEqual(
                        getattr(production, field_name),
                        getattr(frozen, field_name),
                        field_name,
                    )
                self.assertEqual(
                    production.candidate_status,
                    frozen.candidate_status.value,
                )
                self.assertEqual(
                    production.quality_status,
                    frozen.quality_status.value,
                )

    def test_executable_pool_requires_all_four_gates(self):
        base = {
            "recommend_status": "ALLOW",
            "signal_status": "BUY_READY",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": 1,
        }
        self.assertTrue(is_executable_recommendation(base))
        for field, blocked in (
            ("recommend_status", "SUSPENDED"),
            ("signal_status", "WATCH"),
            ("chase_risk_status", "BLOCK"),
            ("ordinary_buy_eligible", 0),
        ):
            with self.subTest(field=field):
                self.assertFalse(
                    is_executable_recommendation({**base, field: blocked})
                )

    def test_preliminary_top80_uses_stable_stock_code_tie_breaker(self):
        rows = []
        for number in range(81, 0, -1):
            rows.append({
                "stock_code": f"{number:06d}",
                "recommend_status": "ALLOW",
                "signal_status": "BUY_READY",
                "main_wave_signal": "",
                "event_risk_level": "LOW",
                "quality_score": 70.0,
                "final_trade_score": 70.0,
                "main_wave_score": 70.0,
                "entry_score": 70.0,
                "capital_score": 70.0,
                "ai_score": 70.0,
                "short_term_score": 70.0,
                "long_term_score": 70.0,
            })

        selected = build_recommendation_rows(
            pd.DataFrame(rows),
            "2026-08-27",
            top_n=80,
            min_score=62.0,
        )

        self.assertEqual(
            [row["stock_code"] for row in selected],
            [f"{number:06d}" for number in range(1, 81)],
        )

    def test_flow_features_keep_only_exact_target_rows_per_stock(self):
        source = pd.DataFrame([
            {
                "stock_code": "1",
                "trade_date": "2026-08-25",
                "main_net_inflow": 10,
                "max_net_inflow": 1,
                "lg_net_inflow": 2,
                "mid_net_inflow": 3,
                "sm_net_inflow": 4,
                "etl_sync_at": "2026-08-25 17:30:00",
            },
            {
                "stock_code": "1",
                "trade_date": "2026-08-26",
                "main_net_inflow": 20,
                "max_net_inflow": 2,
                "lg_net_inflow": 3,
                "mid_net_inflow": 4,
                "sm_net_inflow": 5,
                "etl_sync_at": "2026-08-26 17:30:00",
            },
            {
                "stock_code": "2",
                "trade_date": "2026-08-25",
                "main_net_inflow": 30,
                "max_net_inflow": 3,
                "lg_net_inflow": 4,
                "mid_net_inflow": 5,
                "sm_net_inflow": 6,
                "etl_sync_at": "2026-08-25 17:30:00",
            },
        ])
        observed = {}

        def read_sql(statement, _engine, params):
            observed["sql"] = str(statement)
            observed["params"] = dict(params)
            return source

        with patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-26", "2026-08-25"],
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            side_effect=read_sql,
        ):
            frame, flow_date = load_flow_features(
                object(),
                "2026-08-26",
                decision_known_at="2026-08-26 18:50:00",
            )

        self.assertEqual(flow_date, "2026-08-26")
        self.assertEqual(frame["stock_code"].tolist(), ["000001"])
        self.assertEqual(str(frame.iloc[0]["flow_trade_date"]), "2026-08-26")
        self.assertEqual(frame.iloc[0]["main_net_inflow_5d"], 30)
        self.assertIn("etl_sync_at <= :decision_known_at", observed["sql"])
        self.assertIn(
            "etl_sync_at >= TIMESTAMP(trade_date, '15:10:00')",
            observed["sql"],
        )
        self.assertEqual(
            observed["params"]["decision_known_at"],
            datetime(2026, 8, 26, 18, 50),
        )

    def test_flow_features_reject_preclose_target_partition(self):
        source = pd.DataFrame([{
            "stock_code": "1",
            "trade_date": "2026-08-27",
            "main_net_inflow": 10,
            "max_net_inflow": 1,
            "lg_net_inflow": 2,
            "mid_net_inflow": 3,
            "sm_net_inflow": 4,
            "etl_sync_at": "2026-08-27 14:22:00",
        }])
        with patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-27"],
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=source,
        ):
            with self.assertRaisesRegex(RuntimeError, "post-close PIT"):
                load_flow_features(
                    object(),
                    "2026-08-27",
                    decision_known_at="2026-08-27 18:50:00",
                )

    def test_flow_quality_uses_each_stock_date_and_accepts_exact_zero(self):
        _, exact_flags = build_data_quality(
            {
                "flow_trade_date": "2026-08-26",
                "main_net_inflow": 0,
                "main_net_inflow_5d": 0,
            },
            trade_date="2026-08-26",
            flow_date="2026-08-26",
        )
        _, stale_flags = build_data_quality(
            {"flow_trade_date": "2026-08-25"},
            trade_date="2026-08-26",
            flow_date="2026-08-26",
        )
        _, missing_flags = build_data_quality(
            {"flow_trade_date": None},
            trade_date="2026-08-26",
            flow_date="2026-08-26",
        )

        self.assertNotIn("missing_flow", exact_flags)
        self.assertNotIn("stale_flow", exact_flags)
        self.assertIn("stale_flow", stale_flags)
        self.assertIn("missing_flow", missing_flags)

    def test_exact_flow_coverage_blocks_missing_traded_stock(self):
        universe = DailyStockUniverse(
            target_date="2026-08-26",
            catalog_batch_id="catalog",
            catalog_manifest_hash="a" * 64,
            catalog_member_set_hash="b" * 64,
            expected_codes=("000001", "000002"),
            expected_code_set_hash="c" * 64,
        )
        kline = pd.DataFrame([
            {"stock_code": "000001", "volume": 100, "amount": 1000},
            {"stock_code": "000002", "volume": 0, "amount": 0},
        ])
        with patch(
            "biz.analysis.sync_analysis_fast.load_daily_stock_universe",
            return_value=universe,
        ):
            with self.assertRaisesRegex(RuntimeError, "DATA_BLOCKED.*capital-flow"):
                validate_exact_daily_flow_coverage(
                    object(),
                    trade_date="2026-08-26",
                    kline=kline,
                    flow=pd.DataFrame({"stock_code": []}),
                )

    def test_hot_rank_never_falls_back_to_an_older_partition(self):
        observed = {}

        def read_sql(statement, _engine, params):
            observed["sql"] = str(statement)
            observed["params"] = dict(params)
            return pd.DataFrame()

        with patch("biz.analysis.sync_analysis_fast.pd.read_sql", side_effect=read_sql):
            frame, hot_date = load_hot_rank(object(), "2026-08-26")

        self.assertTrue(frame.empty)
        self.assertEqual(hot_date, "")
        self.assertIn("snapshot_date = :trade_date", observed["sql"])
        self.assertNotIn("snapshot_date <=", observed["sql"])
        self.assertEqual(observed["params"], {"trade_date": "2026-08-26"})

    def test_hot_rank_rejects_exact_date_single_source_rows(self):
        source = pd.DataFrame([
            {
                "stock_code": "1",
                "fused_rank": 1,
                "total_score": 100,
                "source_flag": "east_only",
            },
            {
                "stock_code": "2",
                "fused_rank": 2,
                "total_score": 99,
                "source_flag": "ths_only",
            },
        ])
        with patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=source,
        ):
            frame, hot_date = load_hot_rank(object(), "2026-08-26")

        self.assertTrue(frame.empty)
        self.assertEqual(hot_date, "")

    def test_hot_rank_keeps_only_exact_date_multi_source_consensus(self):
        source = pd.DataFrame([
            {
                "stock_code": "1",
                "fused_rank": 1,
                "total_score": 100,
                "source_flag": "east_only",
            },
            {
                "stock_code": "2",
                "fused_rank": 2,
                "total_score": 99,
                "source_flag": "east_ths",
            },
            {
                "stock_code": "3",
                "fused_rank": 3,
                "total_score": 98,
                "source_flag": "east_sina",
            },
        ])
        with patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=source,
        ):
            frame, hot_date = load_hot_rank(object(), "2026-08-26")

        self.assertEqual(hot_date, "2026-08-26")
        self.assertEqual(frame["stock_code"].tolist(), ["000003"])
        self.assertEqual(frame["source_flag"].tolist(), ["east_sina"])

    def test_formal_decision_disables_mutable_unversioned_learning_inputs(self):
        decision_at = datetime(2026, 8, 26, 18, 50)
        with patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            side_effect=AssertionError("formal PIT path must not read mutable history"),
        ) as read_sql, patch(
            "biz.analysis.sync_analysis_fast._table_exists",
            side_effect=AssertionError("formal PIT path must not inspect mutable history"),
        ) as table_exists, patch(
            "biz.analysis.sync_analysis_fast._validate_learning_tables",
            side_effect=AssertionError("formal PIT path must not inspect failure history"),
        ) as validate_learning:
            hot, hot_date = load_hot_rank(
                object(),
                "2026-08-26",
                decision_at=decision_at,
            )
            confidence = load_confidence_features(
                object(),
                "2026-08-26",
                decision_at=decision_at,
            )
            recommendations = load_recommendation_history(
                object(),
                "2026-08-26",
                decision_at=decision_at,
            )
            failures = load_failure_features(
                object(),
                "2026-08-26",
                decision_at=decision_at,
            )

        self.assertTrue(hot.empty)
        self.assertEqual(hot_date, "")
        self.assertTrue(confidence.empty)
        self.assertTrue(recommendations.empty)
        self.assertTrue(failures.empty)
        read_sql.assert_not_called()
        table_exists.assert_not_called()
        validate_learning.assert_not_called()

    def test_sector_membership_prefers_complete_immutable_snapshot(self):
        memberships = pd.DataFrame([
            {"stock_code": "000001", "industry_name": "Bank"},
            {"stock_code": "000002", "industry_name": "AI"},
        ])

        with patch(
            "biz.analysis.sync_analysis_fast._table_exists", return_value=True
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=memberships,
        ), patch(
            "integrations.bigqmt.membership_snapshot."
            "verify_existing_membership_snapshot",
            return_value={
                "snapshot_date": "2026-08-11",
                "source": "gj_big_qmt_inner",
                "quality_status": "QMT_VALIDATED",
                "capture_mode": "qmt_close_full_refresh",
                "captured_at": "2026-08-11 15:12:00",
                "concept_count": 500,
                "concept_relation_count": 30000,
                "concept_stock_count": 3000,
                "concept_hash": "a" * 64,
                "industry_count": 20,
                "industry_relation_count": 5000,
                "industry_stock_count": 4500,
                "industry_hash": "b" * 64,
            },
        ):
            result = _load_sector_industry_memberships(object(), "2026-08-11")

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result.set_index("stock_code").loc["000002", "industry_name"],
            "AI",
        )
        self.assertEqual(
            result["industry_snapshot_source"].unique().tolist(),
            ["gj_big_qmt_inner"],
        )

    def test_sector_membership_uses_strict_previous_open_session_binding(self):
        binding = {
            "snapshot_date": "2026-08-28",
            "source": "gj_big_qmt_inner",
            "proof_sha256": "c" * 64,
            "proof_mode": "PREVIOUS_OPEN_SESSION_INDUSTRY_CARRY_FORWARD",
            "source_snapshot_date": "2026-08-27",
            "previous_session_fallback": True,
            "fallback_reason": "QMT_HISTORICAL_SECTOR_API_UNAVAILABLE",
            "captured_at": "2026-08-27T15:12:00",
            "industry_relation_count": 2,
            "concept_relation_count": 0,
        }
        history_rows = [
            {"stock_code": "000001", "industry_name": "Bank"},
            {"stock_code": "000002", "industry_name": "AI"},
        ]
        with patch(
            "biz.analysis.sync_analysis_fast._table_exists",
            return_value=True,
        ), patch(
            "server.engine.strategy_industry_history."
            "resolve_analysis_industry_membership_binding",
            return_value=binding,
        ), patch(
            "server.engine.strategy_industry_history.prepare_industry_history",
            return_value=({"snapshot_id": "c" * 64}, history_rows),
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            side_effect=AssertionError("fallback must use verified history rows"),
        ):
            result = _load_sector_industry_memberships(
                object(),
                "2026-08-28",
                decision_known_at="2026-08-30 02:00:00",
            )

        self.assertEqual(result["stock_code"].tolist(), ["000001", "000002"])
        self.assertEqual(result["membership_proof_sha256"].unique().tolist(), ["c" * 64])
        self.assertEqual(result["industry_snapshot_date"].unique().tolist(), ["2026-08-28"])
        self.assertEqual(result["industry_source_snapshot_date"].unique().tolist(), ["2026-08-27"])
        self.assertTrue(result["industry_previous_session_fallback"].all())

    def test_sector_rotation_filters_kline_and_flow_by_decision_cutoff(self):
        memberships = pd.DataFrame([{
            "stock_code": "000001",
            "industry_name": "Bank",
            "industry_snapshot_date": "2026-08-26",
            "industry_snapshot_source": "gj_big_qmt_inner",
            "membership_proof_sha256": "d" * 64,
        }])
        observations = pd.DataFrame([{
            "stock_code": "000001",
            "trade_date": "2026-08-26",
            "change_pct": 1.0,
            "amount": 1000,
            "main_net_inflow": 10,
            "flow_etl_sync_at": "2026-08-26 17:30:00",
        }])
        observed = {}

        def read_sql(statement, _engine, params):
            observed["sql"] = str(statement)
            observed["params"] = dict(params)
            return observations

        with patch(
            "biz.analysis.sync_analysis_fast._load_sector_industry_memberships",
            return_value=memberships,
        ), patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-26"],
        ), patch(
            "biz.analysis.sync_analysis_fast._table_exists",
            return_value=True,
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            side_effect=read_sql,
        ):
            result = load_sector_rotation_features(
                object(),
                "2026-08-26",
                decision_known_at="2026-08-26 18:50:00",
            )

        self.assertEqual(result["stock_code"].tolist(), ["000001"])
        self.assertIn("k.received_at <= :decision_known_at", observed["sql"])
        self.assertIn("f.etl_sync_at <= :decision_known_at", observed["sql"])
        self.assertIn(
            "f.etl_sync_at >= TIMESTAMP(f.trade_date, '15:10:00')",
            observed["sql"],
        )
        self.assertEqual(
            observed["params"]["decision_known_at"],
            datetime(2026, 8, 26, 18, 50),
        )
        self.assertEqual(result["membership_proof_sha256"].tolist(), ["d" * 64])

    def test_sector_rotation_keeps_membership_proof_when_flow_factor_blocks(self):
        memberships = pd.DataFrame([{
            "stock_code": "000001",
            "industry_name": "Bank",
            "industry_snapshot_date": "2026-08-31",
            "industry_snapshot_source": "gj_big_qmt_inner",
            "membership_proof_sha256": "d" * 64,
            "industry_source_snapshot_date": "2026-08-27",
            "industry_previous_session_fallback": True,
            "industry_fallback_reason": "MISSED_RELEASE_CAPTURE",
        }])
        observations = pd.DataFrame([{
            "stock_code": "000001",
            "trade_date": "2026-08-31",
            "change_pct": 1.0,
            "amount": 1000,
            "main_net_inflow": 0,
            "flow_etl_sync_at": None,
        }])

        with patch(
            "biz.analysis.sync_analysis_fast._load_sector_industry_memberships",
            return_value=memberships,
        ), patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-31"],
        ), patch(
            "biz.analysis.sync_analysis_fast._table_exists",
            return_value=True,
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=observations,
        ):
            result = load_sector_rotation_features(
                object(),
                "2026-08-31",
                decision_known_at="2026-09-01 09:00:00",
            )

        self.assertEqual(result["stock_code"].tolist(), ["000001"])
        self.assertEqual(result["membership_proof_sha256"].tolist(), ["d" * 64])
        self.assertEqual(result["sector_rotation_score"].tolist(), [55.0])
        self.assertTrue(result["industry_previous_session_fallback"].all())

    def test_membership_snapshot_proof_covers_codes_without_l1_row(self):
        sector = pd.DataFrame([{
            "stock_code": "000001",
            "industry_name": "Bank",
            "sector_rotation_score": 60.0,
            "industry_pit_status": "AVAILABLE",
            "industry_pit_reason": "PIT_EXACT_DATE_QMT_SNAPSHOT",
            "industry_snapshot_date": "2026-08-31",
            "industry_snapshot_source": "gj_big_qmt_inner",
            "membership_proof_sha256": "d" * 64,
            "industry_source_snapshot_date": "2026-08-27",
            "industry_previous_session_fallback": True,
            "industry_fallback_reason": "MISSED_RELEASE_CAPTURE",
        }])

        result = _complete_membership_proof_scope(
            sector,
            ["000001", "001326"],
        ).set_index("stock_code")

        self.assertEqual(result.loc["001326", "membership_proof_sha256"], "d" * 64)
        self.assertEqual(
            result.loc["001326", "industry_pit_reason"],
            "PIT_INDUSTRY_L1_MEMBERSHIP_ABSENT",
        )
        self.assertEqual(result.loc["001326", "sector_rotation_score"], 55.0)

    def test_clamp_score_handles_invalid_values(self):
        self.assertEqual(clamp_score(120), 100.0)
        self.assertEqual(clamp_score(-5), 0.0)
        self.assertEqual(clamp_score(None), 50.0)

    def test_linear_score_maps_range(self):
        self.assertEqual(linear_score(5, 0, 10), 50.0)
        self.assertEqual(linear_score(20, 0, 10), 100.0)
        self.assertEqual(linear_score(None, 0, 10), 50.0)

    def test_notice_title_classification(self):
        result = classify_notice_title("关于公司被立案调查及股份回购计划的公告")
        self.assertGreater(result["critical"], 0)
        self.assertGreater(result["positive"], 0)

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

    def test_analysis_rows_store_missing_optional_dates_as_null(self):
        row = {
            "stock_code": "000001",
            "short_name": "sample",
            "flow_trade_date": "2026-08-31",
            "hot_trade_date": "",
        }

        result = build_analysis_rows(pd.DataFrame([row]), "2026-08-31")

        self.assertEqual(result[0]["flow_trade_date"], "2026-08-31")
        self.assertIsNone(result[0]["hot_trade_date"])

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

    def test_recommend_gate_never_allows_stale_flow_even_with_high_score(self):
        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=99,
            short_term_score=98,
            long_term_score=97,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=88,
            data_quality_flags=["stale_flow"],
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("目标交易日", reason)

    def test_recommendation_rows_keep_soft_risk_in_observation_ledger(self):
        row = {
            "stock_code": "600001",
            "short_name": "candidate",
            "ai_score": 78,
            "long_term_score": 72,
            "short_term_score": 76,
            "quality_score": 79,
            "final_trade_score": 77,
            "entry_score": 68,
            "capital_score": 70,
            "main_wave_score": 65,
            "main_wave_signal": "WATCH",
            "signal_status": "WATCH",
            "recommend_status": "SUSPENDED",
            "recommend_reason": "keep observing",
            "event_risk_level": "LOW",
        }

        rows = build_recommendation_rows(
            pd.DataFrame([row]), "2026-08-11", top_n=80, min_score=62
        )

        self.assertEqual([item["stock_code"] for item in rows], ["600001"])
        self.assertEqual(rows[0]["recommend_status"], "SUSPENDED")
        self.assertEqual(rows[0]["signal_status"], "WATCH")

    def test_missing_upper_history_keeps_ranked_pick_research_only(self):
        row = {
            "stock_code": "600001",
            "short_name": "candidate",
            "ai_score": 88,
            "long_term_score": 82,
            "short_term_score": 86,
            "quality_score": 90,
            "final_trade_score": 89,
            "entry_score": 85,
            "capital_score": 84,
            "main_wave_score": 80,
            "main_wave_signal": "BUY_READY",
            "signal_status": "BUY_READY",
            "recommend_status": "ALLOW",
            "recommend_reason": "ranked candidate",
            "event_risk_level": "LOW",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": 1,
        }

        rows = build_recommendation_rows(
            pd.DataFrame([row]), "2026-08-11", top_n=80, min_score=62
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["candidate_recommend_status"], "SUSPENDED")
        self.assertEqual(rows[0]["recommend_status"], "SUSPENDED")
        self.assertEqual(rows[0]["chase_risk_status"], "DATA_BLOCKED")
        self.assertEqual(rows[0]["candidate_ordinary_buy_eligible"], 0)
        self.assertEqual(rows[0]["ordinary_buy_eligible"], 0)
        self.assertFalse(is_executable_recommendation(rows[0]))

    def test_recommendation_rows_exclude_exit_signal(self):
        row = {
            "stock_code": "600001",
            "short_name": "candidate",
            "ai_score": 90,
            "long_term_score": 85,
            "short_term_score": 88,
            "quality_score": 92,
            "final_trade_score": 91,
            "entry_score": 86,
            "capital_score": 84,
            "main_wave_score": 82,
            "main_wave_signal": "SELL_ALERT",
            "signal_status": "WATCH",
            "recommend_status": "SUSPENDED",
            "recommend_reason": "exit",
            "event_risk_level": "LOW",
        }

        rows = build_recommendation_rows(
            pd.DataFrame([row]), "2026-08-11", top_n=80, min_score=62
        )

        self.assertEqual(rows, [])

    def test_primary_strategy_prefers_highest_qualified_score(self):
        strategy = select_primary_strategy({
            "ultra_short_score": 82,
            "short_term_score": 74,
            "swing_score": 66,
        })
        self.assertEqual(strategy, "ultra_short")

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

    def test_run_batch_emits_progress_events(self):
        progress_events = []
        empty_df = pd.DataFrame()

        with patch("biz.analysis.sync_analysis_fast.load_kline_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_finance", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_flow_features", return_value=(empty_df, "2026-06-13")), \
             patch("biz.analysis.sync_analysis_fast.validate_exact_daily_flow_coverage"), \
             patch("biz.analysis.sync_analysis_fast.load_hot_rank", return_value=(empty_df, "2026-06-13")), \
             patch("biz.analysis.sync_analysis_fast.load_notice_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_confidence_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_recommendation_history", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_failure_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_sector_rotation_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.compute_market_mood", return_value=55.0), \
             patch("biz.analysis.sync_analysis_fast.compute_scores", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast._build_text_fields", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.build_analysis_rows", return_value=[]), \
             patch("biz.analysis.sync_analysis_fast.build_recommendation_rows", return_value=[]), \
             patch("biz.analysis.sync_analysis_fast.save_outputs"):
            from biz.analysis.sync_analysis_fast import run_batch

            stats = run_batch(
                engine=object(),
                trade_date="2026-06-13",
                progress_callback=progress_events.append,
            )

        self.assertEqual(stats.trade_date, "2026-06-13")
        self.assertTrue(progress_events)
        self.assertEqual(progress_events[0]["stage"], "load_kline")
        self.assertEqual(progress_events[-1]["stage"], "done")
        self.assertEqual(progress_events[-1]["analysis_count"], 0)

    def test_publication_requires_exact_decision_time_before_any_input_read(self):
        from biz.analysis.sync_analysis_fast import run_batch

        identity = {
            "publication_run_uid": "a" * 32,
            "publisher_task_type": "analysis_fast",
            "publisher_build_sha": "b" * 40,
        }
        with patch(
            "biz.analysis.sync_analysis_fast._prepare_batch_outputs"
        ) as prepare:
            with self.assertRaisesRegex(RuntimeError, "execution time"):
                run_batch(
                    object(),
                    trade_date="2026-08-27",
                    execution_time=None,
                    **identity,
                )
            with self.assertRaisesRegex(RuntimeError, "execution time"):
                run_batch(
                    object(),
                    trade_date="2026-08-27",
                    execution_time="2026-08-27T10:50:00+00:00",
                    **identity,
                )
        prepare.assert_not_called()

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
             patch("biz.analysis.sync_analysis_fast._prepare_batch_outputs", return_value=([], [], 55.0, "2026-06-26", "2026-06-26")), \
             patch("biz.analysis.sync_analysis_fast.save_outputs"):
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


if __name__ == "__main__":
    unittest.main()
