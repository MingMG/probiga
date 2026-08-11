# -*- coding: utf-8 -*-
import os
import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

from tools import run_single_table


class RunSingleTableTest(unittest.TestCase):
    def test_minute_crawl_honors_scheduler_closed_market_guard(self):
        captured = {}

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            captured["cmd"] = cmd
            return types.SimpleNamespace(returncode=0)

        with patch("tools.run_single_table.subprocess.run", side_effect=fake_run):
            rc = run_single_table._run_minute_crawl(
                "stock",
                {"MINUTE_SKIP_CLOSED": "1"},
            )

        self.assertEqual(rc, 0)
        self.assertIn("--skip-closed", captured["cmd"])

    def test_stock_market_subprocess_defaults_to_full_stock_universe(self):
        captured = {}

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            captured["cmd"] = cmd
            captured["cwd"] = cwd
            captured["env"] = env or {}
            captured["timeout"] = timeout
            return types.SimpleNamespace(returncode=0)

        with patch.dict(run_single_table.os.environ, {}, clear=True), patch(
            "tools.run_single_table.subprocess.run",
            side_effect=fake_run,
        ):
            rc = run_single_table._sub_run_stock_market("stock_current")

        self.assertEqual(rc, 0)
        self.assertEqual(captured["env"]["SM_MAX_STOCKS"], "0")
        self.assertEqual(captured["env"]["SM_MAX_INDEXES"], "0")
        self.assertEqual(captured["env"]["SM_MAX_CONCEPTS"], "0")
        self.assertEqual(captured["timeout"], 2 * 60 * 60)

    def test_qmt_daily_kline_falls_back_to_external_fetch(self):
        calls = []

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            calls.append(cmd)
            return types.SimpleNamespace(returncode=1 if len(calls) == 1 else 0)

        with patch.dict(run_single_table.os.environ, {"DATA_SOURCE_KLINE": "qmt"}, clear=True), patch(
            "tools.run_single_table.subprocess.run",
            side_effect=fake_run,
        ):
            rc = run_single_table._sub_run_kline_daily("2026-07-01")

        self.assertEqual(rc, 0)
        self.assertEqual(calls[0][1:4], ["-m", "biz.stock_market.sync_stock_market", "--only"])
        self.assertIn("tools/fetch_sm_stock_kline_daily.py", calls[1])

    def test_provenance_strict_qmt_daily_kline_does_not_fall_back(self):
        calls = []

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            calls.append(cmd)
            return types.SimpleNamespace(returncode=1)

        with patch.dict(
            run_single_table.os.environ,
            {
                "DATA_SOURCE_KLINE": "bigqmt",
                "QMT_PRIMARY_ALLOW_EXTERNAL_FALLBACK": "0",
            },
            clear=True,
        ), patch(
            "tools.run_single_table.subprocess.run",
            side_effect=fake_run,
        ):
            rc = run_single_table._sub_run_kline_daily("2026-07-27")

        self.assertEqual(rc, 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("stock_kline", calls[0])

    def test_qmt_daily_flow_without_date_falls_back_to_fast_batch_fetch(self):
        calls = []

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            calls.append(cmd)
            return types.SimpleNamespace(returncode=1 if len(calls) == 1 else 0)

        with patch.dict(run_single_table.os.environ, {"DATA_SOURCE_FLOW_DAILY": "qmt"}, clear=True), patch(
            "tools.run_single_table.subprocess.run",
            side_effect=fake_run,
        ):
            rc = run_single_table._sub_run_flow_daily("")

        self.assertEqual(rc, 0)
        self.assertEqual(calls[0][1:4], ["-m", "biz.stock_market.sync_stock_market", "--only"])
        self.assertIn("tools/crawl_realtime_batch.py", calls[1])
        self.assertIn("--only", calls[1])
        self.assertIn("flow", calls[1])

    def test_qmt_minute_date_is_passed_through_child_environment(self):
        captured = {}

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            captured["cmd"] = cmd
            captured["env"] = env or {}
            return types.SimpleNamespace(returncode=0)

        with patch.dict(run_single_table.os.environ, {"DATA_SOURCE_MINUTE": "qmt"}, clear=True), patch(
            "tools.run_single_table.subprocess.run",
            side_effect=fake_run,
        ):
            rc = run_single_table._sub_run_minute("stock", "2026-07-17")

        self.assertEqual(rc, 0)
        self.assertEqual(captured["env"]["MYQUANT_MINUTE_DATE"], "2026-07-17")
        self.assertIn("stock_minute", captured["cmd"])

    def test_provenance_strict_qmt_minute_does_not_fall_back(self):
        calls = []

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            calls.append(cmd)
            return types.SimpleNamespace(returncode=1)

        with patch.dict(
            run_single_table.os.environ,
            {
                "DATA_SOURCE_MINUTE": "bigqmt",
                "QMT_PRIMARY_ALLOW_EXTERNAL_FALLBACK": "0",
            },
            clear=True,
        ), patch(
            "tools.run_single_table.subprocess.run",
            side_effect=fake_run,
        ):
            rc = run_single_table._sub_run_minute("stock", "2026-07-27")

        self.assertEqual(rc, 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("stock_minute", calls[0])

    def test_concept_east_current_uses_reference_source_and_full_sync(self):
        captured = {}

        def fake_sub_run(only, extra_args=None):
            captured["only"] = only
            captured["extra_args"] = extra_args
            captured["source"] = run_single_table.os.environ.get(
                "DATA_SOURCE_CONCEPT_CURRENT"
            )
            return 0

        with patch.dict(
            run_single_table.os.environ,
            {
                "DATA_SOURCE_CONCEPT_LIST": "bigqmt",
                "DATA_SOURCE_CONCEPT_CURRENT": "east",
            },
            clear=True,
        ), patch(
            "tools.run_single_table._sub_run_stock_market",
            side_effect=fake_sub_run,
        ):
            rc = run_single_table._run_one_table("sm_concept_east_current")

        self.assertEqual(rc, 0)
        self.assertEqual(captured["only"], "concept_east_current")
        self.assertEqual(captured["source"], "bigqmt")

    def test_a_list_date_is_passed_to_sentiment_child_env(self):
        captured = {}

        def fake_run(cmd, cwd=None, env=None, timeout=None):
            captured["cmd"] = cmd
            captured["env"] = env or {}
            return types.SimpleNamespace(returncode=0)

        with patch("tools.run_single_table.subprocess.run", side_effect=fake_run):
            rc = run_single_table._run_one_table("st_a_list_daily", date_str="2026-07-08")

        self.assertEqual(rc, 0)
        self.assertEqual(captured["env"]["SE_A_LIST_DATE"], "2026-07-08")
        self.assertEqual(captured["cmd"][1:4], ["-m", "biz.sentiment.sync_sentiment", "--only"])

    def test_qmt_index_kline_defaults_to_latest_trade_date_only(self):
        with patch.dict(
            run_single_table.os.environ,
            {"DATA_SOURCE_INDEX_KLINE": "qmt"},
            clear=True,
        ), patch(
            "tools.run_single_table._latest_trade_date",
            return_value="2026-07-17",
        ), patch(
            "tools.run_single_table._sub_run_stock_market",
            return_value=0,
        ) as run_market:
            rc = run_single_table._run_one_table("sm_index_kline")

        self.assertEqual(rc, 0)
        run_market.assert_called_once_with(
            "index_kline",
            extra_args=["--kline-start", "2026-07-17", "--kline-end", "2026-07-17"],
        )

    def test_qmt_index_kline_failure_falls_back_to_external_source(self):
        seen_sources = []

        def fake_run(*_args, **_kwargs):
            seen_sources.append(run_single_table.os.environ.get("DATA_SOURCE_INDEX_KLINE"))
            return 1 if len(seen_sources) == 1 else 0

        with patch.dict(
            run_single_table.os.environ,
            {"DATA_SOURCE_INDEX_KLINE": "qmt"},
            clear=True,
        ), patch(
            "tools.run_single_table._latest_trade_date",
            return_value="2026-07-17",
        ), patch(
            "tools.run_single_table._sub_run_stock_market",
            side_effect=fake_run,
        ):
            rc = run_single_table._run_one_table("sm_index_kline")

        self.assertEqual(rc, 0)
        self.assertEqual(seen_sources, ["qmt", "tencent"])

    def test_si_index_constituent_uses_batch_db_helpers(self):
        fake_sync_stock_info = types.ModuleType("biz.stock_info.sync_stock_info")
        engine = object()
        info = object()
        frame = pd.DataFrame([{"index_code": "000001"}])
        calls = []

        fake_sync_stock_info.load_info = lambda: info

        def fake_run_ddl(eng):
            self.assertEqual(os.environ["SI_SKIP_GLOBAL_TRUNCATE"], "1")
            self.assertEqual(os.environ["SI_SYNC_SKIP_ALL_CODE"], "1")
            calls.append(("ddl", eng))

        fake_sync_stock_info.run_ddl = fake_run_ddl
        fake_sync_stock_info.sync_all_index_code = lambda eng, loaded_info: pd.DataFrame()
        fake_sync_stock_info.sync_index_constituent = lambda eng, loaded_info, df: calls.append(
            ("sync", eng, loaded_info, df)
        )

        with patch.dict(run_single_table.os.environ, {}, clear=True), patch.dict(
            sys.modules,
            {"biz.stock_info.sync_stock_info": fake_sync_stock_info},
        ), patch(
            "tools.run_single_table.create_batch_engine",
            return_value=engine,
        ) as create_batch_engine, patch("tools.run_single_table.read_frame", return_value=frame) as read_frame:
            rc = run_single_table.run_si_index_constituent()

        self.assertEqual(rc, 0)
        create_batch_engine.assert_called_once_with()
        read_frame.assert_called_once()
        self.assertIn("si_all_index_code", str(read_frame.call_args.args[0]))
        self.assertIs(read_frame.call_args.args[1], engine)
        self.assertEqual(calls[0], ("ddl", engine))
        self.assertEqual(calls[1][0], "sync")
        self.assertIs(calls[1][1], engine)
        self.assertIs(calls[1][2], info)
        self.assertIs(calls[1][3], frame)
        self.assertNotIn("SI_SKIP_GLOBAL_TRUNCATE", os.environ)
        self.assertNotIn("SI_SYNC_SKIP_ALL_CODE", os.environ)


if __name__ == "__main__":
    unittest.main()
