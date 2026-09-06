#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ensure reliability-focused scheduled tasks exist.

This script is intentionally non-destructive: it validates the privileged-
installed scheduler schema and only upserts quality-gate task rows used to
catch stale data before dashboard or paper-trading workflows trust it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, inspect, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.config import get_mysql_url
from server.common.engine_factory import create_pooled_engine
from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)
from server.common.release_data_readiness_contract import (
    RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES,
    RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES,
    RELEASE_DATA_READINESS_TASK_TYPES,
    release_catchup_closed_ready_time,
)
from server.common.scheduler_validation import (
    scheduler_output_status,
    validate_scheduler_task_result,
)
from server.common.scheduler_tasks import (
    ensure_scheduler_columns as validate_scheduler_columns,
)
from tools.final_pool_delivery_task_contract import (
    TASK as FINAL_POOL_DELIVERY_TASK,
)
from tools.qmt_announcement_task_contract import (
    ANALYSIS_FAST_CRON,
    TASK as QMT_ANNOUNCEMENT_TASK,
)
from tools.qmt_host_ownership_contract import (
    ANALYSIS_UPPER_EVIDENCE_TASK,
    ETF_FORWARD_DAILY_TASK,
    LINUX_PROVIDER_TASKS,
    QMT_CANONICAL_HISTORY_GAP_REPAIR_TASK,
    QMT_CATALOG_CAPABILITY_TASK,
    QMT_INDEX_CURRENT_TASK,
    QMT_INDEX_KLINE_TASK,
    QMT_INDEX_MINUTE_TASK,
    QMT_INTRADAY_REALTIME_TASK,
    QMT_MEMBERSHIP_SNAPSHOT_TASK,
    QMT_STOCK_DAILY_CANONICAL_TASK,
    QMT_STOCK_MINUTE_CANONICAL_TASK,
    QMT_STOCK_MINUTE_FLOW_CANONICAL_TASK,
)
from tools.qmt_operations_task_contract import TASKS as QMT_OPERATIONS_TASKS
from tools.add_trading_v3_tasks import TASKS as TRADING_V3_TASKS


SCHEDULER_COLUMNS = {
    "task_type": "VARCHAR(50) DEFAULT 'python'",
    "group_name": "VARCHAR(32) DEFAULT 'system'",
    "script_args": "VARCHAR(500) DEFAULT ''",
    "date_param": "VARCHAR(100) DEFAULT ''",
    "date_param_desc": "VARCHAR(200) DEFAULT ''",
    "interval_minutes": "INT DEFAULT 0",
    "sort_order": "INT DEFAULT 0",
    "last_triggered_at": "DATETIME DEFAULT NULL",
    "last_run_output": "TEXT DEFAULT NULL",
    "last_run_duration": "INT DEFAULT 0",
    "etl_sync_at": "DATETIME DEFAULT NULL",
    "updated_at": "DATETIME DEFAULT NULL",
    "description": "VARCHAR(500) DEFAULT ''",
}
REVIEW_DELIVERY_TASK_TYPES = frozenset(
    {
        "qmt_membership_snapshot",
        "news_daily",
        "daily_review",
        "evening_review",
    }
)
CORE_REQUIRED_DATA_TASK_CONTRACT_TYPES = frozenset(
    {
        "stock_finance",
        "notice_eastmoney",
        "notice_eastmoney_historical_repair",
        "stock_dividend_baidu",
    }
)
DAILY_STRATEGY_PIPELINE_TASK_CONTRACT_TYPES = frozenset(
    {
        "qmt_stock_daily_canonical",
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
        "analysis_fast",
        "strategy_external_overlay",
        # These legacy publishers remain present but disabled. Omitting them
        # would allow an old deployment row to overwrite the verified pool.
        "analysis_morning_strict",
        "analysis_premarket_external",
    }
)
REQUIRED_DATA_TASK_CONTRACT_TYPES = frozenset(
    CORE_REQUIRED_DATA_TASK_CONTRACT_TYPES
    | DAILY_STRATEGY_PIPELINE_TASK_CONTRACT_TYPES
)
# Backward compatibility for release scripts that still pass the old CLI
# spelling.  This set proves scheduler-row ownership only; it never proves
# that a data backfill completed.
REQUIRED_DATA_COMPLETION_TASK_TYPES = REQUIRED_DATA_TASK_CONTRACT_TYPES
NOTICE_HISTORY_LEDGER_PATH = (
    "/var/lib/probiga/jobs/notice-eastmoney-history-repair-v1.json"
)

RELEASE_DATA_READINESS_DEFAULT_MAX_AGE = timedelta(hours=36)
RELEASE_DATA_READINESS_MAX_AGE_BY_TASK = {
    "news_sync": timedelta(minutes=30),
    "notice_eastmoney_historical_repair": timedelta(minutes=45),
}
RELEASE_STRATEGY_INPUT_SESSION_COUNT = 5
RELEASE_VALIDATION_EVIDENCE_SCHEMA = (
    "probiga.scheduler-validation-evidence.v1"
)
_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")

NOW_COLUMNS = {"created_at", "updated_at", "etl_sync_at"}
INTRADAY_ALERT_TASK_TYPES = frozenset({"intraday_market_alert"})
OPT_IN_TASK_TYPES = INTRADAY_ALERT_TASK_TYPES
TASK_PAYLOAD_COLUMNS = frozenset(
    {
        "task_name",
        "task_type",
        "group_name",
        "script_path",
        "script_args",
        "cron_time",
        "interval_minutes",
        "enabled",
        "description",
        "sort_order",
        "date_param",
    }
)
REVIEW_DELIVERY_RUNTIME_COLUMNS = frozenset(
    {
        "task_type",
        "script_path",
        "script_args",
        "cron_time",
        "interval_minutes",
        "enabled",
        "date_param",
    }
)
LEGACY_CAPITAL_FLOW_BATCH_TASK = {
    "task_name": "盘后快速资金流同步",
    "task_type": "capital_flow_batch_fast",
    "group_name": "系统管理",
    "script_path": "tools/crawl_realtime_batch.py",
    "script_args": "--only flow --min-coverage 0.70 --json",
    "cron_time": "15:20",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 84,
    "date_param": "",
    "description": "盘后用东财全市场批量接口快速补齐最新交易日资金流，作为逐股慢任务前置保障。",
}
DIRECT_CAPITAL_FLOW_BATCH_TASK = {
    "task_name": "国金 QMT 日资金流验收",
    "task_type": "capital_flow_batch_fast",
    "group_name": "系统管理",
    "script_path": "tools/verify_direct_capital_flow_daily.py",
    "script_args": "--json",
    "cron_time": "15:45",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 84,
    "date_param": "",
    "description": "只读验收 Windows 国金 QMT 已原子发布的当日资金流分区；不联网、不写表。",
}
DERIVED_MARKET_TASKS = (
    {
        "task_name": "同花顺热门概念",
        "task_type": "hot_concept",
        "group_name": "热门数据",
        "script_path": "tools/fetch_hot_concept_ths_daily.py",
        "script_args": "",
        "cron_time": "17:10",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 11,
        "date_param": "",
        "description": "同花顺当日热门概念/行业完整双板块快照；任一板块缺失即失败关闭。",
    },
    {
        "task_name": "同花顺热股TOP100",
        "task_type": "hot_rank_ths",
        "group_name": "热门数据",
        "script_path": "tools/fetch_hot_rank_ths.py",
        "script_args": "",
        "cron_time": "17:12",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 12,
        "date_param": "",
        "description": "同花顺当日热股榜专用同步；严格绑定执行日快照。",
    },
    {
        "task_name": "东财人气榜TOP100",
        "task_type": "hot_pop_east",
        "group_name": "热门数据",
        "script_path": "tools/fetch_hot_pop_rank_east.py",
        "script_args": "",
        "cron_time": "17:14",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 13,
        "date_param": "",
        "description": "东方财富当日个股人气榜专用同步；禁止复用旧日期结果。",
    },
    {
        "task_name": "新浪关注度榜（来源语义不可验证，已停用）",
        "task_type": "hot_rank_sina",
        "group_name": "热门数据",
        "script_path": "tools/fetch_hot_rank_sina.py",
        "script_args": "",
        "cron_time": "17:16",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 14,
        "date_param": "",
        "description": (
            "新浪行情接口不返回可验证的关注度字段，永久失败关闭；"
            "保留禁用任务只用于显示来源不可用原因，不参与融合或发布门禁。"
        ),
    },
    {
        "task_name": "融合榜单(当天)",
        "task_type": "hot_fused",
        "group_name": "热门数据",
        "script_path": "tools/merge_hot_rank.py",
        "script_args": "--top 100",
        "cron_time": "17:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 15,
        "date_param": "",
        "description": "仅在当日至少两个独立来源完整后生成当日融合榜。",
    },
    {
        "task_name": "融合榜单(3天)",
        "task_type": "hot_fused_3",
        "group_name": "热门数据",
        "script_path": "tools/merge_hot_rank.py",
        "script_args": "--top 100 --days 3",
        "cron_time": "17:22",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 16,
        "date_param": "",
        "description": "基于三个完整同日多源快照生成三日融合榜；缺源即失败关闭。",
    },
    {
        "task_name": "融合榜单(5天)",
        "task_type": "hot_fused_5",
        "group_name": "热门数据",
        "script_path": "tools/merge_hot_rank.py",
        "script_args": "--top 100 --days 5",
        "cron_time": "17:24",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 17,
        "date_param": "",
        "description": "基于五个完整同日多源快照生成五日融合榜；缺源即失败关闭。",
    },
    {
        "task_name": "全市场每日概览刷新",
        "task_type": "market_overview_daily",
        "group_name": "系统管理",
        "script_path": "tools/refresh_market_overview_daily.py",
        "script_args": "",
        "cron_time": "18:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 85,
        "date_param": "",
        "description": "按不可变股票目录与完整日K生成当日全市场概览；缺口时失败关闭。",
    },
    {
        "task_name": "全市场股票每日快照",
        "task_type": "stock_snapshot_daily",
        "group_name": "系统管理",
        "script_path": "biz/stock_market/sync_stock_snapshot.py",
        "script_args": "",
        "cron_time": "18:25",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 86,
        "date_param": "",
        "description": "按不可变股票目录、完整日K和资金流生成当日全市场快照；禁止部分发布。",
    },
)
TASKS = [
    dict(QMT_ANNOUNCEMENT_TASK),
    dict(QMT_CATALOG_CAPABILITY_TASK),
    dict(QMT_INDEX_CURRENT_TASK),
    dict(QMT_INDEX_KLINE_TASK),
    dict(QMT_INDEX_MINUTE_TASK),
    dict(QMT_STOCK_DAILY_CANONICAL_TASK),
    dict(QMT_STOCK_MINUTE_CANONICAL_TASK),
    dict(QMT_STOCK_MINUTE_FLOW_CANONICAL_TASK),
    dict(QMT_CANONICAL_HISTORY_GAP_REPAIR_TASK),
    dict(ANALYSIS_UPPER_EVIDENCE_TASK),
    dict(ETF_FORWARD_DAILY_TASK),
    *(dict(task) for task in LINUX_PROVIDER_TASKS),
    *(dict(task) for task in DERIVED_MARKET_TASKS),
    {
        "task_name": "东财公告全历史错配修复",
        "task_type": "notice_eastmoney_historical_repair",
        "group_name": "资讯公告",
        "script_path": "biz/notice/sync_notice_em.py",
        "script_args": (
            "--mode historical-repair --from-si-all-code --limit 0 "
            f"--history-state-file {NOTICE_HISTORY_LEDGER_PATH} "
            "--history-shard-size 250 --page-size 100 --max-pages 1000 "
            "--sleep 0.15"
        ),
        "cron_time": "00:05",
        "interval_minutes": 5,
        "enabled": 1,
        "sort_order": 33,
        "date_param": "",
        "description": (
            "每5分钟恢复下一段当前目录与历史公告代码并集（单批最多250只）；"
            "PROGRESS失败重试，"
            "仅整池hash账本COMPLETE成功；代码集增加时保留旧COMPLETE代次，"
            "新建不可变代次并只抓新增代码。"
        ),
    },
    {
        "task_name": "东财个股公告同步",
        "task_type": "notice_eastmoney",
        "group_name": "资讯公告",
        "script_path": "biz/notice/sync_notice_em.py",
        "script_args": (
            "--mode incremental --from-si-all-code --limit 0 "
            "--lookback-days 45 --forward-days 1 --page-size 100 "
            "--max-pages 1000 --sleep 0.15 --min-coverage 1.00 "
            "--min-row-coverage 0.00"
        ),
        "cron_time": "20:15",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 34,
        "date_param": "",
        "description": (
            "按 si_all_code 完整目录逐股精确同步东财公告日常日期窗；"
            "合法空也原子清理窗口旧错配，历史修复由可恢复分片批次执行。"
        ),
    },
    {
        "task_name": "全市场股票财务PIT同步",
        "task_type": "stock_finance",
        "group_name": "资讯公告",
        "script_path": "biz/stock_finance/sync_finance.py",
        "script_args": (
            "--daily-incremental --workers 4 --sleep 0.3 "
            "--min-code-coverage 1.0 --checkpoint-file "
            "/var/lib/probiga/jobs/stock-finance-daily-v2.json"
        ),
        "cron_time": "21:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 35,
        "date_param": "",
        "description": (
            "复用昨日不可变全市场封印，仅以4路有界并发刷新公告、目录、"
            "失败、缺口、到期复核与来源版本变化候选；按目标日+股票+输入"
            "根断点续跑，并发布父根+增量根的新PIT封印。"
        ),
    },
    {
        "task_name": "股票财务历史基线独立修复",
        "task_type": "stock_finance_historical_repair",
        "group_name": "资讯公告",
        "script_path": "biz/stock_finance/sync_finance.py",
        "script_args": (
            "--full-baseline --limit 0 --workers 4 --sleep 0.3 "
            "--min-code-coverage 1.0"
        ),
        "cron_time": "00:30",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 36,
        "date_param": "",
        "description": (
            "仅在不可变基线缺失、法定报告期整体推进或确认的历史缺口修复"
            "窗口中人工启用；不属于正常日链路，也不阻塞早晨结果。"
        ),
    },
    {
        "task_name": "自选股新浪腾讯双源行情",
        "task_type": "portfolio_quote_refresh",
        "group_name": "盘中交易",
        "script_path": "tools/run_portfolio_quote_refresh.py",
        "script_args": "",
        "cron_time": "09:25",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 69,
        "date_param": "",
        "description": (
            "交易时段每分钟仅刷新自选股；新浪和腾讯逐股一致后原子发布，"
            "与QMT和全市场同步任务隔离。"
        ),
    },
    {
        "task_name": "盘中实时行情同步",
        "task_type": "intraday_realtime",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_realtime_batch.py",
        "script_args": "--only snapshot --min-coverage 0.70 --archive-snapshot --skip-closed --json",
        "cron_time": "09:25",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 70,
        "date_param": "",
        "description": "交易时段每分钟刷新 sm_stock_current，并归档到 sm_rt_quote_snapshot；覆盖率不足时失败。",
    },
    {
        "task_name": "QMT故障公共多源行情替补",
        "task_type": "public_quote_failover",
        "group_name": "盘中交易",
        "script_path": "tools/run_public_quote_failover.py",
        "script_args": "",
        "cron_time": "09:25",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 71,
        "date_param": "",
        "description": (
            "QMT健康时跳过；QMT异常时每分钟采集新浪和腾讯全市场行情，"
            "仅双源一致且全市场质量门禁通过的原子快照可供自选股与交易读取。"
        ),
    },
    dict(QMT_INTRADAY_REALTIME_TASK),
    {
        "task_name": "盘中分钟K线同步",
        "task_type": "intraday_minute_kline",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": "--type stock --min-coverage 0.70 --skip-closed",
        "cron_time": "09:35",
        "interval_minutes": 15,
        "enabled": 1,
        "sort_order": 72,
        "date_param": "",
        "description": "交易时段周期性同步全市场分钟K线；覆盖率不足时失败。",
    },
    {
        "task_name": "盘中分钟资金流同步",
        "task_type": "intraday_minute_flow",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": "--type flow --min-coverage 0.50 --skip-closed",
        "cron_time": "09:40",
        "interval_minutes": 15,
        "enabled": 1,
        "sort_order": 74,
        "date_param": "",
        "description": "交易时段周期性同步分钟资金流；覆盖率不足时失败。",
    },
    {
        "task_name": "盘中实时质量体检",
        "task_type": "intraday_quality_check",
        "group_name": "盘中交易",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json --include-realtime --fail-on-warn --skip-closed",
        "cron_time": "09:45",
        "interval_minutes": 5,
        "enabled": 1,
        "sort_order": 76,
        "date_param": "",
        "description": "交易时段严格检查实时、分钟、资金流基础数据；非交易时段跳过。",
    },
    {
        "task_name": "盘中模拟交易执行Tick",
        "task_type": "sim_trade",
        "group_name": "盘中交易",
        "script_path": "biz/analysis/sync_sim_trade.py",
        "script_args": "--tick --skip-outside-intraday --json",
        "cron_time": "09:31",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 78,
        "date_param": "",
        "description": "事件驱动模拟交易tick：买入只执行信号池NEW信号，卖出做风控检查；非盘中时段快速跳过。",
    },
    {
        "task_name": "盘前模拟交易信号池准备",
        "task_type": "sim_trade_signal_prepare",
        "group_name": "盘中交易",
        "script_path": "biz/analysis/sync_sim_trade.py",
        "script_args": "--prepare-signals --reset --json",
        "cron_time": "09:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 77,
        "date_param": "",
        "description": "开盘前按上一交易日允许推荐重建今日模拟信号池；代码与策略身份、决策计数及数据库回读必须完全一致。",
    },
    {
        "task_name": "09:20盘前候选竞价确认",
        "task_type": "premarket_theme_auction_confirmation",
        "group_name": "交易决策",
        "script_path": "tools/run_premarket_auction_confirmation.py",
        "script_args": "--push --json",
        "cron_time": "09:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 78,
        "date_param": "",
        "description": "09:20只对09:08已冻结候选追加竞价确认、追高否决和数据门禁，单独落库并推送早报机器人，绝不回写09:08原始排名。",
    },
    {
        "task_name": "盘前生产候选榜自动交付",
        "task_type": "screener_premarket_delivery",
        "group_name": "交易决策",
        "script_path": "tools/run_screener_delivery.py",
        "script_args": "--preset capital_support --top 100 --json",
        "cron_time": "09:08",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 78,
        "date_param": "",
        "description": "09:08使用开盘前已知数据生成盘前生产候选榜；结果先固定落库，再将前五名主动发送到早报机器人。错过执行时间会在当日上午自动补跑。",
    },
    {
        "task_name": "开盘生产融合候选榜自动交付",
        "task_type": "screener_intraday_delivery",
        "group_name": "交易决策",
        "script_path": "tools/run_screener_delivery.py",
        "script_args": "--preset intraday_sector --top 100 --json",
        "cron_time": "09:32",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 79,
        "date_param": "",
        "description": "09:32基于当日全市场快照生成V3/V4/V5/V6生产融合榜；结果先固定落库，再将前五名主动发送到早报机器人。错过执行时间会在当日上午自动补跑。",
    },
    dict(LEGACY_CAPITAL_FLOW_BATCH_TASK),
    {
        "task_name": "采集自动完整性巡检",
        "task_type": "acquisition_quality_check",
        "group_name": "系统管理",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--acquisition --json --fail-on-warn",
        "cron_time": "00:00",
        "interval_minutes": 15,
        "enabled": 1,
        "sort_order": 87,
        "date_param": "",
        "description": (
            "每15分钟只读核对交易日历、两端心跳、近期日线/资金流漏日和"
            "当日资金流范围；周末及策略阻塞时继续检查，FAIL/WARN保留到调度历史。"
            "仅观察采集缺口，不授予发布或交易权限。"
        ),
    },
    {
        "task_name": "盘前数据质量体检",
        "task_type": "quality_check_pre",
        "group_name": "系统管理",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json --fail-on-warn",
        "cron_time": "08:45",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 88,
        "date_param": "",
        "description": "盘前只读体检；有 WARN/FAIL 时任务失败，提醒不要信任过期推荐。",
    },
    *(dict(task) for task in QMT_OPERATIONS_TASKS),
    {
        "task_name": "盘后快速分析推荐",
        "task_type": "analysis_fast",
        "group_name": "系统管理",
        "script_path": "tools/run_ai_recommendation_premarket.py",
        "script_args": "--top-n 80 --min-score 62 --json",
        "cron_time": ANALYSIS_FAST_CRON,
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 90,
        "date_param": "",
        "description": (
            "在22:20固定上海PIT截止之后，复算与Windows不可变upper证据"
            "相同的有序Top80，再原子生成、验证并激活正式推荐票池。"
        ),
    },
    {
        "task_name": "AI推荐盘前严格生成",
        "task_type": "analysis_morning_strict",
        "group_name": "AI推荐",
        "script_path": "tools/run_ai_recommendation_premarket.py",
        "script_args": "--strict-prev-trade-day --top-n 80 --min-score 62 --min-kline-coverage 0.80 --json",
        "cron_time": "08:30",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 91,
        "date_param": "",
        "description": "已停用：canonical票池仅由22:20固定PIT流水线发布；早盘不得用不同cutoff覆盖前夜已验证票池。",
    },
    {
        "task_name": "08:30外盘评分修正及票池更新",
        "task_type": "strategy_external_overlay",
        "group_name": "AI推荐",
        "script_path": "tools/run_strategy_external_overlay.py",
        "script_args": "--json",
        "cron_time": "08:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 92,
        "date_param": "",
        "description": (
            "单次采集隔夜美股及早盘日韩市场，对前夜已验证候选做±3分封顶"
            "的全局风险修正并生成治理修订；缺失按中性处理，不触发QMT补抓。"
        ),
    },
    {
        "task_name": "AI推荐09:08盘前主线预判",
        "task_type": "analysis_premarket_external",
        "group_name": "AI推荐",
        "script_path": "tools/run_ai_recommendation_premarket.py",
        "script_args": "--strict-prev-trade-day --external-market --theme-forecast --push-theme-forecast --theme-top-n 12 --theme-stocks-per-theme 5 --top-n 80 --min-score 62 --min-kline-coverage 0.80 --json",
        "cron_time": "09:07",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 93,
        "date_param": "",
        "description": "已停用旧的一体化publisher：主题早报需拆为只读交付任务；不得在缺少同cutoff upper证据时覆盖canonical票池。",
    },
    {
        "task_name": "盘后数据质量体检",
        "task_type": "quality_check_post",
        "group_name": "系统管理",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json --fail-on-warn",
        "cron_time": "19:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 93,
        "date_param": "",
        "description": "盘后只读体检；确认采集、分析、推荐和调度链路是否跟上最新交易日。",
    },
    {
        "task_name": "Intraday key market event alerts",
        "task_type": "intraday_market_alert",
        "group_name": "Intraday alerts",
        "script_path": "tools/run_intraday_market_alert.py",
        "script_args": "--mode shadow --json",
        "cron_time": "09:25",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 95,
        "date_param": "",
        "description": "Linux-owned event-driven market, sector, key-stock, style rotation, and broad-index flow alert evaluator.",
    },
    dict(QMT_MEMBERSHIP_SNAPSHOT_TASK),
    {
        "task_name": "A股早报推送",
        "task_type": "news_daily",
        "group_name": "资讯公告",
        "script_path": "biz/early_briefing/generate.py",
        "script_args": "",
        "cron_time": "08:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 93,
        "date_param": "",
        "description": "工作日生成并严格投递早报；缺配置或企微未确认成功时任务失败并在当天补跑。",
    },
    {
        "task_name": "盘后量化复盘",
        "task_type": "daily_review",
        "group_name": "系统管理",
        "script_path": "biz/review/generate.py",
        "script_args": "",
        "cron_time": "18:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 96,
        "date_param": "",
        "description": "生成三段式量化复盘并写库；数据未通过门禁时失败并在当天持续补跑。",
    },
    {
        "task_name": "A股晚报推送",
        "task_type": "evening_review",
        "group_name": "资讯公告",
        "script_path": "biz/evening_review/generate.py",
        "script_args": "",
        "cron_time": "20:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 97,
        "date_param": "",
        "description": "投递已通过质量门禁的三段式量化复盘；数据未就绪或企微未确认时失败并在当天补跑。",
    },
    dict(FINAL_POOL_DELIVERY_TASK),
]


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    try:
        return {
            str(column["name"])
            for column in inspect(engine).get_columns(table_name)
        }
    except Exception:
        # Some MySQL accounts intentionally cannot use SHOW FULL COLUMNS even
        # though information_schema metadata is available to them.
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :table_name
            """), {"table_name": table_name}).fetchall()
        return {str(row[0]) for row in rows}


def ensure_scheduler_columns(engine: Engine) -> None:
    validate_scheduler_columns(
        engine,
        table_name="st_scheduled_tasks",
        column_definitions=SCHEDULER_COLUMNS,
    )


def _task_payload(task: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    return {
        key: value
        for key, value in task.items()
        if key in TASK_PAYLOAD_COLUMNS and key in columns
    }


def upsert_task(engine: Engine, task: dict[str, Any]) -> str:
    columns = _table_columns(engine, "st_scheduled_tasks")
    payload = _task_payload(task, columns)
    if not payload:
        raise RuntimeError("no compatible scheduler columns found")

    with engine.begin() as conn:
        lock_clause = " FOR UPDATE" if conn.dialect.name == "mysql" else ""
        existing_rows = [
            dict(row)
            for row in conn.execute(
                text(f"""
                    SELECT id, script_path
                    FROM st_scheduled_tasks
                    WHERE task_name = :task_name
                       OR task_type = :task_type
                    ORDER BY id{lock_clause}
                """),
                {"task_name": task["task_name"], "task_type": task["task_type"]},
            ).mappings()
        ]
        if len(existing_rows) > 1:
            raise RuntimeError(
                "duplicate scheduler task identity for "
                f"{task['task_type']}: ids={[row['id'] for row in existing_rows]}"
            )
        if (
            task["task_type"] == "capital_flow_batch_fast"
            and existing_rows
            and existing_rows[0].get("script_path")
            == DIRECT_CAPITAL_FLOW_BATCH_TASK["script_path"]
        ):
            # Preserve the explicitly selected verifier mode across later
            # full ensures.  Data readiness remains a separate fail-closed
            # verifier/history/partition check.
            task = DIRECT_CAPITAL_FLOW_BATCH_TASK
            payload = _task_payload(task, columns)
        existing_id = existing_rows[0]["id"] if existing_rows else None

        if existing_id:
            assignments = ", ".join(f"`{key}` = :{key}" for key in payload)
            if "updated_at" in columns:
                assignments += ", `updated_at` = NOW()"
            conn.execute(
                text(f"UPDATE st_scheduled_tasks SET {assignments} WHERE id = :id"),
                {**payload, "id": existing_id},
            )
            return "updated"

        insert_payload = dict(payload)
        for column in NOW_COLUMNS:
            if column in columns:
                insert_payload[column] = None
        names = ", ".join(f"`{key}`" for key in insert_payload)
        values = ", ".join("NOW()" if key in NOW_COLUMNS else f":{key}" for key in insert_payload)
        bind_payload = {k: v for k, v in insert_payload.items() if k not in NOW_COLUMNS}
        conn.execute(text(f"INSERT INTO st_scheduled_tasks ({names}) VALUES ({values})"), bind_payload)
        return "inserted"


def validate_review_delivery(engine: Engine) -> dict[str, str]:
    """Fail closed unless all review/delivery task rows match this release."""

    columns = _table_columns(engine, "st_scheduled_tasks")
    required_columns = REVIEW_DELIVERY_RUNTIME_COLUMNS | {"id"}
    missing_columns = required_columns - columns
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise RuntimeError(f"scheduler validation is missing columns: {missing}")

    expected = {
        str(task["task_type"]): {
            key: task[key] for key in REVIEW_DELIVERY_RUNTIME_COLUMNS
        }
        for task in TASKS
        if task["task_type"] in REVIEW_DELIVERY_TASK_TYPES
    }
    statement = text(
        "SELECT id, task_type, script_path, script_args, cron_time, "
        "interval_minutes, enabled, date_param "
        "FROM st_scheduled_tasks WHERE task_type IN :task_types ORDER BY task_type"
    ).bindparams(bindparam("task_types", expanding=True))
    with engine.connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                statement,
                {"task_types": sorted(REVIEW_DELIVERY_TASK_TYPES)},
            ).mappings()
        ]
    actual: dict[str, dict[str, Any]] = {}
    for row in rows:
        task_type = str(row["task_type"])
        if task_type in actual:
            raise RuntimeError(f"duplicate scheduler task type: {task_type}")
        actual[task_type] = row

    missing_tasks = REVIEW_DELIVERY_TASK_TYPES - set(actual)
    if missing_tasks:
        raise RuntimeError(
            "missing review delivery scheduler tasks: "
            + ", ".join(sorted(missing_tasks))
        )
    for task_type, expected_payload in expected.items():
        row = actual[task_type]
        drift = {
            key: (row.get(key), expected_value)
            for key, expected_value in expected_payload.items()
            if row.get(key) != expected_value
        }
        if drift:
            fields = ", ".join(sorted(drift))
            raise RuntimeError(f"scheduler task {task_type} drifted fields: {fields}")
    return {task_type: "validated" for task_type in sorted(expected)}


def validate_required_task_contracts(engine: Engine) -> dict[str, str]:
    """Verify exact scheduler rows; this is not data-completion evidence."""

    columns = _table_columns(engine, "st_scheduled_tasks")
    required_columns = TASK_PAYLOAD_COLUMNS | {"id"}
    missing_columns = required_columns - columns
    if missing_columns:
        raise RuntimeError(
            "required task contract validation is missing columns: "
            + ", ".join(sorted(missing_columns))
        )
    expected = {
        str(task["task_type"]): {
            key: task[key] for key in TASK_PAYLOAD_COLUMNS
        }
        for task in TASKS
        if task["task_type"] in REQUIRED_DATA_COMPLETION_TASK_TYPES
    }
    statement = text(
        "SELECT id, task_name, task_type, group_name, script_path, script_args, "
        "cron_time, interval_minutes, enabled, description, sort_order, date_param "
        "FROM st_scheduled_tasks WHERE task_type IN :task_types "
        "OR task_name IN :task_names ORDER BY id"
    ).bindparams(
        bindparam("task_types", expanding=True),
        bindparam("task_names", expanding=True),
    )
    with engine.connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                statement,
                {
                    "task_types": sorted(expected),
                    "task_names": sorted(
                        item["task_name"] for item in expected.values()
                    ),
                },
            ).mappings()
        ]
    actual: dict[str, dict[str, Any]] = {}
    for row in rows:
        task_type = str(row.get("task_type") or "")
        if task_type not in expected:
            raise RuntimeError(
                "scheduler task name is owned by another task type: "
                f"name={row.get('task_name')} type={task_type}"
            )
        if task_type in actual:
            raise RuntimeError(f"duplicate scheduler task type: {task_type}")
        actual[task_type] = row
    missing = REQUIRED_DATA_COMPLETION_TASK_TYPES - set(actual)
    if missing:
        raise RuntimeError(
            "missing required scheduler task contracts: "
            + ", ".join(sorted(missing))
        )
    for task_type, payload in expected.items():
        drift = {
            key: (actual[task_type].get(key), expected_value)
            for key, expected_value in payload.items()
            if actual[task_type].get(key) != expected_value
        }
        if drift:
            raise RuntimeError(
                f"scheduler task {task_type} drifted fields: "
                + ", ".join(sorted(drift))
            )
    return {task_type: "validated" for task_type in sorted(expected)}


def validate_required_data_completion(engine: Engine) -> dict[str, str]:
    """Compatibility alias for the scheduler-row contract validator.

    The historical name was misleading: installing a task does not backfill a
    table.  Call :func:`validate_release_data_readiness` for completion proof.
    """

    return validate_required_task_contracts(engine)


def validate_managed_task_contracts(
    engine: Engine,
    *,
    task_types: Iterable[str] | None = None,
) -> dict[str, str]:
    """Read back every selected release-owned scheduler row exactly once."""

    selected = (
        {str(task["task_type"]) for task in TASKS} - OPT_IN_TASK_TYPES
        if task_types is None
        else {str(item) for item in task_types}
    )
    expected = {
        str(task["task_type"]): {
            key: task[key] for key in TASK_PAYLOAD_COLUMNS
        }
        for task in TASKS
        if str(task["task_type"]) in selected
    }
    if set(expected) != selected:
        raise RuntimeError(
            "managed scheduler validation contains unknown task types: "
            + ", ".join(sorted(selected - set(expected)))
        )
    columns = _table_columns(engine, "st_scheduled_tasks")
    missing_columns = (TASK_PAYLOAD_COLUMNS | {"id"}) - columns
    if missing_columns:
        raise RuntimeError(
            "managed scheduler validation is missing columns: "
            + ", ".join(sorted(missing_columns))
        )
    statement = text(
        "SELECT id, task_name, task_type, group_name, script_path, script_args, "
        "cron_time, interval_minutes, enabled, description, sort_order, date_param "
        "FROM st_scheduled_tasks WHERE task_type IN :task_types "
        "OR task_name IN :task_names ORDER BY id"
    ).bindparams(
        bindparam("task_types", expanding=True),
        bindparam("task_names", expanding=True),
    )
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                statement,
                {
                    "task_types": sorted(expected),
                    "task_names": sorted(
                        payload["task_name"] for payload in expected.values()
                    ),
                },
            ).mappings()
        ]
    actual: dict[str, dict[str, Any]] = {}
    for row in rows:
        task_type = str(row.get("task_type") or "")
        if task_type not in expected:
            raise RuntimeError(
                "managed scheduler task name is owned by another type: "
                f"name={row.get('task_name')} type={task_type}"
            )
        if task_type in actual:
            raise RuntimeError(f"duplicate scheduler task type: {task_type}")
        actual[task_type] = row
    capital_flow = actual.get("capital_flow_batch_fast")
    if (
        capital_flow
        and capital_flow.get("script_path")
        == DIRECT_CAPITAL_FLOW_BATCH_TASK["script_path"]
    ):
        expected["capital_flow_batch_fast"] = {
            key: DIRECT_CAPITAL_FLOW_BATCH_TASK[key]
            for key in TASK_PAYLOAD_COLUMNS
        }
    missing = selected - set(actual)
    if missing:
        raise RuntimeError(
            "missing managed scheduler tasks: " + ", ".join(sorted(missing))
        )
    for task_type, payload in expected.items():
        drift = {
            key
            for key, expected_value in payload.items()
            if actual[task_type].get(key) != expected_value
        }
        if drift:
            raise RuntimeError(
                f"scheduler task {task_type} drifted fields: "
                + ", ".join(sorted(drift))
            )
    return {task_type: "validated" for task_type in sorted(expected)}


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _text_sha256(value: object) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _naive_shanghai(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace("T", " ")[:26]
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise RuntimeError(f"{field} is not an ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed


def _release_task_definitions() -> dict[str, dict[str, Any]]:
    definitions: dict[str, dict[str, Any]] = {}
    for raw in (*TASKS, *TRADING_V3_TASKS):
        task_type = str(raw.get("task_type") or "")
        if task_type not in RELEASE_DATA_READINESS_TASK_TYPES:
            continue
        if task_type in definitions:
            raise RuntimeError(
                f"duplicate release readiness task definition: {task_type}"
            )
        definitions[task_type] = dict(raw)
    missing = RELEASE_DATA_READINESS_TASK_TYPES - set(definitions)
    if missing:
        raise RuntimeError(
            "release readiness task definitions are missing: "
            + ", ".join(sorted(missing))
        )
    return definitions


def _extract_release_validation_evidence(output: object) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for line in str(output or "").splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            payload = json.loads(candidate)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            isinstance(payload, dict)
            and payload.get("schema") == RELEASE_VALIDATION_EVIDENCE_SCHEMA
        ):
            candidates.append(payload)
    if len(candidates) != 1:
        raise RuntimeError(
            "history row must contain one scheduler validation evidence envelope"
        )
    evidence = candidates[0]
    supplied_hash = str(evidence.get("evidence_sha256") or "").lower()
    core = dict(evidence)
    core.pop("evidence_sha256", None)
    if (
        _SHA256.fullmatch(supplied_hash) is None
        or _canonical_sha256(core) != supplied_hash
    ):
        raise RuntimeError("scheduler validation evidence hash differs")
    replay_output = str(evidence.get("replay_output") or "")
    if (
        _SHA256.fullmatch(
            str(evidence.get("machine_output_sha256") or "").lower()
        )
        is None
        or str(evidence.get("replay_output_sha256") or "").lower()
        != _text_sha256(replay_output)
    ):
        raise RuntimeError("scheduler validation evidence replay hash differs")
    return evidence


def _load_release_task_rows(
    engine: Engine,
    definitions: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    statement = text("""
        SELECT id, task_name, task_type, script_path, script_args, date_param,
               enabled
          FROM st_scheduled_tasks
         WHERE task_type IN :task_types OR task_name IN :task_names
         ORDER BY id
    """).bindparams(
        bindparam("task_types", expanding=True),
        bindparam("task_names", expanding=True),
    )
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                statement,
                {
                    "task_types": sorted(definitions),
                    "task_names": sorted(
                        str(item["task_name"])
                        for item in definitions.values()
                    ),
                },
            ).mappings()
        ]
    actual: dict[str, dict[str, Any]] = {}
    for row in rows:
        task_type = str(row.get("task_type") or "")
        expected = definitions.get(task_type)
        if (
            task_type == "capital_flow_batch_fast"
            and row.get("script_path")
            == DIRECT_CAPITAL_FLOW_BATCH_TASK["script_path"]
        ):
            expected = DIRECT_CAPITAL_FLOW_BATCH_TASK
        if expected is None or str(row.get("task_name") or "") != str(
            expected.get("task_name") or ""
        ):
            raise RuntimeError(
                "release scheduler identity is owned by another row: "
                f"name={row.get('task_name')} type={task_type}"
            )
        if task_type in actual:
            raise RuntimeError(
                f"duplicate release scheduler task identity: {task_type}"
            )
        drift = {
            field
            for field in ("script_path", "script_args", "date_param", "enabled")
            if row.get(field) != expected.get(field)
        }
        if drift:
            raise RuntimeError(
                f"release scheduler task {task_type} drifted fields: "
                + ", ".join(sorted(drift))
            )
        actual[task_type] = row
    missing = set(definitions) - set(actual)
    if missing:
        raise RuntimeError(
            "release scheduler tasks are missing: " + ", ".join(sorted(missing))
        )
    return actual


def _latest_release_history_row(
    engine: Engine,
    *,
    task: Mapping[str, Any],
) -> dict[str, Any]:
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT id, run_uid, task_id, task_name, task_type, run_at,
                   finished_at, status, exit_code, output, build_sha,
                   trigger_source
              FROM st_scheduled_task_history
             WHERE finished_at IS NOT NULL
               AND (task_type=:task_type OR task_name=:task_name)
             ORDER BY run_at DESC, id DESC
             LIMIT 1
        """), {
            "task_type": str(task["task_type"]),
            "task_name": str(task["task_name"]),
        }).mappings().all()
    if not rows:
        raise RuntimeError(
            f"release task {task['task_type']} has no terminal history row"
        )
    latest = dict(rows[0])
    if (
        int(latest.get("task_id") or 0) != int(task["id"])
        or str(latest.get("task_name") or "") != str(task["task_name"])
        or str(latest.get("task_type") or "") != str(task["task_type"])
    ):
        raise RuntimeError(
            f"release task {task['task_type']} latest history identity differs"
        )
    return latest


def _load_qmt_coverage_bundle(
    connection: Any,
    *,
    dataset: str,
    trade_date: str,
) -> dict[str, Any]:
    rows = connection.execute(text("""
        SELECT manifest_hash, schema_version, dataset, trade_date, status,
               strategy_eligible, provider, expected_entity_count,
               entity_count, expected_traded_count, actual_traded_count,
               no_trade_count, bar_count, captured_at, manifest_json
          FROM qmt_history_coverage_manifest
         WHERE dataset=:dataset AND trade_date=:trade_date
         ORDER BY captured_at DESC, created_at DESC, manifest_hash DESC
    """), {"dataset": dataset, "trade_date": trade_date}).mappings().all()
    if not rows:
        raise RuntimeError(
            f"QMT {dataset} strategy window is missing {trade_date}"
        )
    row = dict(rows[0])
    try:
        manifest_json = str(row.get("manifest_json") or "")
        core = json.loads(manifest_json)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"QMT {dataset} coverage manifest is unreadable for {trade_date}"
        ) from exc
    if not isinstance(core, dict):
        raise RuntimeError("QMT coverage manifest JSON is not an object")
    manifest = {
        **core,
        "manifest_hash": str(row.get("manifest_hash") or ""),
        "manifest_json": manifest_json,
    }
    row_contract = {
        "schema": row.get("schema_version"),
        "dataset": row.get("dataset"),
        "trade_date": str(row.get("trade_date") or "")[:10],
        "status": row.get("status"),
        "strategy_eligible": bool(row.get("strategy_eligible")),
        "provider": row.get("provider"),
        "expected_entity_count": int(row.get("expected_entity_count") or 0),
        "entity_count": int(row.get("entity_count") or 0),
        "expected_traded_count": int(row.get("expected_traded_count") or 0),
        "actual_traded_count": int(row.get("actual_traded_count") or 0),
        "no_trade_count": int(row.get("no_trade_count") or 0),
        "bar_count": int(row.get("bar_count") or 0),
        "captured_at": _naive_shanghai(
            row.get("captured_at"), field="coverage.captured_at"
        ).isoformat(timespec="seconds"),
    }
    if any(manifest.get(key) != value for key, value in row_contract.items()):
        raise RuntimeError(
            f"QMT {dataset} coverage row differs from manifest for {trade_date}"
        )
    entity_rows = connection.execute(text("""
        SELECT manifest_hash, stock_code, expected_state, classification,
               bar_count, time_set_hash, first_time, last_time,
               source_row_hash, row_hash
          FROM qmt_history_coverage_entity
         WHERE manifest_hash=:manifest_hash
         ORDER BY stock_code
    """), {"manifest_hash": manifest["manifest_hash"]}).mappings().all()
    return {
        "manifest": manifest,
        "entities": [dict(item) for item in entity_rows],
    }


def _validate_qmt_canonical_strategy_partitions(
    bundles: Mapping[tuple[str, str], Mapping[str, Any]],
) -> None:
    from server.common.kline_data import get_kline_engine

    history_engine = get_kline_engine()
    with history_engine.connect() as connection:
        for (dataset, trade_date), bundle in bundles.items():
            manifest = bundle["manifest"]
            entities = list(bundle["entities"])
            if dataset == "stock_daily":
                rows = connection.execute(text("""
                    SELECT stock_code, data_source, quality_status,
                           permission_status
                      FROM sm_stock_kline
                     WHERE trade_date=:trade_date
                       AND k_type=1 AND adjust_type=0
                     ORDER BY stock_code
                """), {"trade_date": trade_date}).mappings().all()
                actual_codes = [
                    str(row.get("stock_code") or "").zfill(6) for row in rows
                ]
                expected_codes = sorted(
                    str(row.get("stock_code") or "").zfill(6)
                    for row in entities
                )
                if (
                    actual_codes != expected_codes
                    or len(rows) != int(manifest.get("bar_count") or 0)
                    or any(
                        row.get("data_source") != "gj_big_qmt_inner"
                        or row.get("quality_status") != "QMT_ATTESTED"
                        or row.get("permission_status") != "SUPPORTED"
                        for row in rows
                    )
                ):
                    raise RuntimeError(
                        f"canonical stock daily partition differs for {trade_date}"
                    )
                continue
            rows = connection.execute(text("""
                SELECT stock_code, COUNT(*) AS row_count,
                       COUNT(DISTINCT trade_time) AS distinct_time_count,
                       MIN(trade_time) AS first_time,
                       MAX(trade_time) AS last_time
                  FROM sm_stock_minute
                 WHERE trade_date=:trade_date
                 GROUP BY stock_code
                 ORDER BY stock_code
            """), {"trade_date": trade_date}).mappings().all()
            expected = {
                str(row.get("stock_code") or "").zfill(6): row
                for row in entities
                if row.get("classification") == "TRADED"
            }
            if [str(row.get("stock_code") or "").zfill(6) for row in rows] != sorted(expected):
                raise RuntimeError(
                    f"canonical stock minute code set differs for {trade_date}"
                )
            total_rows = 0
            for row in rows:
                code = str(row.get("stock_code") or "").zfill(6)
                entity = expected[code]
                row_count = int(row.get("row_count") or 0)
                total_rows += row_count
                if (
                    row_count != int(entity.get("bar_count") or 0)
                    or int(row.get("distinct_time_count") or 0) != row_count
                    or str(row.get("first_time") or "")[-8:]
                    != str(entity.get("first_time") or "")[-8:]
                    or str(row.get("last_time") or "")[-8:]
                    != str(entity.get("last_time") or "")[-8:]
                ):
                    raise RuntimeError(
                        f"canonical stock minute grid differs for {trade_date}:{code}"
                    )
            if total_rows != int(manifest.get("bar_count") or 0):
                raise RuntimeError(
                    f"canonical stock minute row count differs for {trade_date}"
                )


def _validate_qmt_strategy_input_window(
    engine: Engine,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Prove the five closed QMT daily sessions consumed by strategy code.

    Full-market minute bars are maintained and validated by their own QMT
    scheduler tasks, but neither ``analysis_fast`` nor Trading V3's close
    decision consumes ``sm_stock_minute``.  A missing optional minute archive
    must therefore remain a health warning, not a recommendation release
    blocker.
    """

    from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
    from server.common.kline_data import get_kline_engine
    from server.common.qmt_trade_calendar import load_trade_calendar_receipt

    start_date = (now.date() - timedelta(days=30)).isoformat()
    closed_cutoff = authoritative_closed_trade_date(engine, now=now)
    try:
        parsed_closed_cutoff = datetime.strptime(
            closed_cutoff,
            "%Y-%m-%d",
        ).date()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "authoritative closed strategy-input session is unavailable"
        ) from exc
    if parsed_closed_cutoff.isoformat() != closed_cutoff:
        raise RuntimeError(
            "authoritative closed strategy-input session is unavailable"
        )
    end_date = closed_cutoff
    with engine.connect() as connection:
        calendar = load_trade_calendar_receipt(
            connection,
            start_date=start_date,
            end_date=end_date,
            decision_known_at=now,
        )
        sessions = [
            item
            for item in calendar.sessions_between(start_date, end_date)
            if item <= closed_cutoff
        ][-RELEASE_STRATEGY_INPUT_SESSION_COUNT:]
        if len(sessions) != RELEASE_STRATEGY_INPUT_SESSION_COUNT:
            raise RuntimeError(
                "immutable QMT calendar does not cover the strategy input window"
            )
        # Daily bars use the stronger attestation-run consumer truth.  It
        # rebinds every currently consumed row to one immutable catalog and
        # calendar receipt and rejects reused/old attestation rows.
        daily_truths = []
        with get_kline_engine().connect() as daily_connection:
            for trade_date in sessions:
                truth = load_qmt_daily_market_truth(
                    daily_connection,
                    start_date=trade_date,
                    end_date=trade_date,
                    decision_known_at=now,
                )
                if (
                    list(truth.requested_sessions) != [trade_date]
                    or int(truth.attested_row_count) <= 0
                    or _SHA256.fullmatch(str(truth.truth_hash or "")) is None
                ):
                    raise RuntimeError(
                        f"QMT daily strategy input truth differs for {trade_date}"
                    )
                daily_truths.append(truth)
    return {
        "sessions": sessions,
        "session_count": len(sessions),
        "session_set_sha256": _canonical_sha256(sessions),
        "daily_truth_sha256": _canonical_sha256([
            str(item.truth_hash) for item in daily_truths
        ]),
        "daily_attested_row_count": sum(
            int(item.attested_row_count) for item in daily_truths
        ),
    }


def validate_release_data_readiness(
    engine: Engine,
    expected_build_sha: str,
    now: datetime,
) -> dict[str, Any]:
    """Pure-SELECT proof that this build actually completed required data.

    This gate is intentionally a post-activation operation.  It must be run
    only after the release has installed its tasks and those tasks have
    reached terminal states; putting it in the initial code/schema publish
    phase would create an unavoidable deployment deadlock.
    """

    build_sha = str(expected_build_sha or "").strip().lower()
    if _SHA40.fullmatch(build_sha) is None or build_sha == "0" * 40:
        raise RuntimeError("release readiness expected build SHA is invalid")
    decision_time = _naive_shanghai(now, field="now").replace(microsecond=0)
    expected_targets: dict[str, str] = {}
    closed_tasks = (
        RELEASE_DATA_READINESS_TASK_TYPES
        & RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES
    )
    for task_type in sorted(closed_tasks):
        try:
            closed_target = str(
                authoritative_closed_trade_date(
                    engine,
                    now=decision_time,
                    close_ready_time=release_catchup_closed_ready_time(task_type),
                )
            )
            parsed_closed_target = datetime.strptime(
                closed_target,
                "%Y-%m-%d",
            ).date()
        except Exception as exc:
            raise RuntimeError(
                "release readiness authoritative closed target is unavailable"
            ) from exc
        if (
            parsed_closed_target.isoformat() != closed_target
            or parsed_closed_target > decision_time.date()
        ):
            raise RuntimeError(
                "release readiness authoritative closed target is unavailable"
            )
        expected_targets[task_type] = closed_target
    expected_targets.update(
        {
            task_type: decision_time.date().isoformat()
            for task_type in (
                RELEASE_DATA_READINESS_TASK_TYPES
                & RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES
            )
        }
    )
    definitions = _release_task_definitions()
    task_rows = _load_release_task_rows(engine, definitions)
    task_proofs: dict[str, Any] = {}
    for task_type in sorted(definitions):
        task = dict(task_rows[task_type])
        history = _latest_release_history_row(engine, task=task)
        finished_at = _naive_shanghai(
            history.get("finished_at"), field=f"{task_type}.finished_at"
        )
        max_age = RELEASE_DATA_READINESS_MAX_AGE_BY_TASK.get(
            task_type,
            RELEASE_DATA_READINESS_DEFAULT_MAX_AGE,
        )
        if (
            finished_at < decision_time - max_age
            or finished_at > decision_time + timedelta(minutes=5)
        ):
            raise RuntimeError(
                f"release task {task_type} latest terminal run is stale"
            )
        if (
            str(history.get("status") or "") != "success"
            or int(history.get("exit_code") if history.get("exit_code") is not None else -1)
            != 0
            or str(history.get("build_sha") or "").lower() != build_sha
        ):
            raise RuntimeError(
                f"release task {task_type} did not succeed on exact build {build_sha}"
            )
        evidence = _extract_release_validation_evidence(history.get("output"))
        history_started_at = _naive_shanghai(
            history.get("run_at"), field=f"{task_type}.run_at"
        )
        if (
            history_started_at < decision_time - max_age
            or history_started_at > finished_at
        ):
            raise RuntimeError(
                f"release task {task_type} latest terminal run is stale"
            )
        evidence_started_at = _naive_shanghai(
            evidence.get("started_at"), field=f"{task_type}.evidence.started_at"
        )
        if (
            re.fullmatch(
                r"[0-9a-f]{32}", str(history.get("run_uid") or "")
            )
            is None
            or str(evidence.get("run_uid") or "")
            != str(history.get("run_uid") or "")
            or int(evidence.get("task_id") or 0) != int(task["id"])
            or str(evidence.get("task_name") or "") != str(task["task_name"])
            or str(evidence.get("task_type") or "") != task_type
            or str(evidence.get("build_sha") or "").lower() != build_sha
            or evidence.get("status") != "success"
            or int(
                evidence.get("exit_code")
                if evidence.get("exit_code") is not None
                else -1
            )
            != 0
            or evidence.get("validation_checked") is not True
            or evidence.get("validation_ok") is not True
            or evidence_started_at < history_started_at - timedelta(minutes=1)
            or evidence_started_at > finished_at
        ):
            raise RuntimeError(
                f"release task {task_type} validation evidence identity differs"
            )
        replay_output = str(evidence.get("replay_output") or "")
        task["_trigger_source"] = str(history.get("trigger_source") or "")
        # Replay the persisted-data validator against the same immutable
        # scheduler/recommendation audit identity used by the live run.
        task["_scheduler_history_run_uid"] = str(history.get("run_uid") or "")
        task["_scheduler_expected_build_sha"] = build_sha
        evidence_target = evidence.get("release_target_date")
        expected_target = expected_targets.get(task_type)
        if expected_target is not None and str(evidence_target or "") != expected_target:
            raise RuntimeError(
                f"release task {task_type} validation target differs: "
                f"expected={expected_target} actual={evidence_target}"
            )
        if evidence_target is not None:
            try:
                parsed_evidence_target = datetime.strptime(
                    str(evidence_target),
                    "%Y-%m-%d",
                ).date()
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"release task {task_type} validation target is invalid"
                ) from exc
            if parsed_evidence_target.isoformat() != str(evidence_target):
                raise RuntimeError(
                    f"release task {task_type} validation target is invalid"
                )
            task["_release_target_date"] = str(evidence_target)
        disposition = scheduler_output_status(
            task,
            replay_output,
            return_code=0,
        )
        if (disposition or "success") != "success":
            raise RuntimeError(
                f"release task {task_type} receipt replay is not successful"
            )
        validation = validate_scheduler_task_result(
            task,
            engine=engine,
            started_at=history_started_at,
            now=decision_time,
            output=replay_output,
        )
        if validation.checked is not True or validation.ok is not True:
            raise RuntimeError(
                f"release task {task_type} persisted data is not verified: "
                f"checked={validation.checked} message={validation.message}"
            )
        task_proofs[task_type] = {
            "run_uid": str(history.get("run_uid") or ""),
            "finished_at": finished_at.isoformat(sep=" ", timespec="seconds"),
            "evidence_sha256": str(evidence["evidence_sha256"]),
            "validation": validation.message,
        }
        if evidence_target is not None:
            task_proofs[task_type]["target_date"] = str(evidence_target)
    qmt_window = _validate_qmt_strategy_input_window(
        engine,
        now=decision_time,
    )
    return {
        "status": "READY",
        "build_sha": build_sha,
        "validated_at": decision_time.isoformat(sep=" ", timespec="seconds"),
        "task_count": len(task_proofs),
        "tasks": task_proofs,
        "qmt_strategy_input_window": qmt_window,
        "phase": "post_activation_data_readiness",
    }


def quarantine_legacy_canonical_market_writers(engine: Engine) -> int:
    """Disable provider-generic stock writers already replaced by QMT."""

    columns = _table_columns(engine, "st_scheduled_tasks")
    required = {"task_type", "enabled"}
    if not required.issubset(columns):
        raise RuntimeError(
            "legacy market-writer quarantine is missing columns: "
            + ", ".join(sorted(required - columns))
        )
    assignment = "enabled=0"
    if "updated_at" in columns:
        assignment += ", updated_at=NOW()"
    with engine.begin() as connection:
        result = connection.execute(text(f"""
            UPDATE st_scheduled_tasks
               SET {assignment}
             WHERE task_type IN ('stock_kline','stock_minute')
               AND enabled<>0
        """))
    with engine.connect() as connection:
        enabled_count = int(connection.execute(text("""
            SELECT COUNT(*)
              FROM st_scheduled_tasks
             WHERE task_type IN ('stock_kline','stock_minute')
               AND enabled<>0
        """)).scalar() or 0)
    if enabled_count:
        raise RuntimeError("legacy stock market writers remain enabled")
    return max(0, int(getattr(result, "rowcount", 0) or 0))


def run(
    engine: Engine,
    *,
    task_types: Iterable[str] | None = None,
    intraday_alert_mode: str = "shadow",
) -> dict[str, str]:
    requested = (
        {str(task["task_type"]) for task in TASKS} - OPT_IN_TASK_TYPES
        if task_types is None
        else {str(item) for item in task_types}
    )
    known = {str(task["task_type"]) for task in TASKS}
    unknown = requested - known
    if unknown:
        raise ValueError(f"unknown scheduled task types: {', '.join(sorted(unknown))}")

    ensure_scheduler_columns(engine)
    result: dict[str, str] = {}
    for task in TASKS:
        if task["task_type"] not in requested:
            continue
        candidate = dict(task)
        if candidate["task_type"] == "intraday_market_alert":
            candidate["script_args"] = f"--mode {intraday_alert_mode} --json"
        result[candidate["task_name"]] = upsert_task(engine, candidate)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--task-type",
        action="append",
        default=[],
        help="Only upsert this task type; may be repeated",
    )
    parser.add_argument(
        "--intraday-alert-mode",
        choices=("shadow", "live"),
        default="shadow",
        help="Runtime mode used when intraday_market_alert is explicitly selected",
    )
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--review-delivery-only",
        action="store_true",
        help="upsert only the QMT snapshot, early briefing, review, and delivery tasks",
    )
    scope.add_argument("--validate-review-delivery", action="store_true")
    scope.add_argument(
        "--required-data-completion-only",
        action="store_true",
        help=(
            "legacy spelling: upsert only the frozen finance, notice, and "
            "dividend scheduler contracts"
        ),
    )
    scope.add_argument(
        "--validate-required-data-completion",
        action="store_true",
        help=(
            "legacy spelling: validate exact required scheduler task rows; "
            "does not prove data completion"
        ),
    )
    scope.add_argument(
        "--validate-required-task-contracts",
        action="store_true",
        help="validate exact scheduler-row identity and fields only",
    )
    scope.add_argument(
        "--validate-release-data-readiness",
        action="store_true",
        help="post-activation, pure-SELECT verification of completed release data",
    )
    parser.add_argument(
        "--expected-build-sha",
        default="",
        help="exact deployed 40-hex build required by release data readiness",
    )
    parser.add_argument(
        "--readiness-now",
        default="",
        help="optional ISO Asia/Shanghai decision time for deterministic operations",
    )
    args = parser.parse_args(argv)
    engine = create_pooled_engine(get_mysql_url(required=True), pool_pre_ping=True)
    if args.validate_review_delivery:
        result = validate_review_delivery(engine)
    elif args.validate_release_data_readiness:
        decision_time = (
            _naive_shanghai(args.readiness_now, field="readiness_now")
            if args.readiness_now
            else datetime.now(_SHANGHAI).replace(tzinfo=None)
        )
        result = validate_release_data_readiness(
            engine,
            args.expected_build_sha,
            decision_time,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
        return 0
    elif (
        args.validate_required_task_contracts
        or args.validate_required_data_completion
    ):
        result = validate_required_task_contracts(engine)
    else:
        selected_types: Iterable[str] | None = set(args.task_type) or None
        if args.review_delivery_only:
            selected_types = REVIEW_DELIVERY_TASK_TYPES
        elif args.required_data_completion_only:
            selected_types = REQUIRED_DATA_COMPLETION_TASK_TYPES
        result = run(
            engine,
            task_types=selected_types,
            intraday_alert_mode=args.intraday_alert_mode,
        )
        if selected_types is None:
            disabled = quarantine_legacy_canonical_market_writers(engine)
            result["legacy stock_kline/stock_minute quarantine"] = (
                f"disabled={disabled}"
            )
        validate_managed_task_contracts(engine, task_types=selected_types)
        if (
            selected_types is not None
            and REQUIRED_DATA_COMPLETION_TASK_TYPES
            <= set(selected_types)
        ):
            validate_required_task_contracts(engine)
        if args.task_type and not result:
            raise RuntimeError("no scheduled task matched --task-type")
    for task_name, action in result.items():
        print(f"{action}: {task_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
