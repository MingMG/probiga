#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ensure reliability-focused scheduled tasks exist.

This script is intentionally non-destructive: it only adds missing scheduler
columns and upserts the quality-gate tasks used to catch stale data before the
dashboard or paper-trading workflows trust it.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.scheduler_tasks import (
    DEFAULT_SCHEDULER_COLUMNS,
    TASK_PAYLOAD_COLUMNS,
    ensure_scheduler_columns as ensure_shared_scheduler_columns,
    table_columns as scheduler_table_columns,
    task_payload as shared_task_payload,
    upsert_scheduler_task,
)


SCHEDULER_COLUMNS = DEFAULT_SCHEDULER_COLUMNS


TASKS = [
    {
        "task_name": "股票代码池(QMT)",
        "task_type": "all_code",
        "group_name": "系统管理",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "stock_pool --json",
        "cron_time": "04:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 20,
        "date_param": "",
        "description": "每日从国金QMT原子刷新完整A股代码池，过滤债券等非A股标的。",
    },
    {
        "task_name": "指数代码池(QMT)",
        "task_type": "all_index_code",
        "group_name": "系统管理",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "index_pool --json",
        "cron_time": "04:40",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 21,
        "date_param": "",
        "description": "每日优先从国金QMT刷新完整指数代码池。",
    },
    {
        "task_name": "指数成分股(QMT)",
        "task_type": "index_constituent",
        "group_name": "系统管理",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "index_constituent --json",
        "cron_time": "04:50",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 22,
        "date_param": "",
        "description": "每日从国金QMT刷新指数成分及权重关系。",
    },
    {
        "task_name": "概念目录与成分(QMT)",
        "task_type": "concept_constituent_east",
        "group_name": "概念行业",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "concept_reference --json",
        "cron_time": "05:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 23,
        "date_param": "",
        "description": "QMT概念目录与成分分批抓取、覆盖校验后在同一事务中替换，空结果保留旧快照。",
    },
    {
        "task_name": "东财个股公告同步",
        "task_type": "notice_eastmoney",
        "group_name": "资讯公告",
        "script_path": "biz/notice/sync_notice_em.py",
        "script_args": (
            "--from-si-all-code --limit 0 --page-size 50 --max-pages 2 "
            "--sleep 0.15 --min-coverage 0.90 --min-row-coverage 0.50"
        ),
        "cron_time": "20:15",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 34,
        "date_param": "",
        "description": "按 A 股全量股票池同步东财公告；请求覆盖率不足时返回失败，禁止静默形成空公告数据。",
    },
    {
        "task_name": "Guojin QMT API catalog capability refresh",
        "task_type": "qmt_catalog_capability_refresh",
        "group_name": "Guojin QMT",
        "script_path": "tools/setup_guojin_qmt_catalog.py",
        "script_args": "",
        "cron_time": "01:10",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 86,
        "date_param": "",
        "description": "Refresh official QMT API registry and capability ledger every night; unverified sample probes are recorded explicitly.",
    },
    {
        "task_name": "Guojin QMT local history gap repair execute",
        "task_type": "qmt_local_gap_repair_execute",
        "group_name": "Guojin QMT",
        "script_path": "tools/backfill_guojin_qmt_local_history.py",
        "script_args": "from-gaps --gap-limit 2 --apply --json",
        "cron_time": "07:05",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 90,
        "date_param": "",
        "description": "生产未部署 QMT Python 运行时，暂停实际历史补数；运行时安装并通过验收后再启用。",
    },
    {
        "task_name": "盘中实时行情同步",
        "task_type": "intraday_realtime",
        "group_name": "盘中交易",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "realtime --json",
        "cron_time": "09:25",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 70,
        "date_param": "",
        "description": "交易时段每分钟优先从国金QMT全市场快照原子刷新 sm_stock_current；断线、陈旧或覆盖率不足时自动降级到外部行情源。",
    },
    {
        "task_name": "个股K线",
        "task_type": "stock_kline",
        "group_name": "系统管理",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "daily_kline --json",
        "cron_time": "15:05",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 41,
        "date_param": "",
        "description": (
            "收盘后按200只一批同步国金QMT日K，先保存原始QMT证据，"
            "再安全upsert业务表并完成同日逐行补证；严格QMT任务禁止伪装外部回退。"
        ),
    },
    {
        "task_name": "个股分钟行情",
        "task_type": "stock_minute",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": (
            "--type stock --min-coverage 0.85 --request-delay 0.03 "
            "--request-jitter 0.02 --batch-every 0 --fetch-attempts 1"
        ),
        "cron_time": "15:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 42,
        "date_param": "",
        "description": (
            "收盘后从东财逐股未复权接口覆盖完整股票池；先写运行级临时表，"
            "覆盖率达标后按股票原子替换，禁止4000只抽样造成尾部股票停在盘中。"
        ),
    },
    {
        "task_name": "个股分钟资金流(全量)",
        "task_type": "stock_minute_flow",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": (
            "--type flow --min-coverage 0.85 --request-delay 0.03 "
            "--request-jitter 0.02 --batch-every 0 --fetch-attempts 1"
        ),
        "cron_time": "22:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 43,
        "date_param": "",
        "description": (
            "盘后从东财逐股接口覆盖全量股票池的分钟资金流；国金QMT当前许可下"
            "transactioncount1m为空，故仅此数据域保留外部源。先完整写临时表，覆盖率"
            "不足时不替换正式数据，并安排在夜间避免阻塞盘中服务。"
        ),
    },
    {
        "task_name": "东财概念分钟(QMT旧任务停用)",
        "task_type": "concept_east_minute",
        "group_name": "概念行业",
        "script_path": "tools/run_single_table.py",
        "script_args": "sm_concept_east_minute",
        "cron_time": "15:50",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 51,
        "date_param": "",
        "description": "旧 QMT 概念分钟任务覆盖率过低，已停用；待非 QMT 全量概念分钟链路验收后再启用。",
    },
    {
        "task_name": "东财概念K线(QMT旧任务停用)",
        "task_type": "concept_east_kline",
        "group_name": "概念行业",
        "script_path": "tools/run_single_table.py",
        "script_args": "sm_concept_east_kline",
        "cron_time": "15:55",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 52,
        "date_param": "",
        "description": "旧 QMT 概念K线任务会返回空/崩溃输出，已停用；概念资金流和实时概念行情由独立可用任务承担。",
    },
    {
        "task_name": "指数K线(QMT)",
        "task_type": "index_kline",
        "group_name": "行情数据",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "index_kline --json",
        "cron_time": "16:10",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 62,
        "date_param": "",
        "description": "收盘后从国金QMT按40只一批同步最新交易日指数K线，覆盖校验通过后安全写入历史库。",
    },
    {
        "task_name": "国金QMT盘中实时行情同步",
        "task_type": "qmt_intraday_realtime",
        "group_name": "国金QMT",
        "script_path": "tools/sync_qmt_realtime.py",
        "script_args": "--limit 120 --min-coverage 0.60 --no-archive-snapshot --json",
        "cron_time": "09:25",
        "interval_minutes": 5,
        "enabled": 0,
        "sort_order": 71,
        "date_param": "",
        "description": "已由 qmt_live_runtime 常驻轮询和盘中批量快照承担；禁用该子进程型定时任务，避免盘中 QMT worker 反复超时污染调度健康。",
    },
    {
        "task_name": "盘中分钟K线同步",
        "task_type": "intraday_minute_kline",
        "group_name": "盘中交易",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "minute_price --minute-count 20 --json",
        "cron_time": "09:35",
        "interval_minutes": 15,
        "enabled": 1,
        "sort_order": 72,
        "date_param": "",
        "description": "交易时段每15分钟从国金QMT增量同步全市场最近20根分钟线；按200只原子写入并限制内存，覆盖率不足85%不发布。",
    },
    {
        "task_name": "盘中分钟资金流同步",
        "task_type": "intraday_minute_flow",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": "--type flow --min-coverage 0.98 --request-delay 0.03 --request-jitter 0.02 --batch-every 0 --fetch-attempts 2 --skip-closed",
        "cron_time": "09:40",
        "interval_minutes": 30,
        "enabled": 1,
        "sort_order": 74,
        "date_param": "",
        "description": "交易时段每30分钟覆盖最新日K全股票池的分钟资金流；双次抓取重试且覆盖率不足98%不提交。全量实测约16分钟，任务互斥可避免重叠并保留服务器余量。",
    },
    {
        "task_name": "盘中实时质量体检",
        "task_type": "intraday_quality_check",
        "group_name": "盘中交易",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json --include-realtime --skip-closed",
        "cron_time": "09:45",
        "interval_minutes": 5,
        "enabled": 1,
        "sort_order": 76,
        "date_param": "",
        "description": "交易时段检查实时、分钟、资金流基础数据；WARN 写入报告但不让质量任务自我失败形成调度健康闭环。",
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
        "script_args": "--prepare-signals --ensure-recommendations --json",
        "cron_time": "09:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 77,
        "date_param": "",
        "description": "开盘前将上一交易日AI推荐转换为今日模拟交易信号池；若上一交易日推荐缺失则先严格补生成，日期不匹配则禁止自动新开仓。",
    },
    {
        "task_name": "盘后快速资金流同步",
        "task_type": "capital_flow_batch_fast",
        "group_name": "系统管理",
        "script_path": "tools/crawl_realtime_batch.py",
        "script_args": "--only flow --min-coverage 0.98 --json",
        "cron_time": "15:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 84,
        "date_param": "",
        "description": "盘后用东财全市场批量接口快速补齐最新交易日资金流，作为逐股慢任务前置保障。",
    },
    {
        "task_name": "个股资金流向(全量)",
        "task_type": "capital_flow",
        "group_name": "系统管理",
        "script_path": "tools/crawl_realtime_batch.py",
        "script_args": "--only flow --min-coverage 0.98 --json",
        "cron_time": "17:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 30,
        "date_param": "",
        "description": "必需数据健康检查使用的 canonical 资金流任务；用东财批量接口补齐最新交易日 sm_stock_capital_flow_daily，避免 QMT transactioncount1d 空结果阻断全链路。",
    },
    {
        "task_name": "指数行情",
        "task_type": "index_current",
        "group_name": "系统管理",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "index_current --json",
        "cron_time": "18:15",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 85,
        "date_param": "",
        "description": "优先使用国金QMT刷新指数行情；QMT不可用时自动回退外部指数源并保留上一份有效数据。",
    },
    {
        "task_name": "概念资金流向",
        "task_type": "concept_flow",
        "group_name": "系统管理",
        "script_path": "tools/fetch_concept_flow_datacenter.py",
        "script_args": "",
        "cron_time": "19:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 54,
        "date_param": "",
        "description": "必需数据健康检查使用的概念资金流任务；从东财 datacenter-web 写入 sm_concept_capital_flow_east，QMT 概念聚合为空时不再伪成功。",
    },
    {
        "task_name": "盘前数据质量体检",
        "task_type": "quality_check_pre",
        "group_name": "系统管理",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json",
        "cron_time": "08:45",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 88,
        "date_param": "",
        "description": "盘前只读体检；FAIL 才让任务失败，WARN 写入报告但不形成调度自锁。",
    },
    {
        "task_name": "国金QMT凌晨缺口扫描",
        "task_type": "qmt_nightly_reconciliation",
        "group_name": "国金QMT",
        "script_path": "tools/nightly_guojin_qmt_reconciliation.py",
        "script_args": "--scan-days 20 --json",
        "cron_time": "01:30",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 87,
        "date_param": "",
        "description": "每天凌晨扫描国金QMT待写队列、最近20个交易日覆盖率和质量规则；历史缺口登记到 sys_data_gap 后续补。",
    },
    {
        "task_name": "国金QMT本地历史补数(2026)",
        "task_type": "qmt_local_history_2026",
        "group_name": "国金QMT",
        "script_path": "tools/run_guojin_qmt_full_market_history.py",
        "script_args": (
            "--start-date 2026-01-01 --mode all --daily-batch-size 40 "
            "--minute-batch-size 20 --sleep-seconds 0.2 --stop-at 07:00 "
            "--log-path data/logs/qmt_full_market_history_2026.jsonl --json"
        ),
        "cron_time": "00:00",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 88,
        "date_param": "",
        "description": "生产未部署 QMT Python 运行时，暂停该任务，避免反复产生空结果/失败状态；安装并验收 QMT 运行时后再启用。",
    },
    {
        "task_name": "国金QMT基础目录增量同步",
        "task_type": "qmt_reference_incremental",
        "group_name": "国金QMT",
        "script_path": "tools/sync_guojin_qmt_reference_data.py",
        "script_args": "--skip-refresh --json",
        "cron_time": "03:20",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 89,
        "date_param": "",
        "description": "生产未部署 QMT Python 运行时，暂停实际基础目录同步；运行时安装并通过验收后再启用。",
    },
    {
        "task_name": "国金QMT历史缺口修复队列",
        "task_type": "qmt_gap_repair_plan",
        "group_name": "国金QMT",
        "script_path": "tools/repair_guojin_qmt_gaps.py",
        "script_args": "--limit 50 --json",
        "cron_time": "02:00",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 89,
        "date_param": "",
        "description": "每天凌晨列出待修复历史缺口；当前仅计划不自动拉取，避免在QMT历史下载未完全验收前误写。",
    },
    {
        "task_name": "个股行情快照",
        "task_type": "stock_current",
        "group_name": "系统管理",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "realtime --json",
        "cron_time": "15:05",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 89,
        "date_param": "",
        "description": "盘后同样优先使用国金QMT全市场快照原子刷新 sm_stock_current；覆盖率不足或链路异常时自动降级到外部行情源。",
    },
    {
        "task_name": "盘后市场概览刷新",
        "task_type": "market_overview_daily",
        "group_name": "系统管理",
        "script_path": "tools/refresh_market_overview_daily.py",
        "script_args": "",
        "cron_time": "18:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 89,
        "date_param": "",
        "description": "按当天交易日从QMT日K生成 sm_market_overview_daily，供监控页和新鲜度判断使用。",
    },
    {
        "task_name": "盘后股票快照刷新",
        "task_type": "stock_snapshot_daily",
        "group_name": "系统管理",
        "script_path": "biz/stock_market/sync_stock_snapshot.py",
        "script_args": "",
        "cron_time": "18:25",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 90,
        "date_param": "",
        "description": "按当天交易日从QMT日K生成 sm_stock_snapshot，供详情页、组合页和推荐解释使用。",
    },
    {
        "task_name": "盘后快速分析推荐",
        "task_type": "analysis_fast",
        "group_name": "系统管理",
        "script_path": "biz/analysis/sync_analysis_fast.py",
        "script_args": "--top-n 80 --min-score 62",
        "cron_time": "18:50",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 91,
        "date_param": "",
        "description": "基于最新日K批量生成 stock_analysis_result 与 st_recommended_stocks。",
    },
    {
        "task_name": "AI推荐盘前严格生成",
        "task_type": "analysis_morning_strict",
        "group_name": "AI推荐",
        "script_path": "tools/run_ai_recommendation_premarket.py",
        "script_args": "--strict-prev-trade-day --top-n 80 --min-score 62 --min-kline-coverage 0.80 --auto-repair-missing-kline --auto-repair-missing-data --json",
        "cron_time": "08:30",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 92,
        "date_param": "",
        "description": "严格模式依赖生产未部署的 QMT K 线运行时，暂停；当前推荐由盘后快速分析任务生成，QMT 验收后再启用。",
    },
    {
        "task_name": "AI推荐09:05盘前外围增强",
        "task_type": "analysis_premarket_external",
        "group_name": "AI推荐",
        "script_path": "tools/run_ai_recommendation_premarket.py",
        "script_args": "--strict-prev-trade-day --external-market --top-n 80 --min-score 62 --min-kline-coverage 0.80 --json",
        "cron_time": "09:05",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 93,
        "date_param": "",
        "description": "09:05抓取美股、日韩、港股、期货、外汇和美债外围数据，随后使用上一交易日A股数据执行AI推荐；不依赖QMT。",
    },
    {
        "task_name": "盘后数据质量体检",
        "task_type": "quality_check_post",
        "group_name": "系统管理",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json",
        "cron_time": "19:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 94,
        "date_param": "",
        "description": "盘后只读体检；确认采集、分析、推荐和调度链路是否跟上最新交易日，WARN 不让任务自锁为失败。",
    },
    {
        "task_name": "同花顺概念成分股同步",
        "task_type": "sync_concept_ths",
        "group_name": "概念行业",
        "script_path": "tools/sync_concept_ths.py",
        "script_args": "",
        "cron_time": "06:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 43,
        "date_param": "",
        "description": "优先使用同花顺接口；接口返回空结果时回退到已落库的 si_stock_concept_ths 映射，并保留上一份有效快照。",
    },
    {
        "task_name": "同花顺概念行情",
        "task_type": "concept_ths_current",
        "group_name": "概念行业",
        "script_path": "tools/crawl_concept_ths_current.py",
        "script_args": "",
        "cron_time": "19:20",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 44,
        "date_param": "",
        "description": "生产 THS 实时接口当前返回空结果，已暂停以保护上一份快照；待接口恢复后再启用。",
    },
    {
        "task_name": "同花顺概念分钟",
        "task_type": "concept_ths_minute",
        "group_name": "概念行业",
        "script_path": "tools/run_single_table.py",
        "script_args": "sm_concept_ths_minute",
        "cron_time": "18:55",
        "interval_minutes": 0,
        "enabled": 0,
        "sort_order": 45,
        "date_param": "",
        "description": "生产 THS 概念分钟接口当前返回空结果，已暂停以避免清空有效历史；待接口恢复后再启用。",
    },
    {
        "task_name": "指数分钟",
        "task_type": "index_minute",
        "group_name": "行情数据",
        "script_path": "tools/sync_qmt_primary.py",
        "script_args": "index_minute --minute-count 0 --json",
        "cron_time": "16:15",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 46,
        "date_param": "",
        "description": "收盘后从国金QMT按40只一批同步指数240根分钟线，覆盖校验后清理旧时间边界残留。",
    },
]


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    return scheduler_table_columns(engine, table_name)


def ensure_scheduler_columns(engine: Engine) -> None:
    ensure_shared_scheduler_columns(engine, column_definitions=SCHEDULER_COLUMNS)


def _task_payload(task: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    return shared_task_payload(task, columns, allowed_columns=TASK_PAYLOAD_COLUMNS)


def upsert_task(engine: Engine, task: dict[str, Any]) -> str:
    result = upsert_scheduler_task(
        engine,
        task,
        lookup_where="task_name = :task_name OR task_type = :task_type",
        lookup_params={"task_name": task["task_name"], "task_type": task["task_type"]},
        column_definitions=SCHEDULER_COLUMNS,
    )
    return str(result["action"])


def run(engine: Engine) -> dict[str, str]:
    ensure_scheduler_columns(engine)
    result: dict[str, str] = {}
    for task in TASKS:
        result[task["task_name"]] = upsert_task(engine, task)
    return result


def main() -> int:
    engine = create_batch_engine()
    result = run(engine)
    for task_name, action in result.items():
        print(f"{action}: {task_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
