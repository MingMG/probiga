"""Frozen cross-host ownership for production QMT scheduler tasks.

Task names such as ``stock_kline`` historically selected their provider from
ambient configuration.  They cannot prove whether Linux or the signed-in QMT
desktop is capable of executing them, so they are explicitly quarantined until
they are replaced by provider-specific task identities.
"""
from __future__ import annotations

from tools.qmt_announcement_task_contract import TASK as QMT_ANNOUNCEMENT_TASK
from tools.qmt_operations_task_contract import TASKS_BY_TYPE


QMT_CATALOG_CAPABILITY_TASK = {
    "task_name": "Guojin QMT API catalog capability refresh",
    "task_type": "qmt_catalog_capability_refresh",
    "group_name": "Guojin QMT",
    "script_path": "tools/setup_guojin_qmt_catalog.py",
    "script_args": "",
    "cron_time": "01:10",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 86,
    "date_param": "",
    "description": (
        "Validate the privileged-installed QMT API registry, refresh the "
        "capability ledger, and record unverified probes explicitly."
    ),
}

QMT_INTRADAY_REALTIME_TASK = {
    "task_name": "国金QMT盘中实时行情同步",
    "task_type": "qmt_intraday_realtime",
    "group_name": "国金QMT",
    "script_path": "tools/sync_qmt_realtime.py",
    "script_args": "--min-coverage 0.60 --no-archive-snapshot --json",
    "cron_time": "09:25",
    "interval_minutes": 1,
    "enabled": 1,
    "sort_order": 71,
    "date_param": "",
    "description": (
        "国金QMT独立实时行情通道；写入 sm_stock_current 使用安全 Upsert，"
        "不清空正式表。"
    ),
}

QMT_MEMBERSHIP_SNAPSHOT_TASK = {
    "task_name": "QMT industry membership snapshot",
    "task_type": "qmt_membership_snapshot",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_bigqmt_reference.py",
    "script_args": "--apply --force-reference-refresh --json",
    "cron_time": "15:12",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 94,
    "date_param": "",
    "description": (
        "Windows QMT edge-owned daily reference and immutable industry "
        "membership snapshot required by the quantitative review gate."
    ),
}

QMT_INDEX_CURRENT_TASK = {
    "task_name": "Guojin QMT index current exact snapshot",
    "task_type": "qmt_index_current",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_qmt_index_edge.py",
    "script_args": "--dataset current --latest-session --apply --json",
    "cron_time": "09:31",
    "interval_minutes": 1,
    "enabled": 1,
    "sort_order": 72,
    "date_param": "",
    "description": (
        "Build-bound BigQMT index snapshot.  The publisher requires the exact "
        "QMT index catalog and rejects missing, stale, or provider-mixed rows."
    ),
}

QMT_INDEX_KLINE_TASK = {
    "task_name": "Guojin QMT index daily exact publisher",
    "task_type": "qmt_index_kline",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_qmt_index_edge.py",
    "script_args": "--dataset kline --latest-session --apply --json",
    "cron_time": "15:25",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 87,
    "date_param": "",
    "description": (
        "Build-bound BigQMT index daily bars with exact code-by-session "
        "coverage and an immutable calendar-rooted result manifest."
    ),
}

QMT_INDEX_MINUTE_TASK = {
    "task_name": "Guojin QMT index minute exact publisher",
    "task_type": "qmt_index_minute",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_qmt_index_edge.py",
    "script_args": "--dataset minute --latest-session --apply --json",
    "cron_time": "15:35",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 88,
    "date_param": "",
    "description": (
        "Build-bound BigQMT index minutes.  Every eligible index must have "
        "the exact native 241-minute grid before the whole date slice is replaced."
    ),
}

QMT_STOCK_DAILY_CANONICAL_TASK = {
    "task_name": "Guojin QMT canonical stock daily exact publisher",
    "task_type": "qmt_stock_daily_canonical",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_qmt_stock_edge.py",
    "script_args": "--dataset daily --latest-session --apply --json",
    "cron_time": "15:45",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 90,
    "date_param": "",
    "description": (
        "Build-bound BigQMT full-stock daily publisher; requires one exact "
        "catalog/session set, native response identities, a completed immutable "
        "attestation, and an exact canonical database readback."
    ),
}

QMT_STOCK_MINUTE_CANONICAL_TASK = {
    "task_name": "Guojin QMT canonical stock minute exact publisher",
    "task_type": "qmt_stock_minute_canonical",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_qmt_stock_edge.py",
    "script_args": "--dataset minute --latest-session --apply --json",
    "cron_time": "15:55",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 91,
    "date_param": "",
    "description": (
        "Build-bound BigQMT full-stock minute publisher; atomically replaces "
        "the full date scope only after every traded code has the exact native "
        "241-bar grid and every no-trade code has persisted authority."
    ),
}

QMT_STOCK_MINUTE_FLOW_CANONICAL_TASK = {
    "task_name": "Guojin QMT canonical stock minute-flow exact publisher",
    "task_type": "qmt_stock_minute_flow_canonical",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_qmt_minute_flow_exact.py",
    "script_args": "--latest-session --apply --json",
    "cron_time": "16:10",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 92,
    "date_param": "",
    "description": (
        "Build-bound BigQMT transactioncount1m full-stock minute-flow publisher; "
        "requires the exact traded-stock catalog, native 241-minute grid, "
        "non-zero VIP flow evidence, atomic date replacement and database readback."
    ),
}

QMT_CANONICAL_HISTORY_GAP_REPAIR_TASK = {
    "task_name": "国金QMT近期标准历史缺口精确修复",
    "task_type": "qmt_canonical_history_gap_repair",
    "group_name": "国金QMT",
    "script_path": "tools/repair_qmt_canonical_history_gaps.py",
    "script_args": (
        "--lookback-sessions 5 --max-repairs-per-run 30 --apply --json"
    ),
    "cron_time": "00:15",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 93,
    "date_param": "",
    "description": (
        "按不可变QMT交易日历持续核验并修复最近5个已闭市交易日的股票"
        "日线/分钟线/原生分钟资金流、指数日线/分钟线及14只ETF双复权标准"
        "分区；每个分区独立原子发布，失败可恢复且禁止创建历史前向观察。"
    ),
}

LINUX_RECENT_DATA_GAP_REPAIR_TASK = {
    "task_name": "近期 Linux 衍生数据缺口精确修复",
    "task_type": "linux_recent_data_gap_repair",
    "group_name": "系统管理",
    "script_path": "tools/repair_linux_recent_data_gaps.py",
    "script_args": (
        "--lookback-sessions 5 --max-repairs-per-run 20 "
        "--state-file /var/lib/probiga/jobs/"
        "linux-recent-data-gap-repair-v1.json --apply --json"
    ),
    "cron_time": "00:45",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 94,
    "date_param": "",
    "description": (
        "在近期 QMT 标准分区之后按日期主序修复资金流、板块、龙虎榜、"
        "市场概览及最新股票快照；历史分析与 V3 只允许显式审计回放，默认"
        "补数不会把事后不可恢复的 PIT 证据伪装为普通缺口或阻塞最新票池。"
    ),
}

EASTMONEY_ALIST_DAILY_TASK = {
    "task_name": "东财龙虎榜每日完整榜单",
    "task_type": "alist_daily",
    "group_name": "热门数据",
    "script_path": "tools/sync_eastmoney_alist_exact.py",
    "script_args": "--dataset daily --latest-session --apply --json",
    "cron_time": "17:40",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 18,
    "date_param": "",
    "description": (
        "东方财富龙虎榜按权威交易日完整翻页，绑定不可变股票目录后原子发布；"
        "来源权威空结果可接受，部分页、跨日或身份错配均失败关闭。"
    ),
}

EASTMONEY_ALIST_INFO_TASK = {
    "task_name": "东财龙虎榜营业部完整明细",
    "task_type": "alist_info",
    "group_name": "热门数据",
    "script_path": "tools/sync_eastmoney_alist_exact.py",
    "script_args": "--dataset info --latest-session --apply --json",
    "cron_time": "17:45",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 19,
    "date_param": "",
    "description": (
        "逐只覆盖当次完整龙虎榜股票集合的买卖营业部明细；取消旧80股上限，"
        "全分页、原子替换并提交后回读。"
    ),
}

EASTMONEY_CONCEPT_FLOW_TASK = {
    "task_name": "Eastmoney concept flow authoritative snapshot",
    "task_type": "eastmoney_concept_flow_snapshot",
    "group_name": "Concept data",
    "script_path": "tools/fetch_concept_flow_datacenter.py",
    "script_args": "--strict-authority --json",
    "cron_time": "19:30",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 89,
    "date_param": "",
    "description": (
        "Eastmoney-specific concept-flow snapshot.  Publication requires a "
        "complete provider pagination receipt for the authoritative closed session."
    ),
}

EASTMONEY_CONCEPT_CURRENT_TASK = {
    "task_name": "东财概念收盘快照",
    "task_type": "eastmoney_concept_current",
    "group_name": "行情数据",
    "script_path": "tools/sync_eastmoney_concept_market.py",
    "script_args": "--dataset current --json",
    "cron_time": "18:05",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 80,
    "date_param": "",
    "description": (
        "东财完整分页概念目录与 f124 闭市源时间绑定后，全量原子替换；"
        "任一代码缺失或混日即失败。"
    ),
}

EASTMONEY_CONCEPT_KLINE_TASK = {
    "task_name": "东财概念日线精确发布",
    "task_type": "eastmoney_concept_kline",
    "group_name": "行情数据",
    "script_path": "tools/sync_eastmoney_concept_market.py",
    "script_args": "--dataset kline --json",
    "cron_time": "18:10",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 81,
    "date_param": "",
    "description": (
        "按权威交易日和完整东财概念目录发布日线笛卡尔矩阵；"
        "范围内任一代码或交易日缺失即失败。"
    ),
}

EASTMONEY_CONCEPT_MINUTE_TASK = {
    "task_name": "东财概念分钟线精确发布",
    "task_type": "eastmoney_concept_minute",
    "group_name": "行情数据",
    "script_path": "tools/sync_eastmoney_concept_market.py",
    "script_args": "--dataset minute --json",
    "cron_time": "18:15",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 82,
    "date_param": "",
    "description": (
        "按完整东财概念目录发布单日严格 240 格分钟线；"
        "任一代码缺格、跨日或源日期不可证明即失败。"
    ),
}

EASTMONEY_SECTOR_HEAT_TASK = {
    "task_name": "东财板块热度",
    "task_type": "sector_heat_east",
    "group_name": "热门数据",
    "script_path": "tools/fetch_sector_heat_east_daily.py",
    "script_args": "--formal --json",
    "cron_time": "17:08",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 10,
    "date_param": "",
    "description": (
        "东财一级/二级行业目标交易日完整快照；固定目录同事务原子替换"
        "并回读，禁止缓存发布。"
    ),
}

FORMAL_NEWS_SYNC_TASK = {
    "task_name": "多源财经快讯同步",
    "task_type": "news_sync",
    "group_name": "资讯公告",
    "script_path": "tools/sync_news_formal.py",
    "script_args": "--pages 2 --json",
    "cron_time": "00:05",
    "interval_minutes": 5,
    "enabled": 1,
    "sort_order": 36,
    "date_param": "",
    "description": (
        "财联社/东财/新浪逐源同步并写后回读；空批次或全部来源失败"
        "即关闭；数据同步与企微投递相互独立。"
    ),
}

ETF_FORWARD_DAILY_TASK = {
    "task_name": "ETF BigQMT双复权日线与冻结策略前向记录",
    "task_type": "etf_forward_daily",
    "group_name": "策略研究",
    "script_path": "tools/run_etf_forward_daily.py",
    "script_args": "--execute",
    "cron_time": "15:20",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 95,
    "date_param": "",
    "description": (
        "固定14只ETF，BigQMT原始/前复权完整日分区原子发布，"
        "并仅为当前最新收盘追加只读冻结策略观察；绝不下单"
    ),
}

STOCK_DIVIDEND_BAIDU_TASK = {
    "task_name": "百度全市场股票分红同步",
    "task_type": "stock_dividend_baidu",
    "group_name": "资讯公告",
    "script_path": "biz/stock_market/sync_dividend_baidu.py",
    "script_args": (
        "--execute --workers 4 --sleep 0.1 --min-nonempty-code-ratio 0.2"
    ),
    "cron_time": "22:00",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 36,
    "date_param": "",
    "description": (
        "基于不可变QMT与si_all_code精确全市场代码集，逐代码验证百度分红"
        "或权威空响应后原子发布；禁止部分写"
    ),
}

# One deterministic post-close recommendation evidence chain.  The Linux
# collector freezes the full-market target-day turnover facts; the signed-in
# Windows edge then computes the deterministic preliminary Top80 and captures
# the MyQuant upper-limit history in the same process.  All three stages bind
# the same D+23:55 Shanghai decision cutoff and exact deployed build.
TARGET_TURNOVER_SNAPSHOT_TASK = {
    "task_name": "目标日全市场换手率不可变快照",
    "task_type": "target_turnover_snapshot",
    "group_name": "AI推荐",
    "script_path": "tools/sync_target_turnover_snapshot.py",
    "script_args": "",
    "cron_time": "15:50",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 89,
    "date_param": "",
    "description": (
        "Linux按完整目标日股票目录逐股采集东财历史K线f61；全覆盖、"
        "QMT OHLCV逐行匹配、PIT截止与不可变回执全部通过后才NULL-only补写。"
    ),
}

ANALYSIS_UPPER_EVIDENCE_TASK = {
    "task_name": "策略预选80与MyQuant涨停价证据",
    "task_type": "analysis_upper_evidence_prepare",
    "group_name": "AI推荐",
    "script_path": "tools/sync_upper_limit_snapshot.py",
    "script_args": "--prepare-preliminary --min-score 62",
    "cron_time": "23:40",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 90,
    "date_param": "",
    "description": (
        "Windows QMT边缘节点以固定PIT截止重算有序Top80，再用MyQuant"
        "history_instruments采集21日涨跌停价；preview hash写入不可变证据账本。"
    ),
}


WINDOWS_QMT_EDGE_TASKS = (
    QMT_CATALOG_CAPABILITY_TASK,
    QMT_INTRADAY_REALTIME_TASK,
    QMT_MEMBERSHIP_SNAPSHOT_TASK,
    QMT_ANNOUNCEMENT_TASK,
    TASKS_BY_TYPE["qmt_local_gap_repair_execute"],
    TASKS_BY_TYPE["qmt_local_history_2024"],
    TASKS_BY_TYPE["qmt_reference_incremental"],
    QMT_INDEX_CURRENT_TASK,
    QMT_INDEX_KLINE_TASK,
    QMT_INDEX_MINUTE_TASK,
    QMT_STOCK_DAILY_CANONICAL_TASK,
    QMT_STOCK_MINUTE_CANONICAL_TASK,
    QMT_STOCK_MINUTE_FLOW_CANONICAL_TASK,
    QMT_CANONICAL_HISTORY_GAP_REPAIR_TASK,
    ETF_FORWARD_DAILY_TASK,
    ANALYSIS_UPPER_EVIDENCE_TASK,
)
LINUX_QMT_TASKS = (
    TASKS_BY_TYPE["qmt_nightly_reconciliation"],
    TASKS_BY_TYPE["qmt_gap_repair_plan"],
)
LINUX_PROVIDER_TASKS = (
    TARGET_TURNOVER_SNAPSHOT_TASK,
    LINUX_RECENT_DATA_GAP_REPAIR_TASK,
    EASTMONEY_ALIST_DAILY_TASK,
    EASTMONEY_ALIST_INFO_TASK,
    EASTMONEY_CONCEPT_FLOW_TASK,
    EASTMONEY_CONCEPT_CURRENT_TASK,
    EASTMONEY_CONCEPT_KLINE_TASK,
    EASTMONEY_CONCEPT_MINUTE_TASK,
    EASTMONEY_SECTOR_HEAT_TASK,
    FORMAL_NEWS_SYNC_TASK,
    STOCK_DIVIDEND_BAIDU_TASK,
)

WINDOWS_QMT_EDGE_TASKS_BY_TYPE = {
    str(task["task_type"]): task for task in WINDOWS_QMT_EDGE_TASKS
}
LINUX_QMT_TASKS_BY_TYPE = {
    str(task["task_type"]): task for task in LINUX_QMT_TASKS
}
LINUX_PROVIDER_TASKS_BY_TYPE = {
    str(task["task_type"]): task for task in LINUX_PROVIDER_TASKS
}
WINDOWS_QMT_EDGE_TASK_TYPES = frozenset(WINDOWS_QMT_EDGE_TASKS_BY_TYPE)
LINUX_QMT_TASK_TYPES = frozenset(LINUX_QMT_TASKS_BY_TYPE)
LINUX_PROVIDER_TASK_TYPES = frozenset(LINUX_PROVIDER_TASKS_BY_TYPE)

# Only these three long-running foundation tasks need recent execution proof.
# The other edge jobs are still exact owned task contracts, but requiring a
# cron success after every arbitrary release time would deadlock deployment.
WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES = (
    "qmt_local_gap_repair_execute",
    "qmt_local_history_2024",
    "qmt_reference_incremental",
)

# This independent non-QMT source is deliberately not part of the QMT set.  It
# remains on the Windows egress lane because production Linux receives a WAF
# page from Xueqiu.
WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE = {
    "fetch_hot_rank_xq": {
        "script_path": "tools/fetch_hot_rank_xq.py",
    },
}
WINDOWS_NON_QMT_EGRESS_TASK_TYPES = frozenset(
    WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE
)

# Legacy generic task identities select a provider from ambient configuration.
# Neither executor may claim them until a provider-specific identity and
# immutable task contract are introduced.
UNFROZEN_PROVIDER_TASK_TYPES = frozenset({
    "all_code",
    "all_index_code",
    "concept_code_east",
    "concept_constituent_east",
    "concept_east_current",
    "concept_east_kline",
    "concept_east_minute",
    "concept_flow",
    "index_constituent",
    "index_current",
    "index_kline",
    "index_minute",
    "stock_current",
    "stock_kline",
    "stock_minute",
    "stock_relations_qmt",
})
UNFROZEN_PROVIDER_SCRIPT_PATHS = frozenset({
    "tools/run_single_table.py",
    "tools/crawl_minute_kline.py",
})

if WINDOWS_QMT_EDGE_TASK_TYPES & LINUX_QMT_TASK_TYPES:
    raise RuntimeError("QMT host ownership contract overlaps")
if WINDOWS_QMT_EDGE_TASK_TYPES & UNFROZEN_PROVIDER_TASK_TYPES:
    raise RuntimeError("frozen QMT task is also provider-unfrozen")
if set(WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES) - WINDOWS_QMT_EDGE_TASK_TYPES:
    raise RuntimeError("QMT execution proof task is not Windows edge-owned")
if len(WINDOWS_QMT_EDGE_TASKS_BY_TYPE) != len(WINDOWS_QMT_EDGE_TASKS):
    raise RuntimeError("duplicate Windows QMT edge task identity")
if len(LINUX_QMT_TASKS_BY_TYPE) != len(LINUX_QMT_TASKS):
    raise RuntimeError("duplicate Linux QMT task identity")
if len(LINUX_PROVIDER_TASKS_BY_TYPE) != len(LINUX_PROVIDER_TASKS):
    raise RuntimeError("duplicate Linux provider task identity")
if (
    WINDOWS_QMT_EDGE_TASK_TYPES | LINUX_QMT_TASK_TYPES
) & LINUX_PROVIDER_TASK_TYPES:
    raise RuntimeError("provider-specific task ownership contract overlaps")


__all__ = [
    "ANALYSIS_UPPER_EVIDENCE_TASK",
    "EASTMONEY_ALIST_DAILY_TASK",
    "EASTMONEY_ALIST_INFO_TASK",
    "EASTMONEY_CONCEPT_FLOW_TASK",
    "ETF_FORWARD_DAILY_TASK",
    "LINUX_PROVIDER_TASKS",
    "LINUX_PROVIDER_TASKS_BY_TYPE",
    "LINUX_PROVIDER_TASK_TYPES",
    "LINUX_RECENT_DATA_GAP_REPAIR_TASK",
    "LINUX_QMT_TASKS",
    "LINUX_QMT_TASKS_BY_TYPE",
    "LINUX_QMT_TASK_TYPES",
    "QMT_CATALOG_CAPABILITY_TASK",
    "QMT_INTRADAY_REALTIME_TASK",
    "QMT_INDEX_CURRENT_TASK",
    "QMT_INDEX_KLINE_TASK",
    "QMT_INDEX_MINUTE_TASK",
    "QMT_MEMBERSHIP_SNAPSHOT_TASK",
    "QMT_STOCK_DAILY_CANONICAL_TASK",
    "QMT_STOCK_MINUTE_CANONICAL_TASK",
    "QMT_STOCK_MINUTE_FLOW_CANONICAL_TASK",
    "STOCK_DIVIDEND_BAIDU_TASK",
    "TARGET_TURNOVER_SNAPSHOT_TASK",
    "UNFROZEN_PROVIDER_TASK_TYPES",
    "UNFROZEN_PROVIDER_SCRIPT_PATHS",
    "WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE",
    "WINDOWS_NON_QMT_EGRESS_TASK_TYPES",
    "WINDOWS_QMT_EDGE_TASKS",
    "WINDOWS_QMT_EDGE_TASKS_BY_TYPE",
    "WINDOWS_QMT_EDGE_TASK_TYPES",
    "WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES",
]
