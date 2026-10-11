"""Versioned, code-owned capability contract for the quantitative workbench.

This contract deliberately describes only capabilities that ProBigA can prove.
It is independent from any user's personal Codex skill and does not grant order
authority or create a second scheduler.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any


CONTRACT_SCHEMA = "probiga.quant-workbench-capabilities.v1"
CONTRACT_VERSION = "2026-10-11"
RECOMMENDED_WORKFLOWS = ("R3", "K4", "R4", "D1", "R2", "T2")

_CATEGORY_LABELS = {
    "MARKET_RESEARCH": "市场与研究",
    "DATA": "数据",
    "STRATEGY_PORTFOLIO": "策略与组合",
    "RISK_PERFORMANCE": "风险与绩效",
    "TRADING_REVIEW": "交易与复盘",
    "DERIVATIVES_RELATIVE_VALUE": "衍生品与相对价值",
    "ENGINEERING_OPERATIONS": "工程与运维",
}


def _capability(
    code: str,
    category: str,
    label: str,
    availability: str,
    scope: str,
    limitations: str,
    *,
    entry_tab: str = "",
    operation: str = "READ_ONLY",
    task_type: str = "",
) -> dict[str, Any]:
    return {
        "code": code,
        "category": category,
        "category_label": _CATEGORY_LABELS[category],
        "label": label,
        "availability": availability,
        "scope": scope,
        "limitations": limitations,
        "entry_tab": entry_tab,
        "operation": operation,
        "task_type": task_type,
        "can_run_now": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


_CAPABILITIES = (
    _capability("R1", "MARKET_RESEARCH", "市场状态识别", "AVAILABLE", "查看趋势、波动、流动性、风险偏好及市场状态证据。", "只读观察，不生成交易指令。", entry_tab="workbench"),
    _capability("R2", "MARKET_RESEARCH", "因子研究", "PARTIAL", "查看现有四因子 T-1 暴露、Q5-Q1、Rank IC 和覆盖率结果。", "仅覆盖趋势、均价偏离、波动结构、市场弹性；不代表完整因子挖掘、中性化或拥挤度引擎。", entry_tab="review"),
    _capability("R3", "MARKET_RESEARCH", "事件研究", "PARTIAL", "当前暴露股票的巨潮官方公告按小时写入既有 PIT，并进入推荐与持仓风险门禁。", "当前生产能力是公告风险捕获，不是任意事件的收益窗统计；任务由独立调度器持久运行。", entry_tab="notice", operation="SCHEDULED", task_type="exposure_event_risk_watch"),
    _capability("R4", "MARKET_RESEARCH", "研究复现", "PARTIAL", "查看每周 3–5 条外部研究候选、来源指标、复现状态与 QMT 可行性。", "候选入账不是论文复现成功；单项实验仍需独立数据、代码、样本外与成本验证。", entry_tab="research-radar"),
    _capability("D1", "DATA", "数据质量审计", "PARTIAL", "查看正式数据质量检查、缺失、重复、异常与新鲜度结果。", "分钟会话完整性和财务 PIT 证据仍有未检查项；报告不具备数据发布权。", entry_tab="datasource"),
    _capability("D2", "DATA", "覆盖与补采", "AVAILABLE", "查看覆盖缺口、采集任务、检查点、完成回执与数据库读回。", "只能由现有采集所有者和调度器执行；上游成功不等于覆盖完成。", entry_tab="datasource"),
    _capability("D3", "DATA", "公司行动校验", "PARTIAL", "现有分红与部分公司行动数据具备 PIT/质量校验。", "尚无覆盖拆股、配股、代码变更和复权连续性的统一审计入口。", entry_tab="datasource"),
    _capability("D4", "DATA", "数据集画像", "PARTIAL", "查看覆盖、新鲜度、来源和部分数据血缘。", "分布、连接泄漏和全链路 lineage 尚未统一到单份报告。", entry_tab="datasource"),
    _capability("S1", "STRATEGY_PORTFOLIO", "回测审计", "PARTIAL", "核查已冻结选股器回执的成本、统计门禁和回测证据。", "只适用于已接入的选股器，不是任意脚本的一键回测审计。", entry_tab="strategy-backtest"),
    _capability("S2", "STRATEGY_PORTFOLIO", "稳健性分析", "PARTIAL", "查看固定 ETF 策略的子期间、参数与压力测试证据。", "尚未形成面向所有策略的通用 walk-forward 引擎。", entry_tab="strategy-backtest"),
    _capability("S3", "STRATEGY_PORTFOLIO", "组合构建", "PARTIAL", "查看治理后的候选池、组合权重与现金配置。", "研究目标保持 RESEARCH_ONLY；不会直接形成真实订单。", entry_tab="strategy-center"),
    _capability("S4", "STRATEGY_PORTFOLIO", "容量与成本", "PARTIAL", "查看既有 ETF 稳健性测试中的换手、成本和容量压力。", "不覆盖所有证券、经纪商成交模型和真实冲击校准。", entry_tab="strategy-backtest"),
    _capability("K1", "RISK_PERFORMANCE", "风险看板", "PARTIAL", "查看持仓、集中度、市场状态和事件风险。", "统一 VaR、相关性矩阵和多情景压力尚未齐备。", entry_tab="trading-v3-positions"),
    _capability("K2", "RISK_PERFORMANCE", "绩效归因", "PARTIAL", "查看策略和复盘中的收益、成本及部分来源拆解。", "尚未形成市场/行业/风格/择时的统一正式归因合同。", entry_tab="review"),
    _capability("K3", "RISK_PERFORMANCE", "策略漂移监测", "PARTIAL", "查看策略治理、影子版本、模型选择和历史验证。", "信号、持仓、成交与收益的统一实时漂移告警尚未完备。", entry_tab="strategy-center"),
    _capability("K4", "RISK_PERFORMANCE", "告警设计", "AVAILABLE", "重大公告按严重度进入当前推荐和持仓决策，缺覆盖时失败关闭。", "仅风险证据影响门禁；不会自动提交模拟或真实订单。", entry_tab="recommended", operation="SCHEDULED", task_type="exposure_event_risk_watch"),
    _capability("T1", "TRADING_REVIEW", "盘前简报", "AVAILABLE", "查看隔夜市场、日历、公告、持仓与当日风险门禁。", "重新生成可能调用外部服务或推送；本入口默认只读已有结果。", entry_tab="trading-day"),
    _capability("T2", "TRADING_REVIEW", "盘后复盘", "AVAILABLE", "查看确定性量化复盘及持久化结果。", "重新生成与企业微信推送属于独立写操作；查看结果不触发它们。", entry_tab="review"),
    _capability("T3", "TRADING_REVIEW", "信号到成交对账", "PARTIAL", "查看 V2 隔离模拟账户的信号、订单、成交与账户状态。", "不是通用实盘只读对账；执行对账会写回模拟账户记录。", entry_tab="trading"),
    _capability("T4", "TRADING_REVIEW", "执行分析", "PARTIAL", "查看 QMT 每日模拟结果、成交和部分滑点证据。", "尚无统一的券商/场所比较与真实市场冲击归因。", entry_tab="qmt-strategy-results"),
    _capability("V1", "DERIVATIVES_RELATIVE_VALUE", "期货结构", "UNAVAILABLE", "尚未验证多合约期货基差、期限结构与展期数据合同。", "外围指数行情不能证明该能力可用。"),
    _capability("V2", "DERIVATIVES_RELATIVE_VALUE", "期权波动率", "UNAVAILABLE", "尚未验证期权链、隐含波动率、偏度和 Greeks 引擎。", "没有正式数据和估值合同时不提供空实现。"),
    _capability("V3", "DERIVATIVES_RELATIVE_VALUE", "相对价值扫描", "UNAVAILABLE", "尚未验证 ETF/期货现货/配对统计套利的统一扫描引擎。", "现有单项行情页面不等于相对价值生产能力。"),
    _capability("V4", "DERIVATIVES_RELATIVE_VALUE", "对冲评估", "UNAVAILABLE", "尚未验证对冲工具、对冲比率、基差风险和压力绩效合同。", "缺少正式衍生品输入时保持不可用。"),
    _capability("O1", "ENGINEERING_OPERATIONS", "QMT 实盘检查", "PARTIAL", "查看独立调度器实例、心跳、构建身份与任务运行记录。", "本入口不提供 QMT 原生运行实例、模型身份或无订单能力探针，不能证明实盘运行验收。", entry_tab="scheduler"),
    _capability("O2", "ENGINEERING_OPERATIONS", "数据管道检查", "AVAILABLE", "按来源、适配、调度、写入、回执、覆盖和读回查看链路。", "每一阶段单独判定，不能用进程存活代替业务完成。", entry_tab="datasource"),
    _capability("O3", "ENGINEERING_OPERATIONS", "任务与调度审计", "AVAILABLE", "查看运行身份、所有者、状态、历史和检查点进度。", "人工 accepted/delegated 只证明提交，不证明业务作业开始或完成。", entry_tab="scheduler"),
    _capability("O4", "ENGINEERING_OPERATIONS", "量化发布检查", "PARTIAL", "查看调度器构建身份与各任务的历史执行版本。", "本入口没有两端发布范围、组件切换和正式验收回执；任务版本不等于发布 COMPLETE。", entry_tab="scheduler"),
)


def quant_workbench_capability_contract() -> dict[str, Any]:
    codes = [item["code"] for item in _CAPABILITIES]
    if len(codes) != 28 or len(set(codes)) != 28:
        raise RuntimeError("quant workbench capability identity is invalid")
    return {
        "schema": CONTRACT_SCHEMA,
        "version": CONTRACT_VERSION,
        "category_count": len(_CATEGORY_LABELS),
        "capability_count": len(_CAPABILITIES),
        "recommended_workflows": list(RECOMMENDED_WORKFLOWS),
        "capabilities": deepcopy(list(_CAPABILITIES)),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


__all__ = [
    "CONTRACT_SCHEMA",
    "CONTRACT_VERSION",
    "RECOMMENDED_WORKFLOWS",
    "quant_workbench_capability_contract",
]
