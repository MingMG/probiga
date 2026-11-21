"""One deployed interpreter for immutable, model-proposed research rules."""
from __future__ import annotations

from server.engine.strategy_rule_language import feature_frame, select_rule_rows, validate_rule


ADAPTER_KEY = "research.declarative.daily"
ADAPTER_VERSION = "1.0.0"
EVALUATOR_TYPES = frozenset({"declarative_daily_research"})


def validate_research_strategy(strategy):
    validate_rule(strategy["parameters"]["rule_spec"])
    return True, "冻结研究规则可重算；模拟资金资格仍由现有治理证据决定"


def research_candidate_rows(strategy, context):
    research = strategy["evaluator_config"]["autonomous_research"]
    if context["trade_date"] <= research["proposed_at"][:10]:
        raise ValueError("研究规则只能在实际提出日期之后产生前向候选，禁止回填事前结果")
    source = context["market"].get("research_input")
    if not source or len(context["recommendation_rows"]) != source["stock_count"]:
        raise ValueError("研究规则缺少完整冻结评分样本及来源身份")
    rule = strategy["parameters"]["rule_spec"]
    rows, market = feature_frame(context["recommendation_rows"], context["trade_date"])
    selected = select_rule_rows(rule, rows, market)
    binding = strategy["evaluator_config"]["execution_adapter"]
    candidates = []
    for row in selected:
        close = row["close"]
        candidates.append({
            "stock_code": row["stock_code"], "stock_name": row["stock_name"],
            "strategy_key": strategy["strategy_key"], "strategy_version": strategy["current_version"],
            "strategy_version_hash": strategy["version_hash"],
            "execution_binding_hash": binding["execution_binding_hash"],
            "adapter_artifact_sha256": binding["artifact_sha256"], "cost_model_hash": binding["cost_model_hash"],
            "signal_direction": "BUY", "signal_status": "READY", "gate_status": "WATCH",
            "effective_score": row["research_rule_score"], "effective_weight": 1.0,
            "entry_low": close * 0.98, "entry_high": close * 1.02,
            "stop_loss": close * (1 - float(rule["stop_loss_pct"]) / 100),
            "initial_stop_pct": float(rule["stop_loss_pct"]), "horizon_days": rule["holding_days"],
            "industry": row["industry"], "research_only": True,
            "automatic_real_order_submission": False, "real_order_authority": False,
        })
    return candidates
