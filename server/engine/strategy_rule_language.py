"""Declarative daily research rules, shared by historical and forward selection."""
from __future__ import annotations

from typing import Any


RULE_SCHEMA = "probiga.research-selection-rule.v1"
STOCK_FIELDS = (
    "change_pct", "previous_change_pct", "pct_5", "pct_20", "dist_ma20",
    "drawdown_60", "from_low_60", "amount_ratio_20", "amount_ratio_5",
    "close_location_value", "volatility_20", "turnover_ratio",
    "capital_score", "technical_score", "sector_rotation_score",
    "ultra_short_score", "short_term_score", "swing_score", "main_wave_score",
    "relative_change_pct", "relative_20d_pct", "sector_breadth_pct",
    "sector_change_pct", "sector_strength_percentile", "sector_top3_amount_pct",
)
MARKET_FIELDS = ("market_change_pct", "market_breadth_pct", "market_return_20d_pct")
FIELDS = frozenset(STOCK_FIELDS + MARKET_FIELDS)
COMPARISONS = frozenset({"gt", "gte", "lt", "lte"})


def finite(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (ValueError, TypeError):
        return None
    return result if -1e12 <= result <= 1e12 else None


def validate_rule(rule: Any, available_fields=None) -> dict:
    if not isinstance(rule, dict) or set(rule) != {
        "schema", "market_all", "entry_all", "entry_any", "ranking", "top_n",
        "holding_days", "stop_loss_pct",
    } or rule.get("schema") != RULE_SCHEMA:
        raise ValueError("研究规则结构或版本无效")
    available = FIELDS if available_fields is None else FIELDS.intersection(available_fields)
    for group in ("market_all", "entry_all", "entry_any"):
        conditions = rule[group]
        if not isinstance(conditions, list) or len(conditions) > 12:
            raise ValueError("研究条件数量无效")
        for condition in conditions:
            if not isinstance(condition, dict) or set(condition) != {"field", "op", "value"}:
                raise ValueError("研究条件结构无效")
            field = condition["field"]
            permitted = frozenset(MARKET_FIELDS) if group == "market_all" else frozenset(STOCK_FIELDS)
            if field not in permitted or field not in available:
                raise ValueError("研究规则使用不可用或越界特征：" + str(field))
            if condition["op"] not in COMPARISONS or finite(condition["value"]) is None:
                raise ValueError("研究比较条件无效")
    if not rule["entry_all"] or not isinstance(rule["ranking"], list) or not 1 <= len(rule["ranking"]) <= 6:
        raise ValueError("研究规则必须有入场条件和排序依据")
    seen = set()
    for item in rule["ranking"]:
        if not isinstance(item, dict) or set(item) != {"field", "ascending", "weight"}:
            raise ValueError("研究排序结构无效")
        if item["field"] not in STOCK_FIELDS or item["field"] not in available or item["field"] in seen:
            raise ValueError("研究排序特征无效或重复")
        seen.add(item["field"])
        weight = finite(item["weight"])
        if not isinstance(item["ascending"], bool) or weight is None or not 0 < weight <= 10:
            raise ValueError("研究排序方向或权重无效")
    for field, maximum in (("top_n", 5), ("holding_days", 20)):
        if (not isinstance(rule[field], int) or isinstance(rule[field], bool)) or not 1 <= rule[field] <= maximum:
            raise ValueError("研究持有期或候选数量越界")
    stop = finite(rule["stop_loss_pct"])
    if stop is None or not 1 <= stop <= 8:
        raise ValueError("研究止损必须在1%至8%之间")
    return rule


def condition_matches(condition, values):
    value = finite(values.get(condition["field"]))
    threshold = finite(condition["value"])
    if value is None or threshold is None:
        return False
    op = condition["op"]
    return ((op == "gt" and value > threshold) or (op == "gte" and value >= threshold)
            or (op == "lt" and value < threshold) or (op == "lte" and value <= threshold))


def select_rule_rows(rule, rows, market):
    """Rank cross-sectional percentiles; missing required values never become zero."""
    validate_rule(rule)
    for condition in rule["market_all"]:
        if finite(market.get(condition["field"])) is None:
            raise ValueError("研究市场特征缺失：" + condition["field"])
        if not condition_matches(condition, market):
            return []
    required = set()
    for condition in rule["entry_all"] + rule["entry_any"] + rule["ranking"]:
        required.add(condition["field"])
    for field in required:
        available = 0
        for row in rows:
            if finite(row.get(field)) is not None:
                available += 1
        if not rows or available / len(rows) < 0.9:
            raise ValueError("研究个股特征覆盖不足：" + field)
    population, eligible = [], []
    for row in rows:
        close = finite(row.get("close"))
        if close is None or close <= 0:
            continue
        population.append(row)
        matched = True
        for condition in rule["entry_all"]:
            if not condition_matches(condition, row):
                matched = False
        if rule["entry_any"]:
            any_match = False
            for condition in rule["entry_any"]:
                if condition_matches(condition, row):
                    any_match = True
            matched = matched and any_match
        for item in rule["ranking"]:
            if finite(row.get(item["field"])) is None:
                matched = False
        if matched:
            eligible.append(row)
    rankings, total_weight = {}, 0.0
    for item in rule["ranking"]:
        field, values = item["field"], []
        total_weight += float(item["weight"])
        for row in population:
            value = finite(row.get(field))
            if value is not None:
                values.append(value)
        values.sort()
        positions = {}
        for index, value in enumerate(values):
            if value not in positions:
                positions[value] = [index, index]
            positions[value][1] = index
        percentiles = {}
        for value, bounds in positions.items():
            percentiles[value] = (bounds[0] + bounds[1] + 1) / (2 * len(values))
        rankings[field] = percentiles
    selected = []
    for row in eligible:
        score = 0.0
        for item in rule["ranking"]:
            rank = rankings[item["field"]][finite(row[item["field"]])]
            score += float(item["weight"]) * (1 - rank if item["ascending"] else rank)
        selected.append({**row, "research_rule_score": round(score / total_weight * 100, 6)})
    return sorted(selected, key=lambda row: (-row["research_rule_score"], str(row["stock_code"])))[:rule["top_n"]]


def feature_frame(raw_rows, trade_date):
    """Only published price/volume factors and exact-date PIT industry members."""
    rows = []
    for raw in raw_rows:
        close, change = finite(raw.get("close")), finite(raw.get("change_pct"))
        if close is None or close <= 0 or change is None:
            continue
        row = {key: finite(raw.get(key)) for key in STOCK_FIELDS}
        # Capital/turnover factors need their own dated source proof.
        if str(raw.get("flow_trade_date") or "")[:10] != trade_date:
            row["capital_score"] = None
        if raw.get("turnover_evidence_validated") is not True:
            row["turnover_ratio"] = None
        row.update(stock_code=str(raw["stock_code"]).zfill(6),
                   stock_name=str(raw.get("short_name") or raw.get("stock_name") or ""),
                   close=close, amount=finite(raw.get("amount")), industry=None)
        if (raw.get("industry_name") and raw.get("industry_snapshot_source") == "gj_big_qmt_inner"
                and str(raw.get("industry_snapshot_date") or "")[:10] == trade_date
                and raw.get("membership_proof_sha256")):
            row["industry"] = str(raw["industry_name"])
        rows.append(row)
    codes = [r["stock_code"] for r in rows]
    if not rows or len(codes) != len(set(codes)):
        raise ValueError("研究行情样本为空或证券代码重复")
    rows.sort(key=lambda row: row["stock_code"])
    market = {"market_change_pct": sum(r["change_pct"] for r in rows) / len(rows),
              "market_breadth_pct": sum(r["change_pct"] > 0 for r in rows) / len(rows) * 100}
    returns = [r["pct_20"] for r in rows if r["pct_20"] is not None]
    market["market_return_20d_pct"] = sum(returns) / len(returns) if len(returns) == len(rows) else None
    groups = {}
    for row in rows:
        row["relative_change_pct"] = row["change_pct"] - market["market_change_pct"]
        row["relative_20d_pct"] = (row["pct_20"] - market["market_return_20d_pct"]
                                   if row["pct_20"] is not None and market["market_return_20d_pct"] is not None else None)
        if row["industry"]:
            groups.setdefault(row["industry"], []).append(row)
    for members in groups.values():
        breadth = sum(r["change_pct"] > 0 for r in members) / len(members) * 100
        change = sum(r["change_pct"] for r in members) / len(members)
        amounts = [r["amount"] for r in members if r["amount"] is not None and r["amount"] >= 0]
        concentration = (sum(sorted(amounts, reverse=True)[:3]) / sum(amounts) * 100
                         if len(amounts) == len(members) and sum(amounts) > 0 else None)
        for row in members:
            rank = 0
            for other in members:
                if other["change_pct"] <= row["change_pct"]:
                    rank += 1
            row.update(sector_breadth_pct=breadth, sector_change_pct=change,
                       sector_top3_amount_pct=concentration,
                       sector_strength_percentile=rank / len(members) * 100)
    return rows, market


def feature_catalog(rows, market):
    result = {}
    for field in STOCK_FIELDS:
        values = sorted(r[field] for r in rows if r.get(field) is not None)
        if values:
            result[field] = {"coverage": len(values) / len(rows), "minimum": values[0],
                             "median": values[len(values) // 2], "maximum": values[-1]}
    result.update({field: {"value": value} for field, value in market.items() if value is not None})
    return result
