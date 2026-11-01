"""Pure v3 research contract: immutable facts, typed reasoning and rendering.

No provider, database, publication or repair policy belongs to this module.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math
from typing import Any
from zoneinfo import ZoneInfo

INPUT_VERSION = "probiga.research-input.v3"
DRAFT_VERSION = "probiga.research-draft.v3"
# Includes bounded technical/PIT evidence and the eight-unit parent baseline.
# Provider/bridge envelopes enforce their separate transport byte limits.
MAX_PACKET_BYTES = 48 * 1024
MAX_DRAFT_CHARS = 50_000
TZ = ZoneInfo("Asia/Shanghai")
ROLES = ("总策略负责人", "情绪周期分析师", "相对强度分析师", "博弈与资金分析师", "主线交易员", "风险经理", "反方审查员", "复盘审计员")
WINDOWS = ("open", "first_30m", "midday", "close")
TOPICS = ("environment", "sentiment", "mainline", "identity", "risk", "counter_evidence", "event", "continuity", "audit", "action")


class ContractError(ValueError):
    pass


def _fail(reason: str, path: str = "") -> None:
    raise ContractError(f"RESEARCH_CONTRACT_BLOCKED {reason}" + (f" path={path}" if path else ""))


def _json(value: Any) -> str:
    def convert(item):
        if isinstance(item, (date, datetime)):
            return item.isoformat()
        if isinstance(item, Decimal):
            return float(item)
        if hasattr(item, "item"):
            return item.item()
        raise TypeError(type(item).__name__)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=convert, allow_nan=False)


def _object(properties, required=None):
    return {"type": "object", "properties": properties, "required": list(properties) if required is None else required, "additionalProperties": False}


def _array(items, minimum=0, maximum=40):
    return {"type": "array", "items": items, "minItems": minimum, "maxItems": maximum}


def draft_json_schema() -> dict:
    """The same closed JSON schema is used by both providers and local checks."""
    string = {"type": "string", "minLength": 1, "maxLength": 1600}
    identifier = {"type": "string", "minLength": 1, "maxLength": 32}
    ids = _array(identifier, 1, 20)
    span = {"anyOf": [_object({"text": string}), _object({"fact_id": identifier, "field": identifier}),
                       _object({"entity_id": identifier}), _object({"task": {"type": "string", "enum": ["review_date", "next_trade_date", "data_cutoff_at", "context_cutoff_at", "generated_at"]}})]}
    spans = _array(span, 1, 40)
    common = {"id": identifier, "topic": {"type": "string", "enum": list(TOPICS)}, "statement": spans}
    market = _object({**common, "kind": {"type": "string", "const": "market_inference"}, "evidence_ids": ids,
                      "confidence": {"type": "string", "enum": ["high", "medium", "low"]}})
    limitation = _object({**common, "kind": {"type": "string", "const": "data_limitation"}, "evidence_ids": ids,
                          "effect": {"type": "string", "enum": ["reduce_confidence", "disable_turnover", "unverified_identity", "missing_window", "no_portfolio_budget", "limited_coverage", "no_prior_plan"]}})
    baseline = _object({**common, "kind": {"type": "string", "const": "baseline_restatement"}, "evidence_ids": ids})
    condition = _object({"observable_id": identifier, "window": {"type": "string", "enum": list(WINDOWS)},
                         "operator": {"type": "string", "enum": ["improves", "weakens", "turns_positive", "turns_negative", "holds_above", "breaks_below"]},
                         "reference": _object({"kind": {"type": "string", "enum": ["same_session_open", "fact", "zero"]}, "fact_id": {"type": "string", "maxLength": 32}, "field": {"type": "string", "maxLength": 32}})})
    future = _object({**common, "kind": {"type": "string", "const": "future_condition"}, "logic": {"type": "string", "enum": ["all", "any"]}, "conditions": _array(condition, 1, 4)})
    watch = _object({"entity_id": identifier, "identity": {"type": "string", "enum": ["unconfirmed", "front_watch", "capacity_candidate", "leader", "anchor", "follower"]},
                     "assessment_units": ids, "trigger_units": ids, "abandon_units": ids})
    direction = _object({"entity_id": identifier, "classification": {"type": "string", "enum": ["persistent_candidate", "defensive_candidate", "rebound_candidate", "risk_release", "unconfirmed"]},
                         "assessment_units": ids, "watches": _array(watch, 0, 3)})
    continuity = _object({"baseline_fact_id": identifier, "change": {"type": "string", "enum": ["inherited", "strengthened", "weakened", "invalidated", "unassessable"]},
                          "new_fact_ids": _array(identifier, 0, 12), "reason_units": ids})
    report = _object({"conclusion_units": ids, "permission": _object({"action": {"type": "string", "enum": ["WAIT", "OBSERVE", "CONDITIONAL"]}, "basis_units": ids}),
                      "market_reading_units": ids, "directions": _array(direction, 0, 3), "event_units": _array(identifier, 0, 8),
                      "scenarios": _array(_object({"name": string, "trigger_units": ids, "action_units": ids, "abandon_units": ids}), 1, 3),
                      "checks": _object({window: _array(identifier, 0, 12) for window in WINDOWS}), "counter_evidence_units": ids,
                      "limitation_units": _array(identifier, 0, 8), "continuity": _array(continuity, 0, 12), "audit_units": ids})
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", "title": DRAFT_VERSION,
            **_object({"schema_version": {"type": "string", "const": DRAFT_VERSION}, "units": _array({"anyOf": [market, limitation, baseline, future]}, 8, 48),
                       "roles": _array(_object({"role": {"type": "string", "enum": list(ROLES)}, "unit_ids": ids}), 8, 8), "report": report})}


def _check_schema(value, schema, path="$"):
    if "anyOf" in schema:
        for branch in schema["anyOf"]:
            try:
                _check_schema(value, branch, path)
                return
            except ContractError:
                pass
        _fail("schema_union_mismatch", path)
    if "const" in schema and value != schema["const"]:
        _fail("schema_const", path)
    if "enum" in schema and value not in schema["enum"]:
        _fail("schema_enum", path)
    kind = schema.get("type")
    if kind == "object":
        if not isinstance(value, dict):
            _fail("schema_object", path)
        if set(schema.get("required", [])) - value.keys():
            _fail("schema_missing_fields", path)
        if schema.get("additionalProperties") is False and value.keys() - schema.get("properties", {}).keys():
            _fail("schema_extra_fields", path)
        for key, item in value.items():
            if key in schema.get("properties", {}):
                _check_schema(item, schema["properties"][key], path + "." + key)
    elif kind == "array":
        if not isinstance(value, list) or not schema.get("minItems", 0) <= len(value) <= schema.get("maxItems", 10**9):
            _fail("schema_array", path)
        for index, item in enumerate(value):
            _check_schema(item, schema["items"], f"{path}[{index}]")
    elif kind == "string":
        if not isinstance(value, str) or not schema.get("minLength", 0) <= len(value) <= schema.get("maxLength", 10**9):
            _fail("schema_string", path)


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _stamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=TZ) if parsed.tzinfo is None else parsed.astimezone(TZ)
    except (ValueError, TypeError):
        return None


def _time_text(value):
    parsed = _stamp(value)
    return parsed.strftime("%m月%d日 %H:%M") if parsed else str(value)


def _unit(field):
    if field in {"basic_eps", "net_asset_ps", "oper_cf_ps"}:
        return "price"
    if field in {"total_rev", "net_profit_attr_sh", "non_gaap_net_profit"}:
        return "CNY"
    if field in {"total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr", "roe_wtd", "roa_wtd", "gross_margin", "net_margin", "asset_liab_ratio", "cash_flow_ratio"}:
        return "percent"
    if field.endswith("_pct"):
        return "percent"
    if field.endswith(("_share", "_coverage")) or field == "coverage":
        return "ratio"
    if field.endswith(("_amount", "market_cap")) or field == "amount":
        return "CNY"
    if field in {"price", "open", "close", "high", "low", "pre_close", "ma5", "ma10", "ma20", "ma60"}:
        return "price"
    return "number"


def _format(field, value):
    if value is None:
        return "待核验"
    if field.endswith("_at"):
        return _time_text(value)
    if field == "history":
        return "；".join(f"{row[0]} {_format('change_pct', row[1])}" + (f"，热度序位{row[2]}" if len(row) > 2 and row[2] is not None else "") for row in value)
    if not isinstance(value, (int, float, Decimal)) or isinstance(value, bool):
        return str(value)
    unit = _unit(field)
    if unit == "percent":
        return f"{value:+.2f}%"
    if unit == "ratio":
        return f"{value * 100:.2f}%"
    if unit == "CNY":
        return f"{value / 100_000_000:,.2f}亿元"
    if unit == "price":
        return f"{value:.2f}"
    if field.startswith(("macd_", "kdj_", "previous_macd_", "previous_kdj_")):
        return f"{value:.6g}"
    if field.startswith("volume_ratio"):
        return f"{value:.2f}倍"
    return str(int(value)) if value == int(value) else str(value)


def _span_text(spans, packet):
    parts = []
    for span in spans:
        if "text" in span:
            parts.append(span["text"])
        elif "entity_id" in span:
            entity = packet["entities"][span["entity_id"]]
            parts.append(entity["name"] + (f"（{entity['code']}）" if entity.get("kind") == "stock" and entity.get("code") else ""))
        elif "task" in span:
            value = packet["task"][span["task"]]
            parts.append(_time_text(value) if span["task"].endswith("_at") else str(value))
        else:
            card = packet["facts"][span["fact_id"]]
            parts.append(_format(span["field"], card["values"][span["field"]]))
    return "".join(parts)


def build_packet(digest: dict, *, report_kind: str, next_trade_date: str, context: dict | None,
                 previous_research: dict | None, now: datetime, source_digest_sha256: str) -> dict:
    """Project verified source material, never repair it or mutate the snapshot."""
    if report_kind not in {"review", "briefing", "intraday"}:
        _fail("invalid_report_kind")
    context = context or {}
    current = _stamp(now)
    if current is None:
        _fail("invalid_now")
    review_date = date.fromisoformat(str(digest["review_date"])[:10]).isoformat()
    next_trade_date = date.fromisoformat(str(next_trade_date)[:10]).isoformat()
    quality = digest.get("quality_json") or {}
    intraday = report_kind == "intraday"
    observation = context.get("intraday_observation") or {}
    if intraday:
        asof = _stamp(context.get("cutoff_at"))
        source_at = _stamp(observation.get("source_snapshot_at"))
        if (not asof or not source_at or source_at > asof or asof > current
                or asof.date().isoformat() != review_date or not observation.get("market")):
            _fail("unverified_intraday_snapshot")
        market = observation["market"]
        theme = {"sectors": observation.get("sectors", []), "stocks": observation.get("key_stocks", []),
                 "history_dates": observation.get("history_dates", [])}
        indices = market.get("indices", [])
        window = "intraday_snapshot"
    else:
        if (digest.get("publish_status") != "ready" or quality.get("errors") or quality.get("target_date") != review_date
                or (quality.get("source_dates") or {}).get("target_bars") != review_date or review_date >= next_trade_date
                or review_date > current.date().isoformat() or (review_date == current.date().isoformat() and (current.hour, current.minute) < (15, 30))):
            _fail("unverified_closed_session")
        asof = current
        market = digest.get("market_structure_json") or {}
        theme = context.get("theme_evidence") or {}
        indices = context["indices"].get("rows", []) if isinstance(context.get("indices"), dict) and context["indices"].get("status") == "OBSERVED" else []
        window = "full_session"
    packet = {"schema_version": INPUT_VERSION, "task": {"kind": report_kind, "review_date": review_date,
              "next_trade_date": next_trade_date, "generated_at": current.isoformat(),
              "data_cutoff_at": str(observation.get("source_snapshot_at") if intraday else digest.get("data_cutoff_at") or ""),
              "context_cutoff_at": str(context.get("cutoff_at") or ""), "as_of_at": asof.isoformat(),
              "condition_horizon": "remaining_session" if intraday else "next_session",
              "source_digest_sha256": source_digest_sha256, "context_sha256": hashlib.sha256(_json(context).encode()).hexdigest()}, "entities": {}, "facts": {}, "sources": {},
              "observables": {}, "baseline": None}
    if intraday:
        packet["task"].update({key: observation.get(key) for key in ("observation_id", "source_snapshot_at", "observed_at", "source_provider")})
        packet["task"].update({key: context[key] for key in ("mode", "asof_mode") if key in context})
    source_ids = {}

    def source(name):
        name = str(name or "ProBigA")[:100]
        if name not in source_ids:
            source_ids[name] = "S" + str(len(source_ids) + 1)
            packet["sources"][source_ids[name]] = {"name": name}
        return source_ids[name]

    def entity(kind, name, code=None, sectors=None):
        identity = "E" + str(len(packet["entities"]) + 1)
        item = {"kind": kind, "name": str(name)[:60]}
        if code is not None:
            item["code"] = str(code)
        if sectors:
            item["sector_ids"] = list(sectors)
        packet["entities"][identity] = item
        return identity

    def card(kind, metric, values, eid=None, source_name=None, scope=None, **extra):
        values = {key: value for key, value in values.items() if value is not None}
        if not values:
            return None
        identity = "F" + str(len(packet["facts"]) + 1)
        packet["facts"][identity] = {"kind": kind, "entity_id": eid, "metric": metric, "values": values,
                                     "validity": "usable", "source_id": source(source_name), "scope": scope or "verified_price_sample",
                                     "trade_date": review_date, "window": window, **extra}
        return identity

    def limitation(dataset, summary, state="unavailable"):
        return card("data_quality", dataset, {"summary": summary}, scope="research_coverage", dataset_status=state)

    def measurements(value, names):
        return {name: number for name in names if (number := _number(value.get(name))) is not None}

    market_id = entity("market", "A股有效行情样本")
    market_values = measurements(market, ("sample_count", "return_median_pct", "up_coverage", "market_cap_weighted_return_pct"))
    coverage = observation.get("coverage") if intraday else (quality.get("coverage") or {}).get("target")
    if _number(coverage) is not None:
        market_values["coverage"] = float(coverage)
    if intraday:
        market_values.update(measurements(observation, ("expected_count", "observed_count")))
        market_values.update(measurements(market, ("up_count", "down_count", "flat_count", "up_share", "previous_strength_return_pct")))
    card("market_measurement", "market_prices", market_values, market_id)
    facts = {} if intraday else market.get("decision_facts") or {}
    for key, fields in (("breadth", ("up_count", "down_count", "flat_count", "up_share")),
                        ("extremes", ("up_count", "down_count", "threshold_pct")),
                        ("previous_strength", ("sample_count", "mean_return_pct", "median_return_pct", "positive_share", "matched_coverage"))):
        raw = facts.get(key) or {}
        if raw.get("evidence_type") == "OBSERVED":
            values = measurements(raw, fields)
            if key == "extremes":
                values["definition"] = "极端收益样本，不是交易所涨跌停家数"
            if key == "previous_strength":
                values["definition"] = "上一交易日收益前10%（含并列），不是昨日涨停股"
            card("market_measurement", key, values, market_id)
    close = facts.get("close_position") or {}
    if close.get("evidence_type") == "OBSERVED" and _number(close.get("median")) is not None:
        card("market_measurement", "daily_range", {"close_position_share": close["median"]}, market_id)
    amount_quality = market.get("amount_quality") or {}
    amount_state = amount_quality.get("status") if isinstance(amount_quality, dict) else str(amount_quality)
    if amount_state == "OBSERVED":
        card("market_measurement", "turnover", measurements(market, ("total_amount", "previous_total_amount", "amount_change_pct")), market_id)
    else:
        limitation("intraday_amount" if intraday else "daily_amount", "当前快照未提供已核验成交额，不能判断量能变化" if intraday else "日线成交额未通过覆盖与量价核验，金额权重、容量排名和量能比较不可用")
    names = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指", "000688": "科创50", "899050": "北证50"}
    for row in indices[:6]:
        if str(row.get("trade_date", review_date))[:10] != review_date:
            continue
        code = str(row.get("index_code") or row.get("code") or "")
        eid = entity("index", names.get(code, row.get("name") or code), code)
        # Index amount has no independent unit/integrity receipt. Never expose it.
        values = measurements(row, ("change_pct", "open", "close", "high", "low"))
        if intraday and "close" in values:
            values["price"] = values.pop("close")
        card("market_measurement", "index_prices", values, eid, "当时指数快照" if intraday else "同日指数行情", scope="index_quote")
    sector_map = {}
    available_sectors = [row for row in theme.get("sectors", []) if isinstance(row, dict)]
    # Keep each observed source category before filling with attention order.
    representatives = []
    seen_types = set()
    for row in available_sectors:
        kind = row.get("plate_type", "snapshot")
        if kind not in seen_types:
            representatives.append(row)
            seen_types.add(kind)
    raw_sectors = (representatives + [row for row in available_sectors if row not in representatives])[:10]
    for row in raw_sectors:
        if row.get("status", "OBSERVED") not in {"OBSERVED", "VERIFIED", "COMPLETE"}:
            continue
        name = row.get("name") or row.get("concept_name")
        if not name:
            continue
        eid = entity("theme", name, row.get("concept_code"))
        sector_map[str(name)] = eid
        history = [[str(r.get("trade_date")), r.get("change_pct"), r.get("rank")] for r in row.get("history", [])
                   if r.get("status", "OBSERVED") == "OBSERVED" and _number(r.get("change_pct")) is not None]
        values = measurements(row, ("change_pct", "rank", "cumulative_return_5d_pct"))
        if history:
            values.update(history=history, history_count=len(history))
        card("market_measurement", "theme_prices", values, eid, row.get("source") or "题材行情样本", scope="source_theme_sample")
    raw_stocks = [row for row in theme.get("stocks", []) if isinstance(row, dict)]
    raw_stocks.sort(key=lambda row: not any(name in sector_map for name in row.get("sector_names", [])))
    for row in raw_stocks[:12]:
        code = str(row.get("stock_code") or row.get("code") or "")
        if len(code) != 6 or not code.isdecimal():
            continue
        eid = entity("stock", row.get("short_name") or row.get("stock_name") or code, code,
                     [sector_map[name] for name in row.get("sector_names", []) if name in sector_map])
        verified = (row.get("price_verification") or {}).get("status", row.get("status")) == "OBSERVED"
        if not verified:
            limitation("stock_price_" + eid, str(row.get("short_name") or code) + "价格校验未通过，不能作身份判断")
            continue
        values = measurements(row, ("change_pct", "cumulative_return_5d_pct", "market_cap", "total_market_cap"))
        ohlc = row.get("ohlc") or {}
        if ohlc.get("status", "OBSERVED") == "OBSERVED":
            values.update(measurements(ohlc, ("open", "close", "pre_close", "high", "low")))
        if intraday and "close" in values:
            values["price"] = values.pop("close")
        history = [[str(r.get("trade_date")), r.get("change_pct")] for r in row.get("history", [])
                   if r.get("status", "OBSERVED") == "OBSERVED" and _number(r.get("change_pct")) is not None]
        if history:
            values.update(history=history, history_count=len(history))
        if (row.get("amount_quality") or {}).get("status") == "OBSERVED":
            values.update(measurements(row, ("amount",)))
        heat = row.get("heat_ranks") or {}
        for key in ("fused", "ths"):
            if _number(heat.get(key)) is not None:
                values["heat_" + key] = heat[key]
        card("market_measurement", "stock_prices", values, eid, "已校验个股行情", scope="attention_candidate_sample")
    # Optional evidence is projected only from a frozen, independently gated
    # source snapshot. A status receipt is never substituted for a measurement.
    linked = {(item["kind"], item.get("code")): eid for eid, item in packet["entities"].items()}
    technical = context.get("technical_facts") or {}
    baseline_day = str(technical.get("baseline_trade_date") or "")[:10]
    valid_baseline = bool(baseline_day and (baseline_day < review_date if intraday else baseline_day == review_date))
    technical_counts = {"ma_stock_count": 0, "macd_stock_count": 0, "kdj_stock_count": 0, "volume_stock_count": 0}
    missing_technical = []
    for group, kind, key in (("stocks", "stock", "stock_code"), ("indices", "index", "index_code")):
        for row in technical.get(group, []):
            eid = linked.get((kind, str(row.get(key) or "")))
            if not eid:
                continue
            values = {}
            verified = (valid_baseline and str(row.get("trade_date"))[:10] == baseline_day
                        and row.get("status") == "OBSERVED" and row.get("price_revision_matches_frozen") is not False
                        and (row.get("coverage") or {}).get("price_status") == "OBSERVED"
                        and (row.get("coverage") or {}).get("calendar_verified") is True
                        and bool(row.get("source_snapshot_sha256")))
            if verified:
                for period in ("5", "10", "20", "60"):
                    ma = (row.get("ma") or {}).get(period) or {}
                    if ma.get("status") == "OBSERVED" and _number(ma.get("last")) is not None:
                        values["ma" + period] = _number(ma["last"])
                        deviation = _number(ma.get("last_deviation_pct"))
                        if deviation is not None:
                            values["ma" + period + "_deviation_pct"] = deviation
                for name, fields in (("macd", ("dif", "dea", "hist")), ("kdj", ("k", "d", "j"))):
                    indicator = row.get(name) or {}
                    if indicator.get("status") != "OBSERVED":
                        continue
                    for phase, prefix in (("last", ""), ("previous", "previous_")):
                        for field, value in measurements(indicator.get(phase) or {}, fields).items():
                            values[prefix + name + "_" + field] = value
                volume = row.get("volume") or {}
                receipt = volume.get("verification") or {}
                if (volume.get("status") == "OBSERVED" and receipt.get("status") == "VERIFIED"
                        and receipt.get("amount_price_status") == "VERIFIED" and volume.get("unit")):
                    for period in ("5", "20"):
                        if _number(volume.get("ratio" + period)) is not None:
                            values["volume_ratio" + period] = _number(volume["ratio" + period])
            if values:
                card("market_measurement", "technical_indicators", values, eid, row.get("source"),
                     scope="closed_daily_technical_baseline", trade_date=baseline_day, window="closed_daily_baseline",
                     source_snapshot_sha256=row["source_snapshot_sha256"], acquired_at=technical.get("acquired_at"))
                if kind == "stock":
                    for name, fields in (("ma", ("ma5", "ma10", "ma20", "ma60")), ("macd", ("macd_dif",)),
                                        ("kdj", ("kdj_k",)), ("volume", ("volume_ratio5", "volume_ratio20"))):
                        technical_counts[name + "_stock_count"] += int(any(field in values for field in fields))
            absent = [name for name, fields in (("MA", ("ma5", "ma10", "ma20", "ma60")),
                      ("MACD", ("macd_dif",)), ("KDJ", ("kdj_k",)), ("日成交量比", ("volume_ratio5", "volume_ratio20")))
                      if not any(field in values for field in fields)]
            if absent:
                missing_technical.append(packet["entities"][eid]["name"] + "：" + "、".join(absent))
    technical_summary = ("已闭市日线技术证据仅覆盖已核验样本；前收与相邻收盘不一致、历史不足或缺日时相应指标不可用，不能推断原因或填零。"
                         "成交量单位和量价校验未经认证时，不提供量比；报道成交额不能替代个股量比。")
    if missing_technical:
        technical_summary += " 当前缺口：" + "；".join(missing_technical)
    elif not technical:
        technical_summary = "未提供经过连续性、复权口径和日历核验的技术及成交量快照，不能声称使用均线、MACD、KDJ或量比。"
    limitation("technical_coverage", technical_summary, "partial" if any(technical_counts.values()) else "unavailable")
    packet["technical_methods"] = {"baseline_trade_date": baseline_day or None, "price_basis": "unadjusted_closed_daily_bars",
        "ma": "MA5/10/20/60，收盘价算术均值；偏离为收盘相对该均线百分比",
        "macd": "EMA(12,26,9), adjust=False, 首收盘初始化；柱=2*(DIF-DEA)，至少120连续日，前值需121日",
        "kdj": "KDJ(9,3,3)，K和D初始50，平区RSV=50；至少120连续日，前值需121日",
        "volume": "当日日成交量/此前5或20完整日均量，排除当日；不等于盘中同时间量比",
        "availability": "历史日线为本次取得版本，不证明当时盘中已知或技术信号已经触发"}
    fundamental = context.get("fundamental_facts") or {}
    finance_cutoff = _stamp(context.get("cutoff_at"))
    finance_count = 0
    finance_fields = ("basic_eps", "net_asset_ps", "total_rev", "net_profit_attr_sh", "total_rev_yoy_gr", "net_profit_yoy_gr",
                      "roe_wtd", "roa_wtd", "gross_margin", "net_margin", "asset_liab_ratio", "curr_ratio", "quick_ratio",
                      "oper_cf_ps", "non_gaap_net_profit", "non_gaap_net_profit_yoy_gr", "cash_flow_ratio")
    for row in fundamental.get("stocks", []):
        eid = linked.get(("stock", str(row.get("stock_code") or "")))
        latest = row.get("latest") or {}
        known, published = _stamp(latest.get("finance_known_at")), _stamp(latest.get("finance_published_at"))
        publication_date_only = latest.get("finance_publication_time_verified") is not True
        # Date-only disclosures can be used at their certified known_at, without
        # inventing a publication hour or shifting them back to the morning.
        publication_ok = bool(published and finance_cutoff and (published.date() <= finance_cutoff.date() if publication_date_only else published <= finance_cutoff))
        if (not eid or row.get("status") != "OBSERVED" or not latest.get("finance_pit_verified")
                or not fundamental.get("manifest_hash") or not known or not finance_cutoff or known > finance_cutoff
                or not publication_ok or str(latest.get("finance_report_date") or "9999")[:10] > finance_cutoff.date().isoformat()):
            continue
        values = measurements(latest.get("metrics") or {}, finance_fields)
        if not values:
            continue
        values["report_date"] = str(latest["finance_report_date"])[:10]
        values["known_at"] = known.isoformat()
        card("market_measurement", "fundamental_metrics", values, eid, "已认证PIT财务修订",
             scope="certified_public_financial_report", window="financial_report", trade_date=values["report_date"],
             published_at=str(latest.get("finance_published_at")), publication_precision=latest.get("finance_publication_precision"),
             revision_id=latest.get("finance_revision_id"), content_hash=latest.get("finance_content_hash"),
             manifest_hash=fundamental["manifest_hash"], acquired_at=fundamental.get("acquired_at"))
        finance_count += 1
    finance_expected = sum(item["kind"] == "stock" for item in packet["entities"].values())
    limitation("fundamental_coverage", "财务仅使用截止时点已披露且已知、通过PIT完整性认证的报告字段；未认证部分不能判断盈利质量、估值或财务催化。"
               "财务原表或修订记录存在不等于认证完成，不得用单期每股收益冒充TTM或拼估值。",
               "complete" if finance_count == finance_expected and finance_count else "partial" if finance_count else "unavailable")
    packet["evidence_coverage"] = {**technical_counts, "fundamental_stock_count": finance_count, "stock_count": finance_expected}
    if intraday:
        histories = observation.get("history") or []
        for row in histories[-3:]:
            stamp = _stamp(row.get("source_snapshot_at"))
            if not stamp or stamp > asof or stamp.date() != asof.date():
                continue
            values = measurements(row.get("market") or {}, ("up_coverage", "up_share", "up_count", "down_count", "return_median_pct"))
            if values:
                values["observed_at"] = stamp.isoformat()
                card("market_measurement", "intraday_history", values, market_id, "留存盘中快照", scope="same_session_snapshot", window="intraday_history")
    if not raw_sectors:
        limitation("themes", "当前材料未提供可比较的同日方向样本，不能据此断言市场没有主线")
    limitation("roles", "热度、涨幅和单日成交额不能单独证明龙头、中军、跟风或分歧承接关系")
    limitation("audit_windows", "没有逐时段触发与执行回执，不能宣称已验证研究假设或交易过程")
    if not context.get("portfolio") or not context.get("risk_budget"):
        limitation("portfolio", "未提供账户持仓与风险预算，不能给出账户仓位命令或组合损失结论")
    turnover = context.get("market_turnover") or {}
    if not intraday and turnover.get("status") == "OBSERVED":
        card("source_statement", "reported_turnover", {**measurements(turnover, ("reported_total_amount", "reported_change_amount")),
             "scope": str(turnover.get("scope") or "沪深报道口径"), "quote": str(turnover.get("content") or "")[:600],
             "published_at": str(turnover.get("publish_time") or "")}, source_name=turnover.get("source"), scope="reported_sh_sz_not_all_a")
    prior = previous_research or {}
    prior_packet, prior_draft = prior.get("input_packet"), prior.get("draft")
    if isinstance(prior_packet, dict) and prior_packet.get("schema_version") == INPUT_VERSION and isinstance(prior_draft, dict):
        validate_draft(_json(prior_draft), prior_packet)
        if report_kind == "briefing" and (prior_packet["task"]["kind"] != "review" or prior_packet["task"]["review_date"] != review_date or prior_packet["task"]["next_trade_date"] != next_trade_date):
            _fail("parent_period_mismatch")
        parent_id = prior.get("research_sha256") or hashlib.sha256(_json(prior_draft).encode()).hexdigest()
        baseline = {"parent_report_id": parent_id, "context_cutoff_at": prior_packet["task"].get("context_cutoff_at"), "unit_facts": {}}
        priority = {"environment": 0, "sentiment": 1, "counter_evidence": 2, "mainline": 3, "identity": 4}
        for unit in sorted(prior_draft.get("units", []), key=lambda item: priority.get(item.get("topic"), 5))[:8]:
            text_value = _span_text(unit["statement"], prior_packet)
            fid = card("baseline_hypothesis", unit["topic"], {"statement": text_value}, source_name="前份已留存研究", scope="prior_research_only",
                       parent_report_id=parent_id, parent_unit_id=unit["id"], trade_date=prior_packet["task"]["review_date"])
            baseline["unit_facts"][fid] = unit["id"]
        packet["baseline"] = baseline
    elif report_kind == "briefing":
        _fail("v3_parent_required_for_briefing")
    else:
        limitation("prior_plan", "没有本合同的事前留存基线，不能声称已经验证研究过程")
    # Catalogued conditions name variables, not measurements that already exist.
    for eid, item in packet["entities"].items():
        for metric in (("up_share", "previous_strength_return_pct") if item["kind"] == "market" else ("relative_strength", "price")):
            oid = "O" + str(len(packet["observables"]) + 1)
            packet["observables"][oid] = {"entity_id": eid, "metric": metric, "allowed_windows": list(WINDOWS),
                                          "comparisons": ["same_session_open", "zero"] + (["fact"] if metric == "price" else [])}
    events = context.get("intraday_events") if intraday else (context.get("events") or {}).get("rows", [])
    events = events if isinstance(events, list) else []
    if not intraday:
        terms = [item["name"] for item in packet["entities"].values() if item["kind"] in {"theme", "stock"}]
        events = sorted(events, key=lambda row: -sum(term in str(row.get("title", "")) + str(row.get("content", "")) for term in terms) if isinstance(row, dict) else 0)
        # A related-stock headline must not crowd every policy observation out
        # of the bounded sample. The quotation remains a source statement;
        # a media report is never upgraded to an official policy document.
        policies = [row for row in events if isinstance(row, dict) and any(term in str(row.get("title", "")) for term in ("国务院", "商务部", "财政部", "央行", "证监会", "工信部", "发改委", "政策", "关税"))]
        events = policies[:2] + [row for row in events if row not in policies[:2]]
    seen, event_ids = set(), []
    for row in events:
        if not isinstance(row, dict):
            continue
        quote = str(row.get("content") or row.get("title") or row.get("evidence") or "")
        identity = "".join(str(row.get("title") or quote).split()).casefold()
        stamp = _stamp(row.get("publish_time") or row.get("timestamp"))
        first_seen = _stamp(row.get("first_seen_at") or row.get("observed_at") or row.get("publish_time") or row.get("timestamp"))
        cutoff = _stamp(context.get("cutoff_at")) or current
        if not quote or identity in seen or not stamp or not first_seen or stamp > cutoff or first_seen > cutoff:
            continue
        seen.add(identity)
        values = {"quote": quote[:400], "published_at": stamp.isoformat()}
        if intraday:
            values.update({key: row[key] for key in ("state", "up", "invalidation") if key in row})
        fid = card("source_statement", "rule_event" if intraday else "news", values,
                   source_name=row.get("source") or "规则事件", scope="observed_news_sample")
        event_ids.append(fid)
        if len(event_ids) == 8:
            break
    # Preserve the full snapshot outside this packet. Declare any bounded sample.
    packet["coverage"] = {"theme_sample": len(sector_map), "stock_sample": sum(e["kind"] == "stock" for e in packet["entities"].values()),
                          "news_available": len(events), "news_selected": len(event_ids), "history_dates": theme.get("history_dates", [])}
    while len(_json(packet).encode("utf-8")) > MAX_PACKET_BYTES and len(event_ids) > 2:
        packet["facts"].pop(event_ids.pop())
        packet["coverage"]["news_selected"] = len(event_ids)
    if len(_json(packet).encode("utf-8")) > MAX_PACKET_BYTES:
        _fail("packet_budget_exceeded")
    return json.loads(_json(packet))


def validate_draft(raw: str, packet: dict) -> dict:
    """Reject invalid research; never relabel, normalize or rewrite a draft."""
    if packet.get("schema_version") != INPUT_VERSION:
        _fail("invalid_packet_version")
    if not isinstance(raw, str) or len(raw) > MAX_DRAFT_CHARS:
        _fail("invalid_draft_size")
    try:
        draft = json.loads(raw)
    except (TypeError, ValueError):
        _fail("invalid_draft_json")
    _check_schema(draft, draft_json_schema())
    units = {unit["id"]: unit for unit in draft["units"]}
    if len(units) != len(draft["units"]):
        _fail("duplicate_unit_id")
    entities, facts = packet["entities"], packet["facts"]

    def refs(ids, path, *, kinds=None, topics=None):
        if any(uid not in units for uid in ids):
            _fail("unknown_unit", path)
        selected = [units[uid] for uid in ids]
        if kinds and any(unit["kind"] not in kinds for unit in selected):
            _fail("wrong_unit_kind", path)
        if topics and not any(unit["topic"] in topics for unit in selected):
            _fail("required_topic_missing", path)
        return selected

    def evidence(unit):
        return [facts[fid] for fid in unit.get("evidence_ids", [])]

    def own_stock_quantity(selected, eid, capital=False):
        allowed = {"market_cap", "total_market_cap", "circulating_market_cap", "float_market_cap"}
        if not capital:
            allowed.add("amount")
        return any(card["kind"] == "market_measurement" and card.get("entity_id") == eid and allowed.intersection(card["values"])
                   for unit in selected for card in evidence(unit))

    for unit in draft["units"]:
        path = "units." + unit["id"]
        kind = unit["kind"]
        ids = unit.get("evidence_ids", [])
        if len(set(ids)) != len(ids) or any(fid not in facts or facts[fid].get("validity") != "usable" for fid in ids):
            _fail("invalid_evidence_id", path)
        cards = evidence(unit)
        if kind == "market_inference" and not any(card["kind"] in {"market_measurement", "source_statement"} for card in cards):
            _fail("market_inference_requires_market_fact", path)
        if kind == "data_limitation" and any(card["kind"] != "data_quality" for card in cards):
            _fail("limitation_requires_quality_fact", path)
        if kind == "baseline_restatement" and (not packet.get("baseline") or any(card["kind"] != "baseline_hypothesis" for card in cards)):
            _fail("baseline_restatement_requires_parent", path)
        condition_refs = set()
        if kind == "future_condition":
            for condition in unit["conditions"]:
                observable = packet["observables"].get(condition["observable_id"])
                if not observable or condition["window"] not in observable["allowed_windows"]:
                    _fail("unknown_observable_or_window", path)
                reference = condition["reference"]
                if reference["kind"] not in observable["comparisons"]:
                    _fail("incomparable_metric_window", path)
                operator = condition["operator"]
                if operator in {"holds_above", "breaks_below"}:
                    if reference["kind"] != "fact" or observable["metric"] != "price":
                        _fail("price_threshold_requires_price_fact", path)
                elif operator in {"turns_positive", "turns_negative"}:
                    if reference["kind"] != "zero" or observable["metric"] not in {"previous_strength_return_pct", "relative_strength"}:
                        _fail("zero_boundary_requires_signed_metric", path)
                elif reference["kind"] != "same_session_open":
                    _fail("directional_change_requires_same_session_reference", path)
                if reference["kind"] == "fact":
                    card = facts.get(reference["fact_id"], {})
                    if (card.get("kind") != "market_measurement" or card.get("entity_id") != observable["entity_id"]
                            or reference["field"] not in {"price", "open", "close", "high", "low", "pre_close", "ma5", "ma10", "ma20", "ma60"}
                            or reference["field"] not in card.get("values", {})):
                        _fail("threshold_entity_or_unit_mismatch", path)
                    condition_refs.add(reference["fact_id"])
                elif reference["fact_id"] or reference["field"]:
                    _fail("unexpected_threshold_literal", path)
                if packet["task"]["kind"] == "intraday":
                    cutoff = _stamp(packet["task"]["as_of_at"])
                    deadline = {"open": (9, 31), "first_30m": (10, 0), "midday": (11, 30), "close": (15, 0)}[condition["window"]]
                    if (cutoff.hour, cutoff.minute) >= deadline:
                        _fail("future_condition_window_already_passed", path)
        for span in unit["statement"]:
            if "text" in span:
                if any(char.isdigit() for char in span["text"]):
                    _fail("free_text_number_requires_reference", path)
            elif "entity_id" in span:
                if span["entity_id"] not in entities:
                    _fail("unknown_entity", path)
            elif "task" in span:
                if span["task"] not in packet["task"] or not packet["task"][span["task"]]:
                    _fail("unavailable_task_reference", path)
            else:
                fid = span["fact_id"]
                if fid not in facts or span["field"] not in facts[fid]["values"]:
                    _fail("unknown_fact_or_field", path)
                if fid not in set(ids) | condition_refs:
                    _fail("uncited_fact_span", path)
                if kind == "future_condition" and fid not in condition_refs:
                    _fail("future_fact_not_a_condition_reference", path)
        if not any("text" in span and span["text"].strip() for span in unit["statement"]):
            if kind not in {"data_limitation", "baseline_restatement"}:
                _fail("research_requires_interpretation", path)
    if {role["role"] for role in draft["roles"]} != set(ROLES):
        _fail("missing_or_duplicate_role")
    required_topics = ({"environment"}, {"sentiment"}, {"mainline"}, {"mainline", "risk"}, {"identity", "mainline"}, {"risk"}, {"counter_evidence"}, {"audit"})
    for name, topics in zip(ROLES, required_topics):
        role = next(role for role in draft["roles"] if role["role"] == name)
        refs(role["unit_ids"], "roles." + name, topics=topics)
    report = draft["report"]
    conclusions = refs(report["conclusion_units"], "report.conclusion_units")
    for topic in ("environment", "sentiment"):
        if not any(unit["topic"] == topic and unit["kind"] == "market_inference" for unit in conclusions):
            _fail("conclusion_requires_market_research_" + topic)
    refs(report["permission"]["basis_units"], "report.permission")
    reading = refs(report["market_reading_units"], "report.market_reading_units")
    if not any(unit["kind"] == "market_inference" for unit in reading):
        _fail("market_reading_cannot_be_only_limitations")
    available_themes = {card["entity_id"] for card in facts.values() if card["kind"] == "market_measurement" and card["metric"] == "theme_prices"}
    if available_themes and not report["directions"]:
        _fail("available_directions_not_analysed")
    selected_entities = []
    for direction in report["directions"]:
        eid = direction["entity_id"]
        if eid not in available_themes or eid in selected_entities:
            _fail("unknown_or_duplicate_direction")
        selected_entities.append(eid)
        assessments = refs(direction["assessment_units"], "direction.assessment", kinds={"market_inference", "baseline_restatement"}, topics={"mainline"})
        if not any(card.get("entity_id") == eid for unit in assessments for card in evidence(unit)):
            _fail("direction_requires_own_evidence")
        linked = {key for key, item in entities.items() if item["kind"] == "stock" and eid in item.get("sector_ids", [])
                  and any(card.get("entity_id") == key and card["metric"] == "stock_prices" for card in facts.values())}
        if linked and not direction["watches"]:
            _fail("linked_stock_observation_required")
        used = set()
        for watch in direction["watches"]:
            stock_id = watch["entity_id"]
            if stock_id not in linked or stock_id in used:
                _fail("unlinked_or_duplicate_stock")
            used.add(stock_id)
            selected = refs(watch["assessment_units"], "watch.assessment", kinds={"market_inference"}, topics={"identity"})
            if not any(card.get("entity_id") == stock_id for unit in selected for card in evidence(unit)):
                _fail("stock_requires_own_evidence")
            identity = watch["identity"]
            if identity == "capacity_candidate" and not own_stock_quantity(selected, stock_id):
                _fail("capacity_requires_own_quantity")
            if identity in {"leader", "anchor", "follower"} and not any(card.get("entity_id") == stock_id and card["metric"] in {"leadership", "lead_lag", "sector_response"} for unit in selected for card in evidence(unit)):
                _fail("confirmed_identity_requires_relationship")
            refs(watch["trigger_units"], "watch.trigger", kinds={"future_condition"})
            refs(watch["abandon_units"], "watch.abandon", kinds={"future_condition"})
    refs(report["event_units"], "report.events", kinds={"market_inference", "data_limitation", "baseline_restatement"})
    for scenario in report["scenarios"]:
        if any(char.isdigit() for char in scenario["name"]):
            _fail("scenario_name_number_requires_reference")
        refs(scenario["trigger_units"], "scenario.trigger", kinds={"future_condition"})
        refs(scenario["abandon_units"], "scenario.abandon", kinds={"future_condition"})
        refs(scenario["action_units"], "scenario.action")
    for window, ids in report["checks"].items():
        if not ids and packet["task"]["kind"] != "intraday":
            _fail("required_time_window_missing", window)
        for unit in refs(ids, "checks." + window, kinds={"future_condition"}):
            if not any(condition["window"] == window for condition in unit["conditions"]):
                _fail("check_window_mismatch", window)
    counter = refs(report["counter_evidence_units"], "report.counter_evidence", topics={"counter_evidence"})
    if not any(unit["kind"] == "future_condition" for unit in counter):
        _fail("counter_evidence_requires_observable_condition")
    refs(report["limitation_units"], "report.limitations", kinds={"data_limitation"})
    refs(report["audit_units"], "report.audit", kinds={"data_limitation", "baseline_restatement"}, topics={"audit"})
    for change in report["continuity"]:
        baseline = packet.get("baseline")
        fid = change["baseline_fact_id"]
        if not baseline or fid not in baseline["unit_facts"]:
            _fail("unknown_baseline_unit")
        refs(change["reason_units"], "continuity.reason")
        if change["change"] == "inherited":
            if change["new_fact_ids"]:
                _fail("inherited_baseline_not_new_validation")
        elif change["change"] not in {"unassessable"}:
            if not change["new_fact_ids"]:
                _fail("baseline_change_requires_new_facts")
            cutoff = _stamp(baseline["context_cutoff_at"])
            for new_id in change["new_fact_ids"]:
                card = facts.get(new_id, {})
                published = _stamp(card.get("values", {}).get("published_at"))
                if not cutoff or not published or published <= cutoff or card.get("kind") != "source_statement":
                    _fail("baseline_change_fact_not_new")
    if packet.get("baseline") and not report["continuity"]:
        _fail("baseline_update_required")
    return draft


def render_draft(draft: dict, packet: dict, metadata: dict) -> str:
    """Render authored research with trusted value substitution, no prose repair."""
    task = {**packet["task"], **metadata}
    units = {unit["id"]: unit for unit in draft["units"]}
    report = draft["report"]
    kind = task.get("report_kind", task.get("kind"))
    target = task["next_trade_date"] if kind == "briefing" else task["review_date"]
    title = {"review": "A股盘后复盘", "briefing": "A股早报参考", "intraday": "A股盘中播报"}[kind]
    if kind == "intraday":
        target += " " + (_stamp(task.get("as_of_at")) or _stamp(task["generated_at"])).strftime("%H:%M")
        if task.get("mode") in {"replay", "shadow"}:
            target += "（" + {"replay": "回放", "shadow": "影子观察"}[task["mode"]] + "）"
    lines = [f"# {title}｜{target}", ""]
    if kind == "intraday":
        lines += [f"> 行情源时点：{_time_text(task.get('source_snapshot_at') or task.get('data_cutoff_at'))}；观察时点：{_time_text(task.get('as_of_at') or task.get('context_cutoff_at'))}",
                  "> 验证范围：当日剩余交易时段"]
    else:
        lines += [f"> 行情日：{task['review_date']}；下一交易日：{task['next_trade_date']}",
                  f"> 行情同步：{_time_text(task.get('data_cutoff_at'))}；新闻截止：{_time_text(task.get('context_cutoff_at'))}"]
    lines += [f"> 研究生成：{_time_text(task.get('generated_at'))}（北京时间）", ""]
    if kind == "review" and str(task.get("generated_at", ""))[:10] > task["review_date"]:
        lines += ["> 补记：本篇在行情日之后整理，晚入库收盘事实不能冒充当日事前判断。", ""]

    def text_units(ids):
        return "\n\n".join(_span_text(units[uid]["statement"], packet) for uid in dict.fromkeys(ids))

    lines += ["## 先看结论", "", text_units(report["conclusion_units"]), ""]
    permission = {"WAIT": "等待验证", "OBSERVE": "仅作观察", "CONDITIONAL": "条件满足后再评估"}[report["permission"]["action"]]
    lines += ["**当前行动：" + permission + "。**", "", "## 市场与情绪", "", text_units(report["market_reading_units"]), "", "## 主线与个股观察", ""]
    stages = {"persistent_candidate": "持续性待验证", "defensive_candidate": "防守观察", "rebound_candidate": "反抽观察", "risk_release": "风险观察", "unconfirmed": "待确认"}
    identities = {"unconfirmed": "身份待确认", "front_watch": "前排观察候选", "capacity_candidate": "容量候选", "leader": "龙头", "anchor": "中军", "follower": "跟风"}
    for direction in report["directions"]:
        lines += [f"### {packet['entities'][direction['entity_id']]['name']}｜{stages[direction['classification']]}", "", text_units(direction["assessment_units"]), ""]
        for watch in direction["watches"]:
            entity = packet["entities"][watch["entity_id"]]
            lines += [f"**{entity['name']}（{entity['code']}）｜{identities[watch['identity']]}**", "", text_units(watch["assessment_units"]), "",
                      "观察验证：" + text_units(watch["trigger_units"]), "", "放弃条件：" + text_units(watch["abandon_units"]), ""]
    if report["event_units"]:
        lines += ["## 新消息改变了什么", "", text_units(report["event_units"]), ""]
    if report["continuity"]:
        labels = {"inherited": "继承基线，尚未新增验证", "strengthened": "新增支持", "weakened": "判断减弱", "invalidated": "假设失效", "unassessable": "暂不能更新"}
        lines += ["## 前日计划如何更新", ""]
        for change in report["continuity"]:
            lines += ["**" + labels[change["change"]] + "**：" + text_units(change["reason_units"]), ""]
    lines += ["## 验证与放弃条件", ""]
    for scenario in report["scenarios"]:
        lines += ["**" + scenario["name"] + "**", "", text_units(scenario["trigger_units"]), "", "允许行动：" + text_units(scenario["action_units"]), "", "放弃条件：" + text_units(scenario["abandon_units"]), ""]
    labels = {"open": "开盘", "first_30m": "前半小时", "midday": "午间", "close": "尾盘"}
    for window in WINDOWS:
        ids = report["checks"][window]
        if ids:
            lines += ["- **" + labels[window] + "**：" + text_units(ids).replace("\n\n", " ")]
    lines += ["", "## 最强反证", "", text_units(report["counter_evidence_units"])]
    if report["limitation_units"]:
        lines += ["", "## 哪些信息还会改变判断", "", text_units(report["limitation_units"])]
    return "\n".join(lines).strip()
