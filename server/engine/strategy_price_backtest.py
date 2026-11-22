"""Checkpointed, causal daily-price research using existing verified market data.

Reconstructed hypotheses are retrospective experiments, not signals actually
issued in the past. Receipt time and the hypothetical decision clock stay distinct.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
from pathlib import Path
import time

import pandas as pd
from sqlalchemy import bindparam, text

from server.common.analysis_pool_receipt import canonical_sha256
from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE
from server.common.strategy_daily_input_window import daily_input_catalog_join
from server.engine.strategy_rule_backtest import _partitions, partition_key, PART_ENTITY
from server.engine.strategy_rule_language import feature_frame, select_rule_rows


PRICE_FIELDS = frozenset({"change_pct", "previous_change_pct", "pct_5", "pct_20", "dist_ma20",
    "drawdown_60", "from_low_60", "amount_ratio_5", "amount_ratio_20", "volatility_20",
    "close_location_value", "relative_change_pct", "relative_20d_pct", "market_change_pct",
    "market_breadth_pct", "market_return_20d_pct"})
BAR_COLUMNS = ("stock_code", "trade_date", "open", "high", "low", "close", "pre_close", "volume", "amount")


def _now():
    return datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")


def supports_price_rule(rule):
    fields = {c["field"] for g in ("market_all", "entry_all", "entry_any", "ranking") for c in rule[g]}
    return fields <= PRICE_FIELDS


def replay_contract():
    from server.engine import strategy_rule_replay
    return canonical_sha256({"price_research": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "execution": hashlib.sha256(Path(strategy_rule_replay.__file__).read_bytes()).hexdigest(),
        "pandas": pd.__version__, "scope": "RETROSPECTIVE_VERIFIED_INPUTS"})


def technical_history(raw):
    """Same trailing formulas as the close producer, retaining every target date."""
    frame = pd.DataFrame(raw).copy()
    if frame.empty or frame.duplicated(["stock_code", "trade_date"]).any():
        raise ValueError("历史研究行情为空或业务键重复")
    frame["stock_code"] = frame.stock_code.astype(str).str[:6].str.zfill(6)
    frame["trade_date"] = frame.trade_date.astype(str).str[:10]
    for key in BAR_COLUMNS[2:]:
        frame[key] = pd.to_numeric(frame[key], errors="coerce")
    if frame[list(BAR_COLUMNS[2:])].isna().any().any() or (frame[["close", "pre_close"]] <= 0).any().any():
        raise ValueError("历史量价字段缺失或价格无效")
    frame = frame.sort_values(["stock_code", "trade_date"])
    frame["change_pct"] = (frame.close / frame.pre_close - 1) * 100
    grouped = frame.groupby("stock_code", group_keys=False)
    frame["previous_change_pct"] = grouped.change_pct.shift(1)
    for n in (5, 20):
        frame[f"pct_{n}"] = grouped.close.pct_change(n, fill_method=None) * 100
        amounts = grouped.amount.transform(lambda s: s.rolling(n, min_periods=3 if n == 5 else 8).mean())
        frame[f"amount_ratio_{n}"] = frame.amount / amounts.replace(0, float("nan"))
    mean20 = grouped.close.transform(lambda s: s.rolling(20, min_periods=10).mean())
    frame["dist_ma20"] = (frame.close / mean20 - 1) * 100
    frame["volatility_20"] = grouped.change_pct.transform(lambda s: s.rolling(20, min_periods=8).std())
    hi = grouped.high.transform(lambda s: s.rolling(60, min_periods=20).max())
    lo = grouped.low.transform(lambda s: s.rolling(60, min_periods=20).min())
    frame["drawdown_60"] = (frame.close / hi - 1) * 100
    frame["from_low_60"] = (frame.close / lo - 1) * 100
    frame["close_location_value"] = ((frame.close - frame.low) / (frame.high - frame.low).replace(0, float("nan"))).clip(0, 1).fillna(.5).round(6)
    return frame


def _group_inputs(connection, sessions, cutoff):
    from server.common.qmt_datadir_daily_coverage import MANIFEST_TABLE, SCHEMA, PROVIDER
    manifests = {str(r[0])[:10] for r in connection.execute(text(f"""SELECT DISTINCT trade_date
        FROM {MANIFEST_TABLE} WHERE schema_version=:schema AND dataset='stock_daily' AND provider=:provider
        AND status='EXACT' AND strategy_eligible=1 AND trade_date BETWEEN :start AND :end AND captured_at<=:cutoff"""),
        {"schema": SCHEMA, "provider": PROVIDER, "start": sessions[0], "end": sessions[-1], "cutoff": cutoff})}
    from server.common.verified_daily_market_truth import FREE_DAILY_PROVIDERS
    public = {str(r[0])[:10] for r in connection.execute(text("""SELECT DISTINCT target_date
        FROM acquisition_partition_state WHERE dataset='stock_daily' AND partition_key='__daily_partition__'
        AND status='complete' AND source IN :providers AND target_date BETWEEN :start AND :end
    """).bindparams(bindparam("providers", expanding=True)),
        {"providers": sorted(FREE_DAILY_PROVIDERS), "start": sessions[0], "end": sessions[-1]})}
    # An archive day need not have the legacy attestation table populated;
    # the existing retained reader also verifies original QMT rows directly.
    sources = {day: "QMT_MANIFEST" if day in manifests else "VERIFIED_PUBLIC" if day in public
               else "QMT_RETAINED" for day in sessions}
    # Retained archive validation accepts exact days; 16 measured calls stay
    # bounded without re-running the old whole-range finality verifier.
    return [{"sessions": sessions[i:i + 16], "sources": {d: sources[d] for d in sessions[i:i + 16]}}
            for i in range(0, len(sessions), 16)]


def _prepare_native_scopes(connection, run, cutoff):
    """Resolve the original signed universe, including its legal no-row pairs."""
    from server.common.qmt_attestation_contract import validated_universe_manifest, validated_no_row_exception_contract
    from server.common.qmt_daily_market_truth import _validate_bound_daily_entries
    from server.common.qmt_daily_no_row import project_catalog_daily_codes
    from server.common.qmt_stock_catalog import load_stock_catalog
    from server.common.qmt_trade_calendar import load_trade_calendar_receipt
    start, end = str(run["start_date"])[:10], str(run["end_date"])[:10]
    if run["status"] != "COMPLETED" or str(run["finished_at"]) > cutoff:
        raise ValueError("历史原生来源未在研究截止前完成")
    manifest = validated_universe_manifest(run["tolerance_json"], start_date=start, end_date=end)
    no_rows = validated_no_row_exception_contract(run["tolerance_json"], start_date=start, end_date=end)
    first = next(iter(manifest.values()))
    catalog = load_stock_catalog(connection, batch_id=first["catalog_batch_id"], decision_known_at=cutoff)
    calendar = load_trade_calendar_receipt(connection, batch_id=first["calendar_batch_id"],
        start_date=start, end_date=end, decision_known_at=cutoff)
    total = _validate_bound_daily_entries(manifest, catalog=catalog, calendar=calendar,
        run_start_date=start, run_end_date=end, no_row_exception_contract=no_rows)
    if total != int(run["target_rows"]) or total != int(run["matched_rows"]):
        raise ValueError("原生历史目录和已完成来源计数不一致")
    projected = project_catalog_daily_codes(catalog=catalog, calendar=calendar, start_date=start,
        end_date=end, contract=no_rows) if no_rows else {d: catalog.eligible_codes(d) for d in manifest}
    return {d: {"catalog": catalog.batch_id, "catalog_manifest_hash": catalog.manifest_hash,
        "excluded_codes": sorted(set(catalog.eligible_codes(d)) - set(projected[d])),
        "stock_set_hash": entry["stock_set_hash"], "stock_count": int(entry["stock_count"]),
        "native_receipt_id": str(run["run_id"]), "available_at": str(run["finished_at"])}
        for d, entry in manifest.items()}


def _source_scope(connection, truth, day, cutoff):
    base = {"catalog": truth["catalog_batch_id"], "excluded_codes": [],
            "stock_count": int(truth.get("attested_row_count", truth.get("row_count", 0))),
            "stock_set_hash": truth.get("code_set_hash")}
    if truth.get("provider") != "qmt_retained_history":
        return base
    from server.common.formal_analysis_store import FormalAnalysisStore
    receipt = str(truth["run_id"])
    generation = canonical_sha256({"schema": "probiga.price-native-source-scope.v1", "receipt": receipt})
    store = FormalAnalysisStore(connection)
    scope = store.get(generation, "price_native_scope", day)
    if scope is None:
        run = connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ *
            FROM qmt_kline_attestation_run WHERE run_id=:uid AND provider='gj_big_qmt_inner'"""),
            {"uid": receipt}).mappings().one_or_none()
        if run is None:
            if not receipt.startswith("retained-row-proof-"):
                raise ValueError(day + " 的原生历史完成记录不存在")
            return base
        scopes = _prepare_native_scopes(connection, run, cutoff)
        store.put_many(generation, "price_native_scope", scopes)
        scope = scopes[day]
    if (scope["stock_count"] != base["stock_count"] or scope["stock_set_hash"] != base["stock_set_hash"]
            or scope["available_at"] > cutoff):
        raise ValueError(day + " 的原始证券集合与来源证明不一致")
    return scope


def _scope_filters(scopes, days):
    clauses, params, expanding = [], {}, []
    for i, day in enumerate(days):
        excluded = scopes[day]["excluded_codes"]
        if excluded:
            clauses.append(f"NOT (k.trade_date=:scope_day_{i} AND SUBSTR(k.stock_code,1,6) IN :scope_excluded_{i})")
            params[f"scope_day_{i}"], params[f"scope_excluded_{i}"] = day, excluded
            expanding.append(bindparam(f"scope_excluded_{i}", expanding=True))
    return (" AND " + " AND ".join(clauses) if clauses else ""), params, expanding


def _resolved_scopes(connection, inputs, days, cutoff):
    proofs = {d: proof for part in inputs.values() for d, proof in part["proofs"].items()}
    return {d: _source_scope(connection, proofs[d], d, cutoff) for d in days}


def _validate_group(connection, group, cutoff):
    days = group["sessions"]
    from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
    from server.common.verified_daily_market_truth import load_retained_qmt_daily_market_truth, load_verified_daily_market_truth
    bindings, proofs, expected, scopes = {}, {}, {}, {}
    for day in days:
        source = group["sources"][day]
        loader = {"QMT_MANIFEST": load_qmt_daily_market_truth, "QMT_RETAINED": load_retained_qmt_daily_market_truth,
                  "VERIFIED_PUBLIC": load_verified_daily_market_truth}[source]
        truth = loader(connection, start_date=day, end_date=day, decision_known_at=cutoff).as_dict()
        scopes[day] = _source_scope(connection, truth, day, cutoff)
        bindings[day] = scopes[day]["catalog"]
        expected[day] = int(truth.get("attested_row_count", truth.get("row_count", 0)))
        proofs[day] = truth
    join, params = daily_input_catalog_join({"sessions": days, "catalog_batches_by_session": bindings})
    predicate, scope_params, expanding = _scope_filters(scopes, days)
    force = " FORCE INDEX (idx_date_ktype) " if connection.dialect.name == "mysql" else " "
    counts = {str(r[0])[:10]: int(r[1]) for r in connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(12000) */
        k.trade_date,COUNT(*) FROM sm_stock_kline k """ + force + join + """ WHERE k.k_type=1 AND k.adjust_type=0
        AND k.trade_date BETWEEN :start AND :end AND k.received_at<=:cutoff """ + predicate + " GROUP BY k.trade_date").bindparams(*expanding),
        {**params, **scope_params, "start": days[0], "end": days[-1], "cutoff": cutoff})}
    if counts != expected:
        delta = {d: {"expected": expected[d], "actual": counts.get(d, 0)} for d in days if counts.get(d, 0) != expected[d]}
        raise ValueError("已验证来源与回测实际行情覆盖不一致：" + str(delta))
    return {"sessions": days, "catalogs": bindings, "proofs": proofs, "row_counts": counts}


def _read_bars(connection, days, inputs, cutoff):
    scopes = _resolved_scopes(connection, inputs, days, cutoff)
    bindings = {d: scopes[d]["catalog"] for d in days}
    expected = {day: n for part in inputs.values() for day, n in part["row_counts"].items()}
    if sum(expected[d] for d in days) > 500000:
        raise ValueError("单步历史行情超过500000行的内存上限")
    # A six-day join exceeded 12s in production; two A-share days read in 2.6s.
    width = max(1, min(8, 16000 // max(expected[d] for d in days)))
    force = " FORCE INDEX (idx_date_ktype) " if connection.dialect.name == "mysql" else " "
    rows = []
    for offset in range(0, len(days), width):
        chunk = days[offset:offset + width]
        predicate, scope_params, expanding = _scope_filters(scopes, chunk)
        join, params = daily_input_catalog_join({"sessions": chunk, "catalog_batches_by_session": {d: bindings[d] for d in chunk}})
        rows.extend(dict(r) for r in connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(12000) */
            SUBSTR(k.stock_code,1,6) stock_code,k.trade_date,k.`open`,k.high,k.low,k.`close`,k.pre_close,k.volume,k.amount
            FROM sm_stock_kline k """ + force + join + """ WHERE k.k_type=1 AND k.adjust_type=0
            AND k.trade_date BETWEEN :start AND :end AND k.received_at<=:cutoff """ + predicate + " ORDER BY k.stock_code,k.trade_date LIMIT 60001").bindparams(*expanding),
            {**params, **scope_params, "start": chunk[0], "end": chunk[-1], "cutoff": cutoff}).mappings())
    seen = {str(r["trade_date"])[:10] for r in rows}
    if seen != set(days):
        raise ValueError("历史行情读取缺少交易日：" + ",".join(sorted(set(days) - seen)))
    from collections import Counter
    counts = Counter(str(r["trade_date"])[:10] for r in rows)
    if any(counts[day] != expected[day] for day in days):
        raise ValueError("已冻结的回测行情覆盖在读取时发生变化")
    from server.common.qmt_attestation_contract import expected_stock_set_contract
    by_day = {}
    for r in rows:
        by_day.setdefault(str(r["trade_date"])[:10], []).append(r["stock_code"])
    for day, codes in by_day.items():
        expected_hash = scopes[day]["stock_set_hash"]
        actual_hash = (expected_stock_set_contract(day, codes)["stock_set_hash"]
                       if scopes[day].get("native_receipt_id") else canonical_sha256(sorted(codes)))
        if expected_hash is not None and actual_hash != expected_hash:
            raise ValueError(day + " 的行情证券集合与冻结证明不一致")
    return rows


def _identity_rows(connection, day):
    """Never use today's name/ST status or industry membership for an old date."""
    from server.engine.strategy_center import _membership_snapshot_hash
    runs = connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ * FROM qmt_membership_snapshot_run
        WHERE snapshot_date=:day AND source='gj_big_qmt_inner'"""), {"day": day}).mappings().all()
    if not runs:
        return {}, {"status": "UNAVAILABLE", "reason": "无精确日期证券名称和行业冻结快照"}
    if len(runs) != 1:
        raise ValueError(day + " 证券身份冻结快照重复")
    run = runs[0]
    captured = datetime.fromisoformat(str(run["captured_at"]))
    if (run["quality_status"] != "QMT_VALIDATED" or run["capture_mode"] != "qmt_close_full_refresh"
            or not day + " 15:00:00" <= captured.isoformat(sep=" ") < (datetime.fromisoformat(day) + timedelta(days=1)).isoformat(sep=" ")):
        return {}, {"status": "UNAVAILABLE", "reason": "精确日期证券身份快照未通过收盘时间与质量验证"}
    raw = [dict(r) for r in connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ *
        FROM qmt_industry_member_snapshot WHERE snapshot_date=:day AND source='gj_big_qmt_inner'
        ORDER BY industry_code,stock_code"""), {"day": day}).mappings()]
    if (len(raw) != int(run["industry_relation_count"]) or
            len({r["industry_code"] for r in raw}) != int(run["industry_count"]) or
            _membership_snapshot_hash(raw, member_type="industry") != run["industry_hash"] or
            any(r["quality_status"] != "QMT_VALIDATED" or str(r["captured_at"]) != str(run["captured_at"]) for r in raw)):
        raise ValueError(day + " 证券身份快照完整集合哈希、时间或覆盖不一致")
    result = {}
    for r in raw:
        if r["industry_type"] != "申万一级":
            continue
        code = str(r["stock_code"]).zfill(6)
        if code in result:
            raise ValueError(day + " 证券存在重复一级行业")
        result[code] = {"short_name": r["short_name"], "industry_name": r["industry_name"],
            "industry_snapshot_date": day, "industry_snapshot_source": "gj_big_qmt_inner",
            "membership_proof_sha256": run["industry_hash"]}
    return result, {"status": "VERIFIED", "captured_at": str(captured), "sha256": run["industry_hash"], "stock_count": len(result)}


def start_replay(connection, state, proposals, days):
    contract = replay_contract()
    clock = _now()
    fields = {c["field"] for p in proposals for g in ("market_all", "entry_all", "entry_any", "ranking") for c in p["rule"][g]}
    warmup_count = 60 if fields & {"drawdown_60", "from_low_60"} else 20
    warmup = sorted(str(d)[:10] for d in connection.execute(text("""SELECT DISTINCT trade_date FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date<:day ORDER BY trade_date DESC LIMIT :n"""), {"day": days[0], "n": warmup_count}).scalars())
    if len(warmup) != warmup_count:
        raise ValueError("回测预热交易日不足")
    input_sessions = warmup + days
    prior = state.get("previous_price_backtest") or {}
    archives = []
    cutoff = clock
    if prior.get("input_sessions") == input_sessions:
        cutoff = prior["knowledge_cutoff"]
        archives = list(prior.get("input_archives") or []) + [{"run_id": prior["run_id"], "rule_hash": prior["rule_hash"]}]
    identity = {"contract": contract, "sessions": days, "requested_at": clock,
        "versions": [(p["strategy_key"], p["version"], p["rule_hash"]) for p in proposals]}
    campaign = {"run_id": canonical_sha256(identity), "rule_hash": canonical_sha256(identity["versions"]),
        "contract": contract, "phase": "INPUTS", "status": "RUNNING", "cursor": 0,
        "sessions": days, "input_sessions": input_sessions, "warmup_count": warmup_count,
        "knowledge_cutoff": cutoff, "requested_at": clock, "input_archives": archives,
        "groups": _group_inputs(connection, input_sessions, cutoff), "completed_units": 0,
        "total_units": len(input_sessions) * 2 + len(days)}
    inputs = _input_parts(connection, campaign)
    for group in campaign["groups"]:
        existing = inputs.get(group["sessions"][0])
        if existing is None:
            break
        if existing["sessions"] != group["sessions"]:
            raise ValueError("恢复的历史输入分区范围不一致")
        campaign["cursor"] += 1
        campaign["completed_units"] += len(group["sessions"])
    if campaign["cursor"] == len(campaign["groups"]):
        campaign.update(phase="SIGNALS", cursor=0)
    campaign["reused_input_sessions"] = campaign["completed_units"]
    return campaign


def _input_parts(connection, campaign):
    inputs = {}
    for archive in [*(campaign.get("input_archives") or []), campaign]:
        for day, part in _partitions(connection, archive, {"rule_hash": archive["rule_hash"]}, "D").items():
            if day in inputs and inputs[day] != part:
                raise ValueError("已验证历史输入的恢复引用发生冲突")
            inputs[day] = part
    return inputs


def advance_replay(connection, state, proposals, campaign, *, checkpoint=None):
    """One scheduler-owned bounded step; caller atomically persists all events."""
    campaign = dict(campaign)
    campaign["status"] = "RUNNING"
    key = {"rule_hash": campaign["rule_hash"]}
    inputs = _input_parts(connection, campaign)
    signals = _partitions(connection, campaign, key, "S")
    prices = _partitions(connection, campaign, key, "P")
    events, deadline = [], time.monotonic() + 100
    def emit(kind, day, value):
        value = {"day": day, **value}
        events.append({"entity_type": PART_ENTITY, "entity_key": partition_key(campaign, key, kind, day),
                       "action": "PARTITION_COMPLETE", "after": value})
        return value
    phase = campaign["phase"]
    campaign["current_unit"] = phase
    def progress():
        for p in proposals:
            p["validation"] = {"status": "RUNNING", "phase": campaign["phase"], "replay_run_id": campaign["run_id"],
                "backtest_contract": campaign["contract"], "completed_units": campaign["completed_units"],
                "total_units": campaign["total_units"], "current_unit": campaign["current_unit"],
                "reused_input_sessions": campaign.get("reused_input_sessions", 0),
                "sessions": campaign["sessions"], "source_kind": "RECONSTRUCTED_TRAILING_PRICE_FEATURES",
                "reason": "按各历史交易日重算量价特征，复用同批输入并实际回放", "net_return_pct": None}
    if phase == "INPUTS":
        for _ in range(8):
            if campaign["cursor"] >= len(campaign["groups"]) or time.monotonic() >= deadline:
                break
            group = campaign["groups"][campaign["cursor"]]
            day = group["sessions"][0]
            campaign["current_unit"] = "核验行情 " + day + " 至 " + group["sessions"][-1]
            if checkpoint:
                progress()
                checkpoint(campaign, [], completed=False)
            part = _validate_group(connection, group, campaign["knowledge_cutoff"])
            inputs[day] = emit("D", day, part)
            campaign["completed_units"] += len(group["sessions"])
            campaign["cursor"] += 1
            if checkpoint:
                progress()
                checkpoint(campaign, events)
                events = []
        if campaign["cursor"] >= len(campaign["groups"]):
            campaign.update(phase="SIGNALS", cursor=0)
    elif phase == "SIGNALS":
        days = campaign["sessions"][campaign["cursor"]:campaign["cursor"] + 16]
        campaign["current_unit"] = "历史量价与选股 " + days[0] + " 至 " + days[-1]
        if checkpoint:
            progress()
            checkpoint(campaign, [], completed=False)
        end_index = campaign["input_sessions"].index(days[-1])
        start_index = campaign["input_sessions"].index(days[0]) - campaign["warmup_count"]
        raw = _read_bars(connection, campaign["input_sessions"][start_index:end_index + 1], inputs, campaign["knowledge_cutoff"])
        frame = technical_history(raw)
        for day in days:
            identities, proof = _identity_rows(connection, day)
            data = frame.loc[frame.trade_date == day].to_dict("records")
            rows, market = feature_frame([{**r, **identities.get(r["stock_code"], {})} for r in data], day)
            selected = {}
            for p in proposals:
                picked = select_rule_rows(p["rule"], rows, market)
                selected[p["rule_hash"]] = [{"stock_code": r["stock_code"], "stock_name": r["stock_name"],
                    "industry": r["industry"], "signal_date": day, "signal_known_at": max(day + " 15:35:00", proof.get("captured_at", "")),
                    "clock_basis": "HYPOTHETICAL_DAILY_CLOSE", "rule_proposed_at": state["proposal_generated_at"],
                    "identity_proof": proof, "rank": i + 1, "score": r["research_rule_score"]} for i, r in enumerate(picked)]
            signals[day] = emit("S", day, {"signals_by_rule": selected, "market": market,
                "identity_proof": proof, "source_rows": len(rows), "technical_formula": campaign["contract"]})
            campaign["completed_units"] += 1
        campaign["cursor"] += len(days)
        campaign["current_unit"] = "历史选股 " + days[-1]
        if campaign["cursor"] >= len(campaign["sessions"]):
            campaign.update(phase="PRICES", cursor=0)
    elif phase == "PRICES":
        days = campaign["input_sessions"][campaign["cursor"]:campaign["cursor"] + 24]
        campaign["current_unit"] = "成交行情 " + days[0] + " 至 " + days[-1]
        if checkpoint:
            progress()
            checkpoint(campaign, [], completed=False)
        codes = {s["stock_code"] for part in signals.values() for picks in part["signals_by_rule"].values() for s in picks}
        raw = _read_bars(connection, days, inputs, campaign["knowledge_cutoff"]) if codes else []
        for day in days:
            prices[day] = emit("P", day, {"bars": [r for r in raw if str(r["trade_date"])[:10] == day and r["stock_code"] in codes]})
            campaign["completed_units"] += 1
        campaign["cursor"] += len(days)
        campaign["current_unit"] = "成交行情 " + days[-1]
        if campaign["cursor"] >= len(campaign["input_sessions"]):
            campaign.update(phase="REPLAY", cursor=0)
    elif phase == "REPLAY":
        from server.engine.strategy_rule_replay import replay_rule
        bars = [bar for part in prices.values() for bar in part["bars"]]
        common_hash = canonical_sha256({"inputs": inputs, "contract": campaign["contract"], "sessions": campaign["sessions"],
            "read_scopes": _resolved_scopes(connection, inputs, campaign["input_sessions"], campaign["knowledge_cutoff"]),
            "identity_proofs": {day: part["identity_proof"] for day, part in signals.items()}})
        identity_sessions = sum(part["identity_proof"]["status"] == "VERIFIED" for part in signals.values())
        for p in proposals:
            selected = [s for part in signals.values() for s in part["signals_by_rule"][p["rule_hash"]]]
            report = replay_rule(p["rule"], selected, bars, campaign["input_sessions"], evaluation_start=campaign["sessions"][0])
            if identity_sessions < len(campaign["sessions"]) and report["status"] != "BLOCKED":
                report.update(status="PARTIAL", net_return_pct=None, return_scope="VERIFIED_INPUT_SUBSET")
            report["identity_covered_sessions"] = identity_sessions
            report["requested_sessions"] = len(campaign["sessions"])
            p["validation"] = {"status": report["status"], "report": report,
                "reason": "历史量价研究已实际回放；不是事前预测，样本外与前向盈利资格另行验证" if report["status"] == "COMPLETE" else "历史回放已计算；部分拟入场缺少当日身份或成交证据，详见排除明细",
                "sessions": campaign["sessions"], "replay_run_id": campaign["run_id"],
                "requested_at": campaign["requested_at"], "computed_at": _now(),
                "reused_input_sessions": campaign.get("reused_input_sessions", 0),
                "knowledge_cutoff": campaign["knowledge_cutoff"], "backtest_contract": campaign["contract"],
                "source_kind": "RECONSTRUCTED_TRAILING_PRICE_FEATURES", "source_input_sha256": common_hash,
                "dataset_sha256": canonical_sha256({"source": common_hash, "signals": selected, "prices": prices}),
                "completed_units": campaign["total_units"], "total_units": campaign["total_units"],
                "identity_covered_sessions": identity_sessions,
                "out_of_sample_status": "RETROSPECTIVE_NOT_PRISTINE", "funding_eligible": False}
        statuses = {p["validation"]["status"] for p in proposals}
        campaign.update(phase="FINISHED", status="BLOCKED" if "BLOCKED" in statuses else "PARTIAL" if "PARTIAL" in statuses else "COMPLETE")
    if campaign["phase"] != "FINISHED":
        progress()
    return campaign, events
