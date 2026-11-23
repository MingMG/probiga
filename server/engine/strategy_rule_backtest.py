"""Resumable retrospective diagnostics for model-proposed rules.

Each date uses its own immutable publication. Daily-bar fills are explicitly
modelled research, never broker fills or pristine out-of-sample evidence.
"""
from __future__ import annotations

from datetime import datetime, time as wall_time
import json
import time

from sqlalchemy import bindparam, text

from server.common.analysis_pool_receipt import canonical_sha256
from server.common.daily_delivery_control import load_published_analysis_receipt
from server.engine.strategy_rule_language import feature_frame, select_rule_rows


PART_ENTITY = "AUTO_REPLAY_PART"


def partition_key(state, proposal, kind, day):
    return state["run_id"][:24] + ":" + proposal["rule_hash"][:24] + ":" + kind + ":" + day


def _partitions(connection, state, proposal, kind):
    from server.engine.strategy_governance import _digest
    deadline = time.monotonic() + 120
    prefix = state["run_id"][:24] + ":" + proposal["rule_hash"][:24] + ":" + kind + ":%"
    headers = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(4000) */ audit_id,entity_key
        FROM st_strategy_governance_audit WHERE entity_type=:entity AND entity_key LIKE :prefix
        ORDER BY entity_key LIMIT 257
    """), {"entity": PART_ENTITY, "prefix": prefix}).mappings().all()
    if len(headers) > 256:
        raise ValueError("研究回放分区超过256条读取上限")
    mysql = connection.dialect.name == "mysql"
    if mysql:
        from server.common.pit_facts import _decode_coverage_payload
        columns = """audit_id,audit_hash,
            OCTET_LENGTH(payload_json) AS payload_bytes,OCTET_LENGTH(after_json) AS after_bytes,
            CASE WHEN OCTET_LENGTH(payload_json)<=33554432 THEN COMPRESS(payload_json) END AS payload_zip,
            CASE WHEN OCTET_LENGTH(after_json)<=33554432 THEN COMPRESS(after_json) END AS after_zip"""
    else:
        columns = "audit_id,audit_hash,payload_json,after_json"
    parts = {}
    # The secondary-index JSON range read exceeded its deadline in production.
    # Keep its directory covering, then read bounded primary-key payload batches.
    for offset in range(0, len(headers), 4):
        if time.monotonic() >= deadline:
            raise TimeoutError("研究回放分区读取超过120秒总时限")
        ids = [row['audit_id'] for row in headers[offset:offset + 4]]
        records = connection.execute(text(f"""SELECT /*+ MAX_EXECUTION_TIME(12000) */ {columns}
            FROM st_strategy_governance_audit WHERE audit_id IN :ids
        """).bindparams(bindparam('ids', expanding=True)), {'ids': ids}).mappings().all()
        by_id = {row['audit_id']: row for row in records}
        if len(records) != len(ids) or set(by_id) != set(ids):
            raise ValueError("研究回放分区正文缺失或重复")
        for identity in ids:
            row = by_id[identity]
            if mysql:
                if any(type(row[key]) is not int or not 0 < row[key] <= 33554432
                       for key in ('payload_bytes', 'after_bytes')):
                    raise ValueError("研究回放分区正文超过32MiB读取上限")
                payload = _decode_coverage_payload(row['payload_zip'], expected_bytes=row['payload_bytes'], stage='replay_partition')
                after = _decode_coverage_payload(row['after_zip'], expected_bytes=row['after_bytes'], stage='replay_partition')
            else:
                payload, after = row['payload_json'], row['after_json']
            audit, value = json.loads(payload), json.loads(after)
            if _digest(audit) != row["audit_hash"] or audit.get("after") != value:
                raise ValueError("研究回放分区审计哈希无效")
            if value["day"] in parts:
                raise ValueError("研究回放分区重复")
            parts[value["day"]] = value
    return parts


def _signals(connection, state, proposal, day, run_uid):
    snapshots = []
    receipt = load_published_analysis_receipt(connection, day, run_uid=run_uid,
        decision_at=datetime.fromisoformat(state["decision_known_at"]), capture_score=snapshots.append)
    snapshot = snapshots[0]
    rows, market = feature_frame(snapshot["scored_rows"], day)
    if len(rows) < int(snapshot["stock_count"]) * 0.98:
        raise ValueError(day + " 的历史价格样本覆盖不足")
    picked = select_rule_rows(proposal["rule"], rows, market)
    known = max(datetime.fromisoformat(str(snapshot["decision_at"])), datetime.fromisoformat(str(receipt["published_at"])))
    signals = [{"stock_code": row["stock_code"], "stock_name": row["stock_name"], "industry": row["industry"],
                "signal_date": day, "signal_known_at": known.isoformat(sep=" "), "rank": index + 1,
                "score": row["research_rule_score"]} for index, row in enumerate(picked)]
    return {"day": day, "signals": signals, "publication_run_uid": run_uid,
            "publication_payload_sha256": receipt["score_snapshot"]["payload_sha256"]}


def _prices(connection, state, day, codes):
    from server.common.strategy_daily_input_window import validate_daily_input_sessions, daily_input_catalog_join
    window = validate_daily_input_sessions(connection, sessions=[day], decision_known_at=state["decision_known_at"])
    join, params = daily_input_catalog_join(window)
    statement = text("""
        SELECT /*+ MAX_EXECUTION_TIME(8000) */ k.stock_code,k.trade_date,k.`open`,k.high,k.low,k.`close`,
               k.pre_close,k.volume,k.amount,k.batch_id,k.data_version,k.received_at
        FROM sm_stock_kline k
    """ + join + " WHERE k.trade_date=:day AND k.k_type=1 AND k.adjust_type=0 AND k.stock_code IN :codes ORDER BY k.stock_code")
    statement = statement.bindparams(bindparam("codes", expanding=True))
    rows = [dict(row) for row in connection.execute(statement, {**params, "day": day, "codes": sorted(codes)}).mappings()]
    return {"day": day, "bars": rows, "proof": {k: window[k] for k in (
        "daily_partition_roots", "catalog_batches_by_session", "daily_providers_by_session", "decision_known_at")}}


def run_rule_backtest(connection, state, proposal, *, inventory):
    from server.engine.autonomous_strategy_research import POLICY
    days, sources = inventory
    result = dict(proposal["validation"])
    if result["status"] == "NOT_STARTED":
        missing = [day for day in days if not sources.get(day)]
        required = POLICY["historical_sessions"]
        if len(days) < required or missing:
            return {"status": "BLOCKED", "reason": "缺少完整的历史冻结特征批次，禁止用今天的特征倒填回测",
                    "required_sessions": required, "calendar_sessions": len(days), "available_feature_sessions": len(days) - len(missing),
                    "missing_feature_dates": missing, "net_return_pct": None, "out_of_sample_status": "UNKNOWN",
                    "funding_eligible": False}
        warmup = connection.execute(text("""
            SELECT DISTINCT trade_date FROM si_trade_calendar WHERE trade_status=1 AND trade_date<:day
            ORDER BY trade_date DESC LIMIT 20
        """), {"day": days[0]}).scalars().all()
        result = {"status": "RUNNING", "phase": "SIGNALS", "cursor": 0, "sessions": days,
                  "source_runs": sources, "price_sessions": sorted(str(d)[:10] for d in warmup) + days,
                  "completed_units": 0, "total_units": len(days) * 2 + len(warmup),
                  "out_of_sample_status": "RETROSPECTIVE_NOT_PRISTINE", "funding_eligible": False}
    events, deadline = [], time.monotonic() + 85
    phase = result["phase"]
    selected = _partitions(connection, state, proposal, "S")
    priced = _partitions(connection, state, proposal, "P") if phase != "SIGNALS" else {}
    phase_days = result["sessions"] if phase == "SIGNALS" else result["price_sessions"]
    codes = {signal["stock_code"] for part in selected.values() for signal in part["signals"]}
    for _ in range(3):
        cursor = int(result["cursor"])
        if cursor >= len(phase_days) or time.monotonic() >= deadline:
            break
        day = phase_days[cursor]
        result["current_unit"] = phase + ":" + day
        existing = selected if phase == "SIGNALS" else priced
        if day not in existing:
            part = (_signals(connection, state, proposal, day, result["source_runs"][day]) if phase == "SIGNALS"
                    else _prices(connection, state, day, codes) if codes else {"day": day, "bars": [], "proof": {"no_signals": True}})
            existing[day] = part
            events.append({"entity_type": PART_ENTITY, "entity_key": partition_key(state, proposal, "S" if phase == "SIGNALS" else "P", day),
                           "action": "PARTITION_COMPLETE", "after": part})
        result["cursor"] += 1
        result["completed_units"] += 1
    if result["cursor"] >= len(phase_days):
        if phase == "SIGNALS":
            result.update(phase="PRICES", cursor=0)
        else:
            from server.engine.strategy_rule_replay import replay_rule
            signals = [signal for part in selected.values() for signal in part["signals"]]
            bars = [bar for part in priced.values() for bar in part["bars"]]
            report = replay_rule(proposal["rule"], signals, bars, result["price_sessions"], evaluation_start=result["sessions"][0])
            result.update(status=report["status"], phase="FINISHED", report=report,
                          reason="历史诊断已完成，前向证据待积累" if report["status"] == "COMPLETE" else "历史诊断的成交或估值证据存在缺口",
                          dataset_sha256=canonical_sha256({"features": selected, "prices": priced}))
    result["_events"] = events
    return result
