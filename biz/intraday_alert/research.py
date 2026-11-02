"""One frozen intraday observation -> one shared, audited research artifact.

Queue mutation is short and fenced. Model calls occur only after the scan's
transaction and MySQL named lock have ended. Replay never writes the outbox.
"""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import text

from .schema import OBSERVATION_TABLE, OUTBOX_TABLE
from .research_policy import MAX_AGE_SECONDS, TOTAL_DEADLINE_SECONDS, LEASE_SECONDS
from server.ai_bridge.research import get_research_engine

SCHEMA = "probiga.intraday-research.v1"
TZ = ZoneInfo("Asia/Shanghai")
MAX_RESEARCH_STAGES = 7


class IntradayResearchError(RuntimeError):
    pass


def _verified_research_units(research: dict) -> int:
    """Measure only the verified actual path, never its maximum stage budget."""
    from biz.review.market_decision import STAGES
    proof = research.get("verification") or {}
    stages = research.get("generation_attempts") or []
    count = proof.get("stage_count")
    names = [stage.get("stage") for stage in stages]
    if (type(count) is not int or count != MAX_RESEARCH_STAGES
            or len(stages) != count or any(stage.get("status") != "COMPLETE" for stage in stages)
            or names != list(STAGES)
            or proof.get("stage_budget") != MAX_RESEARCH_STAGES
            or proof.get("status") != "PASS"):
        raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=validation reason=stage_completion_proof_mismatch")
    return count


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                      default=lambda item: float(item) if isinstance(item, Decimal) else str(item), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _object(value):
    return dict(value) if isinstance(value, Mapping) else json.loads(value or "{}")


def _time(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(TZ).replace(tzinfo=None) if parsed.tzinfo else parsed


def _now():
    return datetime.now(TZ).replace(tzinfo=None)


def _stamp(value):
    return _time(value).replace(tzinfo=TZ).isoformat()


def _select(engine, sql):
    return text(sql + (" FOR UPDATE" if engine.dialect.name == "mysql" else ""))


def _prepare(connection):
    if connection.dialect.name == "mysql":
        connection.execute(text("SET SESSION innodb_lock_wait_timeout=5"))
        connection.execute(text("SET SESSION max_execution_time=15000"))


def project_observation(observation: Mapping[str, Any]) -> dict:
    """Map retained intraday measurements without inventing a daily close."""
    raw = json.loads(_json(dict(observation)))
    market = dict(raw.get("market") or {})
    market.update(sample_count=market.get("stock_count", raw.get("observed_count")),
                  return_median_pct=market.get("median_return_pct"),
                  amount_quality={"status": "UNKNOWN", "reason": "intraday amount/volume price-range contract unavailable"})
    if market.get("positive_breadth_pct") is not None:
        market["up_coverage"] = float(market["positive_breadth_pct"]) / 100
    sectors = []
    raw_sectors = raw.get("sectors") or {}
    for row in raw_sectors.get("items", []) if isinstance(raw_sectors, dict) else raw_sectors:
        sectors.append({"status": "OBSERVED", "name": row.get("name") or row.get("industry_name"),
                        "concept_code": row.get("code") or row.get("industry_code"),
                        "change_pct": row.get("median_return_pct"),
                        "source": "QMT当时行业成员样本收益中位数，非行业指数", "history": []})
    stocks = {}
    buckets = raw.get("key_stocks") or {}
    for rows in buckets.values() if isinstance(buckets, dict) else [buckets]:
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict) or not row.get("stock_code"):
                continue
            code = str(row["stock_code"])
            stocks[code] = {"status": "OBSERVED", "stock_code": code,
                            "short_name": row.get("short_name") or code, "change_pct": row.get("change_pct"),
                            "ohlc": {"status": "OBSERVED", "close": row.get("price")},
                            "price_verification": {"status": "OBSERVED", "source": raw.get("source_provider")},
                            "sector_names": [], "amount_quality": {"status": "UNKNOWN"}}
    if isinstance(raw_sectors, dict):
        for sector in raw_sectors.get("items", []):
            for leader in sector.get("leaders", []):
                code = str(leader.get("stock_code") or "")
                if code in stocks:
                    stocks[code]["sector_names"].append(sector.get("name") or sector.get("industry_name"))
    benchmarks = raw.get("benchmarks") or {}
    benchmark_rows = benchmarks.get("items", []) if isinstance(benchmarks, dict) else []
    market["indices"] = [{"index_code": row.get("instrument_code") or row.get("code"),
                          "name": row.get("instrument_name") or row.get("name"),
                          "trade_date": str(raw.get("trade_date") or raw["observed_at"])[:10],
                          "change_pct": row.get("change_pct"), "close": row.get("price")}
                         for row in benchmark_rows if row.get("instrument_type", "INDEX") == "INDEX"]
    return {**raw, "market": market, "sectors": sectors, "key_stocks": list(stocks.values())[:12]}


def freeze_inputs(observation: Mapping[str, Any], history: list, emissions: list, *, mode: str) -> dict:
    raw = json.loads(_json(dict(observation)))
    cutoff = _time(raw["observed_at"])
    if (_time(raw["source_snapshot_at"]) > cutoff or raw.get("quality", {}).get("status") != "PASS"
            or float(raw.get("coverage", 0)) < 0.95):
        raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=freeze reason=unverified_source")
    safe_history = [item for item in history if _time(item["observed_at"]) <= cutoff
                    and _time(item.get("source_snapshot_at") or item["observed_at"]) <= cutoff
                    and _time(item["observed_at"]).date() == cutoff.date()]
    projected = project_observation(raw)
    projected["history"] = [project_observation(item) for item in safe_history[-12:]]
    # Event labels and numeric evidence are detector outputs, not fixed prose analysis.
    pure_events = []
    for event in emissions:
        item = {key: event[key] for key in ("event_key", "event_type", "subject_code", "subject_name",
                "direction", "state", "transition_name", "evidence_hash") if key in event}
        evidence = dict(event.get("evidence") or {})
        metrics = evidence.get("metrics") or {}
        item["evidence"] = {"metrics": {key: value for key, value in metrics.items()
            if not any(part in key.lower() for part in ("amount", "turnover", "flow"))},
            **{key: evidence[key] for key in ("support_count", "evidence_count", "source_time") if key in evidence}}
        item.update(source="retained_intraday_rule_detector", timestamp=_stamp(raw["source_snapshot_at"]),
                    observed_at=_stamp(cutoff))
        pure_events.append(item)
    context = {"cutoff_at": _stamp(cutoff), "mode": mode, "asof_mode": "retained_snapshot" if mode == "replay" else "live_snapshot",
               "intraday_observation": projected, "intraday_events": pure_events}
    day = cutoff.date().isoformat()
    digest = {"review_date": day, "adjust_type": 0, "generated_at": _stamp(cutoff),
              "data_cutoff_at": _stamp(raw["source_snapshot_at"]), "publish_status": "ready",
              "quality_json": {"status": "pass", "target_date": day, "errors": [],
                               "coverage": {"target": raw["coverage"]},
                               "source_dates": {"intraday_observation": raw.get("observation_id")}},
              "market_structure_json": {}, "industry_rotation_json": {}, "factor_validation_json": {},
              "compact_review": ""}
    return {"schema_version": SCHEMA, "mode": mode, "observation_id": raw.get("observation_id"),
            "raw_observation": raw, "history": safe_history, "source_events": emissions, "emissions": pure_events,
            "source_receipt": raw.get("source_receipt") or raw.get("quality"),
            "digest": digest, "context": context, "input_sha256": _hash({"digest": digest, "context": context}),
            "checkpoint": {}, "analysis": {}, "research": None}


def research_batch_id(observation_id: str, mode: str) -> str:
    return _hash({"observation_id": observation_id, "mode": mode, "schema": SCHEMA})


def _freeze_research_evidence(engine, frozen: dict, *, now=None, progress=None) -> dict:
    """Read auxiliary evidence once, before the first model input is durable.

    An explicit frozen context or a model checkpoint is immutable.  In
    particular, recovery must not query today's tables to recreate past input.
    """
    expected = _hash({"digest": frozen["digest"], "context": frozen["context"]})
    if frozen.get("input_sha256") != expected:
        raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=input_freeze reason=input_hash_mismatch")
    context = frozen["context"]
    if (all(key in context for key in ("technical_facts", "fundamental_facts"))
            or frozen.get("checkpoint") or frozen.get("research")):
        return frozen
    from biz.review.research_inputs import enrich_context
    result = deepcopy(frozen)
    result["context"] = enrich_context(engine, result["digest"], context,
        report_kind="intraday", now=now, progress=progress)
    result["input_sha256"] = _hash({"digest": result["digest"], "context": result["context"]})
    result["evidence_enrichment"] = {"status": "COMPLETE", "frozen_at": _stamp(now or _now())}
    return result


def queue_research_batch(connection, *, observation: dict, history: list, emissions: list, mode: str, now: datetime) -> str:
    frozen = freeze_inputs(observation, history, emissions, mode=mode)
    identity = research_batch_id(observation["observation_id"], mode)
    event_key = f"RESEARCH:{mode}:{observation['observation_id']}"
    prefix = "INSERT OR IGNORE" if connection.dialect.name == "sqlite" else "INSERT IGNORE"
    connection.execute(text(f"""{prefix} INTO {OUTBOX_TABLE}
        (outbox_id,trade_date,event_key,cycle,transition_name,state,mode,status,evidence_hash,evidence_json,
         content_sha256,content_markdown,attempts,next_retry_at,claimed_at,delivery_id,error_message,created_at,sent_at,updated_at)
        VALUES (:id,:day,:event_key,1,'BATCH_RESEARCH','RESEARCH',:mode,'RESEARCH_PENDING',:hash,:evidence,
                :empty,'',0,NULL,NULL,NULL,NULL,:now,NULL,:now)"""),
        {"id": identity, "day": _time(now).date(), "event_key": event_key, "mode": mode,
         "hash": frozen["input_sha256"], "evidence": _json(frozen), "empty": hashlib.sha256(b"").hexdigest(), "now": now})
    return identity


def _load_locked(connection, engine, outbox_id):
    return connection.execute(_select(engine, f"SELECT /*+ MAX_EXECUTION_TIME(15000) */ * FROM {OUTBOX_TABLE} WHERE outbox_id=:id"),
                              {"id": outbox_id}).mappings().one_or_none()


def _owned(row, owner, generation, now):
    if row is None:
        raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=checkpoint reason=batch_missing")
    state = _object(row["evidence_json"])
    analysis = state.get("analysis") or {}
    if (row["status"] != "ANALYZING" or analysis.get("owner") != owner
            or analysis.get("generation") != generation or _time(analysis["lease_expires_at"]) <= now
            or _time(analysis["deadline_at"]) <= now):
        raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=checkpoint reason=owner_or_deadline_lost")
    return state


def expire_research_batches(engine, *, now: datetime, limit: int = 10) -> int:
    """Terminate lost old owners at their deadline; never claim or regenerate."""
    engine = get_research_engine(engine)
    current = _time(now)
    changed = 0
    with engine.begin() as connection:
        _prepare(connection)
        rows = connection.execute(_select(engine,
            f"SELECT /*+ MAX_EXECUTION_TIME(15000) */ outbox_id,evidence_json FROM {OUTBOX_TABLE} "
            "WHERE status='ANALYZING' ORDER BY created_at,outbox_id LIMIT :limit"),
            {"limit": max(1, min(int(limit), 10))}).mappings().all()
        for row in rows:
            snapshot = _object(row["evidence_json"])
            if snapshot.get("schema_version") != SCHEMA:
                continue
            try:
                deadline = _time(snapshot["analysis"]["deadline_at"])
            except (KeyError, ValueError, TypeError):
                status, error = "BLOCKED", "research owner deadline is not verifiable"
            else:
                if deadline > current:
                    continue
                status, error = "EXPIRED", "research owner deadline expired; no reclaim"
            connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET status=:status,claimed_at=NULL,error_message=:error,updated_at=:now WHERE outbox_id=:id AND status='ANALYZING'"),
                {"id": row["outbox_id"], "status": status, "error": error, "now": current})
            changed += 1
    return changed


def process_research_batch(engine, *, outbox_id: str, generator=None, now_fn: Callable = _now) -> dict:
    engine = get_research_engine(engine)
    now = _time(now_fn())
    owner = str(uuid4())
    with engine.begin() as connection:
        _prepare(connection)
        row = _load_locked(connection, engine, outbox_id)
        if row is None:
            raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=claim reason=missing_batch")
        state = _object(row["evidence_json"])
        if row["status"] in {"SHADOW", "PENDING", "SENDING", "SENT"}:
            return {"status": row["status"], "research": state.get("research"), "outbox_id": outbox_id}
        old_analysis = state.get("analysis") or {}
        if row["status"] == "ANALYZING":
            if _time(old_analysis["deadline_at"]) <= now:
                connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET status='EXPIRED',error_message='research owner deadline expired',updated_at=:now WHERE outbox_id=:id"), {"id": outbox_id, "now": now})
                return {"status": "EXPIRED", "outbox_id": outbox_id}
            return {"status": "ANALYZING", "outbox_id": outbox_id}
        if row["status"] != "RESEARCH_PENDING":
            return {"status": row["status"], "outbox_id": outbox_id}
        source_at = _time(state["raw_observation"]["source_snapshot_at"])
        deadline = min(now + timedelta(seconds=TOTAL_DEADLINE_SECONDS), source_at + timedelta(seconds=MAX_AGE_SECONDS))
        if deadline <= now:
            connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET status='EXPIRED',error_message='frozen source too old',updated_at=:now WHERE outbox_id=:id"), {"id": outbox_id, "now": now})
            return {"status": "EXPIRED", "outbox_id": outbox_id}
        generation = int(old_analysis.get("generation", 0)) + 1
        state["analysis"] = {"owner": owner, "generation": generation, "deadline_at": _stamp(deadline),
                             "lease_expires_at": _stamp(min(deadline, now + timedelta(seconds=LEASE_SECONDS))),
                             "started_at": _stamp(now), "completed_units": 0, "total_units": MAX_RESEARCH_STAGES,
                             "stage_budget": MAX_RESEARCH_STAGES}
        connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET status='ANALYZING',evidence_json=:evidence,claimed_at=:now,updated_at=:now WHERE outbox_id=:id"), {"id": outbox_id, "evidence": _json(state), "now": now})

    def progress(event):
        current = _time(now_fn())
        with engine.begin() as connection:
            _prepare(connection)
            row = _load_locked(connection, engine, outbox_id)
            snapshot = _owned(row, owner, generation, current)
            analysis = snapshot["analysis"]
            total = event.get("total", event.get("total_units", analysis["total_units"]))
            completed = event.get("completed", event.get("completed_units", analysis["completed_units"]))
            actual = event.get("actual_total", analysis.get("declared_actual_total"))
            if (type(total) is not int or not 0 < total <= MAX_RESEARCH_STAGES
                    or type(completed) is not int or not analysis["completed_units"] <= completed <= total
                    or (actual is not None and (type(actual) is not int or not 0 < actual <= MAX_RESEARCH_STAGES
                        or completed != actual))):
                raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=progress reason=stage_count_out_of_bounds")
            checkpoint = event.get("checkpoint")
            if checkpoint is not None:
                if (event.get("completed", event.get("completed_units")) is not None
                        and sum(item.get("status") == "COMPLETE" for item in (checkpoint.get("stages") or {}).values()) != completed):
                    raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=progress reason=checkpoint_count_mismatch")
                snapshot["checkpoint"] = checkpoint
            analysis.update(lease_expires_at=_stamp(min(deadline, current + timedelta(seconds=LEASE_SECONDS))),
                                        completed_units=completed, total_units=total,
                                        current_unit=event.get("stage"), last_progress_at=_stamp(current))
            if actual is not None:
                analysis["declared_actual_total"] = actual
            connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET evidence_json=:evidence,updated_at=:now WHERE outbox_id=:id AND status='ANALYZING'"), {"id": outbox_id, "evidence": _json(snapshot), "now": current})

    try:
        # The scan transaction/named lock and claim transaction have ended.
        # Renew the fenced owner while bounded evidence queries make progress;
        # their budget never extends the source's original freshness deadline.
        progress({"stage": "research_input_freeze", "status": "RUNNING"})
        enriched = _freeze_research_evidence(engine, state, now=now.replace(tzinfo=TZ), progress=progress)
        if enriched is not state:
            current = _time(now_fn())
            with engine.begin() as connection:
                _prepare(connection)
                row = _load_locked(connection, engine, outbox_id)
                snapshot = _owned(row, owner, generation, current)
                if snapshot.get("checkpoint") or snapshot["input_sha256"] != state["input_sha256"]:
                    raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=input_freeze reason=frozen_input_changed")
                snapshot.update(context=enriched["context"], input_sha256=enriched["input_sha256"],
                                evidence_enrichment=enriched["evidence_enrichment"])
                snapshot["analysis"].update(lease_expires_at=_stamp(min(deadline, current + timedelta(seconds=LEASE_SECONDS))),
                                            current_unit="research_input_frozen", last_progress_at=_stamp(current))
                connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET evidence_json=:evidence,evidence_hash=:hash,updated_at=:now WHERE outbox_id=:id AND status='ANALYZING'"),
                    {"id": outbox_id, "evidence": _json(snapshot), "hash": snapshot["input_sha256"], "now": current})
                state = snapshot
        if generator is None:
            from biz.review.market_decision import generate_research
            generator = generate_research
        research = generator(state["digest"], report_kind="intraday",
            next_trade_date=state["digest"]["review_date"], context=state["context"], previous_research=None,
            engine=engine, now=now.replace(tzinfo=TZ), checkpoint=state.get("checkpoint"), progress=progress)
        from biz.review.market_decision import verify_automatic_research
        verify_automatic_research(research)
        actual_units = _verified_research_units(research)
        finished = _time(now_fn())
        with engine.begin() as connection:
            _prepare(connection)
            row = _load_locked(connection, engine, outbox_id)
            snapshot = _owned(row, owner, generation, finished)
            if (snapshot["analysis"].get("declared_actual_total", actual_units) != actual_units
                    or snapshot["analysis"]["completed_units"] > actual_units):
                raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=validation reason=stage_count_mismatch")
            if research.get("status") != "COMPLETE" or not isinstance(research.get("content"), str) or not research["content"]:
                raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=validation reason=incomplete_research")
            content = research["content"]
            if len(content.encode("utf-8")) > 60_000:
                raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=publication reason=content_budget")
            snapshot["research"] = research
            snapshot["analysis"].update(completed_units=actual_units, total_units=actual_units,
                                        stage_budget=MAX_RESEARCH_STAGES, completed_at=_stamp(finished))
            status = "SHADOW" if row["mode"] == "shadow" else "PENDING"
            connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET status=:status,evidence_json=:evidence,content_markdown=:content,content_sha256=:hash,next_retry_at=:retry,claimed_at=NULL,updated_at=:now WHERE outbox_id=:id"),
                {"id": outbox_id, "status": status, "evidence": _json(snapshot), "content": content,
                 "hash": hashlib.sha256(content.encode("utf-8")).hexdigest(), "retry": finished if status == "PENDING" else None, "now": finished})
        return {"status": status, "outbox_id": outbox_id, "research": research}
    except Exception as exc:
        current = _time(now_fn())
        with engine.begin() as connection:
            _prepare(connection)
            row = _load_locked(connection, engine, outbox_id)
            snapshot = _object(row["evidence_json"]) if row is not None else {}
            analysis = snapshot.get("analysis") or {}
            if row is not None and row["status"] == "ANALYZING" and analysis.get("owner") == owner and analysis.get("generation") == generation:
                status = "EXPIRED" if current >= deadline else "BLOCKED"
                connection.execute(text(f"UPDATE {OUTBOX_TABLE} SET status=:status,error_message=:error,claimed_at=NULL,updated_at=:now WHERE outbox_id=:id"),
                    {"id": outbox_id, "status": status, "error": f"{type(exc).__name__}: research did not pass"[:512], "now": current})
        raise


def _decode_retained(row):
    return {**dict(row), "market": _object(row["market_json"]), "sectors": _object(row["sector_json"]),
            "key_stocks": _object(row["key_stock_json"]), "style": _object(row["style_json"]),
            "benchmarks": _object(row["benchmark_json"]),
            "quality": {"status": row["quality_status"], "receipt_id": row["source_receipt_id"], "retained_projection": True},
            "source_receipt": {"receipt_id": row["source_receipt_id"], "source_provider": row["source_provider"],
                "source_generated_at": row["source_snapshot_at"], "coverage": row["coverage"],
                "expected_count": row["expected_count"], "observed_count": row["observed_count"],
                "quality_status": row["quality_status"], "receipt_scope": "persisted_observation_projection",
                "full_raw_receipt": "UNKNOWN"}}


def load_replay_inputs(engine, *, observation_id: str) -> dict:
    """Only retained facts received by this observation's cutoff; no current tables."""
    engine = get_research_engine(engine)
    with engine.connect() as connection:
        _prepare(connection)
        current = connection.execute(text(f"SELECT /*+ MAX_EXECUTION_TIME(15000) */ * FROM {OBSERVATION_TABLE} WHERE observation_id=:id"), {"id": observation_id}).mappings().one_or_none()
        if current is None:
            raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=replay reason=historical_observation_missing")
        cutoff = _time(current["observed_at"])
        rows = connection.execute(text(f"SELECT /*+ MAX_EXECUTION_TIME(15000) */ * FROM {OBSERVATION_TABLE} WHERE trade_date=:day AND observed_at<:cutoff AND source_snapshot_at<=:cutoff AND created_at<=:cutoff ORDER BY observed_at DESC LIMIT 12"), {"day": current["trade_date"], "cutoff": cutoff}).mappings().all()
        boxes = connection.execute(text(f"SELECT /*+ MAX_EXECUTION_TIME(15000) */ event_key,transition_name,state,evidence_hash,evidence_json FROM {OUTBOX_TABLE} WHERE trade_date=:day AND created_at=:cutoff ORDER BY outbox_id LIMIT 12"), {"day": current["trade_date"], "cutoff": cutoff}).mappings().all()
    observation = _decode_retained(current)
    emissions = []
    for row in boxes:
        evidence = _object(row["evidence_json"])
        if evidence.get("schema_version") == SCHEMA:
            replay = {**evidence, "mode": "replay", "retained_research_input": True,
                      "context": {**evidence["context"], "mode": "replay", "asof_mode": "retained_snapshot"}}
            replay["input_sha256"] = _hash({"digest": replay["digest"], "context": replay["context"]})
            return replay
        emissions.append({"event_key": row["event_key"], "transition_name": row["transition_name"],
                          "state": row["state"], "evidence_hash": row["evidence_hash"], "evidence": evidence})
    return freeze_inputs(observation, [_decode_retained(row) for row in reversed(rows)], emissions, mode="replay")


def run_intraday_research_replay(engine, *, observation_id: str, generator=None, checkpoint=None, progress=None,
                                 frozen_inputs: dict | None = None) -> dict:
    if frozen_inputs is not None:
        frozen = deepcopy(frozen_inputs)
        if (frozen.get("observation_id") != observation_id or frozen.get("mode") != "replay"
                or frozen.get("input_sha256") != _hash({"digest": frozen.get("digest"), "context": frozen.get("context")})):
            raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=replay reason=frozen_input_identity_mismatch")
    else:
        frozen = load_replay_inputs(engine, observation_id=observation_id)
        if checkpoint and not frozen.get("retained_research_input"):
            raise IntradayResearchError("INTRADAY_RESEARCH_BLOCKED stage=replay reason=checkpoint_requires_frozen_inputs")
        if not checkpoint and not frozen.get("retained_research_input"):
            frozen = _freeze_research_evidence(engine, frozen, progress=progress)
    if progress:
        progress({"stage": "research_input_frozen", "status": "COMPLETE", "input_snapshot": frozen})
    if generator is None:
        from biz.review.market_decision import generate_research
        generator = generate_research
    research = generator(frozen["digest"], report_kind="intraday", next_trade_date=frozen["digest"]["review_date"],
        context=frozen["context"], previous_research=None, engine=engine,
        now=(_time(checkpoint["generation_started_at"]).replace(tzinfo=TZ) if checkpoint else _now().replace(tzinfo=TZ)),
        checkpoint=checkpoint, progress=progress)
    from biz.review.market_decision import verify_automatic_research
    verify_automatic_research(research)
    actual_units = _verified_research_units(research)
    return {"status": research.get("status"), "mode": "replay", "asof_at": frozen["context"]["cutoff_at"],
            "observation_id": observation_id, "input_sha256": frozen["input_sha256"],
            "research": research, "input_snapshot": frozen, "completed_units": actual_units,
            "total_units": actual_units, "stage_budget": MAX_RESEARCH_STAGES,
            "outbox_write_invoked": False, "delivery_invoked": False}
