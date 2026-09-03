"""Audited, idempotent WeCom delivery of exact canonical daily stock pools."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.wecom.delivery import (
    DeliveryResult,
    WeComDeliveryError,
    deliver_markdown,
)
from server.common.config import get_wecom_webhook
from server.common.daily_delivery_control import read_daily_delivery
from server.trading_v3.premarket_gate import build_premarket_gate


FINAL_POOL_DELIVERY_SCHEMA = "probiga.final-pool-wecom-delivery.v1"
FINAL_POOL_DELIVERY_KIND = "final_pool"
REQUIRED_COMPLETED_SESSION_COUNT = 2
_RUN_UID_RE = re.compile(r"^[0-9a-f]{32}$")
_SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA64_RE = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class FinalPoolDeliveryBlocked(RuntimeError):
    """The exact daily-result identity is not ready for outbound delivery."""


def _canonical_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def _timestamp(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("timestamp is absent")
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed


def _required_trade_dates(engine: Engine, target_trade_date: str) -> list[str]:
    try:
        target = date.fromisoformat(str(target_trade_date or ""))
    except ValueError as exc:
        raise FinalPoolDeliveryBlocked("target trade date is invalid") from exc
    if target.isoformat() != target_trade_date:
        raise FinalPoolDeliveryBlocked("target trade date is invalid")
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT trade_date
            FROM si_trade_calendar
            WHERE trade_status=1 AND trade_date<=:target
            ORDER BY trade_date DESC
            LIMIT :session_count
        """), {
            "target": target_trade_date,
            "session_count": REQUIRED_COMPLETED_SESSION_COUNT,
        }).scalars().all()
    result = list(reversed([str(value)[:10] for value in rows]))
    if (
        len(result) != REQUIRED_COMPLETED_SESSION_COUNT
        or result[-1] != target_trade_date
        or len(set(result)) != len(result)
    ):
        raise FinalPoolDeliveryBlocked(
            "two authoritative completed sessions are unavailable"
        )
    return result


def _next_execution_session(engine: Engine, trade_date: str) -> str:
    with engine.connect() as connection:
        values = connection.execute(text("""
            SELECT trade_date
            FROM si_trade_calendar
            WHERE trade_status=1 AND trade_date>:trade_date
            ORDER BY trade_date
            LIMIT 1
        """), {"trade_date": trade_date}).scalars().all()
    if len(values) != 1:
        raise FinalPoolDeliveryBlocked(
            f"next execution session is unavailable for {trade_date}"
        )
    return str(values[0])[:10]


def _default_canonical_loader(trade_date: str) -> dict[str, Any] | None:
    from server.common.canonical_decision_bridge import (
        canonical_governance_decision,
    )

    return canonical_governance_decision(trade_date, latest_as_of=False)


def _default_ticket_loader(
    trade_date: str,
    analysis_run_uid: str,
    build_sha: str,
    canonical_pool_sha256: str,
) -> dict[str, Any]:
    from server.api.routers.hot_data import recommended_stocks

    return recommended_stocks(
        trade_date=trade_date,
        strategy="",
        signal_status="",
        start_date="",
        end_date="",
        prefer_latest=False,
        expected_run_uid=analysis_run_uid,
        expected_build_sha=build_sha,
        expected_pool_sha256=canonical_pool_sha256,
    )


def _pending_gate(pool: Mapping[str, Any], execution_session: str) -> dict[str, Any]:
    items = [
        dict(item) for item in (pool.get("items") or [])
        if isinstance(item, Mapping) and item.get("is_strategy_candidate") is True
    ]
    assessments = []
    for item in items:
        alternatives = [
            {
                "stock_code": str(other.get("stock_code") or "").zfill(6),
                "stock_name": str(
                    other.get("stock_name") or other.get("stock_code") or ""
                ),
                "decision_rank": None,
                "primary_theme": other.get("primary_theme"),
                "relation": (
                    "SAME_SCENARIO"
                    if other.get("primary_theme") == item.get("primary_theme")
                    else "OTHER_SCENARIO"
                ),
            }
            for other in items
            if str(other.get("stock_code") or "")
            != str(item.get("stock_code") or "")
        ][:5]
        assessments.append({
            "stock_code": str(item.get("stock_code") or "").zfill(6),
            "stock_name": item.get("stock_name"),
            "source_rank_no": item.get("rank_no"),
            "decision_rank": None,
            "primary_theme": item.get("primary_theme"),
            "dynamic_role": item.get("dynamic_role"),
            "gate_status": "PENDING",
            "advisory_action": "WAIT_AUCTION",
            "quote_at": None,
            "reasons": ["目标执行日集合竞价尚未开始，保持盘后排序并等待独立复核"],
            "alternative_set": alternatives,
            "order_authority": False,
        })
    core: dict[str, Any] = {
        "schema": "probiga.final-pool-auction-gate.pending.v1",
        "status": "WAITING_FOR_SESSION",
        "stage": "PENDING_AUCTION",
        "session_date": execution_session,
        "cutoff_at": None,
        "source_run_uid": pool.get("run_uid"),
        "source_data_date": pool.get("trade_date") or pool.get("data_date"),
        "assessments": assessments,
        "decision_scope": "RESEARCH_ONLY",
        "order_authority": False,
        "automatic_substitution": False,
    }
    return {**core, "gate_hash": _canonical_hash(core)}


def _auction_gate(
    engine: Engine,
    pool: Mapping[str, Any],
    *,
    execution_session: str,
    now: datetime,
) -> dict[str, Any]:
    session = date.fromisoformat(execution_session)
    session_start = datetime.combine(session, time(9, 15))
    if now < session_start:
        return _pending_gate(pool, execution_session)
    cutoff = min(now, datetime.combine(session, time(9, 25, 59)))
    gate = build_premarket_gate(
        engine,
        pool,
        session_date=session,
        cutoff_at=cutoff,
    )
    if (
        not isinstance(gate, dict)
        or not _SHA64_RE.fullmatch(str(gate.get("gate_hash") or ""))
        or gate.get("automatic_substitution") is not False
        or gate.get("order_authority") is not False
        or str(gate.get("source_run_uid") or "")
        != str(pool.get("run_uid") or "")
    ):
        raise FinalPoolDeliveryBlocked("auction gate identity is invalid")
    return gate


def _validated_pool(
    engine: Engine,
    trade_date: str,
    *,
    now: datetime,
    receipt_loader: Callable[..., dict[str, Any] | None],
    canonical_loader: Callable[[str], dict[str, Any] | None],
    ticket_loader: Callable[[str, str, str, str], dict[str, Any]],
    gate_loader: Callable[..., dict[str, Any]] | None,
) -> dict[str, Any]:
    materialized = receipt_loader(engine, trade_date=trade_date)
    session = dict((materialized or {}).get("session") or {})
    receipt = dict((materialized or {}).get("receipt") or {})
    strategy_pool = dict(receipt.get("strategy_pool") or {})
    formal_pool = dict(receipt.get("formal_pool") or {})
    build_sha = str(receipt.get("release_id") or "").lower()
    governance_run_uid = str(receipt.get("governance_run_uid") or "").lower()
    analysis_run_uid = str(receipt.get("analysis_run_uid") or "").lower()
    governance_hash = str(strategy_pool.get("root") or "").lower()
    ticket_hash = str(formal_pool.get("root") or "").lower()
    if (
        str(session.get("trade_date") or "")[:10] != trade_date
        or str(session.get("status") or "").upper() != "PASS"
        or str(receipt.get("trade_date") or "")[:10] != trade_date
        or str(receipt.get("status") or "").upper() != "PASS"
        or receipt.get("canonical_batch_status") != "COMPLETED"
        or strategy_pool.get("status") != "ACTIVE"
        or formal_pool.get("status") != "ACTIVE"
        or receipt.get("api_checks") != {
            "strategy_pool": "PASS", "formal_pool": "PASS",
        }
        or receipt.get("automatic_real_order_submission") is not False
        or receipt.get("real_order_authority") is not False
        or not _RUN_UID_RE.fullmatch(governance_run_uid)
        or not _RUN_UID_RE.fullmatch(analysis_run_uid)
        or not _SHA40_RE.fullmatch(build_sha)
        or build_sha == "0" * 40
        or not _SHA64_RE.fullmatch(governance_hash)
        or not _SHA64_RE.fullmatch(ticket_hash)
    ):
        raise FinalPoolDeliveryBlocked(
            f"exact terminal daily-result receipt is incomplete for {trade_date}"
        )

    canonical = canonical_loader(trade_date)
    context = dict((canonical or {}).get("context") or {})
    run = dict((canonical or {}).get("run") or {})
    pool = dict((canonical or {}).get("pool") or {})
    evidence_as_of = str(context.get("evidence_as_of") or "").strip()
    knowledge_cutoff_at = str(context.get("knowledge_cutoff_at") or "").strip()
    retrospective = context.get("retrospective_reconstruction")
    reconstruction_mode = str(context.get("reconstruction_mode") or "")
    reconstruction_sha = str(
        context.get("reconstruction_sha256") or ""
    ).lower()
    reconstructed_at = str(context.get("reconstructed_at") or "").strip()
    reconstruction_provenance = context.get("reconstruction_provenance")
    items = [
        dict(item) for item in (pool.get("items") or [])
        if isinstance(item, Mapping) and item.get("is_strategy_candidate") is True
    ]
    if (
        run.get("status") != "COMPLETED"
        or context.get("run_status") != "COMPLETED"
        or context.get("decision_integrity_verified") is not True
        or pool.get("run_status") != "COMPLETED"
        or pool.get("pool_readable") is not True
        or pool.get("decision_integrity_verified") is not True
        or pool.get("automatic_real_order_submission") is not False
        or pool.get("real_order_authority") is not False
        or str(pool.get("trade_date") or "")[:10] != trade_date
        or str(pool.get("run_uid") or "").lower() != governance_run_uid
        or str(pool.get("build_commit_sha") or "").lower() != build_sha
        or str(pool.get("canonical_result_hash") or "").lower()
        != governance_hash
        or not evidence_as_of
        or not knowledge_cutoff_at
        or not items
    ):
        raise FinalPoolDeliveryBlocked(
            f"canonical governance pool is empty, incomplete, or drifted for {trade_date}"
        )
    if (
        retrospective is None
        and not reconstruction_mode
        and not reconstruction_sha
        and not reconstructed_at
        and reconstruction_provenance is None
    ):
        # Compatibility for canonical bridge payloads persisted before the
        # reconstruction protocol existed.  New sender receipts still carry
        # the explicit NONE identity below.
        retrospective = False
        reconstruction_mode = "NONE"
        reconstruction_provenance = {}
    if retrospective is not False and retrospective is not True:
        raise FinalPoolDeliveryBlocked(
            f"canonical reconstruction identity is absent for {trade_date}"
        )
    if retrospective is True:
        if not isinstance(reconstruction_provenance, dict):
            raise FinalPoolDeliveryBlocked(
                f"canonical reconstruction provenance is absent for {trade_date}"
            )
        core = {
            str(key): value
            for key, value in reconstruction_provenance.items()
            if str(key) != "reconstruction_sha256"
        }
        try:
            reconstructed_time = _timestamp(reconstructed_at)
            source_cutoff_time = _timestamp(
                reconstruction_provenance.get("source_query_cutoff_at")
            )
            evidence_time = _timestamp(evidence_as_of)
            knowledge_time = _timestamp(knowledge_cutoff_at)
        except (TypeError, ValueError) as exc:
            raise FinalPoolDeliveryBlocked(
                f"canonical reconstruction time is invalid for {trade_date}"
            ) from exc
        if (
            reconstruction_mode != "HISTORICAL_RECONSTRUCTION"
            or not _SHA64_RE.fullmatch(reconstruction_sha)
            or reconstruction_provenance.get("schema")
            != "probiga.qmt-announcement-historical-reconstruction.v2"
            or reconstruction_provenance.get("mode") != reconstruction_mode
            or reconstruction_provenance.get("target_trade_date") != trade_date
            or reconstruction_provenance.get("provider")
            != "cninfo.announcement"
            or reconstruction_provenance.get("source")
            != "cninfo.announcement"
            or not _RUN_UID_RE.fullmatch(str(
                reconstruction_provenance.get("scheduler_run_uid") or ""
            ))
            or not _SHA40_RE.fullmatch(str(
                reconstruction_provenance.get("build_sha") or ""
            ))
            or reconstruction_provenance.get("reconstruction_sha256")
            != reconstruction_sha
            or _canonical_hash(core) != reconstruction_sha
            or reconstruction_provenance.get("reconstructed_at")
            != reconstructed_at
            or reconstruction_provenance.get("known_at") != reconstructed_at
            or source_cutoff_time.date().isoformat() != trade_date
            or reconstructed_time <= source_cutoff_time
            or evidence_time < reconstructed_time
            or knowledge_time < reconstructed_time
            or reconstruction_provenance.get(
                "automatic_real_order_submission"
            ) is not False
            or reconstruction_provenance.get("real_order_authority") is not False
        ):
            raise FinalPoolDeliveryBlocked(
                f"canonical reconstruction identity drifted for {trade_date}"
            )
    elif (
        reconstruction_mode != "NONE"
        or reconstruction_sha
        or reconstructed_at
        or reconstruction_provenance not in ({}, None)
    ):
        raise FinalPoolDeliveryBlocked(
            f"canonical live evidence carries reconstruction drift for {trade_date}"
        )

    ticket = ticket_loader(trade_date, analysis_run_uid, build_sha, ticket_hash)
    ticket_rows = ticket.get("data") if isinstance(ticket, Mapping) else None
    if (
        not isinstance(ticket_rows, list)
        or not ticket_rows
        or ticket.get("identity_verified") is not True
        or ticket.get("data_status") != "READY"
        or str(ticket.get("date") or "")[:10] != trade_date
        or str(ticket.get("run_uid") or "").lower() != analysis_run_uid
        or str(ticket.get("build_sha") or "").lower() != build_sha
        or str(ticket.get("canonical_pool_sha256") or "").lower() != ticket_hash
        or int(ticket.get("total") or -1) != len(ticket_rows)
    ):
        raise FinalPoolDeliveryBlocked(
            f"exact ticket-pool API identity is empty or drifted for {trade_date}"
        )

    execution_session = _next_execution_session(engine, trade_date)
    gate = (
        gate_loader(
            engine,
            pool,
            execution_session=execution_session,
            now=now,
        )
        if gate_loader is not None
        else _auction_gate(
            engine,
            pool,
            execution_session=execution_session,
            now=now,
        )
    )
    if (
        not isinstance(gate, dict)
        or not _SHA64_RE.fullmatch(str(gate.get("gate_hash") or "").lower())
        or gate.get("automatic_substitution") is not False
        or gate.get("order_authority") is not False
        or str(gate.get("session_date") or "") != execution_session
        or str(gate.get("source_run_uid") or "") != governance_run_uid
    ):
        raise FinalPoolDeliveryBlocked(
            f"auction gate identity is unavailable for {trade_date}"
        )

    identity = {
        "schema": FINAL_POOL_DELIVERY_SCHEMA,
        "trade_date": trade_date,
        "execution_session_date": execution_session,
        "governance_run_uid": governance_run_uid,
        "analysis_run_uid": analysis_run_uid,
        "build_sha": build_sha,
        "governance_result_sha256": governance_hash,
        "canonical_pool_sha256": ticket_hash,
        "evidence_as_of": evidence_as_of,
        "knowledge_cutoff_at": knowledge_cutoff_at,
        "retrospective_reconstruction": retrospective,
        "reconstruction_mode": reconstruction_mode,
        "reconstruction_sha256": reconstruction_sha,
        "reconstructed_at": reconstructed_at,
        "reconstruction_provenance": (
            dict(reconstruction_provenance)
            if isinstance(reconstruction_provenance, dict) else {}
        ),
        "gate_hash": str(gate["gate_hash"]).lower(),
        "gate_cutoff_at": gate.get("cutoff_at"),
        "automatic_substitution": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {
        "identity": identity,
        "pool": pool,
        "items": items,
        "gate": gate,
        "ticket_total": len(ticket_rows),
    }


def _display(value: Any) -> str:
    return str(value) if value not in (None, "", []) else "—"


def _format_alternatives(values: Any) -> str:
    rows = values if isinstance(values, list) else []
    if not rows:
        return "[]"
    return "[" + ", ".join(
        "{} {}({})".format(
            _display(row.get("stock_code")),
            _display(row.get("stock_name")),
            _display(row.get("relation")),
        )
        for row in rows if isinstance(row, Mapping)
    ) + "]"


def build_final_pool_markdown(prepared: Mapping[str, Any]) -> str:
    identity = dict(prepared.get("identity") or {})
    items = sorted(
        [dict(item) for item in (prepared.get("items") or [])],
        key=lambda item: (
            int(item.get("rank_no") or 999_999),
            str(item.get("stock_code") or ""),
        ),
    )
    gate = dict(prepared.get("gate") or {})
    assessments = {
        str(item.get("stock_code") or "").zfill(6): dict(item)
        for item in (gate.get("assessments") or [])
        if isinstance(item, Mapping)
    }
    lines = [
        f"## 🎯 {identity['trade_date']} 最终策略票池（研究/模拟）",
        "",
        "真实交易关闭；所有买入条件均须在执行日前重新验证，不自动替代。",
        "",
        f"> governance_run_uid={identity['governance_run_uid']}  ",
        f"> analysis_run_uid={identity['analysis_run_uid']}  ",
        f"> build={identity['build_sha']}  ",
        f"> canonical_pool_sha256={identity['canonical_pool_sha256']}  ",
        f"> governance_result_sha256={identity['governance_result_sha256']}  ",
        f"> evidence_as_of={_display(identity.get('evidence_as_of'))}；"
        f"cutoff={_display(identity.get('knowledge_cutoff_at'))}；"
        f"gate_hash={identity['gate_hash']}  ",
        (
            "> ⚠️ 本票池使用历史重建证据，不代表目标日实时产出；"
            f"reconstructed_at={identity['reconstructed_at']}；"
            f"reconstruction_sha256={identity['reconstruction_sha256']}  "
            if identity.get("retrospective_reconstruction") is True
            else "> 证据模式=目标日实时/既有PIT（非历史重建）  "
        ),
        f"> 盘后候选={len(items)}；精确出票={prepared.get('ticket_total')}；"
        f"竞价状态={_display(gate.get('status'))}；"
        "automatic_substitution=false",
    ]
    for item in items:
        code = str(item.get("stock_code") or "").zfill(6)
        action = dict(item.get("action_plan") or {})
        target = dict(item.get("target") or {})
        assessment = assessments.get(code, {})
        buy_range = action.get("buy_range")
        buy_text = (
            f"{_display(buy_range.get('low'))}~{_display(buy_range.get('high'))}"
            if isinstance(buy_range, Mapping)
            else "—"
        )
        reasons = assessment.get("reasons") or item.get("reasons") or []
        lines.extend([
            "",
            f"### #{_display(item.get('rank_no'))} {code} "
            f"{_display(item.get('stock_name'))}",
            f"- 排名：盘后 #{_display(item.get('rank_no'))}；"
            f"竞价 #{_display(assessment.get('decision_rank'))}",
            f"- 角色/主题：dynamic_role={_display(item.get('dynamic_role'))}；"
            f"theme={_display(item.get('primary_theme'))}",
            f"- 买入条件（模拟）：{_display(action.get('label'))}；"
            f"buy_range={buy_text}；reference={_display(target.get('reference_price'))}；"
            f"protective_stop={_display(action.get('protective_stop'))}；"
            f"valid_until={_display(item.get('valid_until'))}",
            f"- 竞价：gate={_display(assessment.get('gate_status'))}；"
            f"action={_display(assessment.get('advisory_action'))}；"
            f"quote_at={_display(assessment.get('quote_at'))}",
            "- 竞价理由：" + "；".join(str(value) for value in reasons),
            f"- alternative_set={_format_alternatives(assessment.get('alternative_set'))}；"
            "automatic_substitution=false",
        ])
    return "\n".join(lines)


def send_final_pool_batch(
    engine: Engine,
    *,
    target_trade_date: str,
    now: datetime | None = None,
    receipt_loader: Callable[..., dict[str, Any] | None] = read_daily_delivery,
    canonical_loader: Callable[[str], dict[str, Any] | None] = _default_canonical_loader,
    ticket_loader: Callable[[str, str, str, str], dict[str, Any]] = _default_ticket_loader,
    gate_loader: Callable[..., dict[str, Any]] | None = None,
    delivery_fn: Callable[..., DeliveryResult] = deliver_markdown,
    webhook_url: str | None = None,
) -> dict[str, Any]:
    """Validate both cold-start sessions before sending either exact pool."""

    current = now or datetime.now(_SHANGHAI).replace(tzinfo=None, microsecond=0)
    if current.tzinfo is not None:
        current = current.astimezone(_SHANGHAI).replace(tzinfo=None)
    trade_dates = _required_trade_dates(engine, target_trade_date)
    prepared = [
        _validated_pool(
            engine,
            trade_date,
            now=current,
            receipt_loader=receipt_loader,
            canonical_loader=canonical_loader,
            ticket_loader=ticket_loader,
            gate_loader=gate_loader,
        )
        for trade_date in trade_dates
    ]
    webhook = (
        webhook_url
        if webhook_url is not None
        else get_wecom_webhook("briefing", required=False)
    )
    deliveries = []
    for item in prepared:
        identity = dict(item["identity"])
        idempotency_key = json.dumps(
            identity,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        result = delivery_fn(
            webhook,
            build_final_pool_markdown(item),
            engine=engine,
            delivery_kind=FINAL_POOL_DELIVERY_KIND,
            webhook_kind="briefing",
            title=f"## 🎯 {identity['trade_date']} 最终策略票池",
            idempotency_key=idempotency_key,
            audit_identity=identity,
        )
        if (
            not isinstance(result, DeliveryResult)
            or result.success is not True
            or result.segment_count <= 0
            or result.delivered_count != result.segment_count
        ):
            raise WeComDeliveryError(
                "final-pool delivery returned an incomplete result",
                delivery_id=(
                    result.delivery_id
                    if isinstance(result, DeliveryResult)
                    else None
                ),
                result=(result if isinstance(result, DeliveryResult) else None),
            )
        deliveries.append({
            **identity,
            "delivery_id": result.delivery_id,
            "content_sha256": result.content_sha256,
            "segment_count": result.segment_count,
            "delivered_count": result.delivered_count,
            "idempotent_replay": result.idempotent_replay,
            "status": "SUCCEEDED",
        })
    return {
        "schema": FINAL_POOL_DELIVERY_SCHEMA,
        "status": "SUCCEEDED",
        "target_trade_date": target_trade_date,
        "covered_trade_dates": trade_dates,
        "delivery_count": len(deliveries),
        "deliveries": deliveries,
        "automatic_substitution": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


__all__ = [
    "FINAL_POOL_DELIVERY_KIND",
    "FINAL_POOL_DELIVERY_SCHEMA",
    "FinalPoolDeliveryBlocked",
    "build_final_pool_markdown",
    "send_final_pool_batch",
]
