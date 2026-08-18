from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from server.common.trading_v3_maintenance import trading_v3_writer

from .audit import build_counterfactual_records, opportunity_recall
from .backtest import _build_features, _dynamic_signal_outcome
from .config import load_v3_config
from .forward_evidence import sync_executed_forward_evidence


def _pending_forecasts(
    engine: Engine,
    *,
    limit: int,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    current = (now or datetime.now()).replace(microsecond=0)
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT f.forecast_id, f.run_uid, f.trade_date, f.rank_no,
                       f.stock_code, f.short_name, f.strategy_key,
                       f.horizon_days, f.expected_return_net_pct,
                       f.return_q10_pct, f.forecast_status,
                       f.initial_stop_pct, f.valid_until,
                       d.portfolio_json,
                       t.target_id,
                       t.strategy_keys_json AS target_strategy_keys_json
                FROM st_alpha_forecast_v3 f
                JOIN st_decision_run_v3 d ON d.run_uid = f.run_uid
                LEFT JOIN st_target_portfolio_v3 t
                  ON t.run_uid = f.run_uid
                 AND t.stock_code = f.stock_code
                LEFT JOIN st_counterfactual_v3 c
                  ON c.source_forecast_id = f.forecast_id
                LEFT JOIN st_counterfactual_queue_v3 q
                  ON q.forecast_id = f.forecast_id
                WHERE c.counterfactual_id IS NULL
                  AND f.trade_date < DATE(:now)
                  AND f.valid_until < :now
                  AND (
                      q.forecast_id IS NULL
                      OR q.next_retry_at <= :now
                  )
                  AND d.status = 'COMPLETED'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM st_decision_run_v3 d2
                      WHERE d2.trade_date = d.trade_date
                        AND d2.status = 'COMPLETED'
                        AND (
                            CASE d2.mode
                                WHEN 'close' THEN 0
                                WHEN 'premarket' THEN 1
                                ELSE 2
                            END
                            <
                            CASE d.mode
                                WHEN 'close' THEN 0
                                WHEN 'premarket' THEN 1
                                ELSE 2
                            END
                            OR (
                                CASE d2.mode
                                    WHEN 'close' THEN 0
                                    WHEN 'premarket' THEN 1
                                    ELSE 2
                                END
                                =
                                CASE d.mode
                                    WHEN 'close' THEN 0
                                    WHEN 'premarket' THEN 1
                                    ELSE 2
                                END
                                AND (
                                    d2.decision_at > d.decision_at
                                    OR (
                                        d2.decision_at = d.decision_at
                                        AND d2.run_uid > d.run_uid
                                    )
                                )
                            )
                        )
                  )
                ORDER BY f.valid_until, f.trade_date,
                         f.rank_no, f.forecast_id
                LIMIT :limit
                """
            ),
            {
                "limit": max(1, min(100000, int(limit))),
                "now": current,
            },
        ).mappings().all()
    result = []
    for raw in rows:
        row = dict(raw)
        try:
            target_strategy_keys = set(json.loads(str(
                row.pop("target_strategy_keys_json") or "[]"
            )))
        except (TypeError, ValueError, json.JSONDecodeError):
            target_strategy_keys = set()
        row["accepted"] = int(
            row.pop("target_id", None) is not None
            and str(row["strategy_key"]) in target_strategy_keys
        )
        result.append(row)
    return result


def _outcomes(
    kline_engine: Engine,
    rows: list[dict[str, Any]],
    *,
    unresolved: dict[str, str] | None = None,
) -> dict[str, dict[str, float]]:
    unresolved = unresolved if unresolved is not None else {}
    if not rows:
        return {}
    codes = sorted({str(row["stock_code"]) for row in rows})
    start_date = min(row["trade_date"] for row in rows)
    history_start = start_date - timedelta(days=180)
    statement = text(
        """
        SELECT stock_code, short_name, trade_date,
               open, close, high, low, pre_close, amount, change_pct
        FROM sm_stock_kline
        WHERE k_type = 1
          AND stock_code IN :codes
          AND trade_date BETWEEN :history_start AND :end_date
        ORDER BY stock_code, trade_date
        """
    ).bindparams(bindparam("codes", expanding=True))
    with kline_engine.connect() as connection:
        bars = connection.execute(
            statement,
            {
                "codes": codes,
                "history_start": history_start,
                "end_date": date.today(),
            },
        ).mappings().all()
    if not bars:
        for row in rows:
            unresolved[str(row["forecast_id"])] = "KLINE_RANGE_EMPTY"
        return {}
    features = _build_features(pd.DataFrame([dict(bar) for bar in bars]))
    groups = {
        str(code): group.reset_index(drop=True)
        for code, group in features.groupby(
            "stock_code",
            sort=False,
            observed=True,
        )
    }
    locations = {
        code: {
            pd.Timestamp(trade_date): index
            for index, trade_date in enumerate(group["trade_date"])
        }
        for code, group in groups.items()
    }
    config = load_v3_config()
    outcomes: dict[str, dict[str, float]] = {}
    for row in rows:
        code = str(row["stock_code"])
        horizon = int(row["horizon_days"])
        signal_index = locations.get(code, {}).get(
            pd.Timestamp(row["trade_date"])
        )
        if signal_index is None:
            unresolved[str(row["forecast_id"])] = (
                "SIGNAL_BAR_MISSING"
            )
            continue
        outcome = _dynamic_signal_outcome(
            groups[code],
            signal_index=signal_index,
            config=config,
            initial_stop_pct=float(row["initial_stop_pct"]),
            paper_discovery=(
                str(row.get("forecast_status") or "")
                == "PAPER_DISCOVERY_CANDIDATE"
            ),
        )
        if outcome is None:
            unresolved[str(row["forecast_id"])] = (
                "OUTCOME_NOT_AVAILABLE"
            )
            continue
        outcomes[str(row["forecast_id"])] = {
            "net_return_pct": float(outcome["net_return_pct"]),
            "mae_pct": float(outcome["mae_pct"]),
            "mfe_pct": float(outcome["mfe_pct"]),
            "outcome_date": pd.Timestamp(
                outcome["exit_date"]
            ).date(),
        }
    return outcomes


def _defer_unresolved(
    connection,
    unresolved: dict[str, str],
    *,
    now: datetime,
) -> None:
    for forecast_id, reason in unresolved.items():
        retry_days = (
            7
            if reason in {"KLINE_RANGE_EMPTY", "SIGNAL_BAR_MISSING"}
            else 1
        )
        connection.execute(
            text(
                """
                INSERT INTO st_counterfactual_queue_v3 (
                    forecast_id, queue_status, defer_reason,
                    attempt_count, first_attempt_at,
                    last_attempt_at, next_retry_at
                ) VALUES (
                    :forecast_id, 'DEFERRED', :defer_reason,
                    1, :now, :now, :next_retry_at
                )
                ON DUPLICATE KEY UPDATE
                    queue_status = 'DEFERRED',
                    defer_reason = VALUES(defer_reason),
                    attempt_count = attempt_count + 1,
                    last_attempt_at = VALUES(last_attempt_at),
                    next_retry_at = VALUES(next_retry_at)
                """
            ),
            {
                "forecast_id": forecast_id,
                "defer_reason": reason,
                "now": now,
                "next_retry_at": now + timedelta(days=retry_days),
            },
        )


def _refresh_recall_groups(
    connection,
    group_keys: set[tuple[date, int, str]],
    *,
    now: datetime,
) -> int:
    refreshed = 0
    for trade_date, horizon, strategy_key in sorted(group_keys):
        rows = connection.execute(
            text(
                """
                SELECT f.stock_code, f.short_name, f.rank_no,
                       c.accepted, c.reason_code,
                       c.expected_return_net_pct,
                       f.return_q10_pct,
                       c.realized_net_return_pct
                FROM st_counterfactual_v3 c
                JOIN st_alpha_forecast_v3 f
                  ON f.forecast_id = c.source_forecast_id
                WHERE f.trade_date = :trade_date
                  AND c.horizon_days = :horizon_days
                  AND c.strategy_key = :strategy_key
                ORDER BY f.rank_no, f.stock_code
                """
            ),
            {
                "trade_date": trade_date,
                "horizon_days": horizon,
                "strategy_key": strategy_key,
            },
        ).mappings().all()
        if not rows:
            continue
        decisions = [{
            "stock_code": row["stock_code"],
            "stock_name": row["short_name"],
            "strategy_key": strategy_key,
            "rank": row["rank_no"],
            "accepted": bool(row["accepted"]),
            "reason_code": row["reason_code"],
            "expected_return_net_pct": row[
                "expected_return_net_pct"
            ],
            "return_q10_pct": row["return_q10_pct"],
        } for row in rows]
        outcomes = {
            str(row["stock_code"]): {
                "net_return_pct": float(
                    row["realized_net_return_pct"] or 0.0
                )
            }
            for row in rows
        }
        recall = opportunity_recall(decisions, outcomes)
        connection.execute(
            text(
                """
                INSERT INTO st_opportunity_recall_v3 (
                    recall_id, trade_date, horizon_days,
                    strategy_key, evidence_kind, protocol_version,
                    winner_threshold_pct, winner_count,
                    accepted_winner_count, missed_winner_count,
                    recall_at_20, recall_at_50,
                    accepted_average_net_return_pct,
                    missed_reason_json, created_at
                ) VALUES (
                    :recall_id, :trade_date, :horizon_days,
                    :strategy_key, 'SHADOW',
                    'COUNTERFACTUAL_TECHNICAL_PROXY_V2',
                    :winner_threshold_pct, :winner_count,
                    :accepted_winner_count, :missed_winner_count,
                    :recall_at_20, :recall_at_50,
                    :accepted_average_net_return_pct,
                    :missed_reason_json, :created_at
                )
                ON DUPLICATE KEY UPDATE
                    evidence_kind = VALUES(evidence_kind),
                    protocol_version = VALUES(protocol_version),
                    winner_count = VALUES(winner_count),
                    accepted_winner_count =
                        VALUES(accepted_winner_count),
                    missed_winner_count = VALUES(missed_winner_count),
                    recall_at_20 = VALUES(recall_at_20),
                    recall_at_50 = VALUES(recall_at_50),
                    accepted_average_net_return_pct =
                        VALUES(accepted_average_net_return_pct),
                    missed_reason_json = VALUES(missed_reason_json),
                    created_at = VALUES(created_at)
                """
            ),
            {
                "recall_id": uuid.uuid4().hex,
                "trade_date": trade_date,
                "horizon_days": horizon,
                "strategy_key": strategy_key,
                "winner_threshold_pct": recall[
                    "winner_threshold_pct"
                ],
                "winner_count": recall["winner_count"],
                "accepted_winner_count": recall[
                    "accepted_winner_count"
                ],
                "missed_winner_count": recall[
                    "missed_winner_count"
                ],
                "recall_at_20": recall.get("recall_at_20"),
                "recall_at_50": recall.get("recall_at_50"),
                "accepted_average_net_return_pct": recall.get(
                    "accepted_average_net_return_pct"
                ),
                "missed_reason_json": json.dumps(
                    recall["missed_reason_counts"],
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "created_at": now,
            },
        )
        refreshed += 1
    return refreshed


def rebuild_counterfactual_recall(engine: Engine) -> int:
    now = datetime.now().replace(microsecond=0)
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT DISTINCT f.trade_date, c.horizon_days,
                                c.strategy_key
                FROM st_counterfactual_v3 c
                JOIN st_alpha_forecast_v3 f
                  ON f.forecast_id = c.source_forecast_id
                """
            )
        ).all()
        group_keys = {
            (row[0], int(row[1]), str(row[2])) for row in rows
        }
        return _refresh_recall_groups(
            connection,
            group_keys,
            now=now,
        )


def counterfactual_queue_stats(
    engine: Engine,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = (now or datetime.now()).replace(microsecond=0)
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    COALESCE(SUM(CASE
                        WHEN c.counterfactual_id IS NULL
                         AND f.valid_until < :now
                         AND (
                             q.forecast_id IS NULL
                             OR q.next_retry_at <= :now
                         )
                        THEN 1 ELSE 0 END), 0)
                        AS eligible_due_count,
                    COALESCE(SUM(CASE
                        WHEN c.counterfactual_id IS NULL
                         AND f.valid_until < :now
                         AND q.next_retry_at > :now
                        THEN 1 ELSE 0 END), 0)
                        AS deferred_due_count,
                    COALESCE(SUM(CASE
                        WHEN c.counterfactual_id IS NULL
                         AND f.valid_until >= :now
                        THEN 1 ELSE 0 END), 0)
                        AS immature_count,
                    MIN(CASE
                        WHEN c.counterfactual_id IS NULL
                         AND q.next_retry_at > :now
                        THEN q.next_retry_at END)
                        AS next_retry_at
                FROM st_alpha_forecast_v3 f
                JOIN st_decision_run_v3 d ON d.run_uid = f.run_uid
                LEFT JOIN st_counterfactual_v3 c
                  ON c.source_forecast_id = f.forecast_id
                LEFT JOIN st_counterfactual_queue_v3 q
                  ON q.forecast_id = f.forecast_id
                WHERE d.status = 'COMPLETED'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM st_decision_run_v3 d2
                      WHERE d2.trade_date = d.trade_date
                        AND d2.status = 'COMPLETED'
                        AND (
                            CASE d2.mode
                                WHEN 'close' THEN 0
                                WHEN 'premarket' THEN 1
                                ELSE 2
                            END
                            <
                            CASE d.mode
                                WHEN 'close' THEN 0
                                WHEN 'premarket' THEN 1
                                ELSE 2
                            END
                            OR (
                                CASE d2.mode
                                    WHEN 'close' THEN 0
                                    WHEN 'premarket' THEN 1
                                    ELSE 2
                                END
                                =
                                CASE d.mode
                                    WHEN 'close' THEN 0
                                    WHEN 'premarket' THEN 1
                                    ELSE 2
                                END
                                AND (
                                    d2.decision_at > d.decision_at
                                    OR (
                                        d2.decision_at = d.decision_at
                                        AND d2.run_uid > d.run_uid
                                    )
                                )
                            )
                        )
                  )
                """
            ),
            {"now": current},
        ).mappings().first()
        reasons = connection.execute(
            text(
                """
                SELECT defer_reason, COUNT(*) AS row_count,
                       MAX(attempt_count) AS maximum_attempt_count
                FROM st_counterfactual_queue_v3
                WHERE queue_status = 'DEFERRED'
                GROUP BY defer_reason
                ORDER BY defer_reason
                """
            )
        ).mappings().all()
    return {
        "eligible_due_count": int(
            (row or {}).get("eligible_due_count") or 0
        ),
        "deferred_due_count": int(
            (row or {}).get("deferred_due_count") or 0
        ),
        "immature_count": int(
            (row or {}).get("immature_count") or 0
        ),
        "next_retry_at": (row or {}).get("next_retry_at"),
        "deferred_reason_counts": {
            str(item["defer_reason"]): int(item["row_count"] or 0)
            for item in reasons
        },
        "maximum_attempt_count": max(
            (
                int(item["maximum_attempt_count"] or 0)
                for item in reasons
            ),
            default=0,
        ),
    }


@trading_v3_writer
def drain_counterfactual_backlog(
    primary_engine: Engine,
    kline_engine: Engine,
    *,
    batch_size: int = 10000,
    max_batches: int = 10,
    rebuild_recall: bool = True,
) -> dict[str, Any]:
    batch_size = max(1, min(100000, int(batch_size)))
    max_batches = max(1, min(100, int(max_batches)))
    forward_evidence = sync_executed_forward_evidence(
        primary_engine,
        kline_engine,
    )
    batches = []
    totals = {
        "pending_count": 0,
        "matured_count": 0,
        "inserted_count": 0,
        "deferred_count": 0,
    }
    for batch_number in range(1, max_batches + 1):
        result = run_counterfactual_audit(
            primary_engine,
            kline_engine,
            limit=batch_size,
            sync_forward=False,
        )
        batch = {
            key: value
            for key, value in result.items()
            if key != "forward_evidence"
        }
        batch["batch_number"] = batch_number
        batches.append(batch)
        for key in totals:
            totals[key] += int(result.get(key) or 0)
        stats = counterfactual_queue_stats(primary_engine)
        if int(stats["eligible_due_count"]) == 0:
            break
        if int(result.get("pending_count") or 0) == 0:
            break
    recall_group_count = (
        rebuild_counterfactual_recall(primary_engine)
        if rebuild_recall
        else sum(
            int(item.get("recall_group_count") or 0)
            for item in batches
        )
    )
    stats = counterfactual_queue_stats(primary_engine)
    return {
        "status": (
            "drained"
            if int(stats["eligible_due_count"]) == 0
            else "partial"
        ),
        "forward_evidence": forward_evidence,
        "batch_size": batch_size,
        "max_batches": max_batches,
        "batch_count": len(batches),
        **totals,
        "recall_group_count": recall_group_count,
        "queue": stats,
        "batches": batches,
    }


def run_counterfactual_audit(
    primary_engine: Engine,
    kline_engine: Engine,
    *,
    limit: int = 50000,
    sync_forward: bool = True,
) -> dict[str, Any]:
    now = datetime.now().replace(microsecond=0)
    forward_evidence = (
        sync_executed_forward_evidence(
            primary_engine,
            kline_engine,
        )
        if sync_forward
        else {"status": "already_synced"}
    )
    pending = _pending_forecasts(
        primary_engine,
        limit=limit,
        now=now,
    )
    unresolved: dict[str, str] = {}
    outcomes = _outcomes(
        kline_engine,
        pending,
        unresolved=unresolved,
    )
    inserted = 0
    group_keys: set[tuple[date, int, str]] = set()
    records_to_insert = []
    for row in pending:
        outcome = outcomes.get(str(row["forecast_id"]))
        if not outcome:
            continue
        portfolio = json.loads(str(row.pop("portfolio_json") or "{}"))
        rejected = {
            str(item.get("stock_code")): item
            for item in portfolio.get("rejected", [])
        }
        rejection = rejected.get(str(row["stock_code"]), {})
        reason_code = (
            ""
            if row["accepted"]
            else str(
                rejection.get("reason_code")
                or row["forecast_status"]
            )
        )
        decision = {
            "stock_code": row["stock_code"],
            "stock_name": row["short_name"],
            "strategy_key": row["strategy_key"],
            "rank": row["rank_no"],
            "accepted": bool(row["accepted"]),
            "reason_code": reason_code,
            "expected_return_net_pct": row[
                "expected_return_net_pct"
            ],
            "return_q10_pct": row["return_q10_pct"],
        }
        audit = build_counterfactual_records(
            [decision],
            {str(row["stock_code"]): outcome},
        )[0]
        audit.update({
            "forecast_id": row["forecast_id"],
            "run_uid": row["run_uid"],
            "horizon_days": row["horizon_days"],
            "outcome_date": outcome["outcome_date"],
        })
        records_to_insert.append(audit)
        group_key = (
            row["trade_date"],
            int(row["horizon_days"]),
            str(row["strategy_key"]),
        )
        group_keys.add(group_key)
    with primary_engine.begin() as connection:
        _defer_unresolved(connection, unresolved, now=now)
        for item in records_to_insert:
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_counterfactual_v3 (
                        counterfactual_id, source_run_uid,
                        source_forecast_id, stock_code, strategy_key,
                        evidence_kind, selection_status,
                        execution_status, protocol_version,
                        horizon_days, accepted, reason_code,
                        expected_return_net_pct,
                        realized_net_return_pct, realized_mae_pct,
                        realized_mfe_pct, missed_opportunity,
                        false_positive, calibration_breach,
                        attribution, outcome_date, created_at
                    ) VALUES (
                        :counterfactual_id, :source_run_uid,
                        :source_forecast_id, :stock_code, :strategy_key,
                        'SHADOW', :selection_status,
                        'NOT_APPLICABLE',
                        'COUNTERFACTUAL_TECHNICAL_PROXY_V2',
                        :horizon_days, :accepted, :reason_code,
                        :expected_return_net_pct,
                        :realized_net_return_pct, :realized_mae_pct,
                        :realized_mfe_pct, :missed_opportunity,
                        :false_positive, :calibration_breach,
                        :attribution, :outcome_date, :created_at
                    )
                    """
                ),
                {
                    "counterfactual_id": uuid.uuid4().hex,
                    "source_run_uid": item["run_uid"],
                    "source_forecast_id": item["forecast_id"],
                    "stock_code": item["stock_code"],
                    "strategy_key": item["strategy_key"],
                    "selection_status": (
                        "POLICY_SELECTED"
                        if item["accepted"]
                        else "POLICY_REJECTED"
                    ),
                    "horizon_days": item["horizon_days"],
                    "accepted": int(item["accepted"]),
                    "reason_code": item["reason_code"],
                    "expected_return_net_pct": item[
                        "expected_return_net_pct"
                    ],
                    "realized_net_return_pct": item[
                        "realized_net_return_pct"
                    ],
                    "realized_mae_pct": item["realized_mae_pct"],
                    "realized_mfe_pct": item["realized_mfe_pct"],
                    "missed_opportunity": int(
                        item["missed_opportunity"]
                    ),
                    "false_positive": int(item["false_positive"]),
                    "calibration_breach": int(
                        item["calibration_breach"]
                    ),
                    "attribution": item["attribution"],
                    "outcome_date": item["outcome_date"],
                    "created_at": now,
                },
            )
            inserted += 1
            connection.execute(
                text(
                    "DELETE FROM st_counterfactual_queue_v3 "
                    "WHERE forecast_id = :forecast_id"
                ),
                {"forecast_id": item["forecast_id"]},
            )
            connection.execute(
                text(
                    """
                    UPDATE st_shadow_portfolio_v3
                    SET result_status = 'MATURED',
                        realized_net_return_pct =
                            :realized_net_return_pct,
                        realized_mae_pct = :realized_mae_pct,
                        realized_mfe_pct = :realized_mfe_pct,
                        missed_opportunity = :missed_opportunity,
                        false_positive = :false_positive,
                        outcome_date = :outcome_date,
                        updated_at = :updated_at
                    WHERE source_forecast_id = :source_forecast_id
                      AND evidence_kind = 'SHADOW'
                      AND order_allowed = 0
                      AND can_activate_model = 0
                    """
                ),
                {
                    "source_forecast_id": item["forecast_id"],
                    "realized_net_return_pct": item[
                        "realized_net_return_pct"
                    ],
                    "realized_mae_pct": item["realized_mae_pct"],
                    "realized_mfe_pct": item["realized_mfe_pct"],
                    "missed_opportunity": int(
                        item["missed_opportunity"]
                    ),
                    "false_positive": int(item["false_positive"]),
                    "outcome_date": item["outcome_date"],
                    "updated_at": now,
                },
            )
        recall_group_count = _refresh_recall_groups(
            connection,
            group_keys,
            now=now,
        )
    return {
        "status": "ok",
        "forward_evidence": forward_evidence,
        "pending_count": len(pending),
        "matured_count": len(records_to_insert),
        "inserted_count": inserted,
        "deferred_count": len(unresolved),
        "deferred_reason_counts": {
            reason: sum(value == reason for value in unresolved.values())
            for reason in sorted(set(unresolved.values()))
        },
        "recall_group_count": recall_group_count,
    }
