#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Attest existing raw A-share daily bars against row-level BigQMT history.

Only rows whose OHLC, volume and amount match within frozen tolerances receive
QMT provenance. Missing or mismatched source rows remain untouched.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.spool import PROVIDER_ID
from integrations.qmt.local_history import get_local_history_engine
from server.common.batch_db import (
    create_batch_engine,
    qualified_table_name,
)
from tools.env_config import load_project_env

PRICE_TOLERANCE = 0.0001
VOLUME_ABSOLUTE_TOLERANCE = 100.0
VOLUME_REL_TOLERANCE = 0.0001
AMOUNT_REL_TOLERANCE = 0.001


def values_match(
    target: dict[str, Any],
    source: dict[str, Any],
    *,
    price_tolerance: float = PRICE_TOLERANCE,
    volume_rel_tolerance: float = VOLUME_REL_TOLERANCE,
    amount_rel_tolerance: float = AMOUNT_REL_TOLERANCE,
) -> bool:
    def close_enough(left: Any, right: Any, absolute: float, relative: float = 0.0) -> bool:
        if left is None or right is None:
            return left is None and right is None
        left_value = float(left)
        right_value = float(right)
        tolerance = max(absolute, abs(right_value) * relative)
        return math.isfinite(left_value) and math.isfinite(right_value) and abs(left_value - right_value) <= tolerance

    return all(
        close_enough(target.get(field), source.get(field), price_tolerance)
        for field in ("open", "close", "high", "low")
    ) and close_enough(
        target.get("volume"),
        source.get("volume"),
        VOLUME_ABSOLUTE_TOLERANCE,
        volume_rel_tolerance,
    ) and close_enough(
        target.get("amount"),
        source.get("amount"),
        1.0,
        amount_rel_tolerance,
    )


def _table_names(engine: Engine) -> tuple[str, str]:
    target_url = make_url(str(engine.url))
    local_engine = get_local_history_engine()
    local_url = make_url(str(local_engine.url))
    target_host = (target_url.host or "localhost").lower()
    local_host = (local_url.host or "localhost").lower()
    localhost_aliases = {"localhost", "127.0.0.1"}
    hosts_match = target_host == local_host or {
        target_host,
        local_host,
    }.issubset(localhost_aliases)
    if (
        not hosts_match
        or int(target_url.port or 3306) != int(local_url.port or 3306)
        or str(target_url.username or "") != str(local_url.username or "")
    ):
        raise RuntimeError(
            "QMT attestation must run on the Windows local MySQL boundary; "
            "target and QMT history schemas are not on the same server"
        )
    if not target_url.database or not local_url.database:
        raise RuntimeError("target and QMT history database names are required")
    return (
        qualified_table_name(target_url.database, "sm_stock_kline"),
        qualified_table_name(local_url.database, "qmt_local_stock_kline"),
    )


def ensure_attestation_tables(engine: Engine) -> None:
    statements = (
        """
        CREATE TABLE IF NOT EXISTS qmt_kline_attestation_run (
            run_id VARCHAR(64) PRIMARY KEY,
            provider VARCHAR(32) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            status VARCHAR(40) NOT NULL,
            target_rows BIGINT NOT NULL DEFAULT 0,
            qmt_rows BIGINT NOT NULL DEFAULT 0,
            matched_rows BIGINT NOT NULL DEFAULT 0,
            missing_qmt_rows BIGINT NOT NULL DEFAULT 0,
            mismatched_rows BIGINT NOT NULL DEFAULT 0,
            already_attested_rows BIGINT NOT NULL DEFAULT 0,
            updated_rows BIGINT NOT NULL DEFAULT 0,
            tolerance_json TEXT NOT NULL,
            started_at DATETIME NOT NULL,
            finished_at DATETIME NULL,
            error_message TEXT NULL,
            KEY idx_qmt_kline_attestation_range (start_date, end_date, status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_kline_attestation_mismatch (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            run_id VARCHAR(64) NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            reason VARCHAR(40) NOT NULL,
            target_close DECIMAL(20,6) NULL,
            qmt_close DECIMAL(20,6) NULL,
            target_volume DECIMAL(24,6) NULL,
            qmt_volume DECIMAL(24,6) NULL,
            target_amount DECIMAL(24,6) NULL,
            qmt_amount DECIMAL(24,6) NULL,
            created_at DATETIME NOT NULL,
            UNIQUE KEY uk_qmt_kline_attestation_mismatch
                (run_id, trade_date, stock_code),
            KEY idx_qmt_kline_mismatch_lookup (trade_date, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _match_sql(target_alias: str = "t", source_alias: str = "q") -> str:
    price_checks = " AND ".join(
        (
            f"(({target_alias}.`{field}` IS NULL AND {source_alias}.`{field}` IS NULL) OR "
            f"({target_alias}.`{field}` IS NOT NULL AND {source_alias}.`{field}` IS NOT NULL "
            f"AND ABS({target_alias}.`{field}` - {source_alias}.`{field}`) <= :price_tolerance))"
        )
        for field in ("open", "close", "high", "low")
    )
    volume_check = (
        f"(({target_alias}.volume IS NULL AND {source_alias}.volume IS NULL) OR "
        f"({target_alias}.volume IS NOT NULL AND {source_alias}.volume IS NOT NULL "
        f"AND ABS({target_alias}.volume - {source_alias}.volume) <= "
        f"GREATEST(:volume_absolute_tolerance, "
        f"ABS({source_alias}.volume) * :volume_rel_tolerance)))"
    )
    amount_check = (
        f"(({target_alias}.amount IS NULL AND {source_alias}.amount IS NULL) OR "
        f"({target_alias}.amount IS NOT NULL AND {source_alias}.amount IS NOT NULL "
        f"AND ABS({target_alias}.amount - {source_alias}.amount) <= "
        f"GREATEST(1.0, ABS({source_alias}.amount) * :amount_rel_tolerance)))"
    )
    return f"{price_checks} AND {volume_check} AND {amount_check}"


def attest_range(
    engine: Engine,
    *,
    start_date: str,
    end_date: str,
    apply: bool,
    provider: str = PROVIDER_ID,
    mismatch_sample_limit: int = 5000,
) -> dict[str, Any]:
    ensure_attestation_tables(engine)
    target_table, source_table = _table_names(engine)
    run_id = f"qmt_attest_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    tolerances = {
        "price_absolute": PRICE_TOLERANCE,
        "volume_absolute": VOLUME_ABSOLUTE_TOLERANCE,
        "volume_relative": VOLUME_REL_TOLERANCE,
        "amount_relative": AMOUNT_REL_TOLERANCE,
    }
    params = {
        "run_id": run_id,
        "provider": provider,
        "start_date": start_date,
        "end_date": end_date,
        "price_tolerance": PRICE_TOLERANCE,
        "volume_absolute_tolerance": VOLUME_ABSOLUTE_TOLERANCE,
        "volume_rel_tolerance": VOLUME_REL_TOLERANCE,
        "amount_rel_tolerance": AMOUNT_REL_TOLERANCE,
    }
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO qmt_kline_attestation_run
                (run_id, provider, start_date, end_date, status,
                 tolerance_json, started_at)
                VALUES
                (:run_id, :provider, :start_date, :end_date, 'RUNNING',
                 :tolerance_json, NOW())
                """
            ),
            {**params, "tolerance_json": json.dumps(tolerances, sort_keys=True)},
        )
    match_sql = _match_sql()
    target_temp = "tmp_qmt_attest_target"
    source_temp = "tmp_qmt_attest_source"
    compare_temp = "tmp_qmt_attest_compare"
    try:
        with engine.begin() as connection:
            # Pull each indexed date range sequentially once.  Comparing the
            # two compact temporary tables is substantially faster than
            # repeating random cross-schema lookups for counters, mismatch
            # samples, and the final update.
            for temporary in (compare_temp, source_temp, target_temp):
                connection.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{temporary}`"))
            connection.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{target_temp}` ENGINE=InnoDB AS
                    SELECT id AS target_id,
                           stock_code COLLATE utf8mb4_general_ci AS stock_code,
                           trade_date, `open`, `close`, `high`, `low`,
                           volume, amount, data_source, quality_status
                    FROM {target_table}
                    WHERE trade_date BETWEEN :start_date AND :end_date
                      AND k_type=1 AND adjust_type=0
                      AND stock_code REGEXP '^(0|3|6)'
                    """
                ),
                params,
            )
            connection.execute(
                text(
                    f"""
                    ALTER TABLE `{target_temp}`
                    ADD PRIMARY KEY (target_id),
                    ADD UNIQUE KEY uk_tmp_qmt_attest_target
                        (stock_code, trade_date)
                    """
                )
            )
            connection.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{source_temp}` ENGINE=InnoDB AS
                    SELECT id AS qmt_id, stock_code, trade_date,
                           `open`, `close`, `high`, `low`, volume, amount,
                           qmt_code, provider, source_time, received_at,
                           batch_id, data_version, permission_status
                    FROM {source_table}
                    WHERE trade_date BETWEEN :start_date AND :end_date
                      AND period='1d' AND adjust_type=0
                      AND provider=:provider
                      AND stock_code REGEXP '^(0|3|6)'
                    """
                ),
                params,
            )
            connection.execute(
                text(
                    f"""
                    ALTER TABLE `{source_temp}`
                    ADD PRIMARY KEY (qmt_id),
                    ADD UNIQUE KEY uk_tmp_qmt_attest_source
                        (stock_code, trade_date)
                    """
                )
            )
            qmt_rows = int(
                connection.execute(
                    text(f"SELECT COUNT(*) FROM `{source_temp}`")
                ).scalar()
                or 0
            )
            connection.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{compare_temp}` ENGINE=InnoDB AS
                    SELECT t.target_id, q.qmt_id,
                           t.trade_date, t.stock_code,
                           ({match_sql}) AS is_match,
                           COALESCE((
                               t.data_source=:provider
                               AND t.quality_status='QMT_ATTESTED'
                           ), 0) AS provenance_already,
                           t.close AS target_close, q.close AS qmt_close,
                           t.volume AS target_volume, q.volume AS qmt_volume,
                           t.amount AS target_amount, q.amount AS qmt_amount,
                           q.qmt_code, q.provider, q.source_time,
                           q.received_at, q.batch_id, q.data_version,
                           q.permission_status
                    FROM `{target_temp}` t
                    LEFT JOIN `{source_temp}` q
                      ON q.stock_code=t.stock_code
                     AND q.trade_date=t.trade_date
                    """
                ),
                params,
            )
            connection.execute(
                text(
                    f"""
                    ALTER TABLE `{compare_temp}`
                    ADD PRIMARY KEY (target_id),
                    ADD KEY idx_tmp_qmt_attest_match (is_match),
                    ADD KEY idx_tmp_qmt_attest_source (qmt_id)
                    """
                )
            )
            aggregate = connection.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS target_rows,
                           COALESCE(SUM(is_match), 0) AS matched_rows,
                           COALESCE(SUM(qmt_id IS NULL), 0)
                               AS missing_qmt_rows,
                           COALESCE(SUM(qmt_id IS NOT NULL), 0)
                               AS joined_rows,
                           COALESCE(SUM(
                               is_match AND provenance_already
                           ), 0) AS already_attested_rows
                    FROM `{compare_temp}`
                    """
                )
            ).mappings().one()
            target_rows = int(aggregate["target_rows"] or 0)
            matched_rows = int(aggregate["matched_rows"] or 0)
            missing_qmt_rows = int(aggregate["missing_qmt_rows"] or 0)
            joined_rows = int(aggregate["joined_rows"] or 0)
            mismatched_rows = max(0, joined_rows - matched_rows)
            already_attested_rows = int(aggregate["already_attested_rows"] or 0)
            sample_limit = max(0, min(50000, int(mismatch_sample_limit)))
            if sample_limit:
                mismatch_rows = []
                if target_rows:
                    mismatch_rows = connection.execute(
                        text(
                            f"""
                            SELECT t.trade_date, t.stock_code,
                                   CASE WHEN t.qmt_id IS NULL THEN 'MISSING_QMT'
                                        ELSE 'VALUE_MISMATCH' END AS reason,
                                   t.target_close, t.qmt_close,
                                   t.target_volume, t.qmt_volume,
                                   t.target_amount, t.qmt_amount
                            FROM `{compare_temp}` t
                            WHERE t.qmt_id IS NULL OR NOT t.is_match
                            ORDER BY t.trade_date, t.stock_code
                            LIMIT {sample_limit}
                            """
                        ),
                    ).mappings().all()
                if mismatch_rows:
                    connection.execute(
                        text(
                            """
                            INSERT INTO qmt_kline_attestation_mismatch
                            (run_id, trade_date, stock_code, reason,
                             target_close, qmt_close, target_volume, qmt_volume,
                             target_amount, qmt_amount, created_at)
                            VALUES
                            (:run_id, :trade_date, :stock_code, :reason,
                             :target_close, :qmt_close, :target_volume, :qmt_volume,
                             :target_amount, :qmt_amount, NOW())
                            """
                        ),
                        [{"run_id": run_id, **dict(row)} for row in mismatch_rows],
                    )
            updated_rows = 0
            if apply and matched_rows:
                updated_rows = int(
                    connection.execute(
                        text(
                            f"""
                            UPDATE {target_table} t
                            INNER JOIN `{compare_temp}` q
                              ON q.target_id=t.id
                            SET t.qmt_code=q.qmt_code,
                                t.data_source=q.provider,
                                t.source_time=q.source_time,
                                t.received_at=q.received_at,
                                t.batch_id=q.batch_id,
                                t.data_version=q.data_version,
                                t.quality_status='QMT_ATTESTED',
                                t.permission_status=q.permission_status,
                                t.etl_sync_at=NOW()
                            WHERE q.is_match
                              AND NOT COALESCE(q.provenance_already, 0)
                            """
                        ),
                    ).rowcount
                    or 0
                )
            if target_rows == 0:
                status = "EMPTY_TARGET"
            elif matched_rows == target_rows:
                status = "COMPLETED" if apply else "DRY_RUN_COMPLETE"
            elif matched_rows == 0 and missing_qmt_rows == target_rows:
                status = "BLOCKED_SOURCE_INCOMPLETE"
            else:
                status = "PARTIAL" if apply else "DRY_RUN_PARTIAL"
            connection.execute(
                text(
                    """
                    UPDATE qmt_kline_attestation_run
                    SET status=:status, target_rows=:target_rows,
                        qmt_rows=:qmt_rows, matched_rows=:matched_rows,
                        missing_qmt_rows=:missing_qmt_rows,
                        mismatched_rows=:mismatched_rows,
                        already_attested_rows=:already_attested_rows,
                        updated_rows=:updated_rows, finished_at=NOW()
                    WHERE run_id=:run_id
                    """
                ),
                {
                    **params,
                    "status": status,
                    "target_rows": target_rows,
                    "qmt_rows": qmt_rows,
                    "matched_rows": matched_rows,
                    "missing_qmt_rows": missing_qmt_rows,
                    "mismatched_rows": mismatched_rows,
                    "already_attested_rows": already_attested_rows,
                    "updated_rows": updated_rows,
                },
            )
            for temporary in (compare_temp, source_temp, target_temp):
                connection.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{temporary}`"))
        return {
            "run_id": run_id,
            "status": status,
            "apply": apply,
            "provider": provider,
            "start_date": start_date,
            "end_date": end_date,
            "target_rows": target_rows,
            "qmt_rows": qmt_rows,
            "matched_rows": matched_rows,
            "missing_qmt_rows": missing_qmt_rows,
            "mismatched_rows": mismatched_rows,
            "already_attested_rows": already_attested_rows,
            "updated_rows": updated_rows,
            "tolerances": tolerances,
        }
    except Exception as exc:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE qmt_kline_attestation_run
                    SET status='FAILED', error_message=:error,
                        finished_at=NOW()
                    WHERE run_id=:run_id
                    """
                ),
                {"run_id": run_id, "error": str(exc)[:4000]},
            )
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--provider", default=PROVIDER_ID)
    parser.add_argument("--mismatch-sample-limit", type=int, default=5000)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    load_project_env()
    engine = create_batch_engine(future=True)
    result = attest_range(
        engine,
        start_date=args.start_date,
        end_date=args.end_date,
        apply=args.apply,
        provider=args.provider,
        mismatch_sample_limit=args.mismatch_sample_limit,
    )
    print(
        json.dumps(result, ensure_ascii=False, default=str)
        if args.json
        else result
    )
    return 0 if result["status"] in {"COMPLETED", "DRY_RUN_COMPLETE", "EMPTY_TARGET"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
