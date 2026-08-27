"""Privileged, fail-closed physical schema for AI analysis outputs."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

from sqlalchemy import text

from server.common.schema_recovery_evidence import (
    ensure_evidence_table,
    load_pending_physical_rewrite_plan,
    make_evidence_record,
    persist_and_verify_evidence,
    plan_sha256,
    row_sha256,
    table_content_fingerprint,
    verify_pending_plan_content,
)


EXPECTED_ENGINE = "InnoDB"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"
RECOMMENDATION_TABLE = "st_recommended_stocks"
ANALYSIS_TABLE = "stock_analysis_result"
FAILURE_TABLE = "st_ai_failure_samples"
RECOVERY_VERSION = "analysis-output-business-key-recovery.v1"
PHYSICAL_RECOVERY_VERSION = "analysis-output-physical-normalization.v1"
_LEGACY_UTF8MB4_COLLATIONS = frozenset(
    {"utf8mb4_general_ci", EXPECTED_COLLATION}
)


def _spec(
    column_type: str,
    nullable: bool = True,
    default: str | None = None,
    *,
    character: bool = False,
    extra_contains: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "column_type": column_type,
        "is_nullable": "YES" if nullable else "NO",
        "column_default": default,
        "character_set_name": "utf8mb4" if character else None,
        "collation_name": EXPECTED_COLLATION if character else None,
        "extra_contains": extra_contains,
    }


def _varchar(length: int, default: str | None = None, *, nullable: bool = True):
    return _spec(
        f"varchar({length})", nullable, default, character=True
    )


def _text():
    return _spec("text", True, character=True)


RECOMMENDATION_COLUMN_CONTRACT = {
    "id": _spec("bigint", False, extra_contains=("auto_increment",)),
    "stock_code": _varchar(10, nullable=False),
    "short_name": _varchar(20),
    "ai_score": _spec("int"),
    "fundamental": _spec("int"),
    "capital_score": _spec("int"),
    "valuation": _spec("int"),
    "technical": _spec("int"),
    "reason": _text(),
    "sources": _varchar(100),
    "pick_date": _spec("date", False),
    "created_at": _spec("datetime", True, "current_timestamp"),
    "long_term_score": _spec("decimal(5,1)"),
    "short_term_score": _spec("decimal(5,1)"),
    "recommend_status": _varchar(10, "BLOCK"),
    "recommend_reason": _varchar(500, ""),
    "candidate_recommend_status": _varchar(
        10, "BLOCK", nullable=False
    ),
    "chase_risk_status": _varchar(
        20, "DATA_BLOCKED", nullable=False
    ),
    "ordinary_buy_eligible": _spec("tinyint(1)", False, "0"),
    "candidate_ordinary_buy_eligible": _spec(
        "tinyint(1)", False, "0"
    ),
    "publisher_run_uid": _spec(
        "char(32)", False, "", character=True
    ),
    "publication_status": _varchar(
        16, "PENDING", nullable=False
    ),
    "membership_snapshot_date": _spec("date"),
    "membership_snapshot_source": _varchar(64, "", nullable=False),
    "membership_proof_sha256": _spec(
        "char(64)", False, "", character=True
    ),
    "turnover_evidence_json": _text(),
    "upper_limit_evidence_json": _text(),
    "event_risk_level": _varchar(10, "LOW"),
    "last_check_time": _spec("datetime"),
    "sentiment_score": _spec("decimal(5,1)"),
    "market_mood_score": _spec("decimal(5,1)"),
    "event_score": _spec("decimal(5,1)"),
    "ultra_short_score": _spec("decimal(5,1)"),
    "swing_score": _spec("decimal(5,1)"),
    "primary_strategy": _varchar(20, ""),
    "strategy_profile": _varchar(20, ""),
    "suitable_strategies": _text(),
    "signal_status": _varchar(20, "WATCH"),
    "signal_reason": _varchar(500, ""),
    "entry_price_low": _spec("decimal(12,4)"),
    "entry_price_high": _spec("decimal(12,4)"),
    "stop_loss_price": _spec("decimal(12,4)"),
    "take_profit_1": _spec("decimal(12,4)"),
    "take_profit_2": _spec("decimal(12,4)"),
    "position_weight": _spec("decimal(5,2)"),
    "max_holding_days": _spec("int"),
    "entry_conditions_json": _text(),
    "sell_rules_json": _text(),
    "invalidation_reason": _varchar(500, ""),
    "quality_score": _spec("decimal(5,1)"),
    "entry_score": _spec("decimal(5,1)"),
    "final_trade_score": _spec("decimal(5,1)"),
    "expected_return_score": _spec("decimal(5,1)"),
    "expected_return_pct": _spec("decimal(8,2)"),
    "resistance_price": _spec("decimal(12,4)"),
    "heat_overload_score": _spec("decimal(5,1)"),
    "confidence_score": _spec("decimal(5,1)"),
    "sector_rotation_score": _spec("decimal(5,1)"),
    "failure_penalty_score": _spec("decimal(5,1)"),
    "data_quality_score": _spec("decimal(5,1)"),
    "data_quality_flags": _text(),
    "cooldown_days_left": _spec("int", True, "0"),
    "cooldown_until": _spec("date"),
    "main_wave_score": _spec("decimal(5,1)"),
    "trend_hold_score": _spec("decimal(5,1)"),
    "main_wave_stage": _varchar(30, ""),
    "main_wave_signal": _varchar(30, ""),
    "main_wave_reason": _varchar(500, ""),
    "trend_stop_price": _spec("decimal(12,4)"),
    "trend_reduce_price": _spec("decimal(12,4)"),
    "model_version": _varchar(64, ""),
}

ANALYSIS_COLUMN_CONTRACT = {
    "id": _spec("bigint", False, extra_contains=("auto_increment",)),
    "stock_code": _varchar(10, nullable=False),
    "stock_name": _varchar(20, nullable=False),
    "analysis_date": _spec("date", False),
    "last_news_time": _spec("datetime"),
    "long_term_score": _spec("decimal(5,1)"),
    "fundamental_score": _spec("decimal(5,1)"),
    "growth_score": _spec("decimal(5,1)"),
    "valuation_score": _spec("decimal(5,1)"),
    "risk_score": _spec("decimal(5,1)"),
    "short_term_score": _spec("decimal(5,1)"),
    "capital_score": _spec("decimal(5,1)"),
    "technical_score": _spec("decimal(5,1)"),
    "sentiment_score": _spec("decimal(5,1)"),
    "event_score": _spec("decimal(5,1)"),
    "event_risk_score": _spec("decimal(5,1)", True, "100.0"),
    "event_risk_level": _varchar(10, "LOW"),
    "event_risk_detail": _text(),
    "recommend_status": _varchar(10, "BLOCK"),
    "recommend_reason": _varchar(500, ""),
    "summary": _varchar(500, ""),
    "recommendation": _varchar(500, ""),
    "strengths": _text(),
    "risks": _text(),
    "created_at": _spec("datetime"),
    "updated_at": _spec("datetime"),
    "model_version": _varchar(64, ""),
    "data_quality_score": _spec("decimal(5,1)"),
    "data_quality_flags": _text(),
    "flow_trade_date": _spec("date"),
    "hot_trade_date": _spec("date"),
}

FAILURE_SAMPLE_COLUMN_CONTRACT = {
    "id": _spec("bigint", False, extra_contains=("auto_increment",)),
    "stock_code": _varchar(10, nullable=False),
    "short_name": _varchar(40, ""),
    "strategy_profile": _varchar(20, ""),
    "signal_date": _spec("date"),
    "result": _varchar(20, "fail"),
    "fail_tag": _varchar(40, ""),
    "fail_reason": _varchar(500, ""),
    "return_pct": _spec("decimal(8,4)"),
    "created_at": _spec("datetime"),
}

RECOMMENDATION_ADDITIVE_COLUMNS = {
    "long_term_score": "DECIMAL(5,1) DEFAULT NULL",
    "short_term_score": "DECIMAL(5,1) DEFAULT NULL",
    "recommend_status": "VARCHAR(10) DEFAULT 'BLOCK'",
    "recommend_reason": "VARCHAR(500) DEFAULT ''",
    "candidate_recommend_status": (
        "VARCHAR(10) NOT NULL DEFAULT 'BLOCK'"
    ),
    "chase_risk_status": (
        "VARCHAR(20) NOT NULL DEFAULT 'DATA_BLOCKED'"
    ),
    "ordinary_buy_eligible": "TINYINT(1) NOT NULL DEFAULT 0",
    "candidate_ordinary_buy_eligible": (
        "TINYINT(1) NOT NULL DEFAULT 0"
    ),
    "publisher_run_uid": (
        "CHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci "
        "NOT NULL DEFAULT ''"
    ),
    "publication_status": (
        "VARCHAR(16) NOT NULL DEFAULT 'PENDING'"
    ),
    "membership_snapshot_date": "DATE NULL",
    "membership_snapshot_source": (
        "VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci "
        "NOT NULL DEFAULT ''"
    ),
    "membership_proof_sha256": (
        "CHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci "
        "NOT NULL DEFAULT ''"
    ),
    "turnover_evidence_json": "TEXT NULL",
    "upper_limit_evidence_json": "TEXT NULL",
    "event_risk_level": "VARCHAR(10) DEFAULT 'LOW'",
    "last_check_time": "DATETIME DEFAULT NULL",
    "sentiment_score": "DECIMAL(5,1) DEFAULT NULL",
    "market_mood_score": "DECIMAL(5,1) DEFAULT NULL",
    "event_score": "DECIMAL(5,1) DEFAULT NULL",
    "ultra_short_score": "DECIMAL(5,1) DEFAULT NULL",
    "swing_score": "DECIMAL(5,1) DEFAULT NULL",
    "primary_strategy": "VARCHAR(20) DEFAULT ''",
    "strategy_profile": "VARCHAR(20) DEFAULT ''",
    "suitable_strategies": "TEXT NULL",
    "signal_status": "VARCHAR(20) DEFAULT 'WATCH'",
    "signal_reason": "VARCHAR(500) DEFAULT ''",
    "entry_price_low": "DECIMAL(12,4) DEFAULT NULL",
    "entry_price_high": "DECIMAL(12,4) DEFAULT NULL",
    "stop_loss_price": "DECIMAL(12,4) DEFAULT NULL",
    "take_profit_1": "DECIMAL(12,4) DEFAULT NULL",
    "take_profit_2": "DECIMAL(12,4) DEFAULT NULL",
    "position_weight": "DECIMAL(5,2) DEFAULT NULL",
    "max_holding_days": "INT DEFAULT NULL",
    "entry_conditions_json": "TEXT NULL",
    "sell_rules_json": "TEXT NULL",
    "invalidation_reason": "VARCHAR(500) DEFAULT ''",
    "quality_score": "DECIMAL(5,1) DEFAULT NULL",
    "entry_score": "DECIMAL(5,1) DEFAULT NULL",
    "final_trade_score": "DECIMAL(5,1) DEFAULT NULL",
    "expected_return_score": "DECIMAL(5,1) DEFAULT NULL",
    "expected_return_pct": "DECIMAL(8,2) DEFAULT NULL",
    "resistance_price": "DECIMAL(12,4) DEFAULT NULL",
    "heat_overload_score": "DECIMAL(5,1) DEFAULT NULL",
    "confidence_score": "DECIMAL(5,1) DEFAULT NULL",
    "sector_rotation_score": "DECIMAL(5,1) DEFAULT NULL",
    "failure_penalty_score": "DECIMAL(5,1) DEFAULT NULL",
    "data_quality_score": "DECIMAL(5,1) DEFAULT NULL",
    "data_quality_flags": "TEXT NULL",
    "cooldown_days_left": "INT DEFAULT 0",
    "cooldown_until": "DATE DEFAULT NULL",
    "main_wave_score": "DECIMAL(5,1) DEFAULT NULL",
    "trend_hold_score": "DECIMAL(5,1) DEFAULT NULL",
    "main_wave_stage": "VARCHAR(30) DEFAULT ''",
    "main_wave_signal": "VARCHAR(30) DEFAULT ''",
    "main_wave_reason": "VARCHAR(500) DEFAULT ''",
    "trend_stop_price": "DECIMAL(12,4) DEFAULT NULL",
    "trend_reduce_price": "DECIMAL(12,4) DEFAULT NULL",
    "model_version": "VARCHAR(64) DEFAULT ''",
}

ANALYSIS_ADDITIVE_COLUMNS = {
    "model_version": "VARCHAR(64) DEFAULT ''",
    "data_quality_score": "DECIMAL(5,1) DEFAULT NULL",
    "data_quality_flags": "TEXT NULL",
    "flow_trade_date": "DATE DEFAULT NULL",
    "hot_trade_date": "DATE DEFAULT NULL",
}

RECOMMENDATION_REQUIRED_COLUMNS = frozenset(RECOMMENDATION_COLUMN_CONTRACT)
ANALYSIS_REQUIRED_COLUMNS = frozenset(ANALYSIS_COLUMN_CONTRACT)
FAILURE_SAMPLE_REQUIRED_COLUMNS = frozenset(FAILURE_SAMPLE_COLUMN_CONTRACT)

_REQUIRED_INDEXES = {
    RECOMMENDATION_TABLE: {
        (True, ("id",)),
        (True, ("stock_code", "pick_date")),
        (False, ("pick_date", "stock_code")),
    },
    ANALYSIS_TABLE: {
        (True, ("id",)),
        (True, ("stock_code", "analysis_date")),
        (False, ("analysis_date", "stock_code")),
    },
    FAILURE_TABLE: {
        (True, ("id",)),
        (False, ("stock_code", "signal_date")),
        (False, ("fail_tag",)),
    },
}


def _value(row: Any, *names: str) -> Any:
    for name in names:
        try:
            if name in row:
                return row[name]
        except TypeError:
            pass
    return None


def _default(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip().strip("'").lower()
    if result.endswith("()"):
        result = result[:-2]
    if result in {"now", "current_timestamp"}:
        return "current_timestamp"
    return result


def _column_inventory(connection, table_name: str) -> dict[str, dict[str, Any]]:
    rows = connection.execute(text(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, "
        "CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
    ), {"table_name": table_name}).mappings().all()
    inventory: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(_value(row, "COLUMN_NAME", "column_name") or "")
        if not name:
            continue
        inventory[name] = {
            "column_type": str(
                _value(row, "COLUMN_TYPE", "column_type") or ""
            ).lower(),
            "is_nullable": str(
                _value(row, "IS_NULLABLE", "is_nullable") or ""
            ).upper(),
            "column_default": _default(
                _value(row, "COLUMN_DEFAULT", "column_default")
            ),
            "extra": str(_value(row, "EXTRA", "extra") or "").lower(),
            "character_set_name": (
                str(_value(row, "CHARACTER_SET_NAME", "character_set_name") or "")
                .lower() or None
            ),
            "collation_name": (
                str(_value(row, "COLLATION_NAME", "collation_name") or "")
                .lower() or None
            ),
        }
    return inventory


def _table_metadata(connection, table_name: str) -> dict[str, str] | None:
    row = connection.execute(text(
        "SELECT ENGINE, TABLE_COLLATION FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
    ), {"table_name": table_name}).mappings().first()
    if row is None:
        return None
    return {
        "engine": str(_value(row, "ENGINE", "engine") or ""),
        "table_collation": str(
            _value(row, "TABLE_COLLATION", "table_collation") or ""
        ),
    }


def _index_inventory(connection, table_name: str):
    rows = connection.execute(
        text(f"SHOW INDEX FROM `{table_name}`")
    ).mappings().all()
    indexes: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(_value(row, "Key_name", "key_name") or "")
        if not name:
            continue
        non_unique = _value(row, "Non_unique", "non_unique")
        item = indexes.setdefault(
            name, {"unique": int(non_unique or 0) == 0, "columns": []}
        )
        item["columns"].append((
            int(_value(row, "Seq_in_index", "seq_in_index") or 0),
            str(_value(row, "Column_name", "column_name") or ""),
        ))
    shapes = {
        (
            bool(item["unique"]),
            tuple(column for _seq, column in sorted(item["columns"])),
        )
        for item in indexes.values()
    }
    return shapes, set(indexes)


def _shape_available(actual, required) -> bool:
    if required in actual:
        return True
    unique, columns = required
    return not unique and (True, columns) in actual


def _column_drift(actual, expected):
    drift = {}
    for name, contract in expected.items():
        observed = actual.get(name)
        if observed is None:
            drift[name] = None
            continue
        base_expected = {
            key: value for key, value in contract.items()
            if key != "extra_contains"
        }
        base_observed = {key: observed.get(key) for key in base_expected}
        base_expected["column_default"] = _default(
            base_expected.get("column_default")
        )
        base_observed["column_default"] = _default(
            base_observed.get("column_default")
        )
        extra = str(observed.get("extra") or "").lower()
        if base_observed != base_expected or any(
            token not in extra for token in contract["extra_contains"]
        ):
            drift[name] = observed
    return drift


def _row_count(connection, table_name: str) -> int:
    return int(connection.execute(
        text(f"SELECT COUNT(*) FROM `{table_name}`")
    ).scalar() or 0)


def _duplicate_key(connection, table_name: str, columns: tuple[str, ...]):
    column_sql = ", ".join(f"`{column}`" for column in columns)
    return connection.execute(text(
        f"SELECT 1 FROM `{table_name}` GROUP BY {column_sql} "
        "HAVING COUNT(*) > 1 LIMIT 1"
    )).first() is not None


def _recommendation_duplicate_rows(
    connection, *, for_update: bool = False
) -> list[dict[str, Any]]:
    suffix = " FOR UPDATE" if for_update else ""
    rows = connection.execute(text(
        f"SELECT candidate.* FROM `{RECOMMENDATION_TABLE}` AS candidate "
        "INNER JOIN (SELECT stock_code, pick_date "
        f"FROM `{RECOMMENDATION_TABLE}` "
        "GROUP BY stock_code, pick_date HAVING COUNT(*) > 1) AS duplicate_key "
        "ON candidate.stock_code=duplicate_key.stock_code "
        "AND candidate.pick_date=duplicate_key.pick_date "
        "ORDER BY candidate.stock_code, candidate.pick_date, candidate.id"
        f"{suffix}"
    )).mappings().all()
    return [dict(row) for row in rows]


def _recommendation_inbound_foreign_keys(connection) -> list[dict[str, str]]:
    rows = connection.execute(text(
        "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME "
        "FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE REFERENCED_TABLE_SCHEMA=DATABASE() "
        "AND REFERENCED_TABLE_NAME=:table_name "
        "ORDER BY TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME"
    ), {"table_name": RECOMMENDATION_TABLE}).mappings().all()
    return [
        {
            "table_name": str(_value(row, "TABLE_NAME", "table_name") or ""),
            "column_name": str(_value(row, "COLUMN_NAME", "column_name") or ""),
            "constraint_name": str(
                _value(row, "CONSTRAINT_NAME", "constraint_name") or ""
            ),
        }
        for row in rows
    ]


def _build_recommendation_duplicate_plan(
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        source_id = int(row.get("id") or 0)
        stock_code = str(row.get("stock_code") or "")
        pick_date = str(row.get("pick_date") or "")[:10]
        if source_id <= 0 or not stock_code or not pick_date:
            raise RuntimeError("recommendation duplicate recovery identity differs")
        grouped[(stock_code.casefold(), pick_date)].append(row)

    groups: list[dict[str, Any]] = []
    source_rows: dict[int, dict[str, Any]] = {}
    for (stock_code, pick_date), candidates in sorted(grouped.items()):
        ordered = sorted(candidates, key=lambda item: int(item["id"]))
        if len(ordered) < 2:
            continue
        keep_id = int(ordered[-1]["id"])
        rows_public: list[dict[str, Any]] = []
        for row in ordered:
            source_id = int(row["id"])
            source_rows[source_id] = row
            rows_public.append({
                "source_row_id": source_id,
                "action": "KEEP" if source_id == keep_id else "DELETE_DUPLICATE",
                "source_row_sha256": row_sha256(row),
            })
        groups.append({
            "business_key": {
                "stock_code": stock_code,
                "pick_date": pick_date,
            },
            "canonical_rule": "MAX(id)",
            "keep_id": keep_id,
            "remove_ids": [
                int(item["id"]) for item in ordered if int(item["id"]) != keep_id
            ],
            "rows": rows_public,
        })
    payload = {"duplicate_groups": groups}
    return {
        "schema": "probiga.analysis-output-business-key-recovery-plan.v1",
        "recovery_version": RECOVERY_VERSION,
        "duplicate_group_count": len(groups),
        "delete_row_count": sum(len(group["remove_ids"]) for group in groups),
        "canonical_rule": "MAX(id)",
        "duplicate_groups": groups,
        "destructive_changes_planned": bool(groups),
        "plan_sha256": plan_sha256(
            recovery_version=RECOVERY_VERSION,
            payload=payload,
        ),
        "read_only": True,
        "_plan_payload": payload,
        "_source_rows": source_rows,
    }


def _recommendation_duplicate_plan(
    connection, *, for_update: bool = False
) -> dict[str, Any]:
    return _build_recommendation_duplicate_plan(
        _recommendation_duplicate_rows(connection, for_update=for_update)
    )


def _public_duplicate_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if not key.startswith("_")}


def _only_collation_diff(
    observed: dict[str, Any], expected: dict[str, Any]
) -> bool:
    for field, value in expected.items():
        if field == "extra_contains":
            continue
        actual = observed.get(field)
        if field == "collation_name":
            if actual not in _LEGACY_UTF8MB4_COLLATIONS or value != EXPECTED_COLLATION:
                return False
            continue
        if field == "column_default":
            actual = _default(actual)
            value = _default(value)
        if actual != value:
            return False
    extra = str(observed.get("extra") or "").casefold()
    return all(token in extra for token in expected["extra_contains"])


def _assert_safe_nonempty_collation_rewrite(
    *,
    table_name: str,
    columns: dict[str, dict[str, Any]],
    metadata: dict[str, str],
    expected: Mapping[str, dict[str, Any]],
    drift: dict[str, dict[str, Any] | None],
) -> None:
    if metadata["engine"].casefold() != EXPECTED_ENGINE.casefold():
        raise RuntimeError(f"nonempty {table_name} engine differs")
    if metadata["table_collation"].casefold() not in _LEGACY_UTF8MB4_COLLATIONS:
        raise RuntimeError(f"nonempty {table_name} charset differs")
    for observed in columns.values():
        charset = observed.get("character_set_name")
        collation = observed.get("collation_name")
        if charset is not None and (
            charset != "utf8mb4" or collation not in _LEGACY_UTF8MB4_COLLATIONS
        ):
            raise RuntimeError(f"nonempty {table_name} character encoding differs")
    unsafe = sorted(
        name for name, observed in drift.items()
        if observed is None or not _only_collation_diff(observed, expected[name])
    )
    if unsafe:
        raise RuntimeError(
            f"nonempty {table_name} physical drift cannot be modified in place: "
            f"columns={unsafe}, metadata={metadata}"
        )


def _physical_dry_run(
    connection,
    table_name: str,
    expected: Mapping[str, dict[str, Any]],
    *,
    safe_additive_columns: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    columns = _column_inventory(connection, table_name)
    metadata = _table_metadata(connection, table_name)
    if not columns or metadata is None:
        return {
            "table_exists": False,
            "row_count": 0,
            "rewrite_required": False,
            "safe_automatic_rewrite": False,
            "before_fingerprint": None,
        }
    drift = _column_drift(columns, expected)
    row_count = _row_count(connection, table_name)
    rewrite_required = (
        metadata["engine"].casefold() != EXPECTED_ENGINE.casefold()
        or metadata["table_collation"].casefold()
        != EXPECTED_COLLATION.casefold()
        or bool(drift)
    )
    safe = True
    if row_count and rewrite_required:
        safety_drift = {
            name: observed for name, observed in drift.items()
            if not (observed is None and name in safe_additive_columns)
        }
        try:
            _assert_safe_nonempty_collation_rewrite(
                table_name=table_name,
                columns=columns,
                metadata=metadata,
                expected=expected,
                drift=safety_drift,
            )
        except RuntimeError:
            safe = False
    fingerprint = (
        table_content_fingerprint(connection, table_name)
        if rewrite_required and safe else None
    )
    return {
        "table_exists": True,
        "row_count": row_count,
        "engine": metadata["engine"],
        "table_collation": metadata["table_collation"],
        "column_drift": sorted(drift),
        "rewrite_required": rewrite_required,
        "safe_automatic_rewrite": safe,
        "before_fingerprint": fingerprint,
    }


def plan_analysis_output_recovery(engine) -> dict[str, Any]:
    """Return a read-only, content-hashed plan for the three output tables."""

    with engine.connect() as connection:
        recommendation_columns = _column_inventory(
            connection, RECOMMENDATION_TABLE
        )
        duplicate_plan = (
            _recommendation_duplicate_plan(connection)
            if recommendation_columns else _build_recommendation_duplicate_plan([])
        )
        inbound_foreign_keys = _recommendation_inbound_foreign_keys(connection)
        analysis_duplicate_present = bool(
            _duplicate_key(
                connection, ANALYSIS_TABLE, ("stock_code", "analysis_date")
            )
        ) if _column_inventory(connection, ANALYSIS_TABLE) else False
        physical = {
            table_name: _physical_dry_run(
                connection,
                table_name,
                expected,
                safe_additive_columns=frozenset(additions),
            )
            for table_name, expected, additions in (
                (
                    RECOMMENDATION_TABLE,
                    RECOMMENDATION_COLUMN_CONTRACT,
                    RECOMMENDATION_ADDITIVE_COLUMNS,
                ),
                (
                    ANALYSIS_TABLE,
                    ANALYSIS_COLUMN_CONTRACT,
                    ANALYSIS_ADDITIVE_COLUMNS,
                ),
                (FAILURE_TABLE, FAILURE_SAMPLE_COLUMN_CONTRACT, {}),
            )
        }
    public_duplicate = _public_duplicate_plan(duplicate_plan)
    safe_automatic_rewrite = all(
        bool(detail["safe_automatic_rewrite"])
        for detail in physical.values()
    )
    combined_hash = plan_sha256(
        recovery_version=RECOVERY_VERSION,
        payload={
            "duplicate_groups": public_duplicate["duplicate_groups"],
            "analysis_duplicate_present": analysis_duplicate_present,
            "recommendation_inbound_foreign_keys": inbound_foreign_keys,
            "physical": physical,
        },
    )
    return {
        **public_duplicate,
        "analysis_duplicate_present": analysis_duplicate_present,
        "recommendation_inbound_foreign_keys": inbound_foreign_keys,
        "physical_tables": physical,
        "safe_automatic_rewrite": safe_automatic_rewrite,
        "ready_for_privileged_apply": (
            safe_automatic_rewrite
            and not analysis_duplicate_present
            and not inbound_foreign_keys
        ),
        "recovery_bundle_sha256": combined_hash,
    }


def _persist_duplicate_plan(connection, plan: dict[str, Any]) -> dict[str, Any]:
    source_rows = plan["_source_rows"]
    records = []
    for group in plan["duplicate_groups"]:
        for row in group["rows"]:
            source_id = int(row["source_row_id"])
            records.append(make_evidence_record(
                recovery_version=RECOVERY_VERSION,
                source_table=RECOMMENDATION_TABLE,
                source_row_id=source_id,
                action=str(row["action"]),
                business_key=group["business_key"],
                source_row=source_rows[source_id],
                plan_payload=plan["_plan_payload"],
                plan_hash=plan["plan_sha256"],
            ))
    return persist_and_verify_evidence(connection, records)


def _apply_duplicate_plan(connection, plan: dict[str, Any]) -> int:
    deleted = 0
    for group in plan["duplicate_groups"]:
        key = group["business_key"]
        for source_id in group["remove_ids"]:
            result = connection.execute(text(
                f"DELETE FROM `{RECOMMENDATION_TABLE}` "
                "WHERE id=:source_row_id AND stock_code=:stock_code "
                "AND pick_date=:pick_date"
            ), {
                "source_row_id": source_id,
                "stock_code": key["stock_code"],
                "pick_date": key["pick_date"],
            })
            if int(result.rowcount or 0) != 1:
                raise RuntimeError(
                    "recommendation duplicate changed after evidence capture"
                )
            deleted += 1
    return deleted


def _validate_one(connection, table_name: str, expected):
    columns = _column_inventory(connection, table_name)
    metadata = _table_metadata(connection, table_name)
    indexes, _names = _index_inventory(connection, table_name)
    missing = sorted(set(expected) - set(columns))
    drift = _column_drift(columns, expected)
    table_drift = {}
    if metadata is None:
        table_drift["table"] = "missing"
    else:
        if metadata["engine"].lower() != EXPECTED_ENGINE.lower():
            table_drift["engine"] = metadata["engine"]
        if metadata["table_collation"].lower() != EXPECTED_COLLATION.lower():
            table_drift["table_collation"] = metadata["table_collation"]
    missing_indexes = sorted(
        shape for shape in _REQUIRED_INDEXES[table_name]
        if not _shape_available(indexes, shape)
    )
    if missing or drift or table_drift or missing_indexes:
        raise RuntimeError(
            f"{table_name} physical contract differs: missing={missing}, "
            f"column_drift={drift}, table_drift={table_drift}, "
            f"missing_indexes={missing_indexes}"
        )
    return {"columns": columns, "indexes": indexes}


def validate_ai_failure_sample_schema(engine) -> dict[str, Any]:
    """Read-only validation for the failure-penalty evidence table."""
    with engine.connect() as connection:
        _validate_one(connection, FAILURE_TABLE, FAILURE_SAMPLE_COLUMN_CONTRACT)
    return {
        "table": FAILURE_TABLE,
        "column_names": sorted(FAILURE_SAMPLE_REQUIRED_COLUMNS),
        "physical_contract_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def validate_analysis_output_schema(engine) -> dict[str, Any]:
    """Read-only runtime proof for output tables and failure evidence."""
    with engine.connect() as connection:
        _validate_one(
            connection, RECOMMENDATION_TABLE, RECOMMENDATION_COLUMN_CONTRACT
        )
        _validate_one(connection, ANALYSIS_TABLE, ANALYSIS_COLUMN_CONTRACT)
        _validate_one(connection, FAILURE_TABLE, FAILURE_SAMPLE_COLUMN_CONTRACT)
    return {
        "tables": [ANALYSIS_TABLE, RECOMMENDATION_TABLE, FAILURE_TABLE],
        "physical_contract_verified": True,
        "business_unique_keys_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def _ddl_for_spec(spec: Mapping[str, Any]) -> str:
    column_type = str(spec["column_type"]).upper()
    parts = [column_type]
    if spec["character_set_name"]:
        parts.extend(("CHARACTER SET utf8mb4", f"COLLATE {EXPECTED_COLLATION}"))
    parts.append("NULL" if spec["is_nullable"] == "YES" else "NOT NULL")
    default = spec["column_default"]
    if default is not None:
        if default == "current_timestamp":
            parts.append("DEFAULT CURRENT_TIMESTAMP")
        elif column_type.startswith(("CHAR", "VARCHAR", "TEXT")):
            escaped = str(default).replace("'", "''")
            parts.append(f"DEFAULT '{escaped}'")
        else:
            parts.append(f"DEFAULT {default}")
    if "auto_increment" in spec["extra_contains"]:
        parts.append("AUTO_INCREMENT")
    if "on update current_timestamp" in spec["extra_contains"]:
        parts.append("ON UPDATE CURRENT_TIMESTAMP")
    return " ".join(parts)


def _normalize_empty_table(connection, table_name, expected, drift, metadata):
    if metadata is None:
        raise RuntimeError(f"required table is unavailable: {table_name}")
    if (
        metadata["engine"].lower() != EXPECTED_ENGINE.lower()
        or metadata["table_collation"].lower() != EXPECTED_COLLATION.lower()
    ):
        connection.execute(text(
            f"ALTER TABLE `{table_name}` ENGINE={EXPECTED_ENGINE}, "
            "DEFAULT CHARACTER SET utf8mb4 "
            f"COLLATE {EXPECTED_COLLATION}"
        ))
    for name in sorted(drift):
        connection.execute(text(
            f"ALTER TABLE `{table_name}` MODIFY COLUMN `{name}` "
            f"{_ddl_for_spec(expected[name])}"
        ))


def _add_index(connection, table_name, unique, columns, preferred_name):
    index_kind = "UNIQUE INDEX" if unique else "INDEX"
    columns_sql = ", ".join(f"`{column}`" for column in columns)
    connection.execute(text(
        f"ALTER TABLE `{table_name}` ADD {index_kind} "
        f"`{preferred_name}` ({columns_sql})"
    ))


def migrate_analysis_output_schema(engine) -> dict[str, Any]:
    """Run the fenced, evidence-backed forward migration for output tables."""
    added_columns: list[str] = []
    normalized_tables: list[str] = []
    added_indexes: list[str] = []
    duplicate_plan = _build_recommendation_duplicate_plan([])
    duplicate_evidence: dict[str, Any] = {
        "evidence_row_count": 0,
        "evidence_verified": True,
    }
    deleted_duplicate_count = 0
    physical_evidence: dict[str, Any] = {}
    physical_contexts: dict[str, dict[str, Any]] = {}
    resumed_physical_tables: set[str] = set()
    with engine.begin() as connection:
        ensure_evidence_table(connection)
        for table_name in (
            RECOMMENDATION_TABLE,
            ANALYSIS_TABLE,
            FAILURE_TABLE,
        ):
            pending = load_pending_physical_rewrite_plan(
                connection,
                recovery_version=PHYSICAL_RECOVERY_VERSION,
                source_table=table_name,
            )
            if pending is None:
                continue
            if (
                pending["business_key"] != {"table": table_name}
                or int(pending["record"]["source_row_id"]) != 0
                or pending["plan_payload"].get("table") != table_name
            ):
                raise RuntimeError(
                    f"{table_name} pending physical PLAN differs"
                )
            verify_pending_plan_content(connection, pending)
            physical_contexts[table_name] = pending
            resumed_physical_tables.add(table_name)

        # Reject every unrelated nonempty reinterpretation before deleting a
        # duplicate.  Missing additive columns are handled later and are not
        # part of this physical-safety decision.
        for table_name, expected, additions in (
            (
                RECOMMENDATION_TABLE,
                RECOMMENDATION_COLUMN_CONTRACT,
                RECOMMENDATION_ADDITIVE_COLUMNS,
            ),
            (ANALYSIS_TABLE, ANALYSIS_COLUMN_CONTRACT, ANALYSIS_ADDITIVE_COLUMNS),
        ):
            existing = _column_inventory(connection, table_name)
            if not existing:
                raise RuntimeError(f"required base table is unavailable: {table_name}")
            missing_base = sorted(set(expected) - set(additions) - set(existing))
            if missing_base:
                raise RuntimeError(
                    f"{table_name} required base columns are unavailable: {missing_base}"
                )
            metadata = _table_metadata(connection, table_name)
            if metadata is None:
                raise RuntimeError(f"required base table is unavailable: {table_name}")
            row_count = _row_count(connection, table_name)
            drift = {
                name: observed
                for name, observed in _column_drift(existing, expected).items()
                if not (name in additions and observed is None)
            }
            table_drift = (
                metadata["engine"].casefold() != EXPECTED_ENGINE.casefold()
                or metadata["table_collation"].casefold()
                != EXPECTED_COLLATION.casefold()
            )
            if row_count and (drift or table_drift):
                _assert_safe_nonempty_collation_rewrite(
                    table_name=table_name,
                    columns=existing,
                    metadata=metadata,
                    expected=expected,
                    drift=drift,
                )

        failure_columns = _column_inventory(connection, FAILURE_TABLE)
        if failure_columns:
            missing_failure = sorted(
                set(FAILURE_SAMPLE_COLUMN_CONTRACT) - set(failure_columns)
            )
            if missing_failure:
                raise RuntimeError(
                    f"{FAILURE_TABLE} columns are unavailable: {missing_failure}"
                )
            failure_metadata = _table_metadata(connection, FAILURE_TABLE)
            if failure_metadata is None:
                raise RuntimeError(f"required table is unavailable: {FAILURE_TABLE}")
            failure_rows = _row_count(connection, FAILURE_TABLE)
            failure_drift = _column_drift(
                failure_columns, FAILURE_SAMPLE_COLUMN_CONTRACT
            )
            failure_table_drift = (
                failure_metadata["engine"].casefold()
                != EXPECTED_ENGINE.casefold()
                or failure_metadata["table_collation"].casefold()
                != EXPECTED_COLLATION.casefold()
            )
            if failure_rows and (failure_drift or failure_table_drift):
                _assert_safe_nonempty_collation_rewrite(
                    table_name=FAILURE_TABLE,
                    columns=failure_columns,
                    metadata=failure_metadata,
                    expected=FAILURE_SAMPLE_COLUMN_CONTRACT,
                    drift=failure_drift,
                )

        inbound_foreign_keys = _recommendation_inbound_foreign_keys(connection)
        if inbound_foreign_keys:
            raise RuntimeError(
                "recommendation duplicate recovery has inbound foreign keys"
            )
        if _duplicate_key(
            connection, ANALYSIS_TABLE, ("stock_code", "analysis_date")
        ):
            raise RuntimeError(
                "analysis business key contains duplicates; no automatic "
                "canonicalization contract exists"
            )
        duplicate_plan = _recommendation_duplicate_plan(
            connection, for_update=True
        )
        duplicate_evidence = _persist_duplicate_plan(
            connection, duplicate_plan
        )
        deleted_duplicate_count = _apply_duplicate_plan(
            connection, duplicate_plan
        )
        if _duplicate_key(
            connection, RECOMMENDATION_TABLE, ("stock_code", "pick_date")
        ):
            raise RuntimeError(
                "recommendation business key remains duplicated after recovery"
            )

        for table_name, expected, additions in (
            (
                RECOMMENDATION_TABLE,
                RECOMMENDATION_COLUMN_CONTRACT,
                RECOMMENDATION_ADDITIVE_COLUMNS,
            ),
            (ANALYSIS_TABLE, ANALYSIS_COLUMN_CONTRACT, ANALYSIS_ADDITIVE_COLUMNS),
        ):
            existing = _column_inventory(connection, table_name)
            for column, ddl in additions.items():
                if column in existing:
                    continue
                connection.execute(text(
                    f"ALTER TABLE `{table_name}` ADD COLUMN `{column}` {ddl}"
                ))
                added_columns.append(f"{table_name}.{column}")

        connection.execute(text(
            f"CREATE TABLE IF NOT EXISTS {FAILURE_TABLE} ("
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            "stock_code VARCHAR(10) NOT NULL, short_name VARCHAR(40) DEFAULT '', "
            "strategy_profile VARCHAR(20) DEFAULT '', signal_date DATE NULL, "
            "result VARCHAR(20) DEFAULT 'fail', fail_tag VARCHAR(40) DEFAULT '', "
            "fail_reason VARCHAR(500) DEFAULT '', return_pct DECIMAL(8,4) NULL, "
            "created_at DATETIME NULL, "
            "KEY idx_stock_date (stock_code, signal_date), "
            "KEY idx_fail_tag (fail_tag)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
            "COLLATE=utf8mb4_unicode_ci"
        ))

        contracts = (
            (RECOMMENDATION_TABLE, RECOMMENDATION_COLUMN_CONTRACT),
            (ANALYSIS_TABLE, ANALYSIS_COLUMN_CONTRACT),
            (FAILURE_TABLE, FAILURE_SAMPLE_COLUMN_CONTRACT),
        )
        for table_name, expected in contracts:
            columns = _column_inventory(connection, table_name)
            metadata = _table_metadata(connection, table_name)
            missing = sorted(set(expected) - set(columns))
            if missing:
                raise RuntimeError(f"{table_name} columns are unavailable: {missing}")
            drift = _column_drift(columns, expected)
            row_count = _row_count(connection, table_name)
            table_drift = metadata is None or (
                metadata["engine"].lower() != EXPECTED_ENGINE.lower()
                or metadata["table_collation"].lower()
                != EXPECTED_COLLATION.lower()
            )
            if (drift or table_drift) and row_count:
                if metadata is None:
                    raise RuntimeError(f"required table is unavailable: {table_name}")
                _assert_safe_nonempty_collation_rewrite(
                    table_name=table_name,
                    columns=columns,
                    metadata=metadata,
                    expected=expected,
                    drift=drift,
                )
                before = table_content_fingerprint(connection, table_name)
                manifest = {
                    "before_fingerprint": before,
                    "fingerprint_columns": list(columns),
                    "column_drift": sorted(drift),
                    "engine": metadata["engine"],
                    "table_collation": metadata["table_collation"],
                }
                physical_plan_payload = {"table": table_name, **manifest}
                physical_plan_hash = plan_sha256(
                    recovery_version=PHYSICAL_RECOVERY_VERSION,
                    payload=physical_plan_payload,
                )
                context = physical_contexts.get(table_name)
                if context is None:
                    plan_record = make_evidence_record(
                        recovery_version=PHYSICAL_RECOVERY_VERSION,
                        source_table=table_name,
                        source_row_id=0,
                        action="PHYSICAL_REWRITE_PLAN",
                        business_key={"table": table_name},
                        source_row=manifest,
                        plan_payload=physical_plan_payload,
                        plan_hash=physical_plan_hash,
                    )
                    persist_and_verify_evidence(connection, [plan_record])
                    context = {
                        "record": plan_record,
                        "business_key": {"table": table_name},
                        "source_row": manifest,
                        "plan_payload": physical_plan_payload,
                        "plan_sha256": physical_plan_hash,
                    }
                    physical_contexts[table_name] = context
                connection.execute(text(
                    f"ALTER TABLE `{table_name}` ENGINE={EXPECTED_ENGINE}, "
                    "CONVERT TO CHARACTER SET utf8mb4 "
                    f"COLLATE {EXPECTED_COLLATION}"
                ))
                remaining = _column_drift(
                    _column_inventory(connection, table_name), expected
                )
                if remaining:
                    raise RuntimeError(
                        f"{table_name} physical normalization left column drift: "
                        f"{sorted(remaining)}"
                    )
                normalized_tables.append(table_name)
            elif drift or table_drift:
                _normalize_empty_table(
                    connection, table_name, expected, drift, metadata
                )
                normalized_tables.append(table_name)

        if _duplicate_key(
            connection, ANALYSIS_TABLE, ("stock_code", "analysis_date")
        ):
            raise RuntimeError("analysis business key contains duplicates")

        index_specs = {
            RECOMMENDATION_TABLE: (
                (True, ("stock_code", "pick_date"), "uk_code_date"),
                (False, ("pick_date", "stock_code"), "idx_pick_date_code"),
            ),
            ANALYSIS_TABLE: (
                (True, ("stock_code", "analysis_date"), "uk_analysis_code_date"),
                (False, ("analysis_date", "stock_code"), "idx_analysis_date_code"),
            ),
            FAILURE_TABLE: (
                (False, ("stock_code", "signal_date"), "idx_stock_date"),
                (False, ("fail_tag",), "idx_fail_tag"),
            ),
        }
        for table_name, specs in index_specs.items():
            shapes, names = _index_inventory(connection, table_name)
            if (True, ("id",)) not in shapes:
                raise RuntimeError(f"{table_name} primary id index differs")
            for unique, columns, preferred in specs:
                shape = (unique, columns)
                if _shape_available(shapes, shape):
                    continue
                name = preferred
                suffix = 2
                while name in names:
                    name = f"{preferred}_{suffix}"
                    suffix += 1
                _add_index(connection, table_name, unique, columns, name)
                names.add(name)
                shapes.add(shape)
                added_indexes.append(f"{table_name}.{name}")

        contract_by_table = {
            RECOMMENDATION_TABLE: RECOMMENDATION_COLUMN_CONTRACT,
            ANALYSIS_TABLE: ANALYSIS_COLUMN_CONTRACT,
            FAILURE_TABLE: FAILURE_SAMPLE_COLUMN_CONTRACT,
        }
        for table_name, context in physical_contexts.items():
            _validate_one(connection, table_name, contract_by_table[table_name])
            manifest = context["source_row"]
            before = manifest["before_fingerprint"]
            after = table_content_fingerprint(
                connection,
                table_name,
                columns=manifest.get("fingerprint_columns") or None,
            )
            if after != before:
                raise RuntimeError(
                    f"{table_name} content fingerprint changed during "
                    "physical normalization"
                )
            persist_and_verify_evidence(connection, [make_evidence_record(
                recovery_version=PHYSICAL_RECOVERY_VERSION,
                source_table=table_name,
                source_row_id=0,
                action="PHYSICAL_REWRITE_VERIFIED",
                business_key={"table": table_name},
                source_row={
                    "before_fingerprint": before,
                    "after_fingerprint": after,
                    "fingerprint_columns": manifest.get("fingerprint_columns"),
                },
                plan_payload=context["plan_payload"],
                plan_hash=str(context["plan_sha256"]),
            )])
            physical_evidence[table_name] = {
                "plan_sha256": str(context["plan_sha256"]),
                "before_fingerprint": before,
                "after_fingerprint": after,
                "content_verified": True,
                "resumed_pending_plan": table_name in resumed_physical_tables,
            }

    validated = validate_analysis_output_schema(engine)
    return {
        **validated,
        "status": "ok",
        "added_columns": sorted(added_columns),
        "normalized_tables": sorted(normalized_tables),
        "added_indexes": sorted(added_indexes),
        "duplicate_recovery_plan": _public_duplicate_plan(duplicate_plan),
        "duplicate_recovery_evidence": duplicate_evidence,
        "deleted_duplicate_count": deleted_duplicate_count,
        "physical_rewrite_evidence": physical_evidence,
    }


__all__ = [
    "ANALYSIS_ADDITIVE_COLUMNS", "ANALYSIS_COLUMN_CONTRACT",
    "ANALYSIS_REQUIRED_COLUMNS", "EXPECTED_COLLATION", "EXPECTED_ENGINE",
    "FAILURE_SAMPLE_COLUMN_CONTRACT", "FAILURE_SAMPLE_REQUIRED_COLUMNS",
    "RECOMMENDATION_ADDITIVE_COLUMNS", "RECOMMENDATION_COLUMN_CONTRACT",
    "RECOMMENDATION_REQUIRED_COLUMNS", "migrate_analysis_output_schema",
    "plan_analysis_output_recovery",
    "validate_ai_failure_sample_schema", "validate_analysis_output_schema",
]
