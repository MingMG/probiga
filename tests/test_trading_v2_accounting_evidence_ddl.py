from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from server.db import migrations_v2
from server.db.accounting_evidence_ddl import (
    ACCOUNTING_EVIDENCE_ALL_DDL_PROPOSAL,
    ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL,
    ACCOUNTING_EVIDENCE_DDL_IS_REGISTERED,
    ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL,
    ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL,
    ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL,
    FINALIZED_ACCOUNTING_OUTCOME_READ_SQL,
)


def _sql(value: str) -> str:
    return " ".join(value.lower().split())


def test_accounting_evidence_ddl_is_registered_as_exact_forward_migration() -> None:
    assert ACCOUNTING_EVIDENCE_DDL_IS_REGISTERED is True
    assert ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL.startswith("20260803_")
    assert ACCOUNTING_EVIDENCE_ALL_DDL_PROPOSAL == (
        ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL
        + ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL
        + ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL
    )

    registered_versions = {str(item["version"]) for item in migrations_v2.MIGRATIONS}
    registered_sql = _sql(
        "\n".join(
            statement
            for migration in migrations_v2.MIGRATIONS
            for statement in migration["statements"]
        )
    )
    assert ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL in registered_versions
    assert "create table if not exists st_fill_accounting_outcome_v2" in registered_sql
    assert "create table if not exists st_lot_transition_evidence_v2" in registered_sql
    assert (
        "create table if not exists "
        "st_fill_accounting_outcome_finalization_v2"
    ) in registered_sql
    migration = next(
        item
        for item in migrations_v2.MIGRATIONS
        if item["version"] == ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL
    )
    assert tuple(migration["statements"]) == ACCOUNTING_EVIDENCE_ALL_DDL_PROPOSAL


def test_table_proposal_orders_outcome_before_dependent_lot_effects() -> None:
    assert len(ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL) == 3
    outcome_sql, lot_sql, finalization_sql = map(
        _sql,
        ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL,
    )

    assert outcome_sql.startswith(
        "create table if not exists st_fill_accounting_outcome_v2"
    )
    assert lot_sql.startswith(
        "create table if not exists st_lot_transition_evidence_v2"
    )
    assert "references st_fill_accounting_outcome_v2" in lot_sql
    assert finalization_sql.startswith(
        "create table if not exists "
        "st_fill_accounting_outcome_finalization_v2"
    )
    assert "references st_fill_accounting_outcome_v2" in finalization_sql
    assert all(
        "engine=innodb row_format=dynamic default charset=utf8mb4" in item
        for item in (outcome_sql, lot_sql, finalization_sql)
    )


def test_outcome_ddl_binds_all_canonical_execution_evidence_and_cash() -> None:
    outcome_sql = _sql(ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL[0])

    for column in (
        "fill_execution_evidence_id char(64) not null",
        "fill_execution_evidence_hash char(64) not null",
        "cash_binding_id char(64) not null",
        "cash_binding_hash char(64) not null",
        "cash_event_id varchar(64) not null",
        "order_transition_id char(64) not null",
        "order_transition_hash char(64) not null",
        "account_cash_before decimal(20,2) not null",
        "account_cash_after decimal(20,2) not null",
        "lot_effect_root_hash char(64) not null",
        "lot_effects_hash char(64) not null",
        "provenance_hash char(64) not null",
    ):
        assert column in outcome_sql

    assert (
        "foreign key ( fill_execution_evidence_id, fill_id, "
        "fill_execution_evidence_hash ) references st_fill_execution_evidence_v2 "
        "( fill_execution_evidence_id, fill_id, evidence_hash )"
    ) in outcome_sql
    assert (
        "foreign key ( cash_binding_id, cash_event_id, cash_binding_hash ) "
        "references st_cash_event_binding_v2 "
        "( cash_binding_id, cash_event_id, binding_hash )"
    ) in outcome_sql
    assert (
        "foreign key (order_transition_id, order_transition_hash) "
        "references st_order_transition_v2 ( transition_id, transition_hash )"
    ) in outcome_sql
    assert "unique key uk_fill_accounting_outcome_v2_fill (fill_id)" in outcome_sql
    assert "check (accounting_outcome_id = outcome_hash)" in outcome_sql
    assert "check (lot_effect_count >= 1)" in outcome_sql
    assert "check (total_effect_quantity >= 1)" in outcome_sql
    assert "check (authority_status = 'content_hash_only')" in outcome_sql
    assert "'start_after_unknown', 'complete_from_declared_origin'" in outcome_sql


def test_lot_transition_ddl_has_canonical_snapshots_and_both_chains() -> None:
    lot_sql = _sql(ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL[1])

    for column in (
        "effect_sequence bigint not null",
        "lot_transition_sequence bigint not null",
        "previous_effect_id char(64) default null",
        "previous_effect_hash char(64) default null",
        "previous_lot_transition_id char(64) default null",
        "previous_lot_transition_hash char(64) default null",
        "before_lot_json longtext default null",
        "before_lot_hash char(64) default null",
        "after_lot_json longtext not null",
        "after_lot_hash char(64) not null",
    ):
        assert column in lot_sql

    assert (
        "unique key uk_lot_transition_evidence_v2_outcome_sequence "
        "(accounting_outcome_id, effect_sequence)"
    ) in lot_sql
    assert (
        "unique key uk_lot_transition_evidence_v2_fill_lot (fill_id, lot_id)"
    ) in lot_sql
    assert (
        "unique key uk_lot_transition_evidence_v2_lot_sequence "
        "(lot_id, lot_transition_sequence)"
    ) in lot_sql
    assert lot_sql.count("references st_lot_transition_evidence_v2") == 2
    assert (
        "foreign key (accounting_outcome_id, fill_id) references "
        "st_fill_accounting_outcome_v2 ( accounting_outcome_id, fill_id )"
    ) in lot_sql
    assert "foreign key (lot_id) references st_position_lot_v2 (lot_id)" in lot_sql
    assert "check (json_valid(after_lot_json))" in lot_sql
    assert "check (before_lot_json is null or json_valid(before_lot_json))" in lot_sql
    assert "'buy_create', 'sell_fifo_consume'" in lot_sql
    assert (
        "history_origin <> 'complete_from_declared_origin' or effect_kind <> "
        "'sell_fifo_consume' or previous_lot_transition_id is not null"
    ) in lot_sql


def test_append_only_guard_proposal_covers_update_and_delete_for_both_tables() -> None:
    assert len(ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL) == 12
    combined = _sql("\n".join(ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL))

    expected = {
        "trg_fill_accounting_outcome_v2_guard_bu": (
            "before update on st_fill_accounting_outcome_v2"
        ),
        "trg_fill_accounting_outcome_v2_guard_bd": (
            "before delete on st_fill_accounting_outcome_v2"
        ),
        "trg_lot_transition_evidence_v2_guard_bu": (
            "before update on st_lot_transition_evidence_v2"
        ),
        "trg_lot_transition_evidence_v2_guard_bd": (
            "before delete on st_lot_transition_evidence_v2"
        ),
        "trg_fill_accounting_finalization_v2_guard_bu": (
            "before update on st_fill_accounting_outcome_finalization_v2"
        ),
        "trg_fill_accounting_finalization_v2_guard_bd": (
            "before delete on st_fill_accounting_outcome_finalization_v2"
        ),
    }
    for trigger_name, action in expected.items():
        assert f"drop trigger if exists {trigger_name}" in combined
        assert f"create trigger {trigger_name} {action}" in combined

    assert combined.count("signal sqlstate '45000'") == 6
    assert combined.count("for each row") == 6
    assert "append only" in combined
    assert "cannot be deleted" in combined
    assert "delimiter" not in combined

    trigger_names = re.findall(r"create trigger ([a-z0-9_]+)", combined)
    assert len(trigger_names) == 6
    assert all(len(name) <= 64 for name in trigger_names)


def test_mysql57_insert_guards_fail_closed_without_check_enforcement() -> None:
    assert len(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL) == 6
    outcome_guard = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[1])
    lot_guard = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[3])

    assert ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[0].strip().endswith(
        "trg_fill_accounting_outcome_v2_guard_bi"
    )
    assert outcome_guard.startswith(
        "create trigger trg_fill_accounting_outcome_v2_guard_bi "
        "before insert on st_fill_accounting_outcome_v2"
    )
    assert ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[2].strip().endswith(
        "trg_lot_transition_evidence_v2_guard_bi"
    )
    assert lot_guard.startswith(
        "create trigger trg_lot_transition_evidence_v2_guard_bi "
        "before insert on st_lot_transition_evidence_v2"
    )
    assert outcome_guard.count("signal sqlstate '45000'") >= 4
    assert lot_guard.count("signal sqlstate '45000'") >= 12
    assert "check (" not in outcome_guard
    assert "check (" not in lot_guard


def test_outcome_insert_guard_binds_canonical_and_nested_facts() -> None:
    sql = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[1])

    for table in (
        "st_fill_execution_evidence_v2 fe",
        "st_fill_v2 f",
        "st_cash_event_binding_v2 cb",
        "st_cash_ledger_v2 cl",
        "st_order_transition_v2 ot",
        "st_order_v2 o",
        "st_trade_account_v2 a",
    ):
        assert table in sql
    for binding in (
        "fe.evidence_hash = binary new.fill_execution_evidence_hash",
        "cb.binding_hash = binary new.cash_binding_hash",
        "ot.transition_hash = binary new.order_transition_hash",
        "ot.transition_kind = binary 'fill_applied'",
        "cb.cash_event_type = binary concat(new.side, '_fill')",
        "new.account_cash_before + cl.amount = new.account_cash_after",
        "a.cash_balance = new.account_cash_after",
        "o.filled_quantity = ot.next_filled_quantity",
    ):
        assert binding in sql
    assert "accounting outcome nested evidence mismatch" in sql
    assert "char_length(new.accounting_outcome_id) <> 64" in sql
    assert "new.accounting_outcome_id regexp '[^0-9a-f]'" in sql
    assert "binary new.accounting_outcome_id <> binary new.outcome_hash" in sql
    assert "new.authority_status <> 'content_hash_only'" in sql
    assert (
        "'start_after_unknown', 'complete_from_declared_origin'"
    ) in sql


def test_lot_insert_guard_enforces_both_predecessors_and_buy_sell_shape() -> None:
    sql = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[3])

    assert sql.count("create trigger trg_lot_transition_evidence_v2_guard_bi") == 1
    assert "previous_effect.effect_sequence + 1 = new.effect_sequence" in sql
    assert (
        "previous_lot.lot_transition_sequence + 1 = "
        "new.lot_transition_sequence"
    ) in sql
    assert "previous_lot.after_lot_hash = binary new.before_lot_hash" in sql
    assert "invalid buy lot creation shape" in sql
    assert "invalid sell lot consumption shape" in sql
    assert "sell lot immutable fields changed" in sql
    assert "sell consumed an unsettled lot" in sql
    assert "sell lot effects violate fifo order" in sql
    assert "sell skipped an earlier eligible fifo lot" in sql
    assert "sell lot effects do not reconcile to fill" in sql
    assert "json_valid(new.after_lot_json) <> 1" in sql
    assert "st_position_lot_v2 lot" in sql
    assert "new.effect_sequence < ao.lot_effect_count" in sql
    assert "binary ao.provenance_hash = binary new.provenance_hash" in sql
    assert "new.history_origin = 'complete_from_declared_origin'" in sql
    assert sql.count("previous_effect.history_origin = new.history_origin") == 1


def test_lot_insert_guard_compares_full_canonical_times_and_quantity_bounds() -> None:
    sql = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[3])

    assert (
        "json_unquote(json_extract( new.after_lot_json, '$.created_at')) "
        "= date_format( convert_tz(lot.created_at, '+08:00', '+00:00'), "
        "'%y-%m-%dt%h:%i:%s.%f+00:00')"
    ) in sql
    assert (
        "json_unquote(json_extract( new.after_lot_json, '$.closed_at')) "
        "= date_format( convert_tz(lot.closed_at, '+08:00', '+00:00'), "
        "'%y-%m-%dt%h:%i:%s.%f+00:00')"
    ) in sql
    assert "from st_position_lot_v2 lot where binary lot.lot_id" in sql
    assert "<> new.occurred_at" in sql
    assert (
        "new.before_lot_json, '$.remaining_quantity') "
        "< new.consumed_quantity"
    ) in sql
    assert (
        "new.before_lot_json, '$.remaining_quantity') > json_extract( "
        "new.before_lot_json, '$.original_quantity')"
    ) in sql
    assert (
        "new.after_lot_json, '$.remaining_quantity') < 0"
    ) in sql
    assert (
        "new.after_lot_json, '$.remaining_quantity') > json_extract( "
        "new.after_lot_json, '$.original_quantity')"
    ) in sql


def test_first_sell_effect_cannot_skip_an_earlier_eligible_canonical_lot() -> None:
    sql = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[3])

    start = sql.index("from st_position_lot_v2 earlier_lot")
    end = sql.index("sell skipped an earlier eligible fifo lot")
    probe = sql[start:end]
    assert "earlier_lot.account_id = binary ao.account_id" in probe
    assert "earlier_lot.stock_code = binary ao.stock_code" in probe
    assert "earlier_lot.settlement_date <= cal.trade_date" in probe
    assert "earlier_lot.remaining_quantity > 0" in probe
    assert "earlier_lot.remaining_quantity = 0" in probe
    assert "earlier_lot.closed_at = new.occurred_at" in probe
    assert "not exists ( select 1 from st_lot_transition_evidence_v2" in probe
    assert "recorded_effect.lot_id = binary earlier_lot.lot_id" in probe
    assert "recorded_effect.effect_sequence < new.effect_sequence" in probe
    assert "earlier_lot.opened_trade_date" in probe
    assert "binary earlier_lot.lot_id < binary new.lot_id" in probe
    assert "if predecessor_count <> 0 then" in probe


def test_lot_insert_guard_requires_exact_snapshot_keyset_and_json_types() -> None:
    sql = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[3])
    expected_paths = (
        "lot_id",
        "account_id",
        "stock_code",
        "theme_code",
        "strategy_version",
        "opened_fill_id",
        "opened_trade_date",
        "settlement_date",
        "original_quantity",
        "remaining_quantity",
        "cost_price",
        "allocated_buy_fee",
        "position_state",
        "approved_target_quantity",
        "add_count",
        "initial_stop",
        "protective_stop",
        "invalidation_condition",
        "version",
        "created_at",
        "closed_at",
    )

    assert "json_length(new.after_lot_json) <> 21" in sql
    assert "json_length(new.before_lot_json) <> 21" in sql
    after_shape = sql[
        sql.index("json_length(new.after_lot_json) <> 21") :
        sql.index("noncanonical after lot snapshot")
    ]
    before_shape = sql[
        sql.index("json_length(new.before_lot_json) <> 21") :
        sql.index("noncanonical before lot snapshot")
    ]
    assert "json_contains_path( new.after_lot_json, 'all'" in after_shape
    assert "json_contains_path( new.before_lot_json, 'all'" in before_shape
    for path in expected_paths:
        assert f"'$.{path}'" in after_shape
        assert f"'$.{path}'" in before_shape

    for payload in ("after_lot_json", "before_lot_json"):
        for integer_field in (
            "original_quantity",
            "remaining_quantity",
            "approved_target_quantity",
            "add_count",
            "version",
        ):
            assert (
                f"new.{payload}, '$.{integer_field}')) <> 'integer'"
            ) in sql
        for decimal_field in (
            "cost_price",
            "allocated_buy_fee",
            "initial_stop",
            "protective_stop",
        ):
            assert f"new.{payload}, '$.{decimal_field}')) <> 'string'" in sql

    assert (
        "new.before_lot_json, '$.closed_at')) <> 'null'"
    ) in before_shape
    assert "not regexp '^-?[0-9]+[.][0-9]{6}$'" in after_shape
    assert "not regexp '^-?[0-9]+[.][0-9]{2}$'" in after_shape
    assert "not regexp '^-?[0-9]+[.][0-9]{6}$'" in before_shape
    assert "not regexp '^-?[0-9]+[.][0-9]{2}$'" in before_shape
    assert sql.index("noncanonical before lot snapshot") < sql.index(
        "invalid sell lot consumption shape"
    )


def test_all_trigger_names_are_unique_and_mysql_identifier_safe() -> None:
    guards = (
        ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL
        + ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL
    )
    combined = _sql("\n".join(guards))
    created = re.findall(r"create trigger ([a-z0-9_]+)", combined)
    dropped = re.findall(r"drop trigger if exists ([a-z0-9_]+)", combined)

    assert len(created) == 9
    assert len(created) == len(set(created))
    assert sorted(created) == sorted(dropped)
    assert all(len(name) <= 64 for name in created)


def test_finalization_table_is_a_single_strong_marker_per_outcome() -> None:
    outcome_sql = _sql(ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL[0])
    finalization_sql = _sql(ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL[2])

    assert (
        "unique key uk_fill_accounting_outcome_v2_finalization_binding "
        "(accounting_outcome_id, fill_id, outcome_hash)"
    ) in outcome_sql
    for column in (
        "finalization_id char(64) primary key",
        "accounting_outcome_id char(64) not null",
        "outcome_hash char(64) not null",
        "fill_execution_evidence_id char(64) not null",
        "fill_execution_evidence_hash char(64) not null",
        "lot_effect_root_hash char(64) not null",
        "lot_effects_hash char(64) not null",
        "effect_hashes_json longtext not null",
        "lot_effect_count bigint not null",
        "total_effect_quantity bigint not null",
        "finalization_status varchar(16) not null",
        "provenance_hash char(64) not null",
        "finalized_at datetime not null",
        "finalization_hash char(64) not null",
    ):
        assert column in finalization_sql
    assert (
        "unique key uk_fill_accounting_finalization_v2_outcome "
        "(accounting_outcome_id)"
    ) in finalization_sql
    assert (
        "foreign key (accounting_outcome_id, fill_id, outcome_hash) "
        "references st_fill_accounting_outcome_v2 "
        "( accounting_outcome_id, fill_id, outcome_hash )"
    ) in finalization_sql
    assert (
        "foreign key ( fill_execution_evidence_id, fill_id, "
        "fill_execution_evidence_hash ) references "
        "st_fill_execution_evidence_v2 "
        "( fill_execution_evidence_id, fill_id, evidence_hash )"
    ) in finalization_sql
    assert "check (finalization_status = 'final')" in finalization_sql
    assert "check (finalization_id = finalization_hash)" in finalization_sql


def test_finalization_insert_guard_revalidates_parent_complete_child_set_and_chains() -> None:
    guard = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[5])

    assert guard.startswith(
        "create trigger trg_fill_accounting_finalization_v2_guard_bi "
        "before insert on st_fill_accounting_outcome_finalization_v2"
    )
    assert "new.finalization_status <> 'final'" in guard
    assert "accounting finalization parent mismatch" in guard
    assert "select count(*) into total_child_count" in guard
    assert "select count(*) into matching_child_count" in guard
    assert "total_child_count <> new.lot_effect_count" in guard
    assert "matching_child_count <> new.lot_effect_count" in guard
    assert "effect.effect_sequence >= 0" in guard
    assert "effect.effect_sequence < new.lot_effect_count" in guard
    assert "while child_sequence < new.lot_effect_count do" in guard
    assert "effect.effect_sequence = child_sequence" in guard
    assert "child_previous_id <> binary previous_child_id" in guard
    assert "previous_lot.lot_transition_sequence + 1" in guard
    assert "previous_lot.after_lot_hash = binary child_before_hash" in guard
    assert guard.count(
        "previous_lot.effect_hash = binary child_previous_lot_hash"
    ) == 1
    assert (
        "child_previous_lot_hash = binary child_previous_lot_hash"
    ) not in guard
    assert "finalized per-fill effect chain mismatch" in guard
    assert "finalized per-lot chain mismatch" in guard
    assert "finalized buy child mismatch" in guard
    assert "finalized sell child quantity mismatch" in guard
    assert "finalized sell fifo order mismatch" in guard
    assert "finalized sell skipped fifo lot" in guard
    assert "observed_sell_quantity <> new.total_effect_quantity" in guard


def test_finalization_hash_array_is_sequence_exact_and_recomputed_without_group_concat() -> None:
    guard = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[5])

    assert "json_type(json_extract( new.effect_hashes_json" in guard
    assert "concat('$[', child_sequence, ']')" in guard
    assert "<> binary child_hash" in guard
    assert "effect hash array is not canonical" in guard
    assert "binary new.effect_hashes_json <> binary canonical_effect_hashes" in guard
    assert "group_concat" not in guard
    assert "lower(sha2(canonical_preimage, 256))" in guard
    assert "finalized lot effect list hash mismatch" in guard

    root_hash = "a" * 64
    effect_hashes = ("b" * 64, "c" * 64)
    domain_preimage = json.dumps(
        {
            "namespace": "trading-v2.lot-accounting-effect-list.v1",
            "payload": {
                "root_hash": root_hash,
                "effect_hashes": effect_hashes,
            },
        },
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    sql_preimage = (
        '{"namespace":"trading-v2.lot-accounting-effect-list.v1",'
        '"payload":{"effect_hashes":["'
        + effect_hashes[0]
        + '","'
        + effect_hashes[1]
        + '"],"root_hash":"'
        + root_hash
        + '"}}'
    )
    assert sql_preimage == domain_preimage
    assert hashlib.sha256(sql_preimage.encode("utf-8")).hexdigest() == (
        hashlib.sha256(domain_preimage.encode("utf-8")).hexdigest()
    )
    assert (
        "trading-v2.lot-accounting-effect-list.v1"
    ) in ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[5]


def test_finalization_content_hash_uses_exact_domain_canonical_preimage() -> None:
    guard = _sql(ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[5])
    raw_guard = ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL[5]

    assert "trading-v2.fill-accounting-finalization.v1" in guard
    assert (
        "convert_tz(new.finalized_at, '+08:00', '+00:00')"
    ) in guard
    assert "'%y-%m-%dt%h:%i:%s.%f+00:00'" in guard
    assert "json_quote(new.fill_id)" in guard
    assert "json_quote(canonical_finalized_at)" in guard
    assert "cast(new.lot_effect_count as char)" in guard
    assert "cast(new.total_effect_quantity as char)" in guard
    assert "sha2(canonical_finalization_preimage, 256)" in guard
    assert (
        "recomputed_finalization_hash <> binary new.finalization_hash"
    ) in guard
    assert "recomputed_finalization_hash <> binary new.finalization_id" in guard
    assert "accounting finalization content hash mismatch" in guard

    ordered_fields = (
        '"accounting_outcome_id"',
        '"effect_hashes"',
        '"fill_execution_evidence_hash"',
        '"fill_execution_evidence_id"',
        '"fill_id"',
        '"finalization_status"',
        '"finalized_at"',
        '"lot_effect_count"',
        '"lot_effect_root_hash"',
        '"lot_effects_hash"',
        '"outcome_hash"',
        '"provenance_hash"',
        '"total_effect_quantity"',
    )
    preimage_sql = raw_guard[raw_guard.index(
        "trading-v2.fill-accounting-finalization.v1"
    ) : raw_guard.index("SET recomputed_finalization_hash")]
    offsets = [preimage_sql.index(item) for item in ordered_fields]
    assert offsets == sorted(offsets)

    finalized = datetime(
        2026,
        8,
        3,
        15,
        4,
        5,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(timezone.utc).isoformat(timespec="microseconds")
    effects = ("b" * 64, "c" * 64)
    payload = {
        "accounting_outcome_id": "a" * 64,
        "outcome_hash": "a" * 64,
        "fill_id": "FILL:example",
        "fill_execution_evidence_id": "d" * 64,
        "fill_execution_evidence_hash": "e" * 64,
        "lot_effect_root_hash": "f" * 64,
        "lot_effects_hash": "1" * 64,
        "effect_hashes": effects,
        "lot_effect_count": 2,
        "total_effect_quantity": 100,
        "finalization_status": "FINAL",
        "finalized_at": finalized,
        "provenance_hash": "2" * 64,
    }
    domain_preimage = json.dumps(
        {
            "namespace": "trading-v2.fill-accounting-finalization.v1",
            "payload": payload,
        },
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    sql_ordered_preimage = (
        '{"namespace":"trading-v2.fill-accounting-finalization.v1",'
        '"payload":{"accounting_outcome_id":'
        + json.dumps(payload["accounting_outcome_id"])
        + ',"effect_hashes":'
        + json.dumps(list(effects), separators=(",", ":"))
        + ',"fill_execution_evidence_hash":'
        + json.dumps(payload["fill_execution_evidence_hash"])
        + ',"fill_execution_evidence_id":'
        + json.dumps(payload["fill_execution_evidence_id"])
        + ',"fill_id":'
        + json.dumps(payload["fill_id"])
        + ',"finalization_status":'
        + json.dumps(payload["finalization_status"])
        + ',"finalized_at":'
        + json.dumps(payload["finalized_at"])
        + ',"lot_effect_count":2'
        + ',"lot_effect_root_hash":'
        + json.dumps(payload["lot_effect_root_hash"])
        + ',"lot_effects_hash":'
        + json.dumps(payload["lot_effects_hash"])
        + ',"outcome_hash":'
        + json.dumps(payload["outcome_hash"])
        + ',"provenance_hash":'
        + json.dumps(payload["provenance_hash"])
        + ',"total_effect_quantity":100}}'
    )
    assert sql_ordered_preimage == domain_preimage
    assert hashlib.sha256(sql_ordered_preimage.encode()).hexdigest() == (
        hashlib.sha256(domain_preimage.encode()).hexdigest()
    )


def test_finalized_read_sql_excludes_unmarked_or_nonfinal_outcomes() -> None:
    read_sql = _sql(FINALIZED_ACCOUNTING_OUTCOME_READ_SQL)

    assert read_sql.startswith("select outcome.* from st_fill_accounting_outcome_v2")
    assert (
        "inner join st_fill_accounting_outcome_finalization_v2 finalization"
    ) in read_sql
    assert (
        "finalization.accounting_outcome_id = binary outcome.accounting_outcome_id"
    ) in read_sql
    assert "finalization.outcome_hash = binary outcome.outcome_hash" in read_sql
    assert (
        "finalization.lot_effects_hash = binary outcome.lot_effects_hash"
    ) in read_sql
    assert (
        "where binary finalization.finalization_status = binary 'final'"
    ) in read_sql
