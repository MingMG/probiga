from __future__ import annotations

import pytest

from server.engine import strategy_funding_checkpoint as funding


@pytest.mark.parametrize(
    ("declared", "mysql84_check_clause"),
    (
        (
            "opening_cash_cny >= 0 AND closing_cash_cny >= 0",
            "((`opening_cash_cny` >= 0) and "
            "(`closing_cash_cny` >= 0))",
        ),
        (
            "(replay_mode = 'FULL_BOOTSTRAP' "
            "AND replay_session_count >= 1) OR "
            "(replay_mode = 'BOUNDED_INCREMENTAL' "
            "AND replay_session_count BETWEEN 1 AND 370)",
            r"(((`replay_mode` = _utf8mb4\'FULL_BOOTSTRAP\') "
            r"and (`replay_session_count` >= 1)) or "
            r"((`replay_mode` = _utf8mb4\'BOUNDED_INCREMENTAL\') "
            r"and (`replay_session_count` between 1 and 370)))",
        ),
    ),
)
def test_mysql84_check_metadata_normalizes_to_declared_contract(
    declared,
    mysql84_check_clause,
):
    assert funding._normalize_check(mysql84_check_clause) == (
        funding._normalize_check(declared)
    )


def test_mysql84_trigger_metadata_normalizes_sqlstate_value_only_outside_literals():
    declared = (
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
        "'strategy funding checkpoint is append only'; END"
    )
    mysql84_action_statement = (
        "BEGIN SIGNAL SQLSTATE VALUE '45000' SET MESSAGE_TEXT = "
        "'strategy funding checkpoint is append only'; END"
    )

    assert funding._normalize_trigger_body(mysql84_action_statement) == (
        funding._normalize_trigger_body(declared)
    )

    literal = "BEGIN SET @message='Keep SQLSTATE VALUE exactly'; END"
    assert "'Keep SQLSTATE VALUE exactly'" in (
        funding._normalize_trigger_body(literal)
    )


def test_mysql84_no_action_fk_metadata_matches_restrict_contract():
    assert funding._normalize_referential_rule("NO ACTION") == "RESTRICT"
    assert funding._normalize_referential_rule("RESTRICT") == "RESTRICT"


def test_funding_contract_names_are_unique_and_every_fk_has_explicit_left_prefix():
    expected_counts = {
        funding.FUNDING_DAILY_FACT_TABLE_NAME: (29, 9, 3, 7),
        funding.FUNDING_CHECKPOINT_TABLE_NAME: (46, 12, 7, 13),
    }

    for table_name, contract in funding._TABLE_CONTRACTS.items():
        column_names = [row[0] for row in contract["columns"]]
        indexes = contract["indexes"]
        foreign_keys = contract["foreign_keys"]
        checks = contract["checks"]

        assert len(column_names) == len(set(column_names))
        assert (
            len(column_names),
            len(indexes),
            len(foreign_keys),
            len(checks),
        ) == expected_counts[table_name]
        for child_columns, *_rest in foreign_keys.values():
            assert any(
                tuple(index_columns[: len(child_columns)])
                == tuple(child_columns)
                for _non_unique, index_columns in indexes.values()
            ), (table_name, child_columns)

        assert all("account" not in name for name in foreign_keys)
        assert any(
            tuple(index_columns) == ("account_id",)
            for _non_unique, index_columns in indexes.values()
        )

    daily_checks = funding._TABLE_CONTRACTS[
        funding.FUNDING_DAILY_FACT_TABLE_NAME
    ]["checks"]
    assert daily_checks["ck_strategy_funding_daily_fact_entity"] == (
        "entity_type = 'STRATEGY'"
    )
