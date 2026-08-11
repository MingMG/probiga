from __future__ import annotations

from tools.audit_mysql55_to_mysql84_schema import (
    compare_rows,
    normalize_column_type,
    normalize_extra,
    normalize_referential_rule,
    normalize_reviewed_datetime_default,
    normalize_sql_mode,
)


def test_integer_display_width_is_the_only_column_type_normalization():
    assert normalize_column_type("int(11)") == "int"
    assert normalize_column_type("BIGINT(20) unsigned") == "BIGINT unsigned"
    assert normalize_column_type("decimal(20,2)") == "decimal(20,2)"
    assert normalize_column_type("varchar(20)") == "varchar(20)"


def test_default_generated_marker_is_metadata_only():
    assert normalize_extra("DEFAULT_GENERATED on update CURRENT_TIMESTAMP") == (
        "on update current_timestamp"
    )
    assert normalize_extra("auto_increment") == "auto_increment"


def test_only_removed_sql_mode_is_dropped():
    assert normalize_sql_mode(
        "STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
    ) == "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
    assert normalize_sql_mode("NO_AUTO_VALUE_ON_ZERO") == "NO_AUTO_VALUE_ON_ZERO"


def test_mysql_no_action_and_restrict_are_normalized_as_equivalent():
    assert normalize_referential_rule("NO ACTION") == "RESTRICT"
    assert normalize_referential_rule("RESTRICT") == "RESTRICT"
    assert normalize_referential_rule("CASCADE") == "CASCADE"


def test_comparison_hashes_canonical_rows_and_reports_both_sides():
    report = compare_rows("columns", ["a", "b"], ["b", "c"])
    assert report.source_count == 2
    assert report.target_count == 2
    assert report.difference_count == 2
    assert report.source_only_sample == ("a",)
    assert report.target_only_sample == ("c",)
    assert report.source_sha256 != report.target_sha256


def test_comparison_is_order_independent():
    report = compare_rows("indexes", ["b", "a"], ["a", "b"])
    assert report.difference_count == 0
    assert report.source_sha256 == report.target_sha256


def test_reviewed_target_superset_requires_every_source_row_but_reports_additions():
    report = compare_rows(
        "tables",
        ["legacy_a", "legacy_b"],
        ["legacy_a", "legacy_b", "new_v4"],
        allow_target_superset=True,
    )
    assert report.difference_count == 0
    assert report.allowed_target_only_count == 1
    assert report.target_only_sample == ("new_v4",)

    missing = compare_rows(
        "tables",
        ["legacy_a", "legacy_b"],
        ["legacy_a", "new_v4"],
        allow_target_superset=True,
    )
    assert missing.difference_count == 1
    assert missing.source_only_sample == ("legacy_b",)


def test_reviewed_source_replacement_is_narrow_and_explicit():
    report = compare_rows(
        "indexes",
        ["old_unique_index", "stable_index"],
        ["new_unique_index", "stable_index"],
        allow_target_superset=True,
        allowed_source_replacements=["old_unique_index"],
    )
    assert report.difference_count == 0
    assert report.allowed_source_replacement_count == 1
    assert report.allowed_target_only_count == 1


def test_only_reviewed_target_datetime_default_repair_is_equivalent():
    zero_hex = "0000-00-00 00:00:00".encode("ascii").hex().upper()
    current_hex = "CURRENT_TIMESTAMP".encode("ascii").hex().upper()
    source = (
        "probiga",
        "jq_strategy_meta",
        "created_at",
        1,
        "datetime",
        "datetime",
        "YES",
        zero_hex,
        "",
        "<NULL>",
        "<NULL>",
    )
    target = (*source[:7], current_hex, *source[8:])

    assert normalize_reviewed_datetime_default(source, target=False) == (
        normalize_reviewed_datetime_default(target, target=True)
    )
    assert normalize_reviewed_datetime_default(target, target=False) == target


def test_unreviewed_datetime_default_is_not_normalized():
    row = (
        "probiga",
        "other_table",
        "created_at",
        1,
        "datetime",
        "datetime",
        "YES",
        "43555252454E545F54494D455354414D50",
        "",
        "<NULL>",
        "<NULL>",
    )
    assert normalize_reviewed_datetime_default(row, target=True) == row
