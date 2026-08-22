from __future__ import annotations

from copy import deepcopy

import pytest

from tools.build_mysql84_final_data_manifest_config import (
    CRC_SAMPLE_MODULUS,
    FULL_HASH_MAX_BYTES,
    KNOWN_POST_MIGRATION_EXTENDED_COLUMNS,
    KNOWN_POST_MIGRATION_TARGET_ONLY_TABLES,
    ConfigBuildError,
    _catalog_relationship,
    build_policy,
    classify_hash_mode,
)


TARGET_UUID = "f40c3202-9260-11f1-86ae-74d4dd7f8500"


def test_forward_allocation_migration_is_in_exact_catalogue_allowlist() -> None:
    assert (
        "probiga.st_forward_exit_allocation_v3"
        in KNOWN_POST_MIGRATION_TARGET_ONLY_TABLES
    )
    assert KNOWN_POST_MIGRATION_EXTENDED_COLUMNS[
        "probiga.st_forward_trade_evidence_v3"
    ] == ("strategy_version",)

    assert (
        "probiga.st_forward_trade_evidence_v3"
        not in KNOWN_POST_MIGRATION_TARGET_ONLY_TABLES
    )
    assert not (
        set(KNOWN_POST_MIGRATION_EXTENDED_COLUMNS)
        & set(KNOWN_POST_MIGRATION_TARGET_ONLY_TABLES)
    )
    source = _catalog()
    for table_ref in KNOWN_POST_MIGRATION_EXTENDED_COLUMNS:
        source["tables"][table_ref] = {
            "table_type": "BASE TABLE",
            "engine": "InnoDB",
            "columns": [{
                "name": "source_id",
                "data_type": "char",
                "nullable": False,
            }],
            "primary_key": ["source_id"],
        }
    target = deepcopy(source)
    for table_ref, columns in KNOWN_POST_MIGRATION_EXTENDED_COLUMNS.items():
        target["tables"][table_ref]["columns"].extend(
            {
                "name": column,
                "data_type": "varchar",
                "nullable": False,
            }
            for column in columns
        )
    for table_ref in KNOWN_POST_MIGRATION_TARGET_ONLY_TABLES:
        target["tables"][table_ref] = {
            "table_type": "BASE TABLE",
            "engine": "InnoDB",
            "columns": [{
                "name": "target_id",
                "data_type": "char",
                "nullable": False,
            }],
            "primary_key": ["target_id"],
        }

    relationship = _catalog_relationship(source, target)

    assert "probiga.st_forward_exit_allocation_v3" in relationship[
        "target_only_tables"
    ]
    assert relationship["target_extended_columns"][
        "probiga.st_forward_trade_evidence_v3"
    ] == ["strategy_version"]


def _catalog() -> dict:
    return {
        "schemas": ["biga", "probiga", "probiga_qmt_history"],
        "tables": {
            "probiga.st_order_v2": {
                "table_type": "BASE TABLE",
                "engine": "InnoDB",
                "columns": [
                    {"name": "order_id", "data_type": "bigint", "nullable": False},
                    {"name": "created_at", "data_type": "datetime", "nullable": False},
                ],
                "primary_key": ["order_id"],
            },
            "biga.market_history": {
                "table_type": "BASE TABLE",
                "engine": "InnoDB",
                "columns": [
                    {"name": "id", "data_type": "bigint", "nullable": False},
                    {"name": "trade_date", "data_type": "date", "nullable": False},
                ],
                "primary_key": ["id"],
            },
            "probiga_qmt_history.no_pk": {
                "table_type": "BASE TABLE",
                "engine": "InnoDB",
                "columns": [
                    {"name": "value", "data_type": "varchar", "nullable": True}
                ],
                "primary_key": [],
            },
            "probiga.composite_history": {
                "table_type": "BASE TABLE",
                "engine": "InnoDB",
                "columns": [
                    {"name": "stock_code", "data_type": "varchar", "nullable": False},
                    {"name": "trade_time", "data_type": "datetime", "nullable": False},
                ],
                "primary_key": ["stock_code", "trade_time"],
            },
            "probiga.text_id_history": {
                "table_type": "BASE TABLE",
                "engine": "InnoDB",
                "columns": [
                    {"name": "event_id", "data_type": "char", "nullable": False},
                    {"name": "created_at", "data_type": "datetime", "nullable": False},
                ],
                "primary_key": ["event_id"],
            },
        },
    }


def test_authority_table_always_receives_full_hash() -> None:
    assert (
        classify_hash_mode(
            "probiga.st_order_v2", allocated_bytes=10 * FULL_HASH_MAX_BYTES
        )
        == "full_ordered_sha256"
    )
    assert (
        classify_hash_mode(
            "biga.market_history", allocated_bytes=10 * FULL_HASH_MAX_BYTES
        )
        == "deterministic_pk_windows_sha256"
    )


def test_policy_covers_counts_dates_and_hash_tiers() -> None:
    catalog = _catalog()
    policy = build_policy(
        source_identity={
            "version": "5.5.20-log",
            "port": 3306,
            "server_uuid": None,
            "ssl_cipher": "",
            "legacy_identity_sha256": "a" * 64,
        },
        target_identity={
            "version": "8.4.11",
            "port": 33090,
            "server_uuid": TARGET_UUID,
            "ssl_cipher": "TLS_AES_256_GCM_SHA384",
        },
        source_catalog=catalog,
        target_catalog=catalog,
        table_sizes={
            "probiga.st_order_v2": 1024 * 1024 * 1024,
            "biga.market_history": 1024 * 1024 * 1024,
            "probiga_qmt_history.no_pk": 1024,
            "probiga.composite_history": 1024 * 1024 * 1024,
            "probiga.text_id_history": 1024 * 1024 * 1024,
        },
        max_workers=2,
    )
    assert policy["counts"]["mode"] == "all"
    assert policy["hashes"]["probiga.st_order_v2"]["mode"] == "full_ordered_sha256"
    assert policy["hashes"]["biga.market_history"]["mode"] == "deterministic_pk_windows_sha256"
    for table in ("probiga.composite_history", "probiga.text_id_history"):
        assert policy["hashes"][table]["mode"] == "deterministic_sample_sha256"
        assert policy["hashes"][table]["sample_modulus"] == CRC_SAMPLE_MODULUS
        assert policy["hashes"][table]["sample_remainders"] == [0]
    assert "probiga_qmt_history.no_pk" not in policy["hashes"]
    assert policy["boundaries"]["date_columns"]["probiga.st_order_v2"] == ["created_at"]
    assert policy["build_evidence"]["no_primary_key_tables"] == [
        "probiga_qmt_history.no_pk"
    ]
    assert policy["build_evidence"]["crc_sample_hash_table_count"] == 2
    assert policy["catalog_comparison"]["mode"] == "exact"


def test_reviewed_target_superset_is_source_projectable_and_fully_pinned() -> None:
    source = _catalog()
    target = deepcopy(source)
    target["tables"]["probiga.st_order_v2"]["columns"].insert(
        1,
        {
            "name": "migration_tag",
            "data_type": "varchar",
            "nullable": False,
        },
    )
    target["tables"]["probiga.new_v4"] = {
        "table_type": "BASE TABLE",
        "engine": "InnoDB",
        "columns": [{"name": "id", "data_type": "bigint", "nullable": False}],
        "primary_key": ["id"],
    }
    relationship = _catalog_relationship(
        source,
        target,
        allowed_target_only=frozenset({"probiga.new_v4"}),
        allowed_extended_columns={"probiga.st_order_v2": ("migration_tag",)},
    )
    assert relationship["mode"] == "reviewed_v2_v3_v4_source_projection"
    assert relationship["target_only_tables"] == ["probiga.new_v4"]
    assert relationship["target_extended_columns"] == {
        "probiga.st_order_v2": ["migration_tag"]
    }
    assert relationship["source_catalog_sha256"] != relationship["target_catalog_sha256"]


def test_target_superset_rejects_unreviewed_or_destructive_drift() -> None:
    source = _catalog()
    unreviewed = deepcopy(source)
    unreviewed["tables"]["probiga.unreviewed"] = {
        "table_type": "BASE TABLE",
        "engine": "InnoDB",
        "columns": [{"name": "id", "data_type": "bigint", "nullable": False}],
        "primary_key": ["id"],
    }
    with pytest.raises(ConfigBuildError, match="exact reviewed V2/V3/V4 set"):
        _catalog_relationship(
            source,
            unreviewed,
            allowed_target_only=frozenset(),
            allowed_extended_columns={},
        )

    changed = deepcopy(source)
    changed["tables"]["probiga.st_order_v2"]["columns"][0]["data_type"] = "int"
    with pytest.raises(ConfigBuildError, match="changed source column data_type"):
        _catalog_relationship(
            source,
            changed,
            allowed_target_only=frozenset(),
            allowed_extended_columns={},
        )
