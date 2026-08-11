from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from tools.mysql55_to_mysql84_data_manifest import (
    CHECKPOINT_NAME,
    DOCUMENT_DIGEST_FIELD,
    FORMAT_NAME,
    FORMAT_VERSION,
    KNOWN_MYSQL84_ZERO_DATE_COLUMNS,
    EndpointExpectation,
    ManifestError,
    _catalog_for_comparison,
    _catalog_signature_sha256,
    _checkpoint_payload,
    _hash_query,
    _metric_query,
    _project_target_catalog,
    _sample_crc_expression,
    _window_hash_query,
    atomic_write_json,
    build_table_plans,
    canonical_row_bytes,
    compare_manifests,
    deterministic_integer_anchors,
    hash_rows_in_chunks,
    load_config,
    load_sealed_json,
    seal_document,
    validate_snapshot_arguments,
    validate_identity,
    verify_document,
)


SOURCE_FINGERPRINT = "a" * 64
TARGET_UUID = "12345678-1234-4234-8234-123456789abc"


def _raw_config(*, hashes: dict | None = None, legacy: list[str] | None = None) -> dict:
    return {
        "format_version": 1,
        "schemas": ["biga"],
        "endpoints": {
            "source": {
                "version": "5.5.20",
                "port": 3306,
                "server_uuid": None,
                "legacy_identity_sha256": SOURCE_FINGERPRINT,
                "require_tls": False,
            },
            "target": {
                "version": "8.4.11",
                "port": 33084,
                "server_uuid": TARGET_UUID,
                "legacy_identity_sha256": None,
                "require_tls": True,
            },
        },
        "execution": {"max_workers": 2},
        "counts": {"mode": "all", "tables": []},
        "boundaries": {
            "primary_key_mode": "all",
            "primary_key_tables": [],
            "date_columns": {"biga.t": ["trade_date"]},
        },
        "aggregates": {
            "biga.t": [
                {
                    "column": "amount",
                    "functions": ["min", "max", "sum", "count_nonnull"],
                    "absolute_tolerance": "0.01",
                }
            ]
        },
        "hashes": hashes
        if hashes is not None
        else {
            "biga.t": {
                "mode": "deterministic_sample_sha256",
                "key_columns": ["id"],
                "columns": "*",
                "chunk_rows": 1000,
                "sample_modulus": 1000,
                "sample_remainders": [7, 17],
            }
        },
        "legacy_zero_date_columns": legacy if legacy is not None else [],
    }


def _write_config(tmp_path: Path, raw: dict) -> Path:
    path = tmp_path / "manifest-config.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


def _catalog(*, zero_default: object = None) -> dict:
    return {
        "schemas": ["biga"],
        "tables": {
            "biga.t": {
                "table_type": "BASE TABLE",
                "engine": "InnoDB",
                "columns": [
                    {
                        "name": "id",
                        "ordinal": 1,
                        "data_type": "bigint",
                        "nullable": False,
                        "default": None,
                    },
                    {
                        "name": "trade_date",
                        "ordinal": 2,
                        "data_type": "date",
                        "nullable": False,
                        "default": zero_default,
                    },
                    {
                        "name": "amount",
                        "ordinal": 3,
                        "data_type": "decimal",
                        "nullable": True,
                        "default": None,
                    },
                ],
                "primary_key": ["id"],
            }
        },
        "catalog_sha256": "catalog-digest-is-informational-here",
    }


def _sample_hash() -> dict:
    empty = {
        "row_count": 1,
        "overall_sha256": "b" * 64,
        "chunks": [
            {
                "ordinal": 0,
                "row_count": 1,
                "first_key": [{"type": "int", "value": "7"}],
                "last_key": [{"type": "int", "value": "7"}],
                "sha256": "c" * 64,
            }
        ],
    }
    return {
        "mode": "deterministic_sample_sha256",
        "key_columns": ["id"],
        "columns": ["id", "trade_date", "amount"],
        "chunk_rows": 1000,
        "selector": {
            "algorithm": "mysql_crc32_of_length_prefixed_primary_key",
            "modulus": 1000,
            "remainders": [7],
            "warning": "sample only",
        },
        "evidence_strength": "deterministic_sample_only_not_full_proof",
        "sampled_row_count": 1,
        "buckets": [{"remainder": 7, **empty}],
    }


def _measurements(*, target: bool = False) -> dict:
    return {
        "biga.t": {
            "exact_count": 10,
            "boundaries": {
                "id": {
                    "min": {"type": "int", "value": "1"},
                    "max": {"type": "int", "value": "10"},
                }
            },
            "aggregates": {
                "amount": {
                    "absolute_tolerance": "0.01",
                    "sum": {"type": "decimal", "value": "100.00" if not target else "100.005"},
                }
            },
            "legacy_zero_dates": {
                "trade_date": {
                    "risk": "mysql84_unsafe_zero_date_default",
                    "column_default": (
                        {"type": "string", "value": "0000-00-00"} if not target else None
                    ),
                    "default_is_zero_date": not target,
                    "stored_zero_count": 0,
                }
            },
            "hash": _sample_hash(),
        }
    }


def _source_manifest() -> dict:
    return seal_document(
        {
            "format": FORMAT_NAME,
            "format_version": FORMAT_VERSION,
            "role": "source",
            "config_sha256": "d" * 64,
            "schemas": ["biga"],
            "endpoint": {
                "version": "5.5.20",
                "port": 3306,
                "server_uuid": None,
                "legacy_identity_sha256": SOURCE_FINGERPRINT,
            },
            "snapshot": {
                "id": "cutover-1",
                "restore_artifact_sha256": "e" * 64,
                "eligible_for_final_cutover_comparison": True,
            },
            "catalog": _catalog(zero_default={"type": "string", "value": "0000-00-00"}),
            "measurements": _measurements(),
            "coverage": {
                "exact_counts_cover_all_base_tables": True,
                "full_logical_sha256_covers_all_base_tables": False,
                "sampled_table_count": 1,
            },
        }
    )


def _target_manifest(source: dict) -> dict:
    return seal_document(
        {
            "format": FORMAT_NAME,
            "format_version": FORMAT_VERSION,
            "role": "target",
            "config_sha256": "d" * 64,
            "schemas": ["biga"],
            "endpoint": {
                "version": "8.4.11",
                "port": 33084,
                "server_uuid": TARGET_UUID,
                "legacy_identity_sha256": "f" * 64,
                "ssl_cipher": "TLS_AES_128_GCM_SHA256",
            },
            "restored_from": {
                "source_manifest_sha256": source[DOCUMENT_DIGEST_FIELD],
                "source_snapshot_id": "cutover-1",
                "restore_artifact_sha256": "e" * 64,
            },
            "catalog": _catalog(zero_default=None),
            "measurements": _measurements(target=True),
            "coverage": {
                "exact_counts_cover_all_base_tables": True,
                "full_logical_sha256_covers_all_base_tables": False,
                "sampled_table_count": 1,
            },
        }
    )


def test_config_builds_all_count_boundary_aggregate_and_sample_plans(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path, _raw_config()))
    plans = build_table_plans(config, _catalog())
    plan = plans["biga.t"]
    assert plan["count"] is True
    assert plan["boundary_columns"] == ("id", "trade_date")
    assert plan["aggregates"][0].functions == ("min", "max", "sum", "count_nonnull")
    assert plan["hash"].mode == "deterministic_sample_sha256"


def test_config_rejects_target_without_uuid_or_tls(tmp_path: Path) -> None:
    raw = _raw_config()
    raw["endpoints"]["target"]["server_uuid"] = None
    raw["endpoints"]["target"]["legacy_identity_sha256"] = "f" * 64
    raw["endpoints"]["target"]["require_tls"] = False
    with pytest.raises(ManifestError, match="must pin server_uuid and require TLS"):
        load_config(_write_config(tmp_path, raw))


def test_identity_gate_pins_port_uuid_and_tls() -> None:
    expectation = EndpointExpectation(
        version="8.4.11",
        port=33084,
        server_uuid=TARGET_UUID,
        legacy_identity_sha256=None,
        require_tls=True,
    )
    identity = {
        "version": "8.4.11",
        "port": 33084,
        "server_uuid": TARGET_UUID,
        "legacy_identity_sha256": "f" * 64,
        "ssl_cipher": "TLS_AES_256_GCM_SHA384",
    }
    validate_identity(identity, expectation, role="target")
    with pytest.raises(ManifestError, match="not using TLS"):
        validate_identity({**identity, "ssl_cipher": ""}, expectation, role="target")
    with pytest.raises(ManifestError, match="server_uuid mismatch"):
        validate_identity({**identity, "server_uuid": "0" * 32}, expectation, role="target")
    with pytest.raises(ManifestError, match="port mismatch"):
        validate_identity({**identity, "port": 3306}, expectation, role="target")


def test_config_rejects_hash_order_that_is_not_exact_primary_key(tmp_path: Path) -> None:
    raw = _raw_config()
    raw["hashes"]["biga.t"]["key_columns"] = ["trade_date"]
    config = load_config(_write_config(tmp_path, raw))
    with pytest.raises(ManifestError, match="must exactly match PRIMARY KEY order"):
        build_table_plans(config, _catalog())


def test_live_consistent_snapshot_forbids_parallel_or_resume() -> None:
    with pytest.raises(ManifestError, match="exactly one connection"):
        validate_snapshot_arguments(
            mode="initial_consistent_snapshot",
            snapshot_id="rehearsal",
            workers=2,
            ddl_frozen=True,
            writes_frozen=False,
            writes_frozen_at=None,
            restore_artifact_sha256=None,
        )


def test_cutover_snapshot_requires_freeze_time_and_dump_sha256() -> None:
    with pytest.raises(ManifestError, match="restore artifact SHA-256"):
        validate_snapshot_arguments(
            mode="cutover_writes_frozen",
            snapshot_id="cutover",
            workers=2,
            ddl_frozen=True,
            writes_frozen=True,
            writes_frozen_at="2026-08-05T08:00:00+08:00",
            restore_artifact_sha256=None,
        )
    snapshot = validate_snapshot_arguments(
        mode="cutover_writes_frozen",
        snapshot_id="cutover",
        workers=2,
        ddl_frozen=True,
        writes_frozen=True,
        writes_frozen_at="2026-08-05T08:00:00+08:00",
        restore_artifact_sha256="e" * 64,
    )
    assert snapshot["eligible_for_final_cutover_comparison"] is True
    assert snapshot["writes_frozen_at"] == "2026-08-05T00:00:00+00:00"


def test_canonical_rows_are_length_prefixed_and_type_sensitive() -> None:
    assert canonical_row_bytes(("1",)) != canonical_row_bytes((1,))
    assert canonical_row_bytes(("a|b", "c")) != canonical_row_bytes(("a", "b|c"))


def test_ordered_hash_is_chunked_without_changing_overall_digest() -> None:
    rows = [(1, "a"), (2, "b"), (3, "c")]
    one = hash_rows_in_chunks(iter(rows), key_width=1, chunk_rows=1000)
    two = hash_rows_in_chunks(iter(rows), key_width=1, chunk_rows=2)
    assert one["overall_sha256"] == two["overall_sha256"]
    assert one["row_count"] == 3
    assert [chunk["row_count"] for chunk in two["chunks"]] == [2, 1]


def test_chunk_metadata_is_bounded_even_for_many_chunks() -> None:
    rows = ((index, f"v{index}") for index in range(10))
    result = hash_rows_in_chunks(rows, key_width=1, chunk_rows=1, max_recorded_chunks=4)
    assert result["chunk_count"] == 10
    assert result["chunks_truncated"] is True
    assert [chunk["ordinal"] for chunk in result["chunks"]] == [0, 1, 8, 9]
    assert len(result["chunk_ledger_sha256"]) == 64


def test_sample_crc_is_only_a_selector_and_query_orders_by_primary_key(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path, _raw_config()))
    spec = config.hashes["biga.t"]
    query, params, columns = _hash_query(
        "biga.t", spec, _catalog()["tables"]["biga.t"], remainder=7
    )
    assert "CRC32" in _sample_crc_expression(("id",))
    assert "MOD(CRC32" in query
    assert "ORDER BY `id`" in query
    assert params == (1000, 7)
    assert columns == ("id", "trade_date", "amount")


def test_pk_window_sample_uses_bounded_primary_key_range_reads(tmp_path: Path) -> None:
    raw = _raw_config(
        hashes={
            "biga.t": {
                "mode": "deterministic_pk_windows_sha256",
                "key_columns": ["id"],
                "columns": ["id", "trade_date", "amount"],
                "chunk_rows": 1000,
                "window_count": 5,
                "window_rows": 200,
            }
        }
    )
    config = load_config(_write_config(tmp_path, raw))
    spec = build_table_plans(config, _catalog())["biga.t"]["hash"]
    assert deterministic_integer_anchors(1, 101, 5) == (1, 26, 51, 76, 101)
    query, columns = _window_hash_query("biga.t", spec, _catalog()["tables"]["biga.t"])
    assert "WHERE `id` >= %s ORDER BY `id` LIMIT 200" in query
    assert "CRC32" not in query
    assert columns == ("id", "trade_date", "amount")


def test_metric_query_combines_count_boundaries_aggregates_and_zero_check(tmp_path: Path) -> None:
    raw = _raw_config(legacy=["biga.t.trade_date"])
    config = load_config(_write_config(tmp_path, raw))
    plan = build_table_plans(config, _catalog())["biga.t"]
    query, labels = _metric_query("biga.t", plan)
    assert query is not None
    assert query.count("SELECT") == 1
    assert "COUNT(*)" in query
    assert "MIN(`id`)" in query
    assert "SUM(`amount`)" in query
    assert "0000-00-00%" in query
    assert len(labels) == 10


def test_atomic_sealed_json_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    document = seal_document({"answer": 42})
    atomic_write_json(path, document)
    assert load_sealed_json(path) == document
    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["answer"] = 43
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ManifestError, match="changed or truncated"):
        load_sealed_json(path)


def test_checkpoint_is_sealed_and_table_granular(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path, _raw_config()))
    checkpoint = _checkpoint_payload(
        role="target",
        config=config,
        identity={"version": "8.4.11", "port": 33084, "server_uuid": TARGET_UUID},
        catalog=_catalog(),
        context={"source_manifest_sha256": "d" * 64},
        measurements={"biga.t": {"exact_count": 10}},
        complete=False,
    )
    verify_document(checkpoint)
    assert checkpoint["format"] == CHECKPOINT_NAME
    assert checkpoint["resume_scope"].startswith("completed tables only")


def test_catalog_comparison_allows_repaired_default_but_not_column_drift() -> None:
    source = _catalog(zero_default={"type": "string", "value": "0000-00-00"})
    target = _catalog(zero_default=None)
    assert _catalog_for_comparison(source) == _catalog_for_comparison(target)
    target["tables"]["biga.t"]["columns"][1]["name"] = "other_date"
    assert _catalog_for_comparison(source) != _catalog_for_comparison(target)


def test_reviewed_target_projection_keeps_every_source_column_and_pins_additions() -> None:
    source = _catalog(zero_default={"type": "string", "value": "0000-00-00"})
    target = deepcopy(source)
    target["tables"]["biga.t"]["columns"].insert(
        1,
        {
            "name": "new_v4_value",
            "ordinal": 2,
            "data_type": "varchar",
            "nullable": False,
            "default": None,
        },
    )
    for ordinal, column in enumerate(target["tables"]["biga.t"]["columns"], start=1):
        column["ordinal"] = ordinal
    target["tables"]["biga.v4_only"] = {
        "table_type": "BASE TABLE",
        "engine": "InnoDB",
        "columns": [
            {
                "name": "id",
                "ordinal": 1,
                "data_type": "bigint",
                "nullable": False,
                "default": None,
            }
        ],
        "primary_key": ["id"],
    }
    target["tables"]["biga.t"]["columns"][2]["default"] = None

    projected, attestation = _project_target_catalog(source, target)

    assert _catalog_for_comparison(projected) == _catalog_for_comparison(source)
    assert projected["tables"]["biga.t"]["columns"][1]["default"] is None
    assert attestation["target_only_tables"] == ["biga.v4_only"]
    assert attestation["target_extended_columns"] == {"biga.t": ["new_v4_value"]}
    assert attestation["source_catalog_sha256"] == _catalog_signature_sha256(source)


def test_compare_reports_sample_as_partial_and_zero_default_as_separate_risk() -> None:
    source = _source_manifest()
    target = _target_manifest(source)
    report = compare_manifests(source, target)
    result = report["result"]
    assert result["configured_checks_match"] is True
    assert result["sample_or_crc_proves_full_equality"] is False
    assert result["full_logical_sha256_coverage"] is False
    assert result["risk_based_cutover_checks_passed"] is True
    assert report["legacy_zero_date_risks"] == [
        {
            "column": "biga.t.trade_date",
            "risk": "mysql84_unsafe_zero_date_default",
            "source_default_is_zero_date": True,
            "target_default_is_safe": True,
            "source_stored_zero_count": 0,
            "target_stored_zero_count": 0,
            "stored_zero_counts_match": True,
            "note": (
                "Stored rows may match while the 5.5 zero-date default remains unsafe on MySQL 8.4."
            ),
        }
    ]


def test_compare_fails_closed_on_resealed_count_mismatch() -> None:
    source = _source_manifest()
    target = _target_manifest(source)
    target["measurements"]["biga.t"]["exact_count"] = 9
    target = seal_document(target)
    report = compare_manifests(source, target)
    assert report["result"]["configured_checks_match"] is False
    assert any(item["kind"] == "exact_count" for item in report["mismatches"])


def test_known_production_zero_date_list_remains_complete() -> None:
    assert len(KNOWN_MYSQL84_ZERO_DATE_COLUMNS) == 8
    assert "probiga.jq_strategy_meta.created_at" in KNOWN_MYSQL84_ZERO_DATE_COLUMNS
    assert "probiga.st_user_portfolio.etl_sync_at" in KNOWN_MYSQL84_ZERO_DATE_COLUMNS
