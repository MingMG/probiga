from __future__ import annotations

import inspect

from biz.sentiment import sync_sentiment
from biz.notice import sync_notice_em
from biz.stock_info import sync_stock_holder, sync_stock_info
from biz.stock_market import sync_stock_market
from biz.stock_market import sync_stock_snapshot
from tools import (
    crawl_concept_east_current,
    crawl_concept_ths_current,
    crawl_minute_kline,
    fetch_concept_flow_datacenter,
)


def _upper_source(callable_object) -> str:
    return inspect.getsource(callable_object).upper()


def test_legacy_collector_run_ddl_entrypoints_are_read_only_validators():
    for function in (
        sync_stock_info.run_ddl,
        sync_stock_holder.run_ddl,
        sync_sentiment.run_ddl,
        sync_stock_market.run_ddl,
        sync_notice_em.run_ddl,
    ):
        source = _upper_source(function)
        assert "VALIDATE_" in source
        assert "CREATE TABLE" not in source
        assert "ALTER TABLE" not in source
        assert "DROP TABLE" not in source
        assert "TRUNCATE TABLE" not in source


def test_snapshot_preclear_helpers_fail_closed():
    for function in (
        sync_stock_info.truncate_all,
        sync_stock_info.truncate_only,
        sync_sentiment.truncate_all_sentiment,
        sync_sentiment.truncate_only,
        sync_stock_market.truncate_all,
        sync_stock_market.truncate_only,
        sync_stock_market.delete_stock_minute_dates,
        sync_stock_market.df_to_table,
        sync_stock_market._prune_snapshot_codes,
        sync_stock_market._prune_snapshot_time_bounds,
    ):
        source = _upper_source(function)
        assert "RAISE RUNTIMEERROR" in source
        assert "DELETE FROM" not in source
        assert "TRUNCATE TABLE" not in source

    sentiment_main = inspect.getsource(sync_sentiment.main).upper()
    stock_market_main = inspect.getsource(sync_stock_market.main).upper()
    assert "TRUNCATE_ALL_SENTIMENT(ENGINE)" not in sentiment_main
    assert "TRUNCATE_ALL(ENGINE)" not in stock_market_main


def test_current_snapshot_replacement_contains_no_persistent_schema_swap():
    source = _upper_source(sync_stock_market.replace_stock_current_snapshot)
    assert "REPLACE_TABLE_ROWS" in source
    assert "_CODE_SCOPE_PREDICATE" in source
    assert "CREATE TABLE" not in source
    assert "DROP TABLE" not in source
    assert "RENAME TABLE" not in source
    assert "TRUNCATE TABLE" not in source


def test_qmt_receipt_writer_contains_dml_and_no_runtime_schema_mutation():
    source = _upper_source(sync_stock_market._record_qmt_minute_receipt)
    assert "VALIDATE_REQUIRED_TABLE_SURFACE" in source
    assert "INSERT INTO" in source
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source


def test_large_collection_stages_are_session_local_not_persistent_tables():
    qmt_create = _upper_source(sync_stock_market._create_qmt_minute_stage)
    generic_create = _upper_source(sync_stock_market._create_temporary_stage)
    kline_create = _upper_source(crawl_minute_kline._create_kline_stage)
    flow_create = _upper_source(crawl_minute_kline._create_flow_stage)
    assert "CREATE TEMPORARY TABLE" in qmt_create
    assert "CREATE TEMPORARY TABLE" in generic_create
    assert "CREATE TEMPORARY TABLE" in kline_create
    assert "CREATE TEMPORARY TABLE" in flow_create
    assert "CREATE TABLE" not in generic_create.replace("CREATE TEMPORARY TABLE", "")
    assert "DROP TABLE" not in _upper_source(sync_stock_market._drop_qmt_minute_stage)
    assert "DROP TABLE" not in _upper_source(crawl_minute_kline._drop_kline_stage)
    assert "DROP TABLE" not in _upper_source(crawl_minute_kline._drop_flow_stage)


def test_short_name_runtime_guard_is_read_only_and_migration_is_explicit():
    runtime_source = _upper_source(sync_stock_market._ensure_sm_stock_kline_short_name)
    migration_source = _upper_source(
        sync_stock_market.privileged_migrate_sm_stock_kline_short_name
    )
    assert "ALTER TABLE" not in runtime_source
    assert "VALIDATE_REQUIRED_TABLE_SURFACE" in runtime_source
    assert "ALTER TABLE" in migration_source
    assert "PRIVILEGED_MIGRATE" in (
        sync_stock_market.privileged_migrate_sm_stock_kline_short_name.__name__.upper()
    )


def test_snapshot_collectors_replace_rows_in_one_dml_transaction():
    for function in (
        sync_stock_snapshot.write_snapshot,
        crawl_concept_east_current.save_to_db,
        crawl_concept_ths_current.save_to_db,
        fetch_concept_flow_datacenter.fetch_concept_flow,
    ):
        source = _upper_source(function)
        assert "REPLACE_TABLE_ROWS" in source
        assert "TRUNCATE TABLE" not in source
