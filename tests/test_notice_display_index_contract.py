"""The notices-page index must be a release contract, not a manual ALTER.

Production observation (2026-10-17) on ``si_notice_eastmoney``:

    8,341,899 rows   data 4001.0 MB   index 1390.8 MB   InnoDB

The table was **not** missing indexes.  It had four:

    PRIMARY                          (id)
    uk_notice_stock_art              (stock_code, art_code)
    idx_notice_stock_date            (stock_code, notice_date)
    idx_notice_stock                 (stock_code)
    idx_notice_validated_stock_date  (association_validated, stock_code, notice_date)

The notices page's default branch

    SELECT ... FROM si_notice_eastmoney
     WHERE association_validated = 1 AND notice_date <= CURDATE()
     ORDER BY notice_date DESC LIMIT 50        (hot_data.py:2889-2893)

did not return within a 120 s cap, while the stock_code sibling branch returned
50 rows in 0.65 s.  The cause is column *order*, not a missing index:
``idx_notice_validated_stock_date`` supplies equality on ``association_validated``
but its next column is ``stock_code``, so the index is ordered by
``(stock_code, notice_date)`` and ``ORDER BY notice_date`` cannot be served from
it — every candidate row has to be sorted.

So ``PRODUCTION_INDEX_SHAPES`` is the regression fixture: it is the exact state
production was in, and the release validator must refuse it.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from biz.notice import sync_notice_em as notice


PRODUCTION_INDEX_SHAPES = {
    ("id",),
    ("stock_code", "art_code"),
    ("stock_code", "notice_date"),
    ("stock_code",),
    ("association_validated", "stock_code", "notice_date"),
}

PRODUCTION_INDEX_NAMES = {
    "PRIMARY",
    "uk_notice_stock_art",
    "idx_notice_stock_date",
    "idx_notice_stock",
    "idx_notice_validated_stock_date",
}


def _engine_double(connection):
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    return engine


def _patch_inventory(monkeypatch, shapes):
    monkeypatch.setattr(
        notice,
        "_notice_index_inventory",
        lambda _engine: (set(shapes), set(PRODUCTION_INDEX_NAMES)),
    )
    monkeypatch.setattr(
        notice,
        "validate_required_table_surface",
        lambda *args, **kwargs: None,
    )


def test_release_validator_rejects_the_index_shape_production_was_in(monkeypatch):
    """The observed production state must fail the release contract.

    This is the regression test for the real outage: the table had an index whose
    *first* column was association_validated, so a check that only asked "is there
    an index starting with association_validated?" would have passed while the page
    was still unable to answer.
    """

    _patch_inventory(monkeypatch, PRODUCTION_INDEX_SHAPES)

    with pytest.raises(RuntimeError, match="no index leads with"):
        notice.validate_notice_runtime_schema(MagicMock())


def test_release_validator_accepts_a_leading_prefix_with_trailing_columns(
    monkeypatch,
):
    """A prefix match is enough: any index led by the two columns serves the query."""

    _patch_inventory(
        monkeypatch,
        PRODUCTION_INDEX_SHAPES | {("association_validated", "notice_date", "stock_code")},
    )

    result = notice.validate_notice_runtime_schema(MagicMock())

    assert result["physical_contract_verified"] is True
    assert result["runtime_ddl_required"] is False
    assert result["read_only"] is True
    assert result["required_index_columns"] == ["association_validated", "notice_date"]


def test_privileged_migration_installs_the_ordered_index(monkeypatch):
    """Starting from the production shape, one release installs the right index."""

    shapes = set(PRODUCTION_INDEX_SHAPES)
    _patch_inventory(monkeypatch, shapes)
    # validate_notice_runtime_schema re-reads the inventory after the ALTER, so the
    # double has to reflect the new index the way MySQL would.
    monkeypatch.setattr(
        notice,
        "_notice_index_inventory",
        lambda _engine: (set(shapes), set(PRODUCTION_INDEX_NAMES)),
    )
    connection = MagicMock()

    def _execute(statement, *args, **kwargs):
        if "ADD INDEX" in str(statement):
            shapes.add(("association_validated", "notice_date"))
        return MagicMock()

    connection.execute.side_effect = _execute

    result = notice.privileged_migrate_notice_tables(_engine_double(connection))

    ddl = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert ddl == [
        "ALTER TABLE `si_notice_eastmoney` ADD INDEX `idx_notice_validated_date` "
        "(`association_validated`, `notice_date`)"
    ]
    assert result["added_indexes"] == ["idx_notice_validated_date"]
    assert result["status"] == "ok"
    assert result["physical_contract_verified"] is True


def test_privileged_migration_is_a_no_op_when_the_index_already_exists(monkeypatch):
    """Re-running a release must not fail on a duplicate key name."""

    _patch_inventory(
        monkeypatch,
        PRODUCTION_INDEX_SHAPES | {("association_validated", "notice_date")},
    )
    connection = MagicMock()

    result = notice.privileged_migrate_notice_tables(_engine_double(connection))

    assert [str(c.args[0]) for c in connection.execute.call_args_list] == []
    assert result["added_indexes"] == []


def test_privileged_migration_avoids_a_colliding_index_name(monkeypatch):
    """A taken preferred name must not turn the release into a duplicate-key error."""

    shapes = set(PRODUCTION_INDEX_SHAPES)
    names = set(PRODUCTION_INDEX_NAMES) | {notice.NOTICE_REQUIRED_INDEX_NAME}
    monkeypatch.setattr(
        notice,
        "_notice_index_inventory",
        lambda _engine: (set(shapes), set(names)),
    )
    monkeypatch.setattr(
        notice,
        "validate_required_table_surface",
        lambda *args, **kwargs: None,
    )
    connection = MagicMock()

    def _execute(statement, *args, **kwargs):
        if "ADD INDEX" in str(statement):
            shapes.add(("association_validated", "notice_date"))
        return MagicMock()

    connection.execute.side_effect = _execute

    result = notice.privileged_migrate_notice_tables(_engine_double(connection))

    assert result["added_indexes"] == ["idx_notice_validated_date_2"]


def test_collector_runtime_guard_does_not_require_the_display_index(monkeypatch):
    """A slow page must never become a data-acquisition outage.

    run_ddl is the collector's own guard.  It has to keep passing on the exact
    index shape production is in today; only the release contract may demand the
    display index.
    """

    _patch_inventory(monkeypatch, PRODUCTION_INDEX_SHAPES)

    assert notice.run_ddl(MagicMock()) is None


def test_collector_runtime_guard_still_requires_the_prepared_unique_key(monkeypatch):
    _patch_inventory(monkeypatch, {("id",), ("stock_code",)})

    with pytest.raises(RuntimeError, match="requires prepared unique key"):
        notice.run_ddl(MagicMock())


def test_bundle_registers_the_notice_contract_for_release():
    from server.common import production_runtime_schema_bundle as bundle

    assert ("notice", bundle.privileged_migrate_notice_tables) in bundle._MIGRATIONS
    assert ("notice", bundle.validate_notice_runtime_schema) in bundle._VALIDATORS
