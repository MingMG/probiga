from __future__ import annotations

import inspect
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from pandas.errors import DatabaseError

from biz.stock_info import sync_stock_info


def _source(function) -> str:
    return inspect.getsource(function)


def _sqlite_table_with_old_row():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE si_atomic_fixture ("
                "stock_code TEXT PRIMARY KEY, short_name TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO si_atomic_fixture (stock_code, short_name) "
                "VALUES ('000001', '旧快照')"
            )
        )
    return engine


def _fixture_rows(engine) -> list[tuple[str, str]]:
    with engine.connect() as connection:
        return list(
            connection.execute(
                text(
                    "SELECT stock_code, short_name FROM si_atomic_fixture "
                    "ORDER BY stock_code"
                )
            ).tuples()
        )


def test_main_never_runs_the_legacy_global_clear(monkeypatch):
    class _Engine:
        url = "mysql+pymysql://runtime:" + "secret@db/probiga"

    monkeypatch.delenv("SI_SYNC_SKIP_ALL_CODE", raising=False)
    monkeypatch.setattr(sync_stock_info, "create_batch_engine", lambda **_kwargs: _Engine())
    monkeypatch.setattr(sync_stock_info, "run_ddl", lambda _engine: None)
    monkeypatch.setattr(sync_stock_info, "load_info", object)
    monkeypatch.setattr(
        sync_stock_info,
        "truncate_all",
        lambda _engine: pytest.fail("production main must not clear all stock-info tables"),
    )
    monkeypatch.setattr(
        sync_stock_info,
        "sync_all_code",
        lambda _engine, _info: pd.DataFrame({"stock_code": ["000001"]}),
    )
    monkeypatch.setattr(sync_stock_info, "sync_trade_calendar", lambda *_args: None)
    monkeypatch.setattr(
        sync_stock_info,
        "sync_all_index_code",
        lambda *_args: pd.DataFrame({"index_code": ["000001"]}),
    )
    monkeypatch.setattr(sync_stock_info, "sync_index_constituent", lambda *_args: None)
    monkeypatch.setattr(
        sync_stock_info,
        "sync_concept_code_east",
        lambda *_args: pd.DataFrame({"concept_code": ["BK001"]}),
    )
    monkeypatch.setattr(sync_stock_info, "sync_concept_constituent_east", lambda *_args: None)
    monkeypatch.setattr(
        sync_stock_info,
        "sync_concept_code_ths",
        lambda *_args: pd.DataFrame({"concept_code": ["THS001"]}),
    )
    monkeypatch.setattr(sync_stock_info, "sync_concept_constituent_ths", lambda *_args: None)
    monkeypatch.setattr(sync_stock_info, "sync_per_stock_tables", lambda *_args: None)
    monkeypatch.setattr(sync_stock_info, "_use_qmt_sector_data", lambda: False)

    assert sync_stock_info.main() == 0

    assert "truncate_all(" not in _source(sync_stock_info.main)


def test_production_snapshot_functions_do_not_call_truncate_only():
    for function in (
        sync_stock_info.sync_trade_calendar,
        sync_stock_info.sync_all_index_code,
        sync_stock_info.sync_concept_code_east,
        sync_stock_info.sync_concept_constituent_east,
        sync_stock_info.sync_concept_code_ths,
        sync_stock_info.sync_concept_constituent_ths,
        sync_stock_info.sync_per_stock_tables,
    ):
        assert "truncate_only(" not in _source(function)


def test_legacy_preclear_helpers_fail_closed():
    for function, args in (
        (sync_stock_info.truncate_all, (object(),)),
        (sync_stock_info.truncate_only, (object(), "si_all_code")),
    ):
        assert "DELETE FROM" not in _source(function).upper()
        with pytest.raises(RuntimeError):
            function(*args)


def test_empty_snapshot_is_rejected_without_removing_previous_rows():
    engine = _sqlite_table_with_old_row()

    with pytest.raises(ValueError, match="empty snapshot"):
        sync_stock_info._replace_full_snapshot(
            engine,
            pd.DataFrame(columns=["stock_code", "short_name"]),
            "si_atomic_fixture",
        )

    assert _fixture_rows(engine) == [("000001", "旧快照")]


def test_failed_insert_rolls_back_delete_and_preserves_previous_rows():
    engine = _sqlite_table_with_old_row()
    invalid_snapshot = pd.DataFrame(
        {
            "stock_code": ["000002", "000002"],
            "short_name": ["重复一", "重复二"],
        }
    )

    with pytest.raises(DatabaseError):
        sync_stock_info._replace_full_snapshot(
            engine,
            invalid_snapshot,
            "si_atomic_fixture",
        )

    assert _fixture_rows(engine) == [("000001", "旧快照")]


def test_related_snapshot_write_failure_rolls_back_every_table():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        for table in ("si_related_a", "si_related_b"):
            connection.execute(
                text(
                    f"CREATE TABLE {table} ("
                    "stock_code TEXT PRIMARY KEY, short_name TEXT NOT NULL)"
                )
            )
            connection.execute(
                text(
                    f"INSERT INTO {table} (stock_code, short_name) "
                    "VALUES ('000001', '旧快照')"
                )
            )

    with pytest.raises(DatabaseError):
        sync_stock_info._replace_full_snapshots_atomically(
            engine,
            {
                "si_related_a": pd.DataFrame(
                    [{"stock_code": "000002", "short_name": "新快照"}]
                ),
                "si_related_b": pd.DataFrame(
                    [
                        {"stock_code": "000003", "short_name": "重复一"},
                        {"stock_code": "000003", "short_name": "重复二"},
                    ]
                ),
            },
        )

    with engine.connect() as connection:
        for table in ("si_related_a", "si_related_b"):
            rows = list(
                connection.execute(
                    text(
                        f"SELECT stock_code, short_name FROM {table} "
                        "ORDER BY stock_code"
                    )
                ).tuples()
            )
            assert rows == [("000001", "旧快照")]


def test_partial_stock_debug_limit_fails_closed_before_any_replacement(
    monkeypatch,
):
    replaced: list[str] = []
    monkeypatch.setenv("SI_MAX_STOCKS", "1")
    monkeypatch.setattr(
        sync_stock_info,
        "replace_table_rows",
        lambda _frame, table, _engine, **_kwargs: replaced.append(table),
    )

    with pytest.raises(RuntimeError, match="partial-universe debug limit"):
        sync_stock_info.sync_per_stock_tables(
            object(),
            object(),
            pd.DataFrame({"stock_code": ["000001", "000002"]}),
        )

    assert replaced == []


def test_empty_per_stock_shard_blocks_all_six_relation_tables(monkeypatch):
    class _Info:
        @staticmethod
        def get_stock_shares(*, stock_code, is_history):
            del is_history
            if stock_code == "000002":
                return pd.DataFrame()
            return pd.DataFrame(
                [{"stock_code": stock_code, "change_date": "2026-08-01"}]
            )

        @staticmethod
        def get_industry_sw(*, stock_code):
            return pd.DataFrame([{"stock_code": code} for code in stock_code])

        @staticmethod
        def get_concept_east(*, stock_code):
            return pd.DataFrame([{"stock_code": stock_code}])

        @staticmethod
        def get_plate_east(*, stock_code, plate_type):
            del plate_type
            return pd.DataFrame([{"stock_code": stock_code}])

        @staticmethod
        def get_concept_baidu(*, stock_code):
            return pd.DataFrame([{"stock_code": code} for code in stock_code])

        @staticmethod
        def get_concept_ths(*, stock_code):
            return pd.DataFrame([{"stock_code": stock_code}])

    published: list[dict[str, pd.DataFrame]] = []
    monkeypatch.delenv("SI_MAX_STOCKS", raising=False)
    monkeypatch.setattr(sync_stock_info, "_use_qmt_sector_data", lambda: False)
    monkeypatch.setattr(sync_stock_info, "_sleep", lambda: None)
    monkeypatch.setattr(
        sync_stock_info,
        "retry_remote",
        lambda function, *args, **kwargs: function(*args, **kwargs),
    )
    monkeypatch.setattr(
        sync_stock_info,
        "_replace_full_snapshots_atomically",
        lambda _engine, snapshots: published.append(snapshots),
    )

    with pytest.raises(RuntimeError, match="preserving all previous tables"):
        sync_stock_info.sync_per_stock_tables(
            object(),
            _Info(),
            pd.DataFrame({"stock_code": ["000001", "000002"]}),
        )

    assert published == []


def test_qmt_relation_snapshot_requires_full_catalog_and_stock_coverage(monkeypatch):
    tables = {
        "concept_catalog": pd.DataFrame(
            {"concept_code": ["C1", "C2"], "source": ["qmt", "qmt"]}
        ),
        "industry_sw": pd.DataFrame(
            {"stock_code": ["000001"], "source": ["qmt"]}
        ),
        "stock_concepts": pd.DataFrame(
            {
                "stock_code": ["000001", "000002"],
                "concept_code": ["C1", "C2"],
                "source": ["qmt", "qmt"],
            }
        ),
        "stock_plates": pd.DataFrame(
            {
                "stock_code": ["000001", "000002"],
                "source": ["qmt", "qmt"],
            }
        ),
    }
    published: list[dict[str, pd.DataFrame]] = []
    monkeypatch.delenv("SI_MAX_STOCKS", raising=False)
    monkeypatch.setattr(sync_stock_info, "_use_qmt_sector_data", lambda: True)
    monkeypatch.setattr(sync_stock_info, "_qmt_sector_tables", lambda: tables)
    monkeypatch.setattr(
        sync_stock_info,
        "_replace_full_snapshots_atomically",
        lambda _engine, snapshots: published.append(snapshots),
    )

    with pytest.raises(RuntimeError, match="snapshot is incomplete"):
        sync_stock_info.sync_per_stock_tables(
            object(),
            object(),
            pd.DataFrame({"stock_code": ["000001", "000002"]}),
        )

    assert published == []


def test_trade_calendar_wrong_year_shard_preserves_full_snapshot(monkeypatch):
    class _Info:
        @staticmethod
        def trade_calendar(*, year):
            returned_year = 2025 if year == 2026 else year
            return pd.DataFrame(
                {
                    "trade_date": [f"{returned_year}-01-02"],
                    "trade_status": [1],
                    "day_week": [4],
                }
            )

    published: list[str] = []
    monkeypatch.setenv("SI_YEAR_START", "2025")
    monkeypatch.setenv("SI_YEAR_END", "2026")
    monkeypatch.setattr(sync_stock_info, "_sleep", lambda: None)
    monkeypatch.setattr(
        sync_stock_info,
        "retry_remote",
        lambda function, *args, **kwargs: function(*args, **kwargs),
    )
    monkeypatch.setattr(
        sync_stock_info,
        "_replace_full_snapshot",
        lambda _engine, _frame, table: published.append(table),
    )

    with pytest.raises(RuntimeError, match="trade calendar snapshot is incomplete"):
        sync_stock_info.sync_trade_calendar(object(), _Info())

    assert published == []


@pytest.mark.parametrize("directory", ("stock", "index", "ths"))
def test_nonempty_truncated_directory_only_updates_successful_partitions(
    monkeypatch,
    directory,
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    definitions = {
        "stock": (
            "si_all_code",
            "stock_code",
            "short_name",
            "CREATE TABLE si_all_code (stock_code TEXT PRIMARY KEY, short_name TEXT, "
            "exchange TEXT, list_date TEXT, etl_sync_at TIMESTAMP)",
        ),
        "index": (
            "si_all_index_code",
            "index_code",
            "name",
            "CREATE TABLE si_all_index_code (index_code TEXT PRIMARY KEY, concept_code TEXT, "
            "name TEXT, source TEXT, etl_sync_at TIMESTAMP)",
        ),
        "ths": (
            "si_concept_code_ths",
            "index_code",
            "name",
            "CREATE TABLE si_concept_code_ths (index_code TEXT PRIMARY KEY, concept_code TEXT, "
            "name TEXT, source TEXT, etl_sync_at TIMESTAMP)",
        ),
    }
    table, key_column, name_column, ddl = definitions[directory]
    with engine.begin() as connection:
        connection.execute(text(ddl))
        for code in ("000001", "000002", "000003"):
            if directory == "stock":
                connection.execute(
                    text(
                        "INSERT INTO si_all_code "
                        "(stock_code, short_name, exchange, list_date, etl_sync_at) "
                        "VALUES (:code, :name, 'SZ', NULL, CURRENT_TIMESTAMP)"
                    ),
                    {"code": code, "name": f"old-{code}"},
                )
            else:
                connection.execute(
                    text(
                        f"INSERT INTO {table} "
                        "(index_code, concept_code, name, source, etl_sync_at) "
                        "VALUES (:code, :concept, :name, 'old', CURRENT_TIMESTAMP)"
                    ),
                    {"code": code, "concept": f"C{code}", "name": f"old-{code}"},
                )

    monkeypatch.setattr(sync_stock_info, "_sleep", lambda: None)
    if directory == "stock":
        monkeypatch.setenv("SI_ALL_CODE_SOURCE", "adata")
        candidate = pd.DataFrame(
            [
                {
                    "stock_code": "000001",
                    "short_name": "new-000001",
                    "exchange": "SZ",
                    "list_date": None,
                }
            ]
        )
        action = lambda: sync_stock_info.sync_all_code(
            engine,
            SimpleNamespace(all_code=lambda: candidate.copy()),
        )
    elif directory == "index":
        monkeypatch.setenv("SI_ALL_INDEX_CODE_SOURCE", "adata")
        monkeypatch.setenv("SI_INDEX_PRIMARY", "sina")
        candidate = pd.DataFrame(
            [
                {
                    "index_code": "000001",
                    "concept_code": "C000001",
                    "name": "new-000001",
                    "source": "test",
                }
            ]
        )
        monkeypatch.setattr(
            sync_stock_info,
            "fetch_all_index_code_sina",
            lambda: candidate.copy(),
        )
        action = lambda: sync_stock_info.sync_all_index_code(engine, object())
    else:
        candidate = pd.DataFrame(
            [
                {
                    "index_code": "000001",
                    "concept_code": "C000001",
                    "name": "new-000001",
                    "source": "test",
                }
            ]
        )
        action = lambda: sync_stock_info.sync_concept_code_ths(
            engine,
            SimpleNamespace(all_concept_code_ths=lambda: candidate.copy()),
        )

    with pytest.raises(
        sync_stock_info.PartialSnapshotPublished,
        match="status=partial exit_code=2",
    ):
        action()

    with engine.connect() as connection:
        rows = list(
            connection.execute(
                text(
                    f"SELECT {key_column}, {name_column} FROM {table} "
                    f"ORDER BY {key_column}"
                )
            ).tuples()
        )
    assert rows == [
        ("000001", "new-000001"),
        ("000002", "old-000002"),
        ("000003", "old-000003"),
    ]


def test_sina_pagination_interruption_is_nonempty_but_not_complete(monkeypatch):
    rows = [
        {"code": f"{index:06d}", "name": f"index-{index}"}
        for index in range(1, 101)
    ]

    class _Response:
        content = json.dumps(rows).encode("utf-8")

        @staticmethod
        def raise_for_status():
            return None

    class _Session:
        def __init__(self):
            self.headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def get(url, timeout):
            del timeout
            if "page=1&" in url:
                return _Response()
            raise sync_stock_info.ConnectionError("page interrupted")

    import requests

    monkeypatch.setenv("SI_INDEX_SINA_MAX_PAGES", "3")
    monkeypatch.setattr(requests, "Session", _Session)
    monkeypatch.setattr(sync_stock_info.time, "sleep", lambda _seconds: None)

    frame = sync_stock_info.fetch_all_index_code_sina()
    evidence = frame.attrs[sync_stock_info._COMPLETENESS_ATTR]

    assert len(frame) == 100
    assert evidence["complete"] is False
    assert evidence["pages_contiguous"] is False
    assert evidence["terminal_page"] is False
    assert "page 2" in evidence["failure"]

    # Explicit interrupted-page evidence cannot be overridden merely because
    # the returned identities happen to equal yesterday's baseline.
    monkeypatch.setattr(
        sync_stock_info,
        "_read_directory_baseline",
        lambda *_args, **_kwargs: frame.copy(),
    )
    complete, reason = sync_stock_info._snapshot_completeness_reason(
        object(),
        frame,
        "si_all_index_code",
        ("index_code",),
        evidence=evidence,
    )
    assert complete is False
    assert "explicitly incomplete" in reason


def test_partial_partition_insert_failure_rolls_back_its_delete():
    engine = _sqlite_table_with_old_row()
    invalid_partition = pd.DataFrame(
        {
            "stock_code": ["000001", "000001"],
            "short_name": ["duplicate-a", "duplicate-b"],
        }
    )

    with pytest.raises(DatabaseError):
        sync_stock_info._replace_directory_partitions(
            engine,
            invalid_partition,
            "si_atomic_fixture",
            ("stock_code",),
        )

    assert _fixture_rows(engine) == [("000001", "旧快照")]


def test_ths_partition_delete_matches_each_returned_code_tuple_not_column_unions():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE si_concept_code_ths ("
                "index_code TEXT PRIMARY KEY, concept_code TEXT, name TEXT, "
                "source TEXT, etl_sync_at TIMESTAMP)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO si_concept_code_ths "
                "(index_code, concept_code, name, source, etl_sync_at) VALUES "
                "('885001', '301001', 'old-name', 'old', CURRENT_TIMESTAMP), "
                "('885002', '301002', 'shared-name', 'old', CURRENT_TIMESTAMP)"
            )
        )

    sync_stock_info._replace_directory_partitions(
        engine,
        pd.DataFrame(
            [
                {
                    "index_code": "885001",
                    "concept_code": "301001",
                    "name": "shared-name",
                    "source": "new",
                    "etl_sync_at": pd.Timestamp("2026-08-25 12:00:00"),
                }
            ]
        ),
        "si_concept_code_ths",
        ("index_code", "concept_code", "name"),
    )

    with engine.connect() as connection:
        rows = list(
            connection.execute(
                text(
                    "SELECT index_code, concept_code, name, source "
                    "FROM si_concept_code_ths ORDER BY index_code"
                )
            ).tuples()
        )
    assert rows == [
        ("885001", "301001", "shared-name", "new"),
        ("885002", "301002", "shared-name", "old"),
    ]


def test_directory_publishers_do_not_use_fixed_row_counts_as_completeness():
    assert "SI_ALL_CODE_MIN_ROWS" not in _source(sync_stock_info.sync_all_code)
    for function in (
        sync_stock_info.sync_all_code,
        sync_stock_info.sync_all_index_code,
        sync_stock_info.sync_concept_code_ths,
    ):
        assert "_publish_directory_snapshot(" in _source(function)


def test_historical_baseline_never_authorizes_even_one_missing_identity(monkeypatch):
    baseline = pd.DataFrame(
        {
            "stock_code": [f"{index:06d}" for index in range(1, 1001)],
            "etl_sync_at": [pd.Timestamp.now()] * 1000,
        }
    )
    candidate = baseline.iloc[:-1][["stock_code"]].copy()
    monkeypatch.setattr(
        sync_stock_info,
        "_read_directory_baseline",
        lambda *_args, **_kwargs: baseline,
    )

    complete, reason = sync_stock_info._snapshot_completeness_reason(
        object(),
        candidate,
        "si_all_code",
        ("stock_code",),
    )

    assert complete is False
    assert "removed=1" in reason


def test_partial_step_is_recorded_as_nonzero():
    failures: list[tuple[str, int]] = []

    def _partial():
        raise sync_stock_info.PartialSnapshotPublished(
            table="si_all_index_code",
            frame=pd.DataFrame({"index_code": ["000001"]}),
            reason="interrupted pagination",
        )

    assert sync_stock_info._step("指数列表", _partial, _failures=failures) is None
    assert failures == [("指数列表", 2)]


@pytest.mark.parametrize(
    ("runner_name", "sync_name", "source_env"),
    (
        ("run_si_all_code", "sync_all_code", "SI_ALL_CODE_SOURCE"),
        (
            "run_si_all_index_code",
            "sync_all_index_code",
            "SI_ALL_INDEX_CODE_SOURCE",
        ),
    ),
)
def test_single_table_qmt_partial_returns_nonzero_without_external_fallback(
    monkeypatch,
    runner_name,
    sync_name,
    source_env,
):
    from tools import run_single_table

    calls = 0

    def _partial(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise sync_stock_info.PartialSnapshotPublished(
            table="si_directory",
            frame=pd.DataFrame({"code": ["000001"]}),
            reason="source set not proven complete",
        )

    monkeypatch.setenv(source_env, "qmt")
    monkeypatch.setattr(run_single_table, "_si_engine_info", lambda: (object(), object()))
    monkeypatch.setattr(sync_stock_info, sync_name, _partial)

    assert getattr(run_single_table, runner_name)() == 2
    assert calls == 1
