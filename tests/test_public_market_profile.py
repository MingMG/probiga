from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text

from tools import apply_public_market_profile as profile


def test_env_profile_updates_only_owned_keys_and_is_idempotent(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MYSQL_URL=secret-value\n"
        "DATA_SOURCE_KLINE=bigqmt\n"
        "BIG_QMT_BRIDGE_ENABLED=true\n"
        "UNRELATED=keep-me\n",
        encoding="utf-8",
    )

    changed = profile.update_env_profile(env_file)

    assert "DATA_SOURCE_KLINE" in changed
    assert "BIG_QMT_BRIDGE_ENABLED" in changed
    text_value = env_file.read_text(encoding="utf-8")
    assert "MYSQL_URL=secret-value" in text_value
    assert "UNRELATED=keep-me" in text_value
    assert "DATA_SOURCE_KLINE=adata" in text_value
    assert "BIG_QMT_BRIDGE_ENABLED=false" in text_value
    assert profile.update_env_profile(env_file) == []


def _database():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_scheduled_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    group_name TEXT,
                    script_path TEXT NOT NULL,
                    script_args TEXT,
                    cron_time TEXT,
                    interval_minutes INTEGER,
                    enabled INTEGER NOT NULL,
                    date_param TEXT,
                    sort_order INTEGER,
                    description TEXT,
                    updated_at TEXT
                )
                """
            )
        )
    return engine


def test_task_profile_retires_qmt_and_enables_public_tasks():
    engine = _database()
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO st_scheduled_tasks
                        (task_name,task_type,script_path,enabled)
                    VALUES
                        ('QMT membership','qmt_membership_snapshot',
                         'tools/sync_bigqmt_reference.py',1)
                    """
                )
            )
            result = profile.apply_task_profile(connection)
            second = profile.apply_task_profile(connection)
            qmt = connection.execute(
                text(
                    "SELECT enabled FROM st_scheduled_tasks "
                    "WHERE task_type='qmt_membership_snapshot'"
                )
            ).scalar_one()
            public_rows = connection.execute(
                text(
                    "SELECT task_type,enabled FROM st_scheduled_tasks "
                    "WHERE enabled=1"
                )
            ).mappings().all()
        assert qmt == 0
        assert result["retired"] == [
            {"id": 1, "task_type": "qmt_membership_snapshot"}
        ]
        assert second["retired"] == []
        assert {
            row["task_type"] for row in public_rows
        } == profile.PUBLIC_TASK_TYPES
        assert all(row["enabled"] == 1 for row in public_rows)
    finally:
        engine.dispose()


def test_profile_keeps_non_qmt_upper_evidence_task_active():
    assert (
        "analysis_upper_evidence_prepare"
        not in profile.RETIRED_QMT_TASK_TYPES
    )
