#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Create a pre-migration schema snapshot for V2 tables and scheduler metadata."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.repository import V2_TABLES
from tools.env_config import load_project_env


def main() -> int:
    load_project_env()
    engine = create_batch_engine()
    tables = (*V2_TABLES, "st_job_v2", "st_strategy_lifecycle_event_v2", "st_scheduled_tasks")
    snapshot: dict[str, object] = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "database_version": "",
        "tables": {},
    }
    with engine.connect() as connection:
        snapshot["database_version"] = str(
            connection.execute(text("SELECT VERSION()")).scalar() or ""
        )
        for table in tables:
            exists = bool(
                connection.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table
                        """
                    ),
                    {"table": table},
                ).scalar()
            )
            if not exists:
                snapshot["tables"][table] = {"exists": False}
                continue
            create_row = connection.execute(
                text(f"SHOW CREATE TABLE `{table}`")
            ).first()
            row_count = int(
                connection.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
                or 0
            )
            snapshot["tables"][table] = {
                "exists": True,
                "row_count": row_count,
                "create_sql": str(create_row[1] if create_row else ""),
            }
    backup_dir = ROOT / "data" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    output = backup_dir / (
        "trading_v2_schema_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"
    )
    output.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "output": str(output),
                "table_count": len(tables),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
