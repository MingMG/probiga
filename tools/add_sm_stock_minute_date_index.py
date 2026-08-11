from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from tools.env_config import load_project_env


def main() -> int:
    load_project_env()
    engine = create_batch_engine(future=True)
    index_name = "idx_smm_date_source_code_time"
    with engine.begin() as conn:
        exists = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND table_name = 'sm_stock_minute'
                      AND index_name = :index_name
                    """
                ),
                {"index_name": index_name},
            ).scalar()
            or 0
        )
        if exists:
            print({"status": "exists", "index": index_name}, flush=True)
            return 0
        print({"status": "creating", "index": index_name}, flush=True)
        conn.execute(
            text(
                """
                ALTER TABLE sm_stock_minute
                ADD INDEX idx_smm_date_source_code_time
                    (trade_date, data_source, stock_code, trade_time)
                """
            )
        )
        print({"status": "created", "index": index_name}, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
