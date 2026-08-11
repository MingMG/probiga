#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ensure scheduler-table compatibility columns on a target MySQL database."""

import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from env_config import create_tool_engine, resolve_tool_mysql_url
from remote_support import remote_host
from server.common.scheduler_tasks import ensure_scheduler_columns


def _mysql_url() -> str:
    if os.environ.get("MYSQL_URL"):
        return os.environ["MYSQL_URL"]

    password = os.environ.get("PROBIGA_REMOTE_MYSQL_PASSWORD", "")
    if password:
        host = os.environ.get("PROBIGA_REMOTE_MYSQL_HOST", remote_host())
        port = int(os.environ.get("PROBIGA_REMOTE_MYSQL_PORT", "3306"))
        user = os.environ.get("PROBIGA_REMOTE_MYSQL_USER", "root")
        database = os.environ.get("PROBIGA_REMOTE_MYSQL_DATABASE", "probiga")
        return (
            f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}?charset=utf8mb4"
        )

    return resolve_tool_mysql_url()


def main() -> None:
    engine = create_tool_engine(_mysql_url())
    columns = ensure_scheduler_columns(engine)
    print("scheduler columns ensured:")
    for column in sorted(columns):
        print(f"  {column}")


if __name__ == "__main__":
    main()
