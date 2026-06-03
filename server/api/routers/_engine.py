# -*- coding: utf-8 -*-
import os
from pathlib import Path as _Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

_ENGINE: Engine | None = None


def _read_dotenv_mysql_url() -> str:
    """从 .env 文件读取 MYSQL_URL"""
    for p in [_Path(__file__).resolve().parents[3] / ".env", _Path("/opt/ProBigA/.env")]:
        if p.is_file():
            for line in open(p):
                line = line.strip()
                if line.startswith("MYSQL_URL="):
                    return line.split("=", 1)[1].strip()
    return ""


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        url = os.environ.get("MYSQL_URL") or _read_dotenv_mysql_url() or DEFAULT_MYSQL_URL
        _ENGINE = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=5,
            pool_recycle=3600,
        )
    return _ENGINE
