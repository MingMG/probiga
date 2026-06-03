import os, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

os.environ["MYSQL_URL"] = "mysql+pymysql://root:ProBigA%4070966@47.113.123.190:3306/probiga?charset=utf8mb4"

from sqlalchemy import create_engine
from tools.merge_hot_rank import run_single_day, run_multi_day

url = os.environ["MYSQL_URL"]
engine = create_engine(url, pool_pre_ping=True)
run_single_day(engine, "2026-05-09", 100, True)
for days in [3, 5]:
    run_multi_day(engine, "2026-05-09", days, 100, True)
