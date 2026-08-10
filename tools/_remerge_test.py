import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from tools.env_config import create_tool_engine, resolve_tool_mysql_url
from tools.merge_hot_rank import run_single_day, run_multi_day


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())
    run_single_day(engine, "2026-05-09", 100, True)
    for days in [3, 5]:
        run_multi_day(engine, "2026-05-09", days, 100, True)


if __name__ == "__main__":
    main()
