本目录下每个 sync_<表名>.cmd 对应一张表的一次同步（双击前请先安装依赖并在本机配置好 MYSQL_URL）。

run_single_table.py 已把项目根加入 sys.path，并给子进程设置 PYTHONPATH；
可在任意当前目录执行: python E:\My Code\ProBigA\tools\run_single_table.py si_all_index_code

统一入口（推荐在「项目根」PowerShell 里执行）:
  python tools\run_single_table.py <表名>
  python tools\run_single_table.py --list
  重新生成本目录全部 .cmd:
  python tools\run_single_table.py --write-windows-cmds
  按固定顺序跑完本脚本支持的全部表（耗时可很长）:
  python tools\run_single_table.py --run-all
  新浪指数 + --run-all 一键:
  tools\run_all_single_tables.cmd
  或 powershell -ExecutionPolicy Bypass -File tools\run_all_single_tables.ps1
  说明汇总见 tools\RUN_SINGLE_TABLES.txt

前置:
  - pip install -e ./adata
  - pip install -r requirements-platform.txt
  - 多数 sm_* 依赖 si_all_code（全市场股票码）；指数/东财概念类还依赖 si_all_index_code、si_concept_code_east 等，若为空请先跑对应脚本或完整 stock_info。
