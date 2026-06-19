# -*- coding: utf-8 -*-
"""
数据分析平台后端（FastAPI）。

启动（在仓库根目录）::

    uvicorn server.api.main:app --reload --host 0.0.0.0 --port 8000

独立调度（推荐小内存服务器使用）::

    python tools/run_scheduler_daemon.py

说明：包名使用 ``server`` 而非 ``platform``，避免与 Python 标准库 ``platform`` 冲突。
"""
