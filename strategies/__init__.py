# -*- coding: utf-8 -*-
"""
策略与回测（占位）。

建议约定：只读你方库表 / Parquet，不直接依赖 ``biz`` 里的采集脚本；
信号产出、绩效汇总可通过 ``server`` API 或 ``integrations.wecom`` 通知。
后续可在此目录接入 vectorbt、backtrader 等，按需增加 ``requirements-strategies.txt``。
"""
