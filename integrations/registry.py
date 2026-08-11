# -*- coding: utf-8 -*-
"""数据源注册表 — 统一管理所有数据源后端。

用法::

    from integrations.registry import get_backend, resolve_source

    # 获取当前配置的 K线数据源后端
    backend = get_backend("kline")
    if backend is not None:
        df = backend.fetch_kline(codes, "2020-01-01", "2026-06-15")
    else:
        # backend 为 None 表示走 adata 传统路径
        _step_stock_kline_adata(...)

添加新后端::

    # 1. 在 integrations/<name>/backend.py 中实现
    # 2. 文件末尾注册：
    from integrations.registry import register
    register("name", lambda: MyBackend())
    # 3. 在 _load_backends() 中添加 try-import
"""
from __future__ import annotations

import logging
import os

from integrations.base import DataSourceBackend

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 默认数据源（与现有行为一致）
# ---------------------------------------------------------------------------
_DEFAULT_SOURCES: dict[str, str] = {
    "kline": "adata",
    "minute": "adata",
    "current": "adata",
}

# 别名 -> 规范名 映射
_SOURCE_ALIASES: dict[str, str] = {
    "gm": "myquant",
    "goldminer": "myquant",
    "emquant": "myquant",
    "east": "akshare",
    "em": "akshare",
    "eastmoney": "akshare",
    "qmt_big": "bigqmt",
    "big_qmt": "bigqmt",
    "ohlc": "myquant",      # minute_data.py 把 ohlc 当 myquant
    "gml": "joinquant",
    "jq": "joinquant",
}

# ---------------------------------------------------------------------------
# 后端工厂注册表：canonical_name -> factory callable
# ---------------------------------------------------------------------------
_BACKEND_FACTORIES: dict[str, callable] = {}


def register(name: str, factory) -> None:
    """注册一个后端工厂。在各 backend 模块加载时调用。"""
    _BACKEND_FACTORIES[name] = factory
    logger.debug("数据源后端已注册: %s", name)


def _resolve_source_name(data_type: str) -> str:
    """解析某数据类型当前配置的数据源名。

    优先级：
      1. DATA_SOURCE_{TYPE}       （新命名）
      2. SM_STOCK_{TYPE}_SOURCE   （旧命名，向后兼容）
      3. SM_MARKET_DATA_SOURCE    （旧命名，minute/current 的 fallback）
      4. _DEFAULT_SOURCES 默认值
    """
    env_candidates = [
        f"DATA_SOURCE_{data_type.upper()}",
        f"SM_STOCK_{data_type.upper()}_SOURCE",
    ]
    if data_type in ("minute", "current"):
        env_candidates.append("SM_MARKET_DATA_SOURCE")

    for env_name in env_candidates:
        raw = os.environ.get(env_name, "").strip().lower()
        if raw:
            canonical = _SOURCE_ALIASES.get(raw, raw)
            return canonical

    # API and standalone processes load project .env through pydantic settings,
    # which intentionally does not mutate os.environ.  Read the corresponding
    # typed setting so datasource selection is consistent in both launch modes.
    try:
        from server.common.config import get_settings

        raw = str(getattr(get_settings(), f"data_source_{data_type}", "") or "").strip().lower()
        if raw:
            return _SOURCE_ALIASES.get(raw, raw)
    except Exception:
        logger.debug("failed to resolve configured datasource for %s", data_type, exc_info=True)

    return _DEFAULT_SOURCES.get(data_type, "adata")


def resolve_source(data_type: str) -> str:
    """返回某数据类型解析后的数据源名（用于日志）。"""
    return _resolve_source_name(data_type)


def has_support(data_type: str) -> bool:
    """检查当前配置的数据源是否支持指定数据类型。"""
    source = _resolve_source_name(data_type)
    if source == "adata":
        return False  # adata 走传统路径
    factory = _BACKEND_FACTORIES.get(source)
    if factory is None:
        return False
    try:
        backend = factory()
        return hasattr(backend, f"fetch_{data_type}")
    except Exception:
        return False


def get_backend(data_type: str) -> DataSourceBackend | None:
    """获取当前配置的数据源后端实例。

    返回 None 表示应走 adata 传统代码路径（向后兼容）。
    返回 DataSourceBackend 实例表示应使用新的统一接口。
    后端不可用时抛出 RuntimeError。
    """
    source = _resolve_source_name(data_type)

    # adata 走传统路径：返回 None 让调用方 fall through
    if source == "adata":
        return None

    factory = _BACKEND_FACTORIES.get(source)
    if factory is None:
        available = sorted(_BACKEND_FACTORIES.keys())
        raise RuntimeError(
            f"未知数据源 '{source}'（{data_type}）。"
            f"已注册: {available}"
        )

    backend = factory()
    if not hasattr(backend, f"fetch_{data_type}"):
        raise RuntimeError(
            f"后端 '{source}' 不支持 {data_type} 数据"
        )
    return backend


# ---------------------------------------------------------------------------
# 后端模块加载（触发各模块的 register() 调用）
# ---------------------------------------------------------------------------
def _load_backends() -> None:
    """延迟加载后端模块，触发注册。"""
    try:
        import integrations.myquant.backend  # noqa: F401
    except Exception as exc:
        logger.debug("MyQuant 后端未加载: %s", exc)

    try:
        import integrations.qmt.backend  # noqa: F401
    except Exception as exc:
        logger.debug("QMT 后端未加载: %s", exc)

    try:
        import integrations.bigqmt.backend  # noqa: F401
    except Exception as exc:
        logger.debug("Big QMT backend was not loaded: %s", exc)

    try:
        import integrations.akshare.backend  # noqa: F401
    except Exception as exc:
        logger.debug("AkShare 后端未加载: %s", exc)


_load_backends()
