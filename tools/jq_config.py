# -*- coding: utf-8 -*-
"""聚宽数据源配置和连接管理"""
import importlib
import logging
import os

logger = logging.getLogger(__name__)

_authed = False
_jq_module = None


def get_jq_client(required: bool = True):
    """Lazy-load jqdatasdk so the main API can boot without this optional dependency."""
    global _jq_module
    if _jq_module is not None:
        return _jq_module
    try:
        _jq_module = importlib.import_module("jqdatasdk")
    except ModuleNotFoundError:
        if required:
            raise RuntimeError("未安装 jqdatasdk，聚宽分钟数据功能不可用。请先执行: pip install jqdatasdk") from None
        return None
    return _jq_module


def _jq_credentials() -> tuple[str, str]:
    phone = (os.environ.get("JQ_PHONE") or "").strip()
    password = (os.environ.get("JQ_PASSWORD") or "").strip()
    if not phone or not password:
        raise RuntimeError("未配置 JQ_PHONE / JQ_PASSWORD，无法使用聚宽数据功能。请在 .env 或环境变量中设置。")
    return phone, password


def jq_auth():
    global _authed
    jq = get_jq_client(required=True)
    phone, password = _jq_credentials()
    if _authed:
        return jq
    jq.auth(phone, password)
    _authed = True
    remaining = jq.get_query_count()
    logger.info("聚宽认证成功，今日剩余查询条数: %s", remaining)
    return jq


def jq_logout():
    global _authed
    jq = get_jq_client(required=False)
    if _authed and jq is not None:
        jq.logout()
    _authed = False


def jq_check_quota():
    jq = jq_auth()
    return jq.get_query_count()


def jq_normalize_code(stock_code: str) -> str:
    jq = jq_auth()
    return str(jq.normalize_code(stock_code))


def _jq_code(stock_code: str) -> str:
    c = str(stock_code).strip().zfill(6)
    if c.startswith(('6', '9')):
        return f"{c}.XSHG"
    return f"{c}.XSHE"


def _from_jq_code(jq_code: str) -> str:
    return str(jq_code).split('.')[0].zfill(6)
