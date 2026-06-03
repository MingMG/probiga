# -*- coding: utf-8 -*-
"""聚宽数据源配置和连接管理"""
import os
import logging
import jqdatasdk as jq
from contextlib import contextmanager

logger = logging.getLogger(__name__)

JQ_PHONE = os.environ.get("JQ_PHONE", "15271897120")
JQ_PASSWORD = os.environ.get("JQ_PASSWORD", "Qwer1234")

_authed = False


def jq_auth():
    global _authed
    if _authed:
        return
    jq.auth(JQ_PHONE, JQ_PASSWORD)
    _authed = True
    remaining = jq.get_query_count()
    logger.info("聚宽认证成功，今日剩余查询条数: %s", remaining)


def jq_logout():
    global _authed
    if _authed:
        jq.logout()
        _authed = False


def jq_check_quota():
    jq_auth()
    return jq.get_query_count()


def _jq_code(stock_code: str) -> str:
    c = str(stock_code).strip().zfill(6)
    if c.startswith(('6', '9')):
        return f"{c}.XSHG"
    return f"{c}.XSHE"


def _from_jq_code(jq_code: str) -> str:
    return str(jq_code).split('.')[0].zfill(6)
