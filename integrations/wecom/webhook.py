# -*- coding: utf-8 -*-
"""
企微群机器人 Webhook（markdown）。

文档：https://developer.work.weixin.qq.com/document/path/91770
"""
from __future__ import annotations

import httpx


class WeComWebhookError(RuntimeError):
    """企微返回非 0 errcode 或 HTTP 异常。"""


def send_markdown(webhook_url: str, content: str, timeout: float = 15.0) -> dict:
    """
    向指定 Webhook URL 发送一条 Markdown 消息。

    :param webhook_url: 完整 URL，含 ``key=``
    :param content: Markdown 正文（注意企微对 markdown 子集有限制）
    :return: 企微 JSON 响应，正常为 ``{"errcode":0,"errmsg":"ok"}``
    """
    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    try:
        with httpx.Client(timeout=timeout) as client:
            res = client.post(webhook_url, json=payload)
            res.raise_for_status()
            data = res.json()
    except httpx.HTTPError as e:
        raise WeComWebhookError(f"HTTP 请求失败: {e}") from e

    errcode = data.get("errcode")
    if errcode not in (None, 0):
        raise WeComWebhookError(data.get("errmsg") or str(data))
    return data
