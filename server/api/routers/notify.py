# -*- coding: utf-8 -*-
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from integrations.wecom.webhook import WeComWebhookError, send_markdown
from server.common.config import Settings, get_settings

router = APIRouter(tags=["notify"])


class WeComTestBody(BaseModel):
    content: str = Field(..., min_length=1, max_length=4000, description="markdown 正文")


@router.post("/notify/wecom-test")
def wecom_test(
    body: WeComTestBody,
    settings: Settings = Depends(get_settings),
):
    """
    使用已配置的 ``WECOM_WEBHOOK_URL`` 发送一条群机器人 Markdown 消息（用于联调）。
    """
    if not settings.wecom_webhook_url:
        raise HTTPException(
            status_code=400,
            detail="未配置 WECOM_WEBHOOK_URL，请在 .env 中填写企微机器人 Webhook 完整地址",
        )
    try:
        return send_markdown(settings.wecom_webhook_url, body.content)
    except WeComWebhookError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
