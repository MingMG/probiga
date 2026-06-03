# -*- coding: utf-8 -*-
"""企业微信：群机器人 Webhook、后续可扩展应用消息等。"""

from integrations.wecom.webhook import WeComWebhookError, send_markdown

__all__ = ["WeComWebhookError", "send_markdown"]
