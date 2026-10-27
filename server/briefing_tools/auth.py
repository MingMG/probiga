# -*- coding: utf-8 -*-
"""Independent bearer authentication for the AI briefing tool surface."""
from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from server.common.config import get_briefing_tools_config


bearer_scheme = HTTPBearer(auto_error=False)


def require_briefing_tools_access(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Security(bearer_scheme),
    ] = None,
) -> None:
    config = get_briefing_tools_config()
    if not bool(config["enabled"]):
        raise HTTPException(
            status_code=503,
            detail={
                "error": "briefing_tools_disabled",
                "message": "A-share briefing tools are not enabled.",
            },
        )
    expected = str(config.get("token") or "")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "briefing_tools_token_missing",
                "message": "PROBIGA_BRIEFING_TOOLS_TOKEN is required.",
            },
        )
    supplied = credentials.credentials.strip() if credentials is not None else ""
    if (
        credentials is None
        or credentials.scheme.lower() != "bearer"
        or not supplied
        or not secrets.compare_digest(supplied, expected)
    ):
        raise HTTPException(
            status_code=401,
            detail={
                "error": "briefing_tools_token_required",
                "message": "A valid briefing-tools bearer token is required.",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )
    request.state.briefing_tool_auth = "dedicated_bearer"


__all__ = ["require_briefing_tools_access"]
