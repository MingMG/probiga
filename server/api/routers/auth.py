# -*- coding: utf-8 -*-
"""Single-operator authentication status.

Account registration, login sessions, user management, and reviewer roles are
not part of the single-operator deployment.  The optional admin token is
validated by the request middleware.
"""
from __future__ import annotations

from fastapi import APIRouter

from server.api.admin_auth import admin_auth_status


router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/status")
def auth_status():
    return {
        "status": "ok",
        "authenticated": True,
        "registration_open": False,
        "user_initialized": True,
        "user_count": 1,
        "session_hours": 0,
        **admin_auth_status(),
    }
