#!/usr/bin/env python3
"""Retired direct-to-production uploader retained as an explicit safety fence."""
from __future__ import annotations

import os

_REQUIRED_SSH_ENV = (
    "PROBIGA_REMOTE_SSH_HOST",
    "PROBIGA_REMOTE_SSH_USER",
    "PROBIGA_REMOTE_SSH_KEY_FILE",
    "PROBIGA_SSH_KNOWN_HOSTS",
)

missing = [name for name in _REQUIRED_SSH_ENV if not os.environ.get(name, "").strip()]
if missing:
    raise SystemExit("missing required key-only SSH configuration")
raise SystemExit(
    "legacy mutable-file production upload is retired; "
    "use the audited production deployment workflow"
)
