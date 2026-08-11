# -*- coding: utf-8 -*-
"""Compatibility import for remote maintenance scripts."""
from tools.remote_support import (
    DEFAULT_REMOTE_ROOT,
    DEFAULT_REMOTE_SSH_HOST,
    DEFAULT_REMOTE_SSH_USER,
    DEFAULT_SSH_AUTH_TIMEOUT_SECONDS,
    DEFAULT_SSH_BANNER_TIMEOUT_SECONDS,
    DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS,
    UnsafeProductionSshError,
    UnsafeRemoteRuntimeError,
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_host,
    remote_pythonpath,
    remote_root,
    remote_user,
    ssh_connect_kwargs,
)
