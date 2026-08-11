#!/usr/bin/env python3
import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)
import os

LEGACY_DEPLOY_OVERRIDE_ENV = "PROBIGA_ALLOW_LEGACY_DEPLOY"
LEGACY_DEPLOY_OVERRIDE_SENTINEL = (
    "I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES"
)


def _require_legacy_deploy_override() -> None:
    if os.environ.get(LEGACY_DEPLOY_OVERRIDE_ENV) != LEGACY_DEPLOY_OVERRIDE_SENTINEL:
        raise SystemExit(
            "Legacy deploy blocked. Set "
            f"{LEGACY_DEPLOY_OVERRIDE_ENV}={LEGACY_DEPLOY_OVERRIDE_SENTINEL} "
            "to override."
        )


def main() -> None:
    _require_legacy_deploy_override()
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs())

    sftp = ssh.open_sftp()

    local_path = os.path.join(os.path.dirname(__file__), '..', 'biz', 'stock_market', 'sync_stock_market.py')
    root = remote_root()
    remote_path = f'{root}/biz/stock_market/sync_stock_market.py'

    print(f'Uploading {local_path} -> {remote_path}')
    sftp.put(os.path.abspath(local_path), remote_path)
    print('Upload done.')
    sftp.close()

    stdin, stdout, stderr = ssh.exec_command(
        f'cd {root} && source venv/bin/activate && python -c "import biz.stock_market.sync_stock_market; print(\'import OK\')"',
        timeout=30
    )
    out = stdout.read().decode()
    err = stderr.read().decode()
    print(out)
    if err:
        print('ERR:', err[:1000])
    ssh.close()


if __name__ == "__main__":
    main()
