#!/usr/bin/env python3
"""上传资金流向修复文件到服务器并重启服务"""
import os
import sys
from pathlib import Path

import paramiko

ROOT_PATH = Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from tools.remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)

ROOT = os.fspath(ROOT_PATH)
LEGACY_DEPLOY_OVERRIDE_ENV = "PROBIGA_ALLOW_LEGACY_DEPLOY"
LEGACY_DEPLOY_OVERRIDE_SENTINEL = (
    "I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES"
)

files = [
    'tools/sync_capital_flow_batch.py',
    'tools/sync_capital_flow_push2delay.py',
    'tools/sync_capital_flow_ths.py',
    'server/engine/data_loader.py',
    'server/api/routers/hot_data.py',
    'adata/adata/common/utils/unit_conver.py',
]


def _require_legacy_deploy_override() -> None:
    if os.environ.get(LEGACY_DEPLOY_OVERRIDE_ENV) != LEGACY_DEPLOY_OVERRIDE_SENTINEL:
        raise SystemExit(
            "Legacy deploy blocked. Set "
            f"{LEGACY_DEPLOY_OVERRIDE_ENV}={LEGACY_DEPLOY_OVERRIDE_SENTINEL} "
            "to override."
        )


def main() -> None:
    _require_legacy_deploy_override()
    remote_root_path = remote_root()
    ssh = production_ssh_client(paramiko)
    sftp = None
    try:
        ssh.connect(**production_ssh_connect_kwargs(timeout=30))

        # 确保远程目录存在
        dirs = set()
        for f in files:
            dirs.add(os.path.dirname(f))
        for d in dirs:
            stdin, stdout, stderr = ssh.exec_command(
                f'mkdir -p {remote_root_path}/{d}', timeout=30
            )
            stdout.channel.recv_exit_status()

        sftp = ssh.open_sftp()
        for f in files:
            local = os.path.join(ROOT, f)
            remote = f'{remote_root_path}/{f}'
            print(f'  上传: {f}')
            sftp.put(local, remote)
            print(f'  OK')

        # 重启服务
        print('重启 probiga 服务...')
        stdin, stdout, stderr = ssh.exec_command(
            'systemctl restart probiga && sleep 2 && systemctl is-active probiga',
            timeout=60,
        )
        print(stdout.read().decode().strip())
        err = stderr.read().decode().strip()
        if err:
            print(f'stderr: {err}')
    finally:
        if sftp is not None:
            sftp.close()
        ssh.close()

    print('完成!')


if __name__ == "__main__":
    main()
