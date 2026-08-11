#!/usr/bin/env python3
import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)
import os
import time

LEGACY_DEPLOY_OVERRIDE_ENV = "PROBIGA_ALLOW_LEGACY_DEPLOY"
LEGACY_DEPLOY_OVERRIDE_SENTINEL = (
    "I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES"
)

SHELL_SCRIPT = '''#!/bin/bash
echo "$(date) - Waiting 10 minutes for rate limit cooldown..."
sleep 600
echo "$(date) - Starting K-line incremental sync..."
cd {root}
source venv/bin/activate
export PYTHONPATH={pythonpath}
export SM_MAX_STOCKS=0
export SM_MAX_WORKERS=1
export SM_REQUEST_SLEEP=0.5
python -m biz.stock_market.sync_stock_market --only stock_kline \\
    --kline-start 2026-04-28 --kline-end 2026-05-08 \\
    --kline-incremental
echo "$(date) - K-line sync done."
'''


def _require_legacy_deploy_override() -> None:
    if os.environ.get(LEGACY_DEPLOY_OVERRIDE_ENV) != LEGACY_DEPLOY_OVERRIDE_SENTINEL:
        raise SystemExit(
            "Legacy deploy blocked. Set "
            f"{LEGACY_DEPLOY_OVERRIDE_ENV}={LEGACY_DEPLOY_OVERRIDE_SENTINEL} "
            "to override."
        )


def main() -> None:
    _require_legacy_deploy_override()
    remote_pythonpath(remote_root())
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs(timeout=10))
    root = remote_root()

    stdin, stdout, stderr = ssh.exec_command(
        "ps aux | grep 'sync_stock_market' | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null; echo 'killed all sync processes'",
        timeout=10
    )
    stdout.channel.settimeout(10)
    print(stdout.read().decode().strip())

    sftp = ssh.open_sftp()
    local_path = os.path.join(os.path.dirname(__file__), '..', 'biz', 'stock_market', 'sync_stock_market.py')
    remote_path = f'{root}/biz/stock_market/sync_stock_market.py'
    sftp.put(os.path.abspath(local_path), remote_path)

    with sftp.open('/tmp/run_kline_delayed.sh', 'w') as f:
        f.write(SHELL_SCRIPT.format(root=root, pythonpath=remote_pythonpath(root)))
    sftp.close()

    chan = ssh.get_transport().open_session()
    chan.settimeout(15)
    chan.exec_command('nohup bash /tmp/run_kline_delayed.sh > /tmp/kline_incremental.log 2>&1 & echo "PID=$!"')
    time.sleep(3)
    out = chan.recv(4096).decode().strip()
    print(out)
    chan.close()
    ssh.close()
    print('Delayed K-line sync scheduled (10 min cooldown)')


if __name__ == "__main__":
    main()
