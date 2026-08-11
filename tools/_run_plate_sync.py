import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
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
    remote = remote_root()

    sftp.put(os.fspath(ROOT / "tools" / "_sync_plates_for_fused.py"), f"{remote}/tools/_sync_plates_for_fused.py")
    print('OK: _sync_plates_for_fused.py uploaded')
    sftp.close()

    print('--- 运行板块数据同步 ---')
    stdin, stdout, stderr = ssh.exec_command(
        f'cd {remote} && {remote}/venv/bin/python tools/_sync_plates_for_fused.py',
        timeout=600
    )
    out = stdout.read().decode()
    err = stderr.read().decode()
    print(out)
    if err:
        print('STDERR:', err[:1000])

    ssh.close()
    print('Done!')


if __name__ == "__main__":
    main()
