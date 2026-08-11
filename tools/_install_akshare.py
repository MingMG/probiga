#!/usr/bin/env python3
import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)


def main() -> None:
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs(timeout=10))

    cmd = f'cd {remote_root()} && source venv/bin/activate && pip install akshare -q 2>&1 | tail -5'
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
    stdout.channel.settimeout(120)
    print(stdout.read().decode().strip())
    err = stderr.read().decode().strip()
    if err:
        print('STDERR:', err[-500:])
    ssh.close()


if __name__ == "__main__":
    main()
