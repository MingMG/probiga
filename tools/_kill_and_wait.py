#!/usr/bin/env python3
import paramiko
from remote_support import production_ssh_client, production_ssh_connect_kwargs
import time


def main() -> None:
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs(timeout=10))

    stdin, stdout, stderr = ssh.exec_command('kill 61378 2>/dev/null; echo "killed"', timeout=10)
    print(stdout.read().decode().strip())

    print('Waiting 3 minutes for rate limit cooldown...')
    ssh.close()


if __name__ == "__main__":
    main()
