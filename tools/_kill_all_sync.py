#!/usr/bin/env python3
import paramiko
from remote_support import production_ssh_client, production_ssh_connect_kwargs

cmds = [
    "ps aux | grep 'sync_stock_market' | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null; echo 'killed'",
    "ps aux | grep 'run_kline' | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null; echo 'killed wrapper'",
]

def main() -> None:
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs(timeout=10))

    for cmd in cmds:
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
        stdout.channel.settimeout(10)
        print(stdout.read().decode().strip())

    ssh.close()
    print('All sync processes killed.')


if __name__ == "__main__":
    main()
