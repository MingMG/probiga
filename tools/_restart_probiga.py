#!/usr/bin/env python3
import paramiko
from remote_support import production_ssh_client, production_ssh_connect_kwargs


def main() -> None:
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs())

    stdin, stdout, stderr = ssh.exec_command('systemctl restart probiga && sleep 2 && systemctl status probiga | head -10', timeout=15)
    print(stdout.read().decode())
    err = stderr.read().decode()
    if err:
        print('ERR:', err[:500])
    ssh.close()


if __name__ == "__main__":
    main()
