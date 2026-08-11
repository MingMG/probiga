#!/usr/bin/env python3
import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)
import os


def main() -> None:
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs(timeout=10))

    sftp = ssh.open_sftp()
    local = os.path.join(os.path.dirname(__file__), '_kline_fill_akshare.py')
    sftp.put(os.path.abspath(local), '/tmp/kline_fill_akshare.py')
    sftp.close()

    chan = ssh.get_transport().open_session()
    chan.settimeout(15)
    root = remote_root()
    chan.exec_command(f'nohup bash -c "cd {root} && source venv/bin/activate && python /tmp/kline_fill_akshare.py" > /tmp/kline_akshare.log 2>&1 & echo "PID=$!"')
    import time
    time.sleep(3)
    out = chan.recv(4096).decode().strip()
    print(out)
    chan.close()
    ssh.close()
    print('K-line akshare fill started.')


if __name__ == "__main__":
    main()
