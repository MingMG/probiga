#!/usr/bin/env python3
import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)

lines = [
    "from sqlalchemy import text",
    "from server.common.batch_db import create_batch_engine",
    "c = create_batch_engine().connect()",
    "",
    "# Current task args",
    "rows = c.execute(text('SELECT id, task_name, script_path, script_args, date_param, cron_time FROM st_scheduled_tasks ORDER BY sort_order')).fetchall()",
    "for r in rows:",
    "    print('id=' + str(r[0]).zfill(2) + ' args=[' + str(r[3] or '') + '] date=[' + str(r[4] or '') + '] cron=' + str(r[5]) + ' | ' + str(r[1]))",
]


def main() -> None:
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs())

    sftp = ssh.open_sftp()
    with sftp.open('/tmp/_check_args.py', 'w') as f:
        f.write(chr(10).join(lines) + chr(10))
    sftp.close()

    stdin, stdout, stderr = ssh.exec_command(f'cd {remote_root()} && source venv/bin/activate && python /tmp/_check_args.py', timeout=15)
    print(stdout.read().decode())
    err = stderr.read().decode()
    if err:
        print('ERR:', err[:500])
    ssh.close()


if __name__ == "__main__":
    main()
