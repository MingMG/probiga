#!/usr/bin/env python3
import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False)

sftp = ssh.open_sftp()
lines = [
    "from sqlalchemy import create_engine, text",
    "c = create_engine('mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4').connect()",
    "",
    "# Current task args",
    "rows = c.execute(text('SELECT id, task_name, script_path, script_args, date_param, cron_time FROM st_scheduled_tasks ORDER BY sort_order')).fetchall()",
    "for r in rows:",
    "    print('id=' + str(r[0]).zfill(2) + ' args=[' + str(r[3] or '') + '] date=[' + str(r[4] or '') + '] cron=' + str(r[5]) + ' | ' + str(r[1]))",
]
with sftp.open('/tmp/_check_args.py', 'w') as f:
    f.write(chr(10).join(lines) + chr(10))
sftp.close()

stdin, stdout, stderr = ssh.exec_command('cd /opt/ProBigA && source venv/bin/activate && python /tmp/_check_args.py', timeout=15)
print(stdout.read().decode())
err = stderr.read().decode()
if err:
    print('ERR:', err[:500])
ssh.close()
