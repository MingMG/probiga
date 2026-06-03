import paramiko
import os

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False)
sftp = ssh.open_sftp()

sftp.put(os.path.abspath('e:/My Code/ProBigA/tools/_sync_plates_for_fused.py'), '/opt/ProBigA/tools/_sync_plates_for_fused.py')
print('OK: _sync_plates_for_fused.py uploaded')
sftp.close()

print('--- 运行板块数据同步 ---')
stdin, stdout, stderr = ssh.exec_command(
    'cd /opt/ProBigA && export MYSQL_URL="mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4" && /opt/ProBigA/venv/bin/python tools/_sync_plates_for_fused.py',
    timeout=600
)
out = stdout.read().decode()
err = stderr.read().decode()
print(out)
if err:
    print('STDERR:', err[:1000])

ssh.close()
print('Done!')
