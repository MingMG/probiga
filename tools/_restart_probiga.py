#!/usr/bin/env python3
import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False)

stdin, stdout, stderr = ssh.exec_command('systemctl restart probiga && sleep 2 && systemctl status probiga | head -10', timeout=15)
print(stdout.read().decode())
err = stderr.read().decode()
if err:
    print('ERR:', err[:500])
ssh.close()
