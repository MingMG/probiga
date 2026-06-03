#!/usr/bin/env python3
import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False, timeout=10)

cmd = 'cd /opt/ProBigA && source venv/bin/activate && pip install akshare -q 2>&1 | tail -5'
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
stdout.channel.settimeout(120)
print(stdout.read().decode().strip())
err = stderr.read().decode().strip()
if err:
    print('STDERR:', err[-500:])
ssh.close()
