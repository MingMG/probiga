#!/usr/bin/env python3
import paramiko
import time

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False, timeout=10)

stdin, stdout, stderr = ssh.exec_command('kill 61378 2>/dev/null; echo "killed"', timeout=10)
print(stdout.read().decode().strip())

print('Waiting 3 minutes for rate limit cooldown...')
ssh.close()
