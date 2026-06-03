#!/usr/bin/env python3
import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False)

cmds = [
    'kill 60753 2>/dev/null; echo "killed 60753"',
    'tail -5 /tmp/kline_incremental.log 2>/dev/null || echo "no log"',
    'wc -l /tmp/kline_incremental.log 2>/dev/null',
]
for cmd in cmds:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
    stdout.channel.settimeout(10)
    print(stdout.read().decode().strip())

ssh.close()
