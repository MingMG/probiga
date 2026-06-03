#!/usr/bin/env python3
import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False, timeout=10)

cmds = [
    "ps aux | grep 'sync_stock_market' | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null; echo 'killed'",
    "ps aux | grep 'run_kline' | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null; echo 'killed wrapper'",
]
for cmd in cmds:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
    stdout.channel.settimeout(10)
    print(stdout.read().decode().strip())

ssh.close()
print('All sync processes killed.')
