#!/usr/bin/env python3
"""上传资金流向修复文件到服务器并重启服务"""
import paramiko
import os

SERVER = '47.113.123.190'
USER = 'root'
PASS = 'ProBigA@2026'
REMOTE = '/opt/ProBigA'
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

files = [
    'tools/sync_capital_flow_batch.py',
    'tools/sync_capital_flow_push2delay.py',
    'tools/sync_capital_flow_ths.py',
    'server/engine/data_loader.py',
    'server/api/routers/hot_data.py',
    'adata/adata/common/utils/unit_conver.py',
]

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(SERVER, username=USER, password=PASS, timeout=30)

# 确保远程目录存在
dirs = set()
for f in files:
    dirs.add(os.path.dirname(f))
for d in dirs:
    ssh.exec_command(f'mkdir -p {REMOTE}/{d}')

sftp = ssh.open_sftp()
for f in files:
    local = os.path.join(ROOT, f)
    remote = f'{REMOTE}/{f}'
    print(f'  上传: {f}')
    sftp.put(local, remote)
    print(f'  OK')

sftp.close()

# 重启服务
print('重启 probiga 服务...')
stdin, stdout, stderr = ssh.exec_command('systemctl restart probiga && sleep 2 && systemctl is-active probiga')
print(stdout.read().decode().strip())
err = stderr.read().decode().strip()
if err:
    print(f'stderr: {err}')

ssh.close()
print('完成!')
