from remote_support import production_ssh_client, production_ssh_connect_kwargs
import os

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())
sftp = ssh.open_sftp()

sftp.put(os.path.abspath('e:/My Code/ProBigA/tools/_sync_plates_for_fused.py'), '/opt/ProBigA/tools/_sync_plates_for_fused.py')
print('OK: _sync_plates_for_fused.py uploaded')
sftp.close()

print('--- 运行板块数据同步 ---')
stdin, stdout, stderr = ssh.exec_command(
    'cd /opt/ProBigA && /opt/ProBigA/venv/bin/python tools/_sync_plates_for_fused.py',
    timeout=600
)
out = stdout.read().decode()
err = stderr.read().decode()
print(out)
if err:
    print('STDERR:', err[:1000])

ssh.close()
print('Done!')
