import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False)

cmd = """cd /opt/ProBigA && export PYTHONPATH=/opt/ProBigA:/opt/ProBigA/adata && nohup /opt/ProBigA/venv/bin/python -c "
from tools.run_single_table import run_si_concept_constituent_east
run_si_concept_constituent_east()
" > /tmp/sync_concept_east.log 2>&1 &
echo "PID: $!"
"""
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
print("Sync started:", stdout.read().decode().strip())
ssh.close()
