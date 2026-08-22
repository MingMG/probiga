from remote_support import production_ssh_client, production_ssh_connect_kwargs

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

cmd = """cd /opt/ProBigA && export PYTHONPATH=/opt/ProBigA:/opt/ProBigA/adata && nohup /opt/ProBigA/venv/bin/python -c "
from tools.run_single_table import run_si_concept_constituent_east
run_si_concept_constituent_east()
" > /tmp/sync_concept_east.log 2>&1 &
echo "PID: $!"
"""
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
print("Sync started:", stdout.read().decode().strip())
ssh.close()
