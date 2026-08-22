import time
from remote_support import production_ssh_client, production_ssh_connect_kwargs

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

cmd = (
    "cd /opt/ProBigA && "
    "export PYTHONPATH=/opt/ProBigA:/opt/ProBigA/adata && "
    "/opt/ProBigA/venv/bin/python -c \""
    "from tools.run_single_table import run_si_concept_constituent_east; "
    "run_si_concept_constituent_east()"
    "\" 2>&1"
)

stdin, stdout, stderr = ssh.exec_command(cmd, timeout=300)
out = stdout.read().decode().strip()
err = stderr.read().decode().strip()
print(out[:2000])
if err:
    print('STDERR:', err[:1000])

ssh.close()
