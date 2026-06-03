import paramiko, time

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False)

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
