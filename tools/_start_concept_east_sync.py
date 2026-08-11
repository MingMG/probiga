import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)


def _command(root: str) -> str:
    pythonpath = remote_pythonpath(root)
    return f"""cd {root} && export PYTHONPATH={pythonpath} && nohup {root}/venv/bin/python -c "
from tools.run_single_table import run_si_concept_constituent_east
run_si_concept_constituent_east()
" > /tmp/sync_concept_east.log 2>&1 &
echo "PID: $!"
"""


def main() -> None:
    command = _command(remote_root())
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs())

    stdin, stdout, stderr = ssh.exec_command(command, timeout=10)
    print("Sync started:", stdout.read().decode().strip())
    ssh.close()


if __name__ == "__main__":
    main()
