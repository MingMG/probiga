import paramiko
from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)


def _command(root: str) -> str:
    pythonpath = remote_pythonpath(root)
    return (
        f"cd {root} && "
        f"export PYTHONPATH={pythonpath} && "
        f"{root}/venv/bin/python -c \""
        "from tools.run_single_table import run_si_concept_constituent_east; "
        "run_si_concept_constituent_east()"
        "\" 2>&1"
    )


def main() -> None:
    command = _command(remote_root())
    ssh = production_ssh_client(paramiko)
    ssh.connect(**production_ssh_connect_kwargs())

    stdin, stdout, stderr = ssh.exec_command(command, timeout=300)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    print(out[:2000])
    if err:
        print('STDERR:', err[:1000])

    ssh.close()


if __name__ == "__main__":
    main()
