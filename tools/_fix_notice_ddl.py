import os
import shlex
import paramiko
from remote_support import ssh_connect_kwargs

cmds = [
    "ALTER TABLE si_notice_eastmoney DROP INDEX uk_notice_art",
    "ALTER TABLE si_notice_eastmoney ADD UNIQUE KEY uk_notice_stock_art (stock_code, art_code)",
]


def main() -> None:
    mysql_password = os.environ.get("PROBIGA_REMOTE_MYSQL_PASSWORD", "")
    if not mysql_password:
        raise SystemExit("Missing PROBIGA_REMOTE_MYSQL_PASSWORD")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**ssh_connect_kwargs())

    for c in cmds:
        full = f"MYSQL_PWD={shlex.quote(mysql_password)} mysql -u root probiga -e \"{c}\" 2>&1"
        stdin, stdout, stderr = ssh.exec_command(full, timeout=60)
        out = stdout.read().decode()
        err = stderr.read().decode()
        print(f"CMD: {c}")
        if out.strip():
            print(f"  OUT: {out.strip()}")
        if err.strip():
            print(f"  ERR: {err.strip()}")

    ssh.close()
    print('DDL fixed - unique key is now (stock_code, art_code)')


if __name__ == "__main__":
    main()
