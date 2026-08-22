import os
import shlex

from remote_support import production_ssh_client, production_ssh_connect_kwargs

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

cmds = [
    "ALTER TABLE si_notice_eastmoney DROP INDEX uk_notice_art",
    "ALTER TABLE si_notice_eastmoney ADD UNIQUE KEY uk_notice_stock_art (stock_code, art_code)",
]

option_file = os.environ.get("PROBIGA_REMOTE_MYSQL_OPTION_FILE", "").strip()
if not option_file or not option_file.startswith("/") or any(
    marker in option_file for marker in ("\0", "\r", "\n")
):
    raise SystemExit(
        "PROBIGA_REMOTE_MYSQL_OPTION_FILE must name an absolute remote option file"
    )

for c in cmds:
    full = (
        f"mysql --defaults-extra-file={shlex.quote(option_file)} probiga "
        f"--execute={shlex.quote(c)} 2>&1"
    )
    stdin, stdout, stderr = ssh.exec_command(full)
    out = stdout.read().decode()
    err = stderr.read().decode()
    print(f"CMD: {c}")
    if out.strip():
        print(f"  OUT: {out.strip()}")
    if err.strip():
        print(f"  ERR: {err.strip()}")

ssh.close()
print('DDL fixed - unique key is now (stock_code, art_code)')
