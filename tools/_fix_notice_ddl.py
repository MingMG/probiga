import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026')

cmds = [
    "ALTER TABLE si_notice_eastmoney DROP INDEX uk_notice_art",
    "ALTER TABLE si_notice_eastmoney ADD UNIQUE KEY uk_notice_stock_art (stock_code, art_code)",
]

for c in cmds:
    full = f"mysql -u root -p'ProBigA@70966' probiga -e \"{c}\" 2>&1"
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
