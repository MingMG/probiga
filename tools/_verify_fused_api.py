import json
from remote_support import production_ssh_client, production_ssh_connect_kwargs

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

cmd1 = 'curl -s "http://127.0.0.1:8000/api/hot-data/fused?snapshot_date=2026-05-09&top=10"'
stdin, stdout, stderr = ssh.exec_command(cmd1, timeout=15)
data1 = json.loads(stdout.read().decode().strip())
print("=== 融合榜 TOP10 ===")
print(f"date={data1.get('date')}, fallback={data1.get('fallback')}, total={data1.get('total')}")
for r in data1.get('data', [])[:10]:
    print(f"  {r['fused_rank']:>3} {r['stock_code']} {r.get('short_name','?'):8s} | 行业: {r.get('industry_name') or '-':8s} | 来源: {r.get('source_flag')}")

cmd2 = 'curl -s "http://127.0.0.1:8000/api/hot-data/multi-day?stat_date=2026-05-09&days=3&top=5"'
stdin, stdout, stderr = ssh.exec_command(cmd2, timeout=15)
data2 = json.loads(stdout.read().decode().strip())
print("\n=== 近3天 TOP5 ===")
print(f"date={data2.get('date')}, total={data2.get('total')}")
for r in data2.get('data', [])[:5]:
    print(f"  {r['fused_rank']:>3} {r['stock_code']} {r.get('short_name','?'):8s} | 行业: {r.get('industry_name') or '-':8s}")

cmd3 = 'curl -s "http://127.0.0.1:8000/api/hot-data/multi-day?stat_date=2026-05-09&days=5&top=5"'
stdin, stdout, stderr = ssh.exec_command(cmd3, timeout=15)
data3 = json.loads(stdout.read().decode().strip())
print("\n=== 近5天 TOP5 ===")
print(f"date={data3.get('date')}, total={data3.get('total')}")
for r in data3.get('data', [])[:5]:
    print(f"  {r['fused_rank']:>3} {r['stock_code']} {r.get('short_name','?'):8s} | 行业: {r.get('industry_name') or '-':8s}")

ssh.close()
