#!/usr/bin/env python3
from sqlalchemy import create_engine, text
import urllib.request, json

engine = create_engine("mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4")
with engine.connect() as conn:
    # Get a real concept_code from hot_daily that has constituents
    rows = conn.execute(text("""
        SELECT DISTINCT d.concept_code, d.concept_name
        FROM st_hot_concept_ths_daily d
        WHERE d.plate_type = 1
        LIMIT 5
    """)).fetchall()

    print("=== Testing concept-stocks API with real codes ===")
    for r in rows:
        code, name = r[0], r[1]
        # Check if this code exists in constituent
        cnt = conn.execute(text(
            "SELECT COUNT(*) FROM si_concept_constituent_ths WHERE query_key = :q"
        ), {"q": code}).scalar()
        cnt2 = conn.execute(text(
            "SELECT COUNT(*) FROM si_concept_code_ths WHERE concept_code = :q OR index_code = :q"
        ), {"q": code}).scalar()
        print(f"  {code} {name}: in_constituent={cnt} in_code_ths={cnt2}")

        if cnt > 0 or cnt2 > 0:
            # Test API
            url = f"http://127.0.0.1:8000/api/hot-data/concept-stocks?concept_code={code}"
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    d = json.loads(resp.read())
                    total = d.get("total", 0)
                    err = d.get("error", "")
                    print(f"    API result: total={total} err={err}")
                    for s in (d.get("data") or [])[:5]:
                        price = s.get("price")
                        chg = s.get("change_pct")
                        print(f"    {s['stock_code']} {s.get('short_name','')} price={price} chg={chg}")
            except Exception as e:
                print(f"    API error: {e}")
            break
