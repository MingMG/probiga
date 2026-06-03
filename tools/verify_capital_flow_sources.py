#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深度验证各数据源返回的资金流向数据:
1. push2.eastmoney.com - 看实际 kline 内容是否包含目标日期
2. push2his.eastmoney.com - 测试是否解封
3. 百度API - 确认数据字段
4. 对比各源数据一致性
"""

import time
import requests as http
from datetime import datetime

SESSION = http.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
})

TARGET_DATE = "2026-05-06"
TEST_STOCKS = ["000001", "600519", "000858"]


def test_push2_detail(stock_code):
    """push2.eastmoney.com - 查看实际kline数据"""
    cid = 1 if stock_code.startswith('6') else 0
    url = (
        "https://push2.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        f"lmt=0&klt=101&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&secid={cid}.{stock_code}"
    )
    try:
        t0 = time.time()
        resp = SESSION.get(url, timeout=10)
        elapsed = time.time() - t0
        j = resp.json()
        if not j.get("data") or not j["data"].get("klines"):
            return None, f"无数据 ({elapsed:.2f}s)"
        klines = j["data"]["klines"]
        print(f"  [{stock_code}] push2 返回 {len(klines)} 条, 耗时 {elapsed:.2f}s")
        for k in klines[-5:]:
            print(f"    {k}")
        target_found = False
        for k in klines:
            if TARGET_DATE in k:
                target_found = True
                fields = k.split(",")
                print(f"  [{stock_code}] 目标日期 {TARGET_DATE} 数据:")
                print(f"    日期={fields[0]}, 主力净流入={fields[1]}, 小单净流入={fields[2]}")
                print(f"    中单净流入={fields[3]}, 大单净流入={fields[4]}, 超大单净流入={fields[5]}")
                return fields, None
        if not target_found:
            return None, f"无 {TARGET_DATE} 数据, 最新: {klines[-1].split(',')[0] if klines else '无'}"
    except Exception as e:
        return None, str(e)[:200]


def test_push2his_detail(stock_code):
    """push2his.eastmoney.com - 测试是否解封"""
    cid = 1 if stock_code.startswith('6') else 0
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        f"lmt=0&klt=101&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&secid={cid}.{stock_code}"
    )
    try:
        t0 = time.time()
        resp = SESSION.get(url, timeout=10)
        elapsed = time.time() - t0
        j = resp.json()
        if not j.get("data") or not j["data"].get("klines"):
            return None, f"无数据 ({elapsed:.2f}s)"
        klines = j["data"]["klines"]
        print(f"  [{stock_code}] push2his 返回 {len(klines)} 条, 耗时 {elapsed:.2f}s")
        for k in klines[-5:]:
            print(f"    {k}")
        for k in klines:
            if TARGET_DATE in k:
                fields = k.split(",")
                print(f"  [{stock_code}] 目标日期 {TARGET_DATE} 数据:")
                print(f"    日期={fields[0]}, 主力净流入={fields[1]}")
                return fields, None
        return None, f"无 {TARGET_DATE} 数据"
    except Exception as e:
        return None, str(e)[:200]


def test_baidu_detail(stock_code):
    """百度API - 查看完整字段"""
    from datetime import timedelta
    dt = datetime.strptime(TARGET_DATE, "%Y-%m-%d")
    next_date = (dt + timedelta(days=1)).strftime("%Y%m%d")
    url = (
        "https://finance.pae.baidu.com/vapi/v1/fundsortlist?"
        f"code={stock_code}&market=ab&finance_type=stock&tab=day"
        f"&from=history&date={next_date}&pn=0&rn=1&finClientType=pc"
    )
    try:
        t0 = time.time()
        resp = SESSION.get(url, timeout=10)
        elapsed = time.time() - t0
        j = resp.json()
        content = j.get("Result", {}).get("content", [])
        if not content:
            return None, f"无数据 ({elapsed:.2f}s)"
        for row in content:
            if not isinstance(row, dict):
                continue
            row_date = row.get("date", "").replace("/", "-")[:10]
            if row_date == TARGET_DATE:
                print(f"  [{stock_code}] 百度 数据字段:")
                for k, v in sorted(row.items()):
                    print(f"    {k} = {v}")
                return row, None
        return None, f"无 {TARGET_DATE} 数据 ({elapsed:.2f}s)"
    except Exception as e:
        return None, str(e)[:200]


if __name__ == "__main__":
    print(f"=== 资金流向数据源深度验证 ===")
    print(f"目标日期: {TARGET_DATE}")
    print()

    print("=" * 60)
    print("1. push2.eastmoney.com (东方财富推送接口)")
    print("=" * 60)
    push2_data = {}
    for s in TEST_STOCKS:
        data, err = test_push2_detail(s)
        if data:
            push2_data[s] = data
        else:
            print(f"  [{s}] 失败: {err}")
        time.sleep(0.3)
    print()

    print("=" * 60)
    print("2. push2his.eastmoney.com (东方财富历史接口)")
    print("=" * 60)
    push2his_data = {}
    for s in TEST_STOCKS:
        data, err = test_push2his_detail(s)
        if data:
            push2his_data[s] = data
        else:
            print(f"  [{s}] 失败: {err}")
        time.sleep(0.3)
    print()

    print("=" * 60)
    print("3. 百度API (finance.pae.baidu.com)")
    print("=" * 60)
    baidu_data = {}
    for s in TEST_STOCKS:
        data, err = test_baidu_detail(s)
        if data:
            baidu_data[s] = data
        else:
            print(f"  [{s}] 失败: {err}")
        time.sleep(0.3)
    print()

    print("=" * 60)
    print("4. 数据源交叉对比")
    print("=" * 60)
    for s in TEST_STOCKS:
        print(f"\n  [{s}]")
        has_push2 = s in push2_data
        has_push2his = s in push2his_data
        has_baidu = s in baidu_data
        print(f"    push2:     {'有数据' if has_push2 else '无数据'}")
        print(f"    push2his:  {'有数据' if has_push2his else '无数据'}")
        print(f"    百度:      {'有数据' if has_baidu else '无数据'}")
        
        if has_push2 and has_baidu:
            push2_main_in = push2_data[s][1] if len(push2_data[s]) > 1 else "?"
            baidu_main_in = baidu_data[s].get("extMainIn", "?")
            print(f"    push2 主力净流入: {push2_main_in}")
            print(f"    百度  主力净流入: {baidu_main_in}")

    print()
    print("=" * 60)
    print("5. 速度排名 (单次请求)")
    print("=" * 60)
    sources_ok = []
    if push2_data:
        sources_ok.append("push2.eastmoney.com")
    if push2his_data:
        sources_ok.append("push2his.eastmoney.com")
    if baidu_data:
        sources_ok.append("百度API")
    
    if not sources_ok:
        print("  无可用数据源!")
    else:
        print(f"  可用数据源: {', '.join(sources_ok)}")

    print()
    print("=" * 60)
    print("6. 批量压力测试 (每个源连续50次请求)")
    print("=" * 60)
    
    for source_name, fetch_func in [
        ("push2", lambda s: test_push2_detail(s)),
        ("push2his", lambda s: test_push2his_detail(s)),
        ("百度", lambda s: test_baidu_detail(s)),
    ]:
        print(f"\n  [{source_name}] 连续50次请求 000001:")
        success = 0
        fail = 0
        total_time = 0
        for i in range(50):
            t0 = time.time()
            try:
                cid = 0
                if source_name == "push2":
                    url = (
                        "https://push2.eastmoney.com/api/qt/stock/fflow/daykline/get?"
                        "lmt=0&klt=101&fields1=f1,f2,f3,f7"
                        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                        "&secid=0.000001"
                    )
                elif source_name == "push2his":
                    url = (
                        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?"
                        "lmt=0&klt=101&fields1=f1,f2,f3,f7"
                        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                        "&secid=0.000001"
                    )
                else:
                    from datetime import timedelta
                    dt = datetime.strptime(TARGET_DATE, "%Y-%m-%d")
                    next_date = (dt + timedelta(days=1)).strftime("%Y%m%d")
                    url = (
                        "https://finance.pae.baidu.com/vapi/v1/fundsortlist?"
                        f"code=000001&market=ab&finance_type=stock&tab=day"
                        f"&from=history&date={next_date}&pn=0&rn=1&finClientType=pc"
                    )
                resp = SESSION.get(url, timeout=10)
                elapsed = time.time() - t0
                total_time += elapsed
                if resp.status_code == 200:
                    success += 1
                else:
                    fail += 1
                    print(f"    #{i+1} HTTP {resp.status_code}")
            except Exception as e:
                fail += 1
                elapsed = time.time() - t0
                total_time += elapsed
                if fail <= 3:
                    print(f"    #{i+1} 错误: {str(e)[:80]}")
            time.sleep(0.05)
        
        print(f"    结果: {success}/50 成功, 平均 {total_time/50:.3f}s/请求")
        if fail > 0:
            print(f"    失败: {fail} 次")
