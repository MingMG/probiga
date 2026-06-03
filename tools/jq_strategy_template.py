# -*- coding: utf-8 -*-
"""
聚宽策略模板 - 复制到聚宽研究平台的 Notebook 中运行
===================================================

使用方法：
1. 登录 https://www.joinquant.com
2. 进入"我的研究" > 新建 Notebook
3. 将此代码粘贴进去
4. 修改 strategy_name 和选股逻辑
5. 运行后会自动同步到 ProBigA 系统

注意：此代码在聚宽平台运行，不在本地运行
"""

import requests
import json
from datetime import datetime, timedelta

# ==================== 配置 ====================
PROBIGA_API = "http://47.113.123.190:5001/api/strategy/picks/sync"
STRATEGY_NAME = "动量选股策略"
STRATEGY_DESC = "基于20日动量和成交量筛选强势股"


# ==================== 选股逻辑（可自定义） ====================
def run_strategy():
    """
    聚宽选股策略主函数
    在此编写你的选股逻辑，返回股票列表
    """
    import jqdatasdk as jq
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 获取所有A股
    stocks = jq.get_all_securities(types=['stock'])
    stock_list = stocks.index.tolist()
    
    # 过滤ST、停牌、次新股
    stock_list = [s for s in stock_list if not s.startswith('68')]  # 排除科创板（可选）
    
    # 获取最近20个交易日的行情
    end_date = today
    start_date = (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')
    
    picks = []
    
    for stock in stock_list[:100]:  # 限制数量避免超时，实际可去掉
        try:
            df = jq.get_price(stock, start_date=start_date, end_date=end_date, frequency='daily')
            if len(df) < 20:
                continue
            
            # 计算20日动量（收益率）
            close_prices = df['close'].values
            momentum_20 = (close_prices[-1] / close_prices[-20] - 1) * 100
            
            # 计算5日平均成交量
            avg_volume_5 = df['volume'].tail(5).mean()
            avg_volume_20 = df['volume'].mean()
            volume_ratio = avg_volume_5 / avg_volume_20 if avg_volume_20 > 0 else 0
            
            # 选股条件：20日动量 > 10%，成交量放大
            if momentum_20 > 10 and volume_ratio > 1.2:
                code = stock.split('.')[0]
                name = stocks.loc[stock, 'display_name'] if stock in stocks.index else code
                
                picks.append({
                    'stock_code': code,
                    'short_name': str(name),
                    'score': round(momentum_20, 2),
                    'reason': f'20日动量{momentum_20:.1f}%，量比{volume_ratio:.2f}'
                })
        except Exception as e:
            continue
    
    # 按分数排序，取前20
    picks.sort(key=lambda x: x['score'], reverse=True)
    return picks[:20]


# ==================== 同步到ProBigA ====================
def sync_to_probiga(picks, strategy_name=STRATEGY_NAME, description=STRATEGY_DESC):
    """将选股结果同步到ProBigA系统"""
    payload = {
        'strategy_name': strategy_name,
        'description': description,
        'pick_date': datetime.now().strftime('%Y-%m-%d'),
        'picks': picks
    }
    
    try:
        resp = requests.post(PROBIGA_API, json=payload, timeout=10)
        result = resp.json()
        if result.get('success'):
            print(f"✅ 同步成功！策略: {strategy_name}, 股票数: {result.get('count')}")
        else:
            print(f"❌ 同步失败: {result.get('error')}")
        return result
    except Exception as e:
        print(f"❌ 同步异常: {e}")
        return None


# ==================== 主程序 ====================
if __name__ == '__main__':
    # 在聚宽Notebook中，直接调用：
    picks = run_strategy()
    print(f"选出 {len(picks)} 只股票:")
    for p in picks:
        print(f"  {p['stock_code']} {p['short_name']} 分数:{p['score']} 原因:{p['reason']}")
    
    # 同步到ProBigA
    sync_to_probiga(picks)
else:
    # 在聚宽Notebook中运行
    print("请在聚宽Notebook中运行此脚本")
    print("或者直接调用: picks = run_strategy()")
