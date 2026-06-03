# -*- coding: utf-8 -*-
"""
统一分析引擎测试

测试四层引擎的计算逻辑是否正确。
"""

import sys
from pathlib import Path as _Path

# 添加项目根目录到 path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from server.engine.schemas import StockAnalysisResult, ScoreDetail, EventRisk, RecommendResult
from server.engine.long_term_engine import LongTermEngine
from server.engine.short_term_engine import ShortTermEngine
from server.engine.event_risk_engine import EventRiskEngine
from server.engine.recommendation_gate import RecommendationGate


# ============================================================
# 测试数据
# ============================================================

def create_mock_data(
    roe=15, gross_margin=35, asset_liab_ratio=45,
    rev_growth=20, profit_growth=25,
    pe_percentile=50, pb_percentile=40,
    main_net_inflow=1000, lg_net_inflow=500,
    flow_5d=3000, flow_20d=5000,
    fused_rank=30, volume_ratio=1.5, turnover_ratio=3.0,
    rsi6=55, ma5=10.5, ma10=10.3, ma20=10.0, ma60=9.5,
    dif=0.1, dea=0.05, golden_cross=False,
    has_lifting=False, mine_score=80,
    has_critical_event=False, has_high_event=False,
):
    """创建模拟数据"""
    return {
        'stock_code': '000001',
        'short_name': '测试股票',
        'trade_date': '2026-05-30',
        'last_news_time': '2026-05-30 15:00:00',
        'market': {
            'price': 10.5,
            'change_pct': 1.5,
            'volume_ratio': volume_ratio,
            'turnover_ratio': turnover_ratio,
        },
        'capital': {
            'today': {
                'main_net_inflow': main_net_inflow,
                'lg_net_inflow': lg_net_inflow,
            },
            'flow_3d': flow_5d * 0.6,
            'flow_5d': flow_5d,
            'flow_20d': flow_20d,
            'dragon_tiger': {
                'count_20d': 2,
                'inst_net_buy': 5000,
                'seats': [],
            },
        },
        'finance': {
            'latest': {
                'roe_wtd': roe,
                'gross_margin': gross_margin,
                'asset_liab_ratio': asset_liab_ratio,
                'net_margin': 15,
                'roa_wtd': 8,
                'basic_eps': 0.5,
                'net_asset_ps': 5.0,
                'total_rev_yoy_gr': rev_growth,
                'net_profit_yoy_gr': profit_growth,
            },
            'quarters': [
                {'total_rev': 100, 'total_rev_yoy_gr': rev_growth, 'net_profit_attr_sh': 20, 'net_profit_yoy_gr': profit_growth},
                {'total_rev': 90, 'total_rev_yoy_gr': 15, 'net_profit_attr_sh': 18, 'net_profit_yoy_gr': 20},
                {'total_rev': 85, 'total_rev_yoy_gr': 10, 'net_profit_attr_sh': 16, 'net_profit_yoy_gr': 15},
                {'total_rev': 80, 'total_rev_yoy_gr': 8, 'net_profit_attr_sh': 15, 'net_profit_yoy_gr': 10},
            ],
        },
        'valuation': {
            'pe_ttm': 21.0,
            'pe_percentile': pe_percentile,
            'pb': 2.1,
            'pb_percentile': pb_percentile,
            'verdict': '合理' if 30 <= pe_percentile <= 70 else ('偏高' if pe_percentile > 70 else '偏低'),
        },
        'technical': {
            'ma': {'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60, 'ma120': 9.0, 'ma250': 8.5},
            'macd': {'dif': dif, 'dea': dea, 'hist': (dif - dea) * 2, 'golden_cross': golden_cross},
            'kdj': {'k': 60, 'd': 55, 'j': 70},
            'rsi': {'rsi6': rsi6, 'rsi12': 50, 'rsi24': 48},
            'boll': {'upper': 11.5, 'mid': 10.5, 'lower': 9.5},
            'support': 10.0,
            'resistance': 11.0,
            'support_mid': 9.5,
            'resistance_mid': 11.5,
            'trend': {'short': '上涨', 'mid': '上涨', 'long': '上涨'},
        },
        'news': {
            'notices': [
                {'notice_date': '2026-05-30', 'title': '公司发布重大合同公告' if not has_critical_event and not has_high_event else ('公司被立案调查' if has_critical_event else '大股东减持计划')},
            ],
            'news': [],
        },
        'holder': {
            'report_date': '2026-03-31',
            'holder_num': 50000,
            'holder_num_change': -1000,
            'holder_num_ratio': -2.0,
            'avg_free_shares': 10000,
        },
        'hot_rank': {
            'fused_rank': fused_rank,
        },
        'lifting': {
            'has_lifting_soon': has_lifting,
            'records': [],
        },
        'mine_clearance': {
            'score': mine_score,
        },
        'holding': None,
    }


# ============================================================
# 长线引擎测试
# ============================================================

def test_long_term_engine_basic():
    """测试长线引擎基本功能"""
    engine = LongTermEngine()
    data = create_mock_data()

    result = engine.analyze(data)

    assert 'long_term_score' in result
    assert 'fundamental_score' in result
    assert 'growth_score' in result
    assert 'valuation_score' in result
    assert 'risk_score' in result
    assert 0 <= result['long_term_score'] <= 100
    print(f"长线引擎基本测试通过: {result['long_term_score']}")


def test_long_term_engine_high_roe():
    """测试高ROE股票"""
    engine = LongTermEngine()
    data = create_mock_data(roe=25, gross_margin=50, asset_liab_ratio=30)

    result = engine.analyze(data)

    assert result['fundamental_score'] > 70
    print(f"高ROE测试通过: 基本面={result['fundamental_score']}")


def test_long_term_engine_low_valuation():
    """测试低估值股票"""
    engine = LongTermEngine()
    data = create_mock_data(pe_percentile=20, pb_percentile=15)

    result = engine.analyze(data)

    assert result['valuation_score'] > 70
    print(f"低估值测试通过: 估值={result['valuation_score']}")


# ============================================================
# 短线引擎测试
# ============================================================

def test_short_term_engine_basic():
    """测试短线引擎基本功能"""
    engine = ShortTermEngine()
    data = create_mock_data()

    result = engine.analyze(data)

    assert 'short_term_score' in result
    assert 'capital_score' in result
    assert 'technical_score' in result
    assert 'sentiment_score' in result
    assert 'event_score' in result
    assert 0 <= result['short_term_score'] <= 100
    print(f"短线引擎基本测试通过: {result['short_term_score']}")


def test_short_term_engine_strong_capital():
    """测试资金面强势股票"""
    engine = ShortTermEngine()
    data = create_mock_data(main_net_inflow=8000, lg_net_inflow=3000, flow_5d=15000)

    result = engine.analyze(data)

    assert result['capital_score'] > 70
    print(f"资金强势测试通过: 资金面={result['capital_score']}")


def test_short_term_engine_golden_cross():
    """测试MACD金叉股票"""
    engine = ShortTermEngine()
    data = create_mock_data(dif=0.2, dea=0.1, golden_cross=True)

    result = engine.analyze(data)

    assert result['technical_score'] > 60
    print(f"MACD金叉测试通过: 技术面={result['technical_score']}")


# ============================================================
# 事件风险引擎测试
# ============================================================

def test_event_risk_engine_no_risk():
    """测试无风险情况"""
    engine = EventRiskEngine()
    data = create_mock_data()

    result = engine.analyze(data)

    assert result['event_risk_level'] == 'LOW'
    assert result['event_risk_score'] >= 80
    print(f"无风险测试通过: 等级={result['event_risk_level']}, 分数={result['event_risk_score']}")


def test_event_risk_engine_critical():
    """测试重大风险事件"""
    engine = EventRiskEngine()
    data = create_mock_data(has_critical_event=True)

    result = engine.analyze(data)

    assert result['event_risk_level'] == 'CRITICAL'
    assert result['event_risk_score'] < 20
    print(f"重大风险测试通过: 等级={result['event_risk_level']}, 分数={result['event_risk_score']}")


def test_event_risk_engine_high():
    """测试高风险事件"""
    engine = EventRiskEngine()
    data = create_mock_data(has_high_event=True)

    result = engine.analyze(data)

    assert result['event_risk_level'] == 'HIGH'
    assert result['event_risk_score'] < 40
    print(f"高风险测试通过: 等级={result['event_risk_level']}, 分数={result['event_risk_score']}")


def test_event_risk_engine_lifting():
    """测试解禁风险"""
    engine = EventRiskEngine()
    data = create_mock_data(has_lifting=True)

    # 手动设置解禁日期为未来7天内
    from datetime import datetime, timedelta
    data['lifting'] = {
        'has_lifting_soon': True,
        'lift_date': (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d'),
        'amount': 100000000,
        'ratio': 5.0,
        'records': [{'lift_date': (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d'), 'volume': 10000000, 'amount': 100000000, 'ratio': 5.0}],
    }

    result = engine.analyze(data)

    # 解禁是MEDIUM级别风险
    assert result['event_risk_score'] <= 60
    print(f"解禁风险测试通过: 分数={result['event_risk_score']}")


# ============================================================
# 推荐资格引擎测试
# ============================================================

def test_recommendation_gate_allow():
    """测试允许推荐"""
    gate = RecommendationGate()

    long_term = {'long_term_score': 75, 'strengths': [], 'risks': []}
    short_term = {'short_term_score': 80, 'strengths': [], 'risks': []}
    event_risk = {'event_risk_level': 'LOW', 'event_risk_score': 90, 'triggered_events': [], 'risks': []}

    result = gate.evaluate(long_term, short_term, event_risk)

    assert result['status'] == 'ALLOW'
    print(f"允许推荐测试通过: {result['status']}")


def test_recommendation_gate_block_critical():
    """测试重大风险禁止推荐"""
    gate = RecommendationGate()

    long_term = {'long_term_score': 75, 'strengths': [], 'risks': []}
    short_term = {'short_term_score': 80, 'strengths': [], 'risks': []}
    event_risk = {'event_risk_level': 'CRITICAL', 'event_risk_score': 10, 'triggered_events': [], 'risks': ['重大风险']}

    result = gate.evaluate(long_term, short_term, event_risk)

    assert result['status'] == 'BLOCK'
    print(f"重大风险禁止推荐测试通过: {result['status']}")


def test_recommendation_gate_suspended_high_risk():
    """测试高风险暂停推荐"""
    gate = RecommendationGate()

    long_term = {'long_term_score': 75, 'strengths': [], 'risks': []}
    short_term = {'short_term_score': 80, 'strengths': [], 'risks': []}
    event_risk = {'event_risk_level': 'HIGH', 'event_risk_score': 30, 'triggered_events': [], 'risks': ['高风险']}

    result = gate.evaluate(long_term, short_term, event_risk)

    assert result['status'] == 'SUSPENDED'
    print(f"高风险暂停推荐测试通过: {result['status']}")


def test_recommendation_gate_block_low_score():
    """测试低评分禁止推荐"""
    gate = RecommendationGate()

    long_term = {'long_term_score': 75, 'strengths': [], 'risks': []}
    short_term = {'short_term_score': 40, 'strengths': [], 'risks': []}
    event_risk = {'event_risk_level': 'LOW', 'event_risk_score': 90, 'triggered_events': [], 'risks': []}

    result = gate.evaluate(long_term, short_term, event_risk)

    assert result['status'] == 'BLOCK'
    print(f"低评分禁止推荐测试通过: {result['status']}")


# ============================================================
# 统一结果测试
# ============================================================

def test_stock_analysis_result():
    """测试统一结果结构"""
    result = StockAnalysisResult(
        stock_code='000001',
        stock_name='平安银行',
        analysis_date='2026-05-30',
        long_term_score=75.0,
        short_term_score=82.0,
        scores=ScoreDetail(
            fundamental=80.0,
            growth=70.0,
            valuation=65.0,
            risk=85.0,
            capital=90.0,
            technical=75.0,
            sentiment=80.0,
            event=70.0,
        ),
        event_risk=EventRisk(score=90, level='LOW'),
        recommend=RecommendResult(status='ALLOW', reason='正常'),
        summary='测试总结',
        recommendation='测试建议',
        strengths=['优势1'],
        risks=['风险1'],
    )

    # 测试 to_dict
    d = result.to_dict()
    assert d['stock_code'] == '000001'
    assert d['long_term_score'] == 75.0
    assert d['short_term_score'] == 82.0
    assert d['recommend']['status'] == 'ALLOW'
    assert d['event_risk']['level'] == 'LOW'

    print("统一结果结构测试通过")


# ============================================================
# 运行所有测试
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("开始运行统一分析引擎测试")
    print("=" * 60)

    # 长线引擎测试
    print("\n--- 长线引擎测试 ---")
    test_long_term_engine_basic()
    test_long_term_engine_high_roe()
    test_long_term_engine_low_valuation()

    # 短线引擎测试
    print("\n--- 短线引擎测试 ---")
    test_short_term_engine_basic()
    test_short_term_engine_strong_capital()
    test_short_term_engine_golden_cross()

    # 事件风险引擎测试
    print("\n--- 事件风险引擎测试 ---")
    test_event_risk_engine_no_risk()
    test_event_risk_engine_critical()
    test_event_risk_engine_high()
    test_event_risk_engine_lifting()

    # 推荐资格引擎测试
    print("\n--- 推荐资格引擎测试 ---")
    test_recommendation_gate_allow()
    test_recommendation_gate_block_critical()
    test_recommendation_gate_suspended_high_risk()
    test_recommendation_gate_block_low_score()

    # 统一结果测试
    print("\n--- 统一结果测试 ---")
    test_stock_analysis_result()

    print("\n" + "=" * 60)
    print("所有测试通过！")
    print("=" * 60)
