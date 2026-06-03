# -*- coding: utf-8 -*-
"""
盘后全量分析任务

在收盘后运行，对全市场股票进行统一分析，结果存入 stock_analysis_result 表。
供 AI推荐、自选股、全市场、个股详情四个页面共用。

使用方法：
    python -m biz.analysis.sync_analysis_result
    python -m biz.analysis.sync_analysis_result --limit 100  # 只分析前100只
    python -m biz.analysis.sync_analysis_result --code 000001  # 只分析单只股票
"""

import sys
import argparse
import logging
from pathlib import Path as _Path
from datetime import datetime, date

# 添加项目根目录到 path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from sqlalchemy import text
from server.api.routers._engine import get_engine
from server.engine.stock_analysis_engine import StockAnalysisEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_all_active_stocks(limit: int = None) -> list:
    """获取所有活跃股票代码"""
    sql = """
        SELECT stock_code, short_name
        FROM si_all_code
        WHERE stock_code NOT LIKE '4%'
          AND stock_code NOT LIKE '8%'
          AND stock_code NOT LIKE '9%'
        ORDER BY stock_code
    """
    if limit:
        sql += f" LIMIT {limit}"

    df = pd.read_sql(text(sql), get_engine())
    if df.empty:
        return []
    return df.to_dict(orient='records')


def save_analysis_result(result) -> bool:
    """保存分析结果到数据库"""
    try:
        engine = get_engine()

        # 转换为字典
        data = result.to_dict()

        # 检查是否已存在
        check_sql = """
            SELECT id FROM stock_analysis_result
            WHERE stock_code = :code AND analysis_date = :date
        """
        check_df = pd.read_sql(
            text(check_sql),
            engine,
            params={'code': data['stock_code'], 'date': data['analysis_date']}
        )

        if not check_df.empty:
            # 更新
            update_sql = """
                UPDATE stock_analysis_result SET
                    stock_name = :stock_name,
                    last_news_time = :last_news_time,
                    long_term_score = :long_term_score,
                    fundamental_score = :fundamental_score,
                    growth_score = :growth_score,
                    valuation_score = :valuation_score,
                    risk_score = :risk_score,
                    short_term_score = :short_term_score,
                    capital_score = :capital_score,
                    technical_score = :technical_score,
                    sentiment_score = :sentiment_score,
                    event_score = :event_score,
                    event_risk_score = :event_risk_score,
                    event_risk_level = :event_risk_level,
                    event_risk_detail = :event_risk_detail,
                    recommend_status = :recommend_status,
                    recommend_reason = :recommend_reason,
                    summary = :summary,
                    recommendation = :recommendation,
                    strengths = :strengths,
                    risks = :risks,
                    updated_at = NOW()
                WHERE stock_code = :stock_code AND analysis_date = :analysis_date
            """
            import json
            with engine.connect() as conn:
                conn.execute(text(update_sql), {
                    'stock_code': data['stock_code'],
                    'analysis_date': data['analysis_date'],
                    'stock_name': data['stock_name'],
                    'last_news_time': data.get('last_news_time'),
                    'long_term_score': data.get('long_term_score'),
                    'fundamental_score': data['scores'].get('fundamental'),
                    'growth_score': data['scores'].get('growth'),
                    'valuation_score': data['scores'].get('valuation'),
                    'risk_score': data['scores'].get('risk'),
                    'short_term_score': data.get('short_term_score'),
                    'capital_score': data['scores'].get('capital'),
                    'technical_score': data['scores'].get('technical'),
                    'sentiment_score': data['scores'].get('sentiment'),
                    'event_score': data['scores'].get('event'),
                    'event_risk_score': data['event_risk'].get('score'),
                    'event_risk_level': data['event_risk'].get('level'),
                    'event_risk_detail': json.dumps(data['event_risk'].get('events', []), ensure_ascii=False),
                    'recommend_status': data['recommend'].get('status'),
                    'recommend_reason': data['recommend'].get('reason'),
                    'summary': data.get('summary'),
                    'recommendation': data.get('recommendation'),
                    'strengths': json.dumps(data.get('strengths', []), ensure_ascii=False),
                    'risks': json.dumps(data.get('risks', []), ensure_ascii=False),
                })
                conn.commit()
        else:
            # 插入
            insert_sql = """
                INSERT INTO stock_analysis_result (
                    stock_code, stock_name, analysis_date, last_news_time,
                    long_term_score, fundamental_score, growth_score, valuation_score, risk_score,
                    short_term_score, capital_score, technical_score, sentiment_score, event_score,
                    event_risk_score, event_risk_level, event_risk_detail,
                    recommend_status, recommend_reason,
                    summary, recommendation, strengths, risks
                ) VALUES (
                    :stock_code, :stock_name, :analysis_date, :last_news_time,
                    :long_term_score, :fundamental_score, :growth_score, :valuation_score, :risk_score,
                    :short_term_score, :capital_score, :technical_score, :sentiment_score, :event_score,
                    :event_risk_score, :event_risk_level, :event_risk_detail,
                    :recommend_status, :recommend_reason,
                    :summary, :recommendation, :strengths, :risks
                )
            """
            import json
            with engine.connect() as conn:
                conn.execute(text(insert_sql), {
                    'stock_code': data['stock_code'],
                    'stock_name': data['stock_name'],
                    'analysis_date': data['analysis_date'],
                    'last_news_time': data.get('last_news_time'),
                    'long_term_score': data.get('long_term_score'),
                    'fundamental_score': data['scores'].get('fundamental'),
                    'growth_score': data['scores'].get('growth'),
                    'valuation_score': data['scores'].get('valuation'),
                    'risk_score': data['scores'].get('risk'),
                    'short_term_score': data.get('short_term_score'),
                    'capital_score': data['scores'].get('capital'),
                    'technical_score': data['scores'].get('technical'),
                    'sentiment_score': data['scores'].get('sentiment'),
                    'event_score': data['scores'].get('event'),
                    'event_risk_score': data['event_risk'].get('score'),
                    'event_risk_level': data['event_risk'].get('level'),
                    'event_risk_detail': json.dumps(data['event_risk'].get('events', []), ensure_ascii=False),
                    'recommend_status': data['recommend'].get('status'),
                    'recommend_reason': data['recommend'].get('reason'),
                    'summary': data.get('summary'),
                    'recommendation': data.get('recommendation'),
                    'strengths': json.dumps(data.get('strengths', []), ensure_ascii=False),
                    'risks': json.dumps(data.get('risks', []), ensure_ascii=False),
                })
                conn.commit()

        return True
    except Exception as e:
        logger.error(f"保存分析结果失败 {result.stock_code}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='盘后全量分析')
    parser.add_argument('--limit', type=int, help='只分析前N只股票')
    parser.add_argument('--code', type=str, help='只分析单只股票')
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"开始盘后全量分析，时间：{start_time}")

    # 初始化引擎
    engine = StockAnalysisEngine()

    # 获取股票列表
    if args.code:
        stocks = [{'stock_code': args.code.strip().zfill(6), 'short_name': ''}]
    else:
        stocks = get_all_active_stocks(limit=args.limit)

    logger.info(f"共 {len(stocks)} 只股票待分析")

    # 统计
    success_count = 0
    fail_count = 0
    status_counts = {'ALLOW': 0, 'SUSPENDED': 0, 'BLOCK': 0}

    # 逐只分析
    for i, stock in enumerate(stocks, 1):
        code = stock['stock_code']
        try:
            result = engine.analyze(code, full_data=True)

            if save_analysis_result(result):
                success_count += 1
                status_counts[result.recommend.status] = status_counts.get(result.recommend.status, 0) + 1

                if i % 100 == 0 or i == len(stocks):
                    logger.info(f"进度：{i}/{len(stocks)}，成功：{success_count}，失败：{fail_count}")
                    logger.info(f"推荐状态分布：{status_counts}")
            else:
                fail_count += 1

        except Exception as e:
            logger.error(f"分析 {code} 失败: {e}")
            fail_count += 1

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"分析完成，耗时：{duration:.1f}秒")
    logger.info(f"成功：{success_count}，失败：{fail_count}")
    logger.info(f"推荐状态分布：{status_counts}")


if __name__ == '__main__':
    main()
