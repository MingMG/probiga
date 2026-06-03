# -*- coding: utf-8 -*-
"""
周末/节假日事件风险检测

在周末和节假日运行，检测是否有新的重大公告或新闻导致推荐失效。
解决"周五推荐，周末利空，周一仍然推荐"的问题。

使用方法：
    python -m biz.analysis.sync_event_risk_check
"""

import sys
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


def get_allowed_recommendations() -> list:
    """获取所有允许推荐的股票"""
    sql = """
        SELECT stock_code, stock_name
        FROM stock_analysis_result
        WHERE analysis_date = (
            SELECT MAX(analysis_date) FROM stock_analysis_result
        )
        AND recommend_status = 'ALLOW'
        ORDER BY short_term_score DESC
    """
    df = pd.read_sql(text(sql), get_engine())
    if df.empty:
        return []
    return df.to_dict(orient='records')


def update_analysis_result(result) -> bool:
    """更新分析结果"""
    try:
        engine = get_engine()
        data = result.to_dict()

        import json

        update_sql = """
            UPDATE stock_analysis_result SET
                last_news_time = :last_news_time,
                event_risk_score = :event_risk_score,
                event_risk_level = :event_risk_level,
                event_risk_detail = :event_risk_detail,
                recommend_status = :recommend_status,
                recommend_reason = :recommend_reason,
                summary = :summary,
                recommendation = :recommendation,
                risks = :risks,
                updated_at = NOW()
            WHERE stock_code = :stock_code
              AND analysis_date = (
                  SELECT MAX(analysis_date) FROM stock_analysis_result WHERE stock_code = :stock_code
              )
        """

        with engine.connect() as conn:
            conn.execute(text(update_sql), {
                'stock_code': data['stock_code'],
                'last_news_time': data.get('last_news_time'),
                'event_risk_score': data['event_risk'].get('score'),
                'event_risk_level': data['event_risk'].get('level'),
                'event_risk_detail': json.dumps(data['event_risk'].get('events', []), ensure_ascii=False),
                'recommend_status': data['recommend'].get('status'),
                'recommend_reason': data['recommend'].get('reason'),
                'summary': data.get('summary'),
                'recommendation': data.get('recommendation'),
                'risks': json.dumps(data.get('risks', []), ensure_ascii=False),
            })
            conn.commit()

        # 同时更新推荐股票表
        if data['recommend'].get('status') != 'ALLOW':
            update_rec_sql = """
                UPDATE st_recommended_stocks SET
                    recommend_status = :status,
                    recommend_reason = :reason,
                    event_risk_level = :risk_level,
                    last_check_time = NOW()
                WHERE stock_code = :code
            """
            with engine.connect() as conn:
                conn.execute(text(update_rec_sql), {
                    'code': data['stock_code'],
                    'status': data['recommend'].get('status'),
                    'reason': data['recommend'].get('reason'),
                    'risk_level': data['event_risk'].get('level'),
                })
                conn.commit()

        return True
    except Exception as e:
        logger.error(f"更新分析结果失败 {result.stock_code}: {e}")
        return False


def main():
    start_time = datetime.now()
    logger.info(f"开始事件风险检测，时间：{start_time}")

    # 初始化引擎
    engine = StockAnalysisEngine()

    # 获取所有允许推荐的股票
    stocks = get_allowed_recommendations()
    logger.info(f"共 {len(stocks)} 只股票需要检测")

    # 统计
    status_changes = []
    check_count = 0

    # 逐只检测
    for i, stock in enumerate(stocks, 1):
        code = stock['stock_code']
        name = stock.get('stock_name', '')

        try:
            result = engine.analyze(code, full_data=True)
            check_count += 1

            # 检查推荐状态是否变化
            if result.recommend.status != 'ALLOW':
                status_changes.append({
                    'code': code,
                    'name': name,
                    'status': result.recommend.status,
                    'reason': result.recommend.reason,
                    'risk_level': result.event_risk.level,
                    'events': result.event_risk.events,
                })

                # 更新数据库
                update_analysis_result(result)

            if i % 50 == 0:
                logger.info(f"进度：{i}/{len(stocks)}")

        except Exception as e:
            logger.error(f"检测 {code} 失败: {e}")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"事件风险检测完成，耗时：{duration:.1f}秒")
    logger.info(f"检测：{check_count}只，状态变化：{len(status_changes)}只")

    if status_changes:
        logger.warning(f"有 {len(status_changes)} 只股票推荐状态发生变化：")
        for change in status_changes:
            logger.warning(f"  {change['code']} {change['name']}: {change['status']}")
            logger.warning(f"    原因：{change['reason']}")
            logger.warning(f"    风险等级：{change['risk_level']}")
            if change['events']:
                for event in change['events']:
                    logger.warning(f"    事件：{event.get('type')} - {event.get('title', event.get('message', ''))}")


if __name__ == '__main__':
    main()
