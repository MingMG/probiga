# -*- coding: utf-8 -*-
"""
盘中增量更新任务

在交易时间内运行，只更新自选股和推荐股票的分析结果。
相比全量分析，增量更新更快，适合盘中频繁运行。

使用方法：
    python -m biz.analysis.sync_analysis_incremental
"""

import sys
import logging
from pathlib import Path as _Path
from datetime import datetime

# 添加项目根目录到 path
_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from sqlalchemy import text
from server.api.routers._engine import get_engine
from server.engine.stock_analysis_engine import StockAnalysisEngine
from biz.analysis.sync_analysis_result import save_analysis_result

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_portfolio_stocks() -> list:
    """获取自选股列表"""
    sql = """
        SELECT stock_code
        FROM st_user_portfolio
        WHERE shares > 0 OR is_holding = 1
        ORDER BY sort_order
    """
    df = pd.read_sql(text(sql), get_engine())
    if df.empty:
        return []
    return df['stock_code'].tolist()


def get_recommended_stocks() -> list:
    """获取推荐股票列表"""
    sql = """
        SELECT DISTINCT stock_code
        FROM st_recommended_stocks
        WHERE pick_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
          AND (recommend_status IS NULL OR recommend_status = 'ALLOW')
    """
    df = pd.read_sql(text(sql), get_engine())
    if df.empty:
        return []
    return df['stock_code'].tolist()


def update_recommendation_status(result) -> bool:
    """更新推荐股票的状态"""
    try:
        engine = get_engine()

        if result.recommend.status != 'ALLOW':
            update_sql = """
                UPDATE st_recommended_stocks SET
                    recommend_status = :status,
                    recommend_reason = :reason,
                    event_risk_level = :risk_level,
                    last_check_time = NOW()
                WHERE stock_code = :code
                  AND (recommend_status IS NULL OR recommend_status = 'ALLOW')
            """
            with engine.connect() as conn:
                conn.execute(text(update_sql), {
                    'code': result.stock_code,
                    'status': result.recommend.status,
                    'reason': result.recommend.reason,
                    'risk_level': result.event_risk.level,
                })
                conn.commit()

        return True
    except Exception as e:
        logger.error(f"更新推荐状态失败 {result.stock_code}: {e}")
        return False


def main():
    start_time = datetime.now()
    logger.info(f"开始盘中增量更新，时间：{start_time}")

    # 初始化引擎
    engine = StockAnalysisEngine()

    # 获取需要更新的股票
    portfolio_codes = get_portfolio_stocks()
    recommended_codes = get_recommended_stocks()

    # 合并去重
    all_codes = list(set(portfolio_codes + recommended_codes))
    logger.info(f"自选股：{len(portfolio_codes)}只，推荐股：{len(recommended_codes)}只，合计：{len(all_codes)}只")

    # 统计
    success_count = 0
    fail_count = 0
    status_changes = []

    # 逐只分析
    for i, code in enumerate(all_codes, 1):
        try:
            result = engine.analyze(code, full_data=True)

            # 保存分析结果
            if save_analysis_result(result):
                success_count += 1

                # 检查推荐状态是否变化
                if result.recommend.status != 'ALLOW':
                    update_recommendation_status(result)
                    status_changes.append({
                        'code': code,
                        'name': result.stock_name,
                        'status': result.recommend.status,
                        'reason': result.recommend.reason,
                    })
            else:
                fail_count += 1

        except Exception as e:
            logger.error(f"分析 {code} 失败: {e}")
            fail_count += 1

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"增量更新完成，耗时：{duration:.1f}秒")
    logger.info(f"成功：{success_count}，失败：{fail_count}")

    if status_changes:
        logger.warning(f"有 {len(status_changes)} 只股票推荐状态发生变化：")
        for change in status_changes:
            logger.warning(f"  {change['code']} {change['name']}: {change['status']} - {change['reason']}")


if __name__ == '__main__':
    main()
