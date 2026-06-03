# -*- coding: utf-8 -*-
"""
长线投资引擎

目标：判断未来6个月~3年是否具备投资价值

评分维度和权重：
- 基本面 40%: ROE, ROA, 毛利率, 净利率, 资产负债率
- 成长性 30%: 营收同比, 净利润同比, 近4季度增长趋势
- 估值   20%: PE(TTM), PB, 历史PE分位
- 风险   10%: 资产负债率, 解禁, 扫雷, 减持
"""

from .scoring import (
    score_roe, score_gross_margin, score_asset_liab_ratio,
    score_growth_rate, score_percentile,
)


class LongTermEngine:
    """长线投资引擎"""

    WEIGHTS = {
        'fundamental': 0.40,
        'growth': 0.30,
        'valuation': 0.20,
        'risk': 0.10,
    }

    def analyze(self, data: dict) -> dict:
        """
        返回长线评分和子维度评分

        Args:
            data: StockDataLoader.load_full_data() 返回的数据

        Returns:
            {
                'long_term_score': float,
                'fundamental_score': float,
                'growth_score': float,
                'valuation_score': float,
                'risk_score': float,
                'strengths': list,
                'risks': list,
            }
        """
        fundamental = self._calc_fundamental(data)
        growth = self._calc_growth(data)
        valuation = self._calc_valuation(data)
        risk = self._calc_risk(data)

        long_term_score = (
            fundamental * self.WEIGHTS['fundamental'] +
            growth * self.WEIGHTS['growth'] +
            valuation * self.WEIGHTS['valuation'] +
            risk * self.WEIGHTS['risk']
        )

        # 提取优势和风险点
        strengths = []
        risks = []

        if fundamental >= 65:
            strengths.append("基本面优秀")
        elif fundamental < 45:
            risks.append("基本面较弱")

        if growth >= 65:
            strengths.append("成长性突出")
        elif growth < 45:
            risks.append("成长性不足")

        if valuation >= 65:
            strengths.append("估值偏低")
        elif valuation < 40:
            risks.append("估值偏高")

        if risk >= 65:
            strengths.append("风险可控")
        elif risk < 45:
            risks.append("风险较高")

        return {
            'long_term_score': round(long_term_score, 1),
            'fundamental_score': round(fundamental, 1),
            'growth_score': round(growth, 1),
            'valuation_score': round(valuation, 1),
            'risk_score': round(risk, 1),
            'strengths': strengths,
            'risks': risks,
        }

    def _calc_fundamental(self, data: dict) -> float:
        """
        基本面评分 0-100

        数据来源：si_stock_finance 表
        - ROE (加权平均净资产收益率)
        - ROA (总资产收益率)
        - 毛利率
        - 净利率
        - 资产负债率
        """
        finance = data.get('finance', {})
        latest = finance.get('latest', {})

        if not latest:
            return 50  # 无数据时返回中性分

        scores = []

        # ROE 评分
        roe = float(latest.get('roe_wtd') or 0)
        scores.append(score_roe(roe))

        # 毛利率评分
        gross_margin = float(latest.get('gross_margin') or 0)
        scores.append(score_gross_margin(gross_margin))

        # 资产负债率评分（越低越好）
        asset_liab_ratio = float(latest.get('asset_liab_ratio') or 50)
        scores.append(score_asset_liab_ratio(asset_liab_ratio))

        # 净利率评分
        net_margin = float(latest.get('net_margin') or 0)
        if net_margin > 30: scores.append(90)
        elif net_margin > 20: scores.append(75)
        elif net_margin > 10: scores.append(60)
        elif net_margin > 5: scores.append(45)
        elif net_margin > 0: scores.append(30)
        else: scores.append(10)

        # ROA 评分
        roa = float(latest.get('roa_wtd') or 0)
        if roa > 15: scores.append(90)
        elif roa > 10: scores.append(75)
        elif roa > 5: scores.append(60)
        elif roa > 2: scores.append(45)
        elif roa > 0: scores.append(30)
        else: scores.append(10)

        return sum(scores) / len(scores) if scores else 50

    def _calc_growth(self, data: dict) -> float:
        """
        成长性评分 0-100

        数据来源：si_stock_finance 表
        - 营收同比增速
        - 净利润同比增速
        - 近4季度增长趋势
        """
        finance = data.get('finance', {})
        quarters = finance.get('quarters', [])

        if not quarters:
            return 50

        latest = quarters[0]
        scores = []

        # 营收同比增速评分
        rev_growth = float(latest.get('total_rev_yoy_gr') or 0)
        scores.append(score_growth_rate(rev_growth))

        # 净利润同比增速评分
        profit_growth = float(latest.get('net_profit_yoy_gr') or 0)
        scores.append(score_growth_rate(profit_growth))

        # 连续增长趋势（近4季度营收是否持续增长）
        if len(quarters) >= 4:
            rev_values = []
            for q in quarters[:4]:
                rev = float(q.get('total_rev') or 0)
                if rev > 0:
                    rev_values.append(rev)

            if len(rev_values) >= 3:
                # 检查是否连续增长
                increasing = all(rev_values[i] >= rev_values[i+1] for i in range(len(rev_values)-1))
                if increasing:
                    scores.append(90)
                else:
                    # 计算增长趋势
                    growth_count = sum(1 for i in range(len(rev_values)-1) if rev_values[i] > rev_values[i+1])
                    scores.append(40 + growth_count * 15)
            else:
                scores.append(50)
        else:
            scores.append(50)

        return sum(scores) / len(scores) if scores else 50

    def _calc_valuation(self, data: dict) -> float:
        """
        估值评分 0-100

        数据来源：
        - PE(TTM) 和历史分位
        - PB 和历史分位

        分位越低，估值越便宜，评分越高
        """
        valuation = data.get('valuation', {})

        if not valuation:
            return 50

        scores = []

        # PE分位评分（分位越低越便宜）
        pe_percentile = valuation.get('pe_percentile')
        if pe_percentile is not None:
            scores.append(score_percentile(pe_percentile, reverse=True))

        # PB分位评分（分位越低越便宜）
        pb_percentile = valuation.get('pb_percentile')
        if pb_percentile is not None:
            scores.append(score_percentile(pb_percentile, reverse=True))

        # 估值判定
        verdict = valuation.get('verdict')
        if verdict == "偏低":
            scores.append(80)
        elif verdict == "合理":
            scores.append(60)
        elif verdict == "偏高":
            scores.append(30)
        else:
            scores.append(50)

        return sum(scores) / len(scores) if scores else 50

    def _calc_risk(self, data: dict) -> float:
        """
        风险评分 0-100 (越高越安全)

        数据来源：
        - 资产负债率
        - 解禁风险
        - 通达信扫雷
        - 减持公告
        """
        scores = []

        # 资产负债率
        finance = data.get('finance', {})
        latest = finance.get('latest', {})
        asset_liab_ratio = float(latest.get('asset_liab_ratio') or 50)
        scores.append(score_asset_liab_ratio(asset_liab_ratio))

        # 解禁风险
        lifting = data.get('lifting', {})
        if lifting.get('has_lifting_soon'):
            ratio = float(lifting.get('ratio') or 0)
            if ratio > 5:
                scores.append(20)  # 大额解禁
            elif ratio > 2:
                scores.append(40)
            else:
                scores.append(60)
        else:
            scores.append(85)  # 无解禁

        # 通达信扫雷
        mine = data.get('mine_clearance', {})
        mine_score = mine.get('score')
        if mine_score is not None:
            # 扫雷分数越高越安全
            scores.append(float(mine_score))
        else:
            scores.append(70)  # 无数据时给中性分

        # 减持公告检测（从公告中检测关键词）
        news = data.get('news', {})
        notices = news.get('notices', [])
        has_reduction = False
        for notice in notices[:5]:  # 只检查最近5条
            title = notice.get('title', '')
            if '减持' in title or '大股东减持' in title:
                has_reduction = True
                break
        if has_reduction:
            scores.append(30)
        else:
            scores.append(80)

        return sum(scores) / len(scores) if scores else 60
