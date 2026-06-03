# -*- coding: utf-8 -*-
"""
短线交易引擎

目标：判断未来3~20个交易日是否具备上涨机会

评分维度和权重：
- 资金面 40%: 主力净流入, 超大单净流入, 5日/20日资金流, 龙虎榜
- 技术面 30%: MA, MACD, RSI, BOLL, KDJ, 支撑/压力位
- 情绪面 20%: 板块热度, 个股热度, 搜索热度
- 事件催化 10%: 重大订单, 机构调研, 政策利好, 业绩预告
"""

from .scoring import (
    score_capital_flow, score_hot_rank, score_rsi,
    score_ma_trend, score_macd,
)


class ShortTermEngine:
    """短线交易引擎"""

    WEIGHTS = {
        'capital': 0.40,
        'technical': 0.30,
        'sentiment': 0.20,
        'event': 0.10,
    }

    def analyze(self, data: dict) -> dict:
        """
        返回短线评分和子维度评分

        Args:
            data: StockDataLoader.load_full_data() 返回的数据

        Returns:
            {
                'short_term_score': float,
                'capital_score': float,
                'technical_score': float,
                'sentiment_score': float,
                'event_score': float,
                'strengths': list,
                'risks': list,
            }
        """
        capital = self._calc_capital(data)
        technical = self._calc_technical(data)
        sentiment = self._calc_sentiment(data)
        event = self._calc_event(data)

        short_term_score = (
            capital * self.WEIGHTS['capital'] +
            technical * self.WEIGHTS['technical'] +
            sentiment * self.WEIGHTS['sentiment'] +
            event * self.WEIGHTS['event']
        )

        # 提取优势和风险点
        strengths = []
        risks = []

        if capital >= 65:
            strengths.append("主力资金持续流入")
        elif capital < 45:
            risks.append("资金面偏空")

        if technical >= 65:
            strengths.append("技术面强势")
        elif technical < 45:
            risks.append("技术面走弱")

        if sentiment >= 65:
            strengths.append("市场关注度高")
        elif sentiment < 45:
            risks.append("市场关注度低")

        return {
            'short_term_score': round(short_term_score, 1),
            'capital_score': round(capital, 1),
            'technical_score': round(technical, 1),
            'sentiment_score': round(sentiment, 1),
            'event_score': round(event, 1),
            'strengths': strengths,
            'risks': risks,
        }

    def _calc_capital(self, data: dict) -> float:
        """
        资金面评分 0-100

        数据来源：
        - sm_stock_capital_flow_daily: 主力净流入, 超大单净流入
        - st_a_list_daily: 龙虎榜
        """
        capital = data.get('capital', {})
        scores = []

        # 今日主力净流入
        today = capital.get('today', {})
        main_net = float(today.get('main_net_inflow') or 0)
        scores.append(score_capital_flow(main_net))

        # 今日超大单净流入（同花顺数据无细分，用净额代替）
        lg_net = float(today.get('lg_net_inflow') or 0)
        if lg_net == 0:
            lg_net = main_net  # 同花顺数据无细分，用净额代替
        scores.append(score_capital_flow(lg_net))

        # 5日累计净流入
        flow_5d = capital.get('flow_5d')
        if flow_5d is not None:
            scores.append(score_capital_flow(float(flow_5d)))
        else:
            scores.append(50)

        # 20日累计净流入
        flow_20d = capital.get('flow_20d')
        if flow_20d is not None:
            scores.append(score_capital_flow(float(flow_20d)))
        else:
            scores.append(50)

        # 龙虎榜
        dragon_tiger = capital.get('dragon_tiger', {})
        lhb_count = dragon_tiger.get('count_20d', 0)
        inst_net_buy = float(dragon_tiger.get('inst_net_buy') or 0)

        if lhb_count > 0:
            # 有龙虎榜记录
            if inst_net_buy > 0:
                scores.append(80)  # 机构净买入
            else:
                scores.append(40)  # 机构净卖出
        else:
            scores.append(50)  # 无龙虎榜

        return sum(scores) / len(scores) if scores else 50

    def _calc_technical(self, data: dict) -> float:
        """
        技术面评分 0-100

        数据来源：
        - MA均线
        - MACD
        - RSI
        - KDJ
        - BOLL
        - 支撑/压力位
        """
        technical = data.get('technical', {})

        if not technical:
            return 50

        scores = []

        # MA趋势评分
        ma = technical.get('ma', {})
        ma_score = score_ma_trend(
            ma.get('ma5'), ma.get('ma10'),
            ma.get('ma20'), ma.get('ma60')
        )
        scores.append(ma_score)

        # MACD评分
        macd = technical.get('macd', {})
        macd_score = score_macd(
            macd.get('dif', 0),
            macd.get('dea', 0),
            macd.get('golden_cross', False)
        )
        scores.append(macd_score)

        # RSI评分
        rsi = technical.get('rsi', {})
        rsi6 = rsi.get('rsi6')
        scores.append(score_rsi(rsi6))

        # KDJ评分
        kdj = technical.get('kdj', {})
        k = kdj.get('k', 50)
        d = kdj.get('d', 50)
        j = kdj.get('j', 50)

        # KDJ金叉/死叉
        if k > d and j > k:
            kdj_score = 80  # 金叉
        elif k < d and j < k:
            kdj_score = 25  # 死叉
        elif k > 80:
            kdj_score = 30  # 超买
        elif k < 20:
            kdj_score = 70  # 超卖
        else:
            kdj_score = 50
        scores.append(kdj_score)

        # BOLL位置评分
        boll = technical.get('boll', {})
        boll_upper = boll.get('upper')
        boll_lower = boll.get('lower')
        boll_mid = boll.get('mid')
        price = float(data.get('market', {}).get('price') or 0)

        if boll_upper and boll_lower and boll_mid and price > 0:
            # 计算价格在BOLL通道中的位置
            boll_width = boll_upper - boll_lower
            if boll_width > 0:
                position = (price - boll_lower) / boll_width
                if position > 0.9:
                    boll_score = 30  # 接近上轨，可能回调
                elif position > 0.7:
                    boll_score = 50
                elif position > 0.3:
                    boll_score = 70  # 中轨附近
                elif position > 0.1:
                    boll_score = 60
                else:
                    boll_score = 75  # 接近下轨，可能反弹
            else:
                boll_score = 50
        else:
            boll_score = 50
        scores.append(boll_score)

        # 支撑/压力位距离评分
        support = technical.get('support')
        resistance = technical.get('resistance')
        if support and resistance and price > 0:
            # 距离支撑位越近，越可能反弹
            support_dist = (price - support) / price * 100
            # 距离压力位越远，上涨空间越大
            resistance_dist = (resistance - price) / price * 100

            if support_dist < 2:
                support_score = 75  # 接近支撑位
            elif support_dist < 5:
                support_score = 60
            else:
                support_score = 45

            if resistance_dist > 10:
                resistance_score = 75  # 上涨空间大
            elif resistance_dist > 5:
                resistance_score = 60
            else:
                resistance_score = 40

            scores.append((support_score + resistance_score) / 2)
        else:
            scores.append(50)

        return sum(scores) / len(scores) if scores else 50

    def _calc_sentiment(self, data: dict) -> float:
        """
        情绪面评分 0-100

        数据来源：
        - st_hot_rank_fused: 热门排名
        - si_stock_concept_east: 概念归属
        - st_hot_concept_ths_daily: 热门概念
        """
        scores = []

        # 热门排名评分
        hot_rank = data.get('hot_rank', {})
        fused_rank = hot_rank.get('fused_rank')
        scores.append(score_hot_rank(fused_rank))

        # 所属概念热度
        concepts = data.get('concepts', [])
        if concepts:
            # 概念数量越多，覆盖面越广
            concept_count = len(concepts)
            if concept_count >= 5:
                scores.append(75)
            elif concept_count >= 3:
                scores.append(65)
            elif concept_count >= 1:
                scores.append(55)
            else:
                scores.append(40)
        else:
            scores.append(40)

        # 量比评分（成交量活跃度）
        market = data.get('market', {})
        volume_ratio = market.get('volume_ratio')
        if volume_ratio is not None:
            if volume_ratio > 3:
                scores.append(85)  # 非常活跃
            elif volume_ratio > 2:
                scores.append(75)
            elif volume_ratio > 1.5:
                scores.append(65)
            elif volume_ratio > 1:
                scores.append(55)
            elif volume_ratio > 0.5:
                scores.append(45)
            else:
                scores.append(30)  # 成交低迷
        else:
            scores.append(50)

        # 换手率评分
        turnover = market.get('turnover_ratio')
        if turnover is not None:
            turnover = float(turnover)
            if turnover > 10:
                scores.append(80)  # 非常活跃
            elif turnover > 5:
                scores.append(70)
            elif turnover > 3:
                scores.append(60)
            elif turnover > 1:
                scores.append(50)
            else:
                scores.append(35)
        else:
            scores.append(50)

        return sum(scores) / len(scores) if scores else 50

    def _calc_event(self, data: dict) -> float:
        """
        事件催化评分 0-100

        数据来源：
        - si_notice_eastmoney: 公告
        - st_news_flash: 新闻

        检测利好关键词：业绩预增, 重大合同, 机构调研, 增持, 回购, 政策利好
        检测利空关键词：减持, 质押, 诉讼, 处罚, 问询
        """
        news = data.get('news', {})
        notices = news.get('notices', [])
        news_items = news.get('news', [])

        positive_keywords = ['业绩预增', '重大合同', '机构调研', '增持', '回购', '中标', '战略合作', '政策利好']
        negative_keywords = ['减持', '质押', '诉讼', '处罚', '问询', '业绩预亏', '业绩下降']

        positive_count = 0
        negative_count = 0

        # 检查公告
        for notice in notices[:5]:
            title = notice.get('title', '')
            for kw in positive_keywords:
                if kw in title:
                    positive_count += 1
                    break
            for kw in negative_keywords:
                if kw in title:
                    negative_count += 1
                    break

        # 检查新闻
        for item in news_items[:5]:
            title = item.get('title', '')
            for kw in positive_keywords:
                if kw in title:
                    positive_count += 1
                    break
            for kw in negative_keywords:
                if kw in title:
                    negative_count += 1
                    break

        # 计算事件催化评分
        if positive_count > negative_count:
            return min(90, 60 + (positive_count - negative_count) * 10)
        elif negative_count > positive_count:
            return max(20, 50 - (negative_count - positive_count) * 10)
        else:
            return 50
