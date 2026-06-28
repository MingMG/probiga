# -*- coding: utf-8 -*-
"""
短线交易引擎

目标：判断未来3~20个交易日是否具备上涨机会

评分维度和权重：
- 资金面 35%: 主力净流入, 超大单净流入, 5日/20日资金流, 龙虎榜
- 技术面 25%: MA, MACD, RSI, BOLL, KDJ, 支撑/压力位
- 情绪面 15%: 板块热度, 个股热度, 搜索热度
- 市场情绪 15%: 涨跌比, 涨停跌停比, 赚钱效应趋势
- 消息面 10%: 利好/利空消息, 时效性加权
"""

from datetime import datetime, date, timedelta

from .date_context import extract_analysis_date
from .scoring import (
    score_capital_flow, score_hot_rank, score_rsi,
    score_ma_trend, score_macd,
)


class ShortTermEngine:
    """短线交易引擎"""

    WEIGHTS = {
        'capital': 0.35,
        'technical': 0.25,
        'sentiment': 0.15,
        'market_mood': 0.15,
        'event': 0.10,
    }

    # ─── 消息面关键词库 ───
    POSITIVE_KEYWORDS = [
        # 业绩利好
        '业绩预增', '业绩大增', '净利润增长', '营收增长', '扭亏为盈', '高送转',
        # 重大事项
        '重大合同', '中标', '战略合作', '框架协议', '项目落地',
        # 资本运作
        '增持', '回购', '股权激励', '员工持股', '定增获批',
        # 机构行为
        '机构调研', '机构买入', '北向买入', '外资增持', '社保基金',
        # 政策利好
        '政策利好', '补贴', '税收优惠', '产业政策', '国家级',
        # 行业利好
        '供需紧张', '产能扩张', '订单饱满', '满产满销',
        # 技术突破
        '专利', '技术突破', '新品发布', '量产',
    ]

    NEGATIVE_KEYWORDS = [
        # 减持解禁
        '减持', '解禁', '大股东减持', '高管减持', '清仓减持',
        # 风险事件
        '质押', '诉讼', '仲裁', '处罚', '立案', '调查',
        # 监管问询
        '问询', '关注函', '监管函', '警示函', '通报批评',
        # 业绩利空
        '业绩预亏', '业绩下降', '业绩下滑', '亏损', '暴雷', '商誉减值',
        # 经营风险
        '停产', '减产', '裁员', '债务违约', '资金链',
        # 退市风险
        'ST', '*ST', '退市', '终止上市',
    ]

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
                'market_mood_score': float,
                'event_score': float,
                'strengths': list,
                'risks': list,
            }
        """
        capital = self._calc_capital(data)
        technical = self._calc_technical(data)
        sentiment = self._calc_sentiment(data)
        market_mood = self._calc_market_mood(data)
        event = self._calc_event(data)

        short_term_score = (
            capital * self.WEIGHTS['capital'] +
            technical * self.WEIGHTS['technical'] +
            sentiment * self.WEIGHTS['sentiment'] +
            market_mood * self.WEIGHTS['market_mood'] +
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

        if market_mood >= 65:
            strengths.append("市场情绪积极")
        elif market_mood < 40:
            risks.append("市场整体情绪低迷")

        if event >= 65:
            strengths.append("消息面利好")
        elif event < 40:
            risks.append("消息面偏空")

        return {
            'short_term_score': round(short_term_score, 1),
            'capital_score': round(capital, 1),
            'technical_score': round(technical, 1),
            'sentiment_score': round(sentiment, 1),
            'market_mood_score': round(market_mood, 1),
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
        情绪面评分 0-100（市场关注度/活跃度）

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

    def _calc_market_mood(self, data: dict) -> float:
        """
        市场情绪评分 0-100（全市场赚钱效应）

        数据来源：
        - sm_stock_kline: 全市场涨跌统计
        - 涨跌比、涨停跌停比、近3日趋势

        评分逻辑：
        - 涨跌比 > 2.0 → 85分（强势市场）
        - 涨跌比 1.5~2.0 → 70分（偏强）
        - 涨跌比 1.0~1.5 → 55分（均衡）
        - 涨跌比 0.5~1.0 → 40分（偏弱）
        - 涨跌比 < 0.5 → 25分（弱势）
        - 涨停多于跌停 → 加分
        - 近3日趋势向上 → 加分
        """
        mood = data.get('market_mood', {})
        if not mood:
            return 50  # 无数据时返回中性

        up_count = mood.get('up_count', 0)
        down_count = mood.get('down_count', 0)
        limit_up = mood.get('limit_up', 0)
        limit_down = mood.get('limit_down', 0)

        # 1. 涨跌比评分
        if down_count > 0:
            ratio = up_count / down_count
        else:
            ratio = 3.0 if up_count > 0 else 1.0

        if ratio > 2.5:
            base_score = 90
        elif ratio > 2.0:
            base_score = 80
        elif ratio > 1.5:
            base_score = 68
        elif ratio > 1.0:
            base_score = 55
        elif ratio > 0.7:
            base_score = 42
        elif ratio > 0.5:
            base_score = 35
        else:
            base_score = 22

        # 2. 涨停跌停修正
        limit_diff = limit_up - limit_down
        if limit_diff > 30:
            base_score = min(100, base_score + 15)
        elif limit_diff > 15:
            base_score = min(100, base_score + 10)
        elif limit_diff > 5:
            base_score = min(100, base_score + 5)
        elif limit_diff < -30:
            base_score = max(0, base_score - 15)
        elif limit_diff < -15:
            base_score = max(0, base_score - 10)
        elif limit_diff < -5:
            base_score = max(0, base_score - 5)

        # 3. 近3日趋势修正（赚钱效应是否扩散）
        recent_days = mood.get('recent_days', [])
        if len(recent_days) >= 2:
            ratios = [d.get('up_ratio', 0.5) for d in recent_days]
            # 连续3日上涨比>55% → 赚钱效应扩散
            if all(r > 0.55 for r in ratios):
                base_score = min(100, base_score + 10)
            # 连续3日上涨比<45% → 亏钱效应扩散
            elif all(r < 0.45 for r in ratios):
                base_score = max(0, base_score - 10)
            # 最近一天明显好转
            elif len(ratios) >= 2 and ratios[-1] > ratios[-2] + 0.1:
                base_score = min(100, base_score + 5)
            # 最近一天明显恶化
            elif len(ratios) >= 2 and ratios[-1] < ratios[-2] - 0.1:
                base_score = max(0, base_score - 5)

        return max(0, min(100, base_score))

    def _calc_event(self, data: dict) -> float:
        """
        消息面评分 0-100

        数据来源：
        - si_notice_eastmoney: 公告
        - st_news_flash: 新闻

        改进：
        1. 扩展关键词库（20+正面, 20+负面）
        2. 时效性加权（今天x3, 昨天x2, 更早x1）
        3. 消息密度（近期消息越多关注度越高）
        """
        news = data.get('news', {})
        notices = news.get('notices', [])
        news_items = news.get('news', [])

        analysis_date = extract_analysis_date(data, default=date.today())
        yesterday = analysis_date - timedelta(days=1)

        positive_count = 0.0
        negative_count = 0.0

        def _get_recency_weight(item_date_str: str) -> float:
            """根据消息日期计算时效性权重"""
            if not item_date_str:
                return 1.0
            try:
                if isinstance(item_date_str, str):
                    d = datetime.strptime(item_date_str[:10], '%Y-%m-%d').date()
                else:
                    d = item_date_str
                if d >= analysis_date:
                    return 3.0
                elif d >= yesterday:
                    return 2.0
                else:
                    return 1.0
            except (ValueError, TypeError):
                return 1.0

        # 检查公告（最多10条）
        for notice in notices[:10]:
            title = notice.get('title', '')
            notice_date = str(notice.get('notice_date', ''))
            weight = _get_recency_weight(notice_date)

            matched_positive = False
            for kw in self.POSITIVE_KEYWORDS:
                if kw in title:
                    positive_count += weight
                    matched_positive = True
                    break

            if not matched_positive:
                for kw in self.NEGATIVE_KEYWORDS:
                    if kw in title:
                        negative_count += weight
                        break

        # 检查新闻（最多10条）
        for item in news_items[:10]:
            title = item.get('title', '')
            pub_time = str(item.get('publish_time', ''))
            weight = _get_recency_weight(pub_time)

            matched_positive = False
            for kw in self.POSITIVE_KEYWORDS:
                if kw in title:
                    positive_count += weight
                    matched_positive = True
                    break

            if not matched_positive:
                for kw in self.NEGATIVE_KEYWORDS:
                    if kw in title:
                        negative_count += weight
                        break

        # 计算消息面评分
        net_signal = positive_count - negative_count

        if net_signal > 0:
            # 利好信号，最多加到95
            return min(95, 55 + net_signal * 8)
        elif net_signal < 0:
            # 利空信号，最多降到10
            return max(10, 55 + net_signal * 8)
        else:
            # 无明确信号
            # 但如果消息很多，说明关注度高，给个中性偏上
            total_messages = len(notices[:10]) + len(news_items[:10])
            if total_messages >= 8:
                return 58  # 高关注度
            elif total_messages >= 4:
                return 53
            else:
                return 50
