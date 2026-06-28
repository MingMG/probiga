# -*- coding: utf-8 -*-
"""
事件风险引擎（核心模块）

目标：实时判断是否发生新的重大事件，从而导致原有推荐失效。

这是整个系统最重要的模块，解决"周五推荐，周末利空，周一仍然推荐"的问题。

风险等级：
- LOW: 无风险
- MEDIUM: 中等风险，需关注
- HIGH: 高风险，暂停推荐
- CRITICAL: 重大风险，禁止推荐
"""

from datetime import datetime, timedelta

from .date_context import extract_analysis_date


class EventRiskEngine:
    """事件风险引擎"""

    # 风险等级常量
    LEVEL_LOW = "LOW"
    LEVEL_MEDIUM = "MEDIUM"
    LEVEL_HIGH = "HIGH"
    LEVEL_CRITICAL = "CRITICAL"

    # 关键词库
    CRITICAL_KEYWORDS = [
        '立案', '退市', '暴雷', '造假', 'ST', '*ST',
        '重大违法', '强制退市', '暂停上市', '终止上市',
        '立案调查', '立案侦查', '欺诈发行',
    ]

    HIGH_KEYWORDS = [
        '减持', '解禁', '质押', '诉讼', '处罚', '问询',
        '业绩预亏', '业绩大幅下降', '被ST', '风险警示',
        '监管函', '警示函', '通报批评', '公开谴责',
        '大股东减持', '高管减持', '减持计划',
    ]

    MEDIUM_KEYWORDS = [
        '高管变动', '审计意见', '关联交易',
        '商誉减值', '计提', '坏账', '资产减值',
        '业绩下滑', '盈利下降', '亏损',
    ]

    def analyze(self, data: dict) -> dict:
        """
        返回事件风险评估

        Args:
            data: StockDataLoader.load_full_data() 返回的数据

        Returns:
            {
                'event_risk_score': float,  # 0-100, 越低越危险
                'event_risk_level': str,    # LOW/MEDIUM/HIGH/CRITICAL
                'triggered_events': list,   # 触发事件列表
                'strengths': list,
                'risks': list,
            }
        """
        risk_score = 100  # 初始满分（无风险）
        risk_level = self.LEVEL_LOW
        triggered_events = []
        analysis_date = extract_analysis_date(data)

        # 1. 检查最新公告（24小时内）
        news = data.get('news', {})
        notices = news.get('notices', [])
        for notice in notices[:10]:
            level, keywords = self._check_notice_risk(notice)
            if level:
                triggered_events.append({
                    'type': 'notice',
                    'title': notice.get('title', ''),
                    'date': str(notice.get('notice_date', '')),
                    'level': level,
                    'keywords': keywords,
                })
                risk_score, risk_level = self._update_risk(risk_score, risk_level, level)

        # 2. 检查最新新闻（12小时内）
        news_items = news.get('news', [])
        for item in news_items[:10]:
            level, keywords = self._check_news_risk(item)
            if level:
                triggered_events.append({
                    'type': 'news',
                    'title': item.get('title', ''),
                    'time': str(item.get('publish_time', '')),
                    'level': level,
                    'keywords': keywords,
                })
                risk_score, risk_level = self._update_risk(risk_score, risk_level, level)

        # 3. 检查解禁风险（未来7天内有解禁）
        lifting = data.get('lifting', {})
        if lifting.get('has_lifting_soon'):
            lift_date_str = lifting.get('lift_date', '')
            if lift_date_str:
                try:
                    lift_date = datetime.strptime(str(lift_date_str), '%Y-%m-%d').date()
                    anchor_date = analysis_date or datetime.now().date()
                    days_to_lift = (lift_date - anchor_date).days
                    if days_to_lift <= 7:
                        triggered_events.append({
                            'type': 'lifting',
                            'date': lift_date_str,
                            'amount': lifting.get('amount'),
                            'ratio': lifting.get('ratio'),
                            'level': self.LEVEL_MEDIUM,
                            'keywords': ['解禁'],
                        })
                        risk_score, risk_level = self._update_risk(
                            risk_score, risk_level, self.LEVEL_MEDIUM
                        )
                except (ValueError, TypeError):
                    pass

        # 4. 检查通达信扫雷风险
        mine = data.get('mine_clearance', {})
        mine_score = mine.get('score')
        if mine_score is not None and float(mine_score) < 60:
            f_type = mine.get('f_type', '')
            s_type = mine.get('s_type', '')
            reason = mine.get('reason', '')
            triggered_events.append({
                'type': 'mine_clearance',
                'score': mine_score,
                'f_type': f_type,
                's_type': s_type,
                'reason': reason,
                'level': self.LEVEL_MEDIUM,
                'keywords': ['扫雷风险'],
            })
            risk_score, risk_level = self._update_risk(
                risk_score, risk_level, self.LEVEL_MEDIUM
            )

        # 5. 检查股东人数异常（大幅增加可能意味着筹码分散）
        holder = data.get('holder', {})
        holder_num_ratio = holder.get('holder_num_ratio')
        if holder_num_ratio is not None and float(holder_num_ratio) > 20:
            triggered_events.append({
                'type': 'holder',
                'message': f'股东人数大幅增加{holder_num_ratio}%',
                'level': self.LEVEL_MEDIUM,
                'keywords': ['股东人数增加'],
            })
            risk_score, risk_level = self._update_risk(
                risk_score, risk_level, self.LEVEL_MEDIUM
            )

        # 生成风险提示
        risks = []
        for event in triggered_events:
            if event['level'] in [self.LEVEL_HIGH, self.LEVEL_CRITICAL]:
                risks.append(f"{event['type']}: {event.get('title', event.get('message', ''))}")

        return {
            'event_risk_score': risk_score,
            'event_risk_level': risk_level,
            'triggered_events': triggered_events,
            'strengths': [],
            'risks': risks,
        }

    def _check_notice_risk(self, notice: dict) -> tuple:
        """
        检查公告风险

        Returns:
            (level, matched_keywords) 或 (None, [])
        """
        title = notice.get('title', '')

        for kw in self.CRITICAL_KEYWORDS:
            if kw in title:
                return self.LEVEL_CRITICAL, [kw]

        for kw in self.HIGH_KEYWORDS:
            if kw in title:
                return self.LEVEL_HIGH, [kw]

        for kw in self.MEDIUM_KEYWORDS:
            if kw in title:
                return self.LEVEL_MEDIUM, [kw]

        return None, []

    def _check_news_risk(self, news: dict) -> tuple:
        """
        检查新闻风险

        Returns:
            (level, matched_keywords) 或 (None, [])
        """
        title = news.get('title', '')

        for kw in self.CRITICAL_KEYWORDS:
            if kw in title:
                return self.LEVEL_CRITICAL, [kw]

        for kw in self.HIGH_KEYWORDS:
            if kw in title:
                return self.LEVEL_HIGH, [kw]

        for kw in self.MEDIUM_KEYWORDS:
            if kw in title:
                return self.LEVEL_MEDIUM, [kw]

        return None, []

    def _update_risk(self, current_score: float, current_level: str, new_level: str) -> tuple:
        """
        更新风险评分和等级

        Args:
            current_score: 当前风险评分
            current_level: 当前风险等级
            new_level: 新的风险等级

        Returns:
            (new_score, new_level)
        """
        level_scores = {
            self.LEVEL_CRITICAL: 10,
            self.LEVEL_HIGH: 30,
            self.LEVEL_MEDIUM: 60,
            self.LEVEL_LOW: 100,
        }

        level_priority = {
            self.LEVEL_CRITICAL: 4,
            self.LEVEL_HIGH: 3,
            self.LEVEL_MEDIUM: 2,
            self.LEVEL_LOW: 1,
        }

        new_score = min(current_score, level_scores.get(new_level, 100))

        # 取更高的风险等级
        if level_priority.get(new_level, 0) > level_priority.get(current_level, 0):
            new_level = new_level
        else:
            new_level = current_level

        return new_score, new_level
