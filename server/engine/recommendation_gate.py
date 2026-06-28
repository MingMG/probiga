# -*- coding: utf-8 -*-
"""
推荐资格引擎（最终裁判）

前三层引擎只负责分析，真正决定"推不推荐"、"还能不能买"必须由推荐资格引擎决定。

推荐状态：
- ALLOW: 允许推荐
- SUSPENDED: 暂停推荐（等待市场重新定价）
- BLOCK: 禁止推荐
"""

from datetime import date

from .date_context import coerce_date


class RecommendationGate:
    """推荐资格引擎"""

    STATUS_ALLOW = "ALLOW"
    STATUS_SUSPENDED = "SUSPENDED"
    STATUS_BLOCK = "BLOCK"

    def evaluate(
        self,
        long_term: dict,
        short_term: dict,
        event_risk: dict,
        analysis_date: str | None = None,
    ) -> dict:
        """
        综合评估推荐资格

        Args:
            long_term: LongTermEngine.analyze() 返回的结果
            short_term: ShortTermEngine.analyze() 返回的结果
            event_risk: EventRiskEngine.analyze() 返回的结果

        Returns:
            {
                'status': str,      # ALLOW/SUSPENDED/BLOCK
                'reason': str,      # 状态原因
                'strengths': list,  # 优势
                'risks': list,      # 风险
            }
        """
        strengths = []
        risks = []

        # 合并优势和风险
        strengths.extend(long_term.get('strengths', []))
        strengths.extend(short_term.get('strengths', []))
        risks.extend(long_term.get('risks', []))
        risks.extend(short_term.get('risks', []))
        risks.extend(event_risk.get('risks', []))

        # 1. 检查事件风险（最高优先级）
        event_level = event_risk.get('event_risk_level', 'LOW')
        triggered_events = event_risk.get('triggered_events', [])

        if event_level == 'CRITICAL':
            return {
                'status': self.STATUS_BLOCK,
                'reason': '存在重大风险事件，禁止推荐',
                'events': triggered_events,
                'strengths': strengths,
                'risks': risks,
            }

        if event_level == 'HIGH':
            return {
                'status': self.STATUS_SUSPENDED,
                'reason': '存在高风险事件，暂停推荐等待市场重新定价',
                'events': triggered_events,
                'strengths': strengths,
                'risks': risks,
            }

        # 2. 检查是否有新公告（可能导致推荐失效）
        has_new_notice = self._has_new_announcement(triggered_events, analysis_date)
        if has_new_notice:
            return {
                'status': self.STATUS_SUSPENDED,
                'reason': '有新公告发布，等待市场重新定价',
                'events': triggered_events,
                'strengths': strengths,
                'risks': risks,
            }

        # 3. 检查市场情绪极差的情况
        market_mood_score = short_term.get('market_mood_score', 50)
        short_term_score = short_term.get('short_term_score', 0)
        if market_mood_score < 30 and short_term_score < 65:
            return {
                'status': self.STATUS_SUSPENDED,
                'reason': f'市场整体情绪低迷(情绪评分{market_mood_score})，个股难以独立走强',
                'events': triggered_events,
                'strengths': strengths,
                'risks': risks,
            }

        # 4. 检查短线评分是否达标
        if short_term_score < 50:
            return {
                'status': self.STATUS_BLOCK,
                'reason': f'短线评分{short_term_score}过低，不具备交易价值',
                'events': triggered_events,
                'strengths': strengths,
                'risks': risks,
            }

        # 5. 检查长线评分
        long_term_score = long_term.get('long_term_score', 0)
        if long_term_score < 30:
            return {
                'status': self.STATUS_BLOCK,
                'reason': f'长线评分{long_term_score}过低，基本面存在重大问题',
                'events': triggered_events,
                'strengths': strengths,
                'risks': risks,
            }

        # 6. 允许推荐
        return {
            'status': self.STATUS_ALLOW,
            'reason': '各项指标正常，允许推荐',
            'events': triggered_events,
            'strengths': strengths,
            'risks': risks,
        }

    def _has_new_announcement(self, events: list, analysis_date: str | None = None) -> bool:
        """
        检查是否有新公告发布

        如果公告时间晚于上次分析时间，需要暂停推荐
        """
        anchor_date = coerce_date(analysis_date, default=date.today())
        for event in events:
            if event.get('type') == 'notice':
                # 检查公告日期是否是今天
                notice_date = event.get('date', '')
                if notice_date:
                    from datetime import datetime
                    try:
                        if isinstance(notice_date, str):
                            nd = datetime.strptime(notice_date[:10], '%Y-%m-%d').date()
                        else:
                            nd = notice_date
                        if nd >= anchor_date:
                            return True
                    except (ValueError, TypeError):
                        pass
        return False

    def generate_summary(self, long_term: dict, short_term: dict, event_risk: dict, recommend: dict) -> str:
        """
        生成一句话总结

        Args:
            long_term: 长线分析结果
            short_term: 短线分析结果
            event_risk: 事件风险结果
            recommend: 推荐结果

        Returns:
            一句话总结
        """
        lt_score = long_term.get('long_term_score', 0)
        st_score = short_term.get('short_term_score', 0)

        # 比较长线和短线
        if st_score > lt_score + 10:
            trend = "短线强于长线"
        elif lt_score > st_score + 10:
            trend = "长线优于短线"
        else:
            trend = "长短均衡"

        # 推荐状态
        status = recommend.get('status', 'ALLOW')
        if status == 'BLOCK':
            action = "不建议介入"
        elif status == 'SUSPENDED':
            action = "建议观望"
        else:
            if st_score >= 75:
                action = "可关注"
            elif st_score >= 60:
                action = "谨慎关注"
            else:
                action = "暂不推荐"

        return f"{trend}，{action}"

    def generate_recommendation(self, recommend: dict) -> str:
        """
        生成操作建议

        Args:
            recommend: 推荐结果

        Returns:
            操作建议文本
        """
        status = recommend.get('status', 'ALLOW')
        reason = recommend.get('reason', '')

        if status == 'BLOCK':
            return f"禁止推荐：{reason}"
        elif status == 'SUSPENDED':
            return f"暂停推荐：{reason}"
        else:
            return reason or "可正常关注"
