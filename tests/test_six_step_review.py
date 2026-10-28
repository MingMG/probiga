import pandas as pd

from biz.review.six_step import (
    _limit_pct,
    _limit_price,
    _memberships,
    _minute_times,
    render_six_step,
)


def test_six_step_review_renders_evidence_and_causal_boundaries():
    payload = {
        "date": "2026-10-28",
        "market": {
            "return_median_pct": 1.34,
            "up_coverage": 0.7462,
            "amount_change_pct": 9.34,
            "total_amount": 1_478_581_856_834,
        },
        "breadth": {"status": "普涨"},
        "cycle": {"cycle": "发酵"},
        "board": {
            "limit_up_count": 72,
            "limit_down_count": 3,
            "broken_board_count": 18,
            "broken_rate": 20.0,
            "max_boards": 4,
        },
        "index_lines": ["上证指数 4000.00点 +1.00%，成交额 8000.00亿元"],
        "indices": [{
            "name": "上证指数",
            "status": "已获取",
            "support": 3900.0,
            "resistance": 4050.0,
        }],
        "sectors": {
            "top": [{"name": "半导体", "change_pct": 4.85, "hot_value": 180000}],
            "weak": [{"name": "银行", "change_pct": -0.80, "hot_value": 10000}],
        },
        "mainlines": [{
            "name": "半导体",
            "change_pct": 4.85,
            "reason": "板块+4.85%；成分股涨停5只；上涨占比 72.0%；主力净流入 +8.00亿元；领涨股甲(000001) +20.00%",
            "persistence": "延续",
            "status": "已获取",
            "leader": "甲(000001) +20.00%",
            "limit_up_count": 5,
            "up_ratio": 72.0,
            "net_flow": 800_000_000,
        }],
        "leaders": [{
            "code": "000001",
            "name": "甲",
            "change_pct": 20.0,
            "amount": 3_000_000_000,
            "boards": 2,
            "status": "涨停",
        }],
        "leader_details": {
            "000001": {
                "sectors": ["半导体"],
                "first": "09:35",
                "last": "14:58",
                "state": "封板",
            }
        },
        "smashers": [{
            "code": "000002",
            "name": "乙",
            "change_pct": -9.5,
            "amount": 2_000_000_000,
        }],
        "timeline": [{
            "time": "09:35",
            "event_type": "异动启动",
            "subject": "甲(000001)",
            "sector": "半导体",
            "evidence": "09:35首次触及涨停价，成交额3.00亿元",
            "cause": "同板块后续出现涨停跟随，时间存在先后",
            "result": "封板至14:58",
        }, {
            "time": "14:38",
            "event_type": "分歧转弱",
            "subject": "双环传动(002472)",
            "sector": "机器人概念、自动化设备",
            "evidence": "14:37最后触及涨停价，14:38跌破涨停价",
            "cause": "涨停后价格与板块承接同步转弱",
            "result": "炸板，收盘+8.91%",
        }],
        "late_break_wave": [{
            "code": "002472",
            "name": "双环传动",
            "break": "14:38",
            "last_limit": "14:37",
            "change_pct": 8.91,
            "sectors": ["机器人概念", "自动化设备"],
        }],
        "robot_breaks": [{
            "code": "002472",
            "name": "双环传动",
            "break": "14:38",
            "last_limit": "14:37",
            "change_pct": 8.91,
            "sectors": ["机器人概念", "自动化设备"],
        }],
        "robot_direction": {
            "status": "已获取",
            "sectors": ["机器人概念", "自动化设备"],
            "member_count": 120,
            "up_ratio": 62.5,
            "limit_up_count": 4,
            "broken_count": 7,
            "late_break_count": 1,
            "net_flow": -250_000_000,
        },
        "break_sector_counts": [
            {"sector": "机器人概念", "count": 7},
            {"sector": "半导体", "count": 4},
        ],
        "flows": {
            "inflow": [{"code": "000001", "name": "甲", "value": 500_000_000, "sector": "半导体"}],
            "outflow": [{"code": "000002", "name": "乙", "value": -300_000_000, "sector": "银行"}],
            "north": 1_000_000_000,
            "longhubang": [{
                "code": "000001",
                "name": "甲",
                "value": 200_000_000,
                "reason": "日涨幅偏离值达7%",
            }],
            "notices": [{
                "code": "000001",
                "name": "甲",
                "title": "回购方案",
                "status": "催化词",
                "time": "19:30",
            }],
        },
        "watch_pool": [{
            "sector": "半导体",
            "catalyst": "板块涨幅 +4.85%",
            "stocks": [{
                "code": "000001",
                "name": "甲",
                "evidence": "涨跌幅 +20.00%；成交额 30.00亿元；换手率 12.00%",
                "observation": "观察开板后回封强度与板块跟随",
                "risk": "高位连板波动",
                "status": "已获取",
            }],
        }],
        "notes": ["板块数据来自系统快照"],
    }

    report = render_six_step(payload)

    headings = [
        "## 一、今日基调",
        "## 二、主线与板块",
        "## 三、龙头与个股",
        "## 四、盘中时间轴（核心）",
        "## 五、资金与消息",
        "## 六、明日推演",
        "## 七、一句话总结",
    ]
    positions = [report.index(item) for item in headings]
    assert positions == sorted(positions)
    assert "| 09:35 | 异动启动 | 甲(000001) |" in report
    assert "时间存在先后" in report
    assert "半导体" in report
    assert "明日优先关注：半导体" in report
    assert "尾盘炸板潮（14:30—15:00）：全市场 1 只" in report
    assert "双环传动(002472) 14:38" in report
    assert "机器人方向" in report
    assert "全天炸板 7 只" in report
    assert "尾盘炸板 1 只" in report
    assert "机器人方向二次分歧" in report
    assert "| 14:38 | 分歧转弱 | 双环传动(002472) |" in report
    assert "北向净额：+10.00亿元" in report
    assert "龙虎榜净额靠前：甲(000001) +2.00亿元" in report
    assert "【观察池，不构成推荐】" in report
    assert "以上为信息整理，不构成投资建议。" in report
    assert "建议买入" not in report
    assert "目标价" not in report
    assert "必涨" not in report


def test_minute_times_uses_actual_break_after_last_limit_hit(monkeypatch):
    frame = pd.DataFrame([
        {"stock_code": "002472", "trade_time": "2026-10-28 09:30:00", "price": 27.0},
        {"stock_code": "002472", "trade_time": "2026-10-28 14:37:00", "price": 28.52},
        {"stock_code": "002472", "trade_time": "2026-10-28 14:38:00", "price": 28.20},
        {"stock_code": "002472", "trade_time": "2026-10-28 15:00:00", "price": 28.30},
    ])

    monkeypatch.setattr(
        "biz.review.six_step._read_frame",
        lambda *_args, **_kwargs: frame,
    )
    result = _minute_times(
        object(),
        pd.Timestamp("2026-10-28").date(),
        [{
            "code": "002472",
            "status": "炸板",
            "limit_price": 28.52,
            "pre_close": 25.7,
        }],
    )

    assert result["002472"] == {
        "first": "14:37",
        "last": "14:37",
        "state": "炸板",
        "break": "14:38",
    }


def test_timeline_keeps_tail_events_when_many_early_rows_exist():
    timeline = [
        {
            "time": f"09:{index:02d}",
            "event_type": "异动启动",
            "subject": f"早盘标的{index}(000{index:03d})",
            "sector": "测试",
            "evidence": "早盘事件",
            "cause": "时间先后",
            "result": "记录",
        }
        for index in range(40)
    ]
    timeline.append({
        "time": "14:38",
        "event_type": "分歧转弱",
        "subject": "双环传动(002472)",
        "sector": "机器人概念",
        "evidence": "尾盘跌破涨停价",
        "cause": "涨停后转弱",
        "result": "炸板",
    })

    report = render_six_step({
        "date": "2026-10-28",
        "market": {},
        "breadth": {},
        "cycle": {},
        "board": {},
        "index_lines": [],
        "indices": [],
        "sectors": {},
        "mainlines": [],
        "leaders": [],
        "smashers": [],
        "timeline": timeline,
        "flows": {},
        "watch_pool": [],
        "notes": [],
    })

    assert "| 14:38 | 分歧转弱 | 双环传动(002472) |" in report


def test_membership_fallback_uses_one_date_probe_and_one_relation_query(monkeypatch):
    qmt_calls = []

    def fake_read(_engine, statement, _params=None):
        sql = str(statement)
        if "qmt_concept_member_snapshot" in sql:
            qmt_calls.append(sql)
            if "MAX(snapshot_date)" in sql:
                return pd.DataFrame([{"snapshot_date": "2026-10-23"}])
            return pd.DataFrame([{
                "stock_code": "002472",
                "concept_code": "ROBOT",
                "concept_name": "机器人概念",
            }])
        return pd.DataFrame()

    monkeypatch.setattr("biz.review.six_step._read_frame", fake_read)

    by_stock, by_sector, source = _memberships(
        object(),
        sector_names=["机器人概念"],
        sector_codes={},
        stock_codes=[],
        target=pd.Timestamp("2026-10-28").date(),
    )

    assert len(qmt_calls) == 2
    assert by_stock["002472"] == {"机器人概念"}
    assert by_sector["机器人概念"] == {"002472"}
    assert source == "QMT概念快照 2026-10-23"


def test_review_marks_missing_timeline_instead_of_inventing_causality():
    report = render_six_step({
        "date": "2026-10-28",
        "market": {},
        "breadth": {},
        "cycle": {},
        "board": {},
        "index_lines": [],
        "indices": [],
        "sectors": {},
        "mainlines": [],
        "leaders": [],
        "smashers": [],
        "timeline": [],
        "flows": {},
        "watch_pool": [],
        "notes": ["目标日分钟数据未获取"],
    })

    assert "| 未获取 | 未获取 | 未获取 | 未获取 |" in report
    assert "- 分水岭：未获取" in report
    assert "- 今日无高确定性板块映射，观察池为空。" in report


def test_board_limit_thresholds_and_rounding():
    assert _limit_pct("600000", "浦发银行") == 10.0
    assert _limit_pct("300001", "特锐德") == 20.0
    assert _limit_pct("688001", "华兴源创") == 20.0
    assert _limit_pct("920001", "北交所样本") == 30.0
    assert _limit_pct("600001", "*ST样本") == 5.0
    assert _limit_price(10.01, 10.0) == 11.01
