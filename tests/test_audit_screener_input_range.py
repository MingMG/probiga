from tools import audit_screener_input_range


def test_audit_source_diversity_and_delisted_master_gaps_are_informational(monkeypatch):
    class Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

        def mappings(self):
            return self

        def all(self):
            return self._rows

    class Connection:
        def __init__(self, responses):
            self.responses = iter(responses)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return Result(next(self.responses))

    class Engine:
        def __init__(self, responses):
            self.responses = responses

        def connect(self):
            return Connection(self.responses)

    business = Engine([
        [("2026-08-10",)],
        [
            {
                "stock_code": "600000",
                "trade_date": "2026-08-10",
                "main_net_inflow": 3.0,
                "max_net_inflow": 1.0,
                "lg_net_inflow": 2.0,
                "mid_net_inflow": -1.0,
                "sm_net_inflow": -2.0,
                "data_source": "source_a",
            },
            {
                "stock_code": "600001",
                "trade_date": "2026-08-10",
                "main_net_inflow": 7.0,
                "max_net_inflow": 3.0,
                "lg_net_inflow": 4.0,
                "mid_net_inflow": -2.0,
                "sm_net_inflow": -5.0,
                "data_source": "source_b",
            },
        ],
        [{"stock_code": "688287", "trade_date": "2026-08-10"}],
        [("600000", "1999-01-01"), ("600001", "2000-01-01")],
    ])
    kline = Engine([
        [("600000", "2026-08-10"), ("600001", "2026-08-10")],
    ])
    monkeypatch.setattr(audit_screener_input_range, "create_batch_engine", lambda: business)
    monkeypatch.setattr(audit_screener_input_range, "get_kline_engine", lambda: kline)

    report = audit_screener_input_range.audit_inputs("2026-08-10", "2026-08-10")

    assert report["status"] == "pass"
    assert report["hard_failures"] == {
        "missing_trade_dates_for_flow": [],
        "flow_dates_below_minimum_coverage": [],
        "flow_duplicate_business_keys": 0,
        "flow_non_finite_rows": 0,
        "flow_main_component_identity_failures": 0,
        "flow_market_balance_identity_failures": 0,
        "missing_lhb_trade_dates": [],
        "lhb_duplicate_business_keys": 0,
        "prelisting_lhb_rows": 0,
    }
    assert report["informational"] == {
        "flow_source_count": 2,
        "lhb_codes_absent_from_current_master": ["688287"],
    }
