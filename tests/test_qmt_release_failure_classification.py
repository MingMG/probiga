"""Invalid native replies must never spend a terminal-login recovery budget."""
import pytest

from tools import sync_qmt_index_edge as index
from tools import sync_qmt_stock_edge as stock
from tools import sync_qmt_minute_flow_exact as flow


@pytest.mark.parametrize("kind", ["stock", "index", "flow"])
@pytest.mark.parametrize("failure", [TimeoutError, ConnectionError, ValueError, RuntimeError])
def test_only_transport_errors_enter_login_recovery(monkeypatch, kind, failure):
    modules = {
        "stock": (stock.bridge, lambda: stock._release("a" * 40), stock._StockTransportUnavailable),
        "index": (index.bridge, lambda: index._validate_release("a" * 40), index._IndexTransportUnavailable),
        "flow": (flow.bigqmt_bridge, flow.BigQmtFlowSource(expected_build_sha="a" * 40).identity, flow._MinuteFlowConnectionUnavailable),
    }
    bridge, action, transport_type = modules[kind]

    def fail(**_kwargs):
        raise failure("native reply or transport failed")

    monkeypatch.setattr(bridge, "capabilities", fail)
    with pytest.raises(Exception) as observed:
        action()
    assert isinstance(observed.value, transport_type) == issubclass(failure, OSError)
