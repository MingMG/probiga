from datetime import date, datetime, time


SCHEMA = "probiga.trading-v3-premarket-deadline.v1"
CUTOFF = time(9, 30)


def valid_premarket_deadline_receipt(payload):
    try:
        session = date.fromisoformat(payload["as_of"])
        deadline = datetime.fromisoformat(payload["deadline_at"])
        observed = datetime.fromisoformat(payload["observed_at"])
        decision = datetime.fromisoformat(payload["decision_at"])
        return (
            payload.get("schema") == SCHEMA
            and payload.get("status") == "blocked"
            and payload.get("retryable") is False
            and payload.get("reason") == "PREMARKET_DEADLINE_MISSED"
            and payload.get("order_authority") is False
            and all(clock.tzinfo is None for clock in (deadline, observed, decision))
            and deadline == datetime.combine(session, CUTOFF)
            and observed.date() == decision.date() == session
            and decision <= observed and deadline <= observed
            and payload.get("notification") == {
                "status": "suppressed", "reason": "PREMARKET_DEADLINE_MISSED",
            }
        )
    except (KeyError, TypeError, ValueError):
        return False
