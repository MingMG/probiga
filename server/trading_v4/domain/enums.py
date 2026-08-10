"""Value enums owned by the V4 clean-room domain."""

from __future__ import annotations

from enum import Enum


class ValueStrEnum(str, Enum):
    """String enum with stable wire values."""

    def __str__(self) -> str:
        return self.value


class DecisionClock(ValueStrEnum):
    PREMARKET = "PREMARKET"
    INTRADAY = "INTRADAY"
    AFTER_CLOSE = "AFTER_CLOSE"
    EVENT_DRIVEN = "EVENT_DRIVEN"


class ScopeType(ValueStrEnum):
    MARKET = "MARKET"
    SECTOR = "SECTOR"
    INSTRUMENT = "INSTRUMENT"
    PORTFOLIO = "PORTFOLIO"


class AvailabilityStatus(ValueStrEnum):
    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"


class ResearchStatus(ValueStrEnum):
    BACKTEST_READY = "BACKTEST_READY"
    FORWARD_ONLY = "FORWARD_ONLY"
    DISPLAY_ONLY = "DISPLAY_ONLY"


class QualityStatus(ValueStrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class FactorRole(ValueStrEnum):
    """How a factor may influence the clean-room decision pipeline."""

    GATE = "GATE"
    STATE = "STATE"
    ALPHA = "ALPHA"
    RISK = "RISK"
    COST = "COST"
    PORTFOLIO = "PORTFOLIO"
    EXPLANATION = "EXPLANATION"


class ReplayEligibility(ValueStrEnum):
    """Whether a source can be used for historical point-in-time replay."""

    PIT_CERTIFIED = "PIT_CERTIFIED"
    FORWARD_ONLY = "FORWARD_ONLY"
    DISPLAY_ONLY = "DISPLAY_ONLY"
    REPLAY_INELIGIBLE = "REPLAY_INELIGIBLE"


class CertificationStatus(ValueStrEnum):
    PENDING = "PENDING"
    PASSED = "PASSED"
    FAILED = "FAILED"
    REVOKED = "REVOKED"


class ProbabilityKind(ValueStrEnum):
    HEURISTIC_PRIOR = "HEURISTIC_PRIOR"
    EMPIRICALLY_CALIBRATED = "EMPIRICALLY_CALIBRATED"
    MODEL_PREDICTED = "MODEL_PREDICTED"


class CandidateStatus(ValueStrEnum):
    DATA_BLOCKED = "DATA_BLOCKED"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    WATCH = "WATCH"
    CONDITIONAL = "CONDITIONAL"
    PAPER_ACTIONABLE = "PAPER_ACTIONABLE"
    EXECUTION_BLOCKED = "EXECUTION_BLOCKED"
    HOLD_ONLY = "HOLD_ONLY"
    EXIT_ALERT = "EXIT_ALERT"


class ActionType(ValueStrEnum):
    NO_ACTION = "NO_ACTION"
    WATCH = "WATCH"
    BUY = "BUY"
    ADD = "ADD"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    EXIT = "EXIT"


class ExecutionSide(ValueStrEnum):
    BUY = "BUY"
    SELL = "SELL"


class LimitPolicy(ValueStrEnum):
    PASSIVE_LIMIT = "PASSIVE_LIMIT"
    MARKETABLE_LIMIT = "MARKETABLE_LIMIT"
    FIXED_LIMIT = "FIXED_LIMIT"
    PROTECTIVE_LIMIT = "PROTECTIVE_LIMIT"


class DecisionBundleStatus(ValueStrEnum):
    DATA_BLOCKED = "DATA_BLOCKED"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    WATCH_ONLY = "WATCH_ONLY"
    PAPER_ACTIONABLE = "PAPER_ACTIONABLE"
    EXECUTION_BLOCKED = "EXECUTION_BLOCKED"


class ExecutionReceiptStatus(ValueStrEnum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    BLOCKED = "BLOCKED"


class CommitStatus(ValueStrEnum):
    COMMITTED = "COMMITTED"
    ALREADY_COMMITTED = "ALREADY_COMMITTED"
    CONFLICT = "CONFLICT"
