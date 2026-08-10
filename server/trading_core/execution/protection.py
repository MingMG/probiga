"""Strategy-neutral supervisor for explicit protective sell instructions.

The supervisor only observes a caller-supplied trigger and validity window.  It
cannot derive stops, trends, scores, targets, or portfolio opinions.  Releasing
an intent is still subject to the normal instrument/T+N/risk checks represented
by ``RuleCheck``; this module never submits an order itself.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json

from ..contracts import ExecutionIntent, OrderSide
from ..market_rules.instruments import RuleCheck, RuleViolation


class ProtectionState(str, Enum):
    ARMED = "ARMED"
    WAIT_QUOTE = "WAIT_QUOTE"
    BLOCKED = "BLOCKED"
    RELEASE = "RELEASE"
    EXPIRED = "EXPIRED"


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _aware(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _positive_decimal(value: object, field_name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            if type(value) is not Decimal:
                raise TypeError(f"{field_name} must not be a Decimal subclass")
            converted = value
        elif type(value) in {str, int, float}:
            converted = Decimal(str(value))
        else:
            raise TypeError(f"{field_name} must be decimal-like")
    except Exception as exc:
        raise ValueError(f"{field_name} must be a decimal") from exc
    if not converted.is_finite() or converted <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return converted


def _sha256(value: object, field_name: str) -> str:
    normalized = _required_text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


def _canonical_decimal(value: Decimal) -> str:
    sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return "0"
    while digits[-1] == 0:
        digits = digits[:-1]
        exponent += 1
    coefficient = "".join(str(digit) for digit in digits)
    point = len(coefficient) + exponent
    if point <= 0:
        rendered = "0." + "0" * (-point) + coefficient
    elif point >= len(coefficient):
        rendered = coefficient + "0" * (point - len(coefficient))
    else:
        rendered = coefficient[:point] + "." + coefficient[point:]
    return f"-{rendered}" if sign else rendered


def _canonical_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _identity_hash(namespace: str, identity: dict[str, object]) -> str:
    payload = json.dumps(
        {"namespace": namespace, "identity": identity},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ProtectiveInstruction:
    protection_id: str
    intent: ExecutionIntent
    trigger_price: Decimal
    trigger_version: str
    account_state_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "protection_id",
            _required_text(self.protection_id, "protection_id"),
        )
        object.__setattr__(
            self,
            "trigger_version",
            _required_text(self.trigger_version, "trigger_version"),
        )
        object.__setattr__(
            self,
            "account_state_hash",
            _sha256(self.account_state_hash, "account_state_hash"),
        )
        if type(self.intent) is not ExecutionIntent:
            raise TypeError("intent must be exactly ExecutionIntent")
        if self.intent.side != OrderSide.SELL:
            raise ValueError("protective instruction requires a SELL intent")
        object.__setattr__(
            self,
            "trigger_price",
            _positive_decimal(self.trigger_price, "trigger_price"),
        )


@dataclass(frozen=True, slots=True)
class ProtectionQuote:
    event_id: str
    instrument_id: str
    last_price: Decimal
    quote_at: datetime
    suspended: bool = False
    quote_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in ("event_id", "instrument_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "last_price",
            _positive_decimal(self.last_price, "last_price"),
        )
        _aware(self.quote_at, "quote_at")
        if not isinstance(self.suspended, bool):
            raise TypeError("suspended must be a bool")
        calculated = _identity_hash(
            "trading-core.protection-quote.v1",
            {
                "event_id": self.event_id,
                "instrument_id": self.instrument_id,
                "last_price": _canonical_decimal(self.last_price),
                "quote_at": _canonical_time(self.quote_at),
                "suspended": self.suspended,
            },
        )
        object.__setattr__(self, "quote_hash", calculated)


def _validate_rule_check(rule_check: RuleCheck) -> None:
    if type(rule_check) is not RuleCheck:
        raise TypeError("rule_check must be exactly RuleCheck")
    if not isinstance(rule_check.allowed, bool):
        raise TypeError("rule_check.allowed must be a bool")
    if type(rule_check.violations) is not tuple or any(
        type(item) is not RuleViolation for item in rule_check.violations
    ):
        raise TypeError("rule_check.violations must contain RuleViolation values")
    if rule_check.allowed == bool(rule_check.violations):
        raise ValueError("rule_check allowed flag and violations are inconsistent")


@dataclass(frozen=True, slots=True)
class ProtectionRuleAttestation:
    intent_id: str
    account_id: str
    account_state_hash: str
    instrument_id: str
    rule_version: str
    checked_at: datetime
    valid_until: datetime
    rule_check: RuleCheck

    def __post_init__(self) -> None:
        for field_name in (
            "intent_id",
            "account_id",
            "instrument_id",
            "rule_version",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "account_state_hash",
            _sha256(self.account_state_hash, "account_state_hash"),
        )
        checked_at = _aware(self.checked_at, "checked_at")
        valid_until = _aware(self.valid_until, "valid_until")
        if valid_until < checked_at:
            raise ValueError("valid_until cannot precede checked_at")
        _validate_rule_check(self.rule_check)


def bind_protection_rule_check(
    intent: ExecutionIntent,
    *,
    account_state_hash: str,
    rule_check: RuleCheck,
    checked_at: datetime,
    valid_until: datetime,
) -> ProtectionRuleAttestation:
    if type(intent) is not ExecutionIntent:
        raise TypeError("intent must be exactly ExecutionIntent")
    return ProtectionRuleAttestation(
        intent_id=intent.intent_id,
        account_id=intent.account_id,
        account_state_hash=account_state_hash,
        instrument_id=intent.instrument_id,
        rule_version=intent.rule_version,
        checked_at=checked_at,
        valid_until=valid_until,
        rule_check=rule_check,
    )


@dataclass(frozen=True, slots=True)
class ProtectionDecision:
    decision_id: str
    protection_id: str
    state: ProtectionState
    reason_code: str
    evaluated_at: datetime
    quote_event_id: str = ""
    quote_hash: str = ""
    released_intent: ExecutionIntent | None = None

    def __post_init__(self) -> None:
        for field_name in ("decision_id", "protection_id", "reason_code"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "state", ProtectionState(self.state))
        _aware(self.evaluated_at, "evaluated_at")
        for field_name in ("quote_event_id", "quote_hash"):
            if not isinstance(getattr(self, field_name), str):
                raise TypeError(f"{field_name} must be a string")
            object.__setattr__(self, field_name, getattr(self, field_name).strip())
        if self.quote_hash:
            object.__setattr__(
                self,
                "quote_hash",
                _sha256(self.quote_hash, "quote_hash"),
            )
        if self.state == ProtectionState.RELEASE:
            if type(self.released_intent) is not ExecutionIntent:
                raise ValueError("RELEASE requires the exact execution intent")
        elif self.released_intent is not None:
            raise ValueError("only RELEASE may carry an execution intent")


def _decision(
    instruction: ProtectiveInstruction,
    *,
    state: ProtectionState,
    reason_code: str,
    evaluated_at: datetime,
    quote: ProtectionQuote | None = None,
) -> ProtectionDecision:
    identity = {
        "protection_id": instruction.protection_id,
        "intent_id": instruction.intent.intent_id,
        "intent_idempotency_key": instruction.intent.idempotency_key,
        "account_state_hash": instruction.account_state_hash,
        "trigger_price": _canonical_decimal(instruction.trigger_price),
        "trigger_version": instruction.trigger_version,
        "state": state.value,
        "reason_code": reason_code,
        "quote_event_id": quote.event_id if quote is not None else "",
        "quote_hash": quote.quote_hash if quote is not None else "",
    }
    return ProtectionDecision(
        decision_id=_identity_hash(
            "trading-core.protection-decision.v1",
            identity,
        ),
        protection_id=instruction.protection_id,
        state=state,
        reason_code=reason_code,
        evaluated_at=evaluated_at,
        quote_event_id=quote.event_id if quote is not None else "",
        quote_hash=quote.quote_hash if quote is not None else "",
        released_intent=(
            instruction.intent if state == ProtectionState.RELEASE else None
        ),
    )


def evaluate_protection(
    instruction: ProtectiveInstruction,
    *,
    quote: ProtectionQuote | None,
    evaluated_at: datetime,
    quote_max_age: timedelta,
    rule_attestation: ProtectionRuleAttestation,
) -> ProtectionDecision:
    """Evaluate an explicit stop without submitting or mutating anything."""

    if type(instruction) is not ProtectiveInstruction:
        raise TypeError("instruction must be exactly ProtectiveInstruction")
    now = _aware(evaluated_at, "evaluated_at")
    if type(quote_max_age) is not timedelta:
        raise TypeError("quote_max_age must be exactly timedelta")
    if quote_max_age < timedelta(0):
        raise ValueError("quote_max_age cannot be negative")
    if type(rule_attestation) is not ProtectionRuleAttestation:
        raise TypeError(
            "rule_attestation must be exactly ProtectionRuleAttestation"
        )
    intent = instruction.intent
    if (
        rule_attestation.intent_id != intent.intent_id
        or rule_attestation.account_id != intent.account_id
        or rule_attestation.account_state_hash != instruction.account_state_hash
        or rule_attestation.instrument_id != intent.instrument_id
        or rule_attestation.rule_version != intent.rule_version
    ):
        raise ValueError("rule attestation is not bound to the protected intent")
    if now < intent.earliest_at:
        return _decision(
            instruction,
            state=ProtectionState.ARMED,
            reason_code="NOT_ACTIVE_YET",
            evaluated_at=now,
        )
    if now >= intent.expires_at:
        return _decision(
            instruction,
            state=ProtectionState.EXPIRED,
            reason_code="PROTECTION_EXPIRED",
            evaluated_at=now,
        )
    if quote is None:
        return _decision(
            instruction,
            state=ProtectionState.WAIT_QUOTE,
            reason_code="QUOTE_UNAVAILABLE",
            evaluated_at=now,
        )
    if type(quote) is not ProtectionQuote:
        raise TypeError("quote must be exactly ProtectionQuote")
    if quote.instrument_id != intent.instrument_id:
        return _decision(
            instruction,
            state=ProtectionState.BLOCKED,
            reason_code="INSTRUMENT_MISMATCH",
            evaluated_at=now,
            quote=quote,
        )
    quote_age = now - quote.quote_at
    if quote_age < timedelta(0) or quote_age > quote_max_age:
        return _decision(
            instruction,
            state=ProtectionState.WAIT_QUOTE,
            reason_code="QUOTE_NOT_FRESH",
            evaluated_at=now,
            quote=quote,
        )
    if quote.suspended:
        return _decision(
            instruction,
            state=ProtectionState.BLOCKED,
            reason_code="INSTRUMENT_SUSPENDED",
            evaluated_at=now,
            quote=quote,
        )
    if quote.last_price > instruction.trigger_price:
        return _decision(
            instruction,
            state=ProtectionState.ARMED,
            reason_code="TRIGGER_NOT_REACHED",
            evaluated_at=now,
            quote=quote,
        )
    if (
        rule_attestation.checked_at > now
        or now > rule_attestation.valid_until
    ):
        return _decision(
            instruction,
            state=ProtectionState.BLOCKED,
            reason_code="RULE_ATTESTATION_NOT_CURRENT",
            evaluated_at=now,
            quote=quote,
        )
    rule_check = rule_attestation.rule_check
    if not rule_check.allowed:
        violations = ",".join(item.value for item in rule_check.violations)
        return _decision(
            instruction,
            state=ProtectionState.BLOCKED,
            reason_code=f"RULE_BLOCKED:{violations or 'UNKNOWN'}",
            evaluated_at=now,
            quote=quote,
        )
    return _decision(
        instruction,
        state=ProtectionState.RELEASE,
        reason_code="EXPLICIT_TRIGGER_REACHED",
        evaluated_at=now,
        quote=quote,
    )


__all__ = [
    "ProtectionDecision",
    "ProtectionQuote",
    "ProtectionRuleAttestation",
    "ProtectionState",
    "ProtectiveInstruction",
    "bind_protection_rule_check",
    "evaluate_protection",
]
