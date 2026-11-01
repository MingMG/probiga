"""A past invalidation cannot cancel a later scenario or watch activation."""
from copy import deepcopy

import pytest

from biz.review.research_contract import ContractError
from test_research_contract import answer, condition, packet, run


WINDOW_ORDER = ("open", "first_30m", "midday", "close")
ALLOWED_ABANDON = {
    "open": {"open", "first_30m", "midday", "close"},
    "first_30m": {"first_30m", "midday", "close"},
    "midday": {"midday", "close"},
    "close": {"close"},
}


def make_plan(scope, trigger_window="midday", abandon_window="first_30m"):
    facts = packet()
    draft = answer(facts)
    observable = next(key for key, value in facts["observables"].items() if value["metric"] == "up_share")
    trigger = condition("Ttime", trigger_window, observable)
    abandon = condition("Atime", abandon_window, observable, topic="risk", operator="weakens")
    draft["units"].extend([trigger, abandon])
    if scope == "scenario":
        plan = {"name": "弱势继续扩散", "trigger_units": ["Ttime"], "action_units": ["Uaction"], "abandon_units": ["Atime"]}
        draft["report"]["scenarios"].append(plan)
    else:
        plan = draft["report"]["directions"][0]["watches"][0]
        plan.update(trigger_units=["Ttime"], abandon_units=["Atime"])
    return facts, draft, plan, trigger, abandon


@pytest.mark.parametrize("scope", ["scenario", "watch"])
@pytest.mark.parametrize("trigger_window", WINDOW_ORDER)
@pytest.mark.parametrize("abandon_window", WINDOW_ORDER)
def test_all_four_window_pairs_have_the_same_temporal_contract_for_scenario_and_watch(scope, trigger_window, abandon_window):
    facts, draft, _, _, _ = make_plan(scope, trigger_window, abandon_window)
    before = deepcopy(draft)
    if abandon_window in ALLOWED_ABANDON[trigger_window]:
        assert run(draft, facts) == before
    else:
        with pytest.raises(ContractError, match="abandon_window_precedes_trigger"):
            run(draft, facts)
    assert draft == before  # Rejection/acceptance never repairs authored text.


def test_observed_midday_weakness_cannot_be_cancelled_by_a_completed_morning_repair_window():
    facts, draft, plan, trigger, abandon = make_plan("scenario")
    # Minimal isomorph of run 7ecbc3a6 scenarios[1]: C3.midday vs C2.first_30m.
    strength = next(key for key, value in facts["observables"].items()
                    if value["metric"] == "previous_strength_return_pct")
    trigger["id"] = "C3"
    trigger["topic"] = "risk"
    trigger["conditions"][0]["operator"] = "weakens"
    trigger["conditions"].append({"observable_id": strength, "window": "midday", "operator": "turns_negative",
                                  "reference": {"kind": "zero", "fact_id": "", "field": ""}})
    abandon["id"] = "C2"
    abandon["topic"] = "counter_evidence"
    abandon["conditions"][0]["operator"] = "improves"
    abandon["conditions"].append({"observable_id": strength, "window": "first_30m", "operator": "turns_positive",
                                  "reference": {"kind": "zero", "fact_id": "", "field": ""}})
    plan.update(trigger_units=["C3"], abandon_units=["C2"])
    with pytest.raises(ContractError, match=r"report.scenarios\[1\].*C2.first_30m<trigger_units:C3.midday"):
        run(draft, facts)


@pytest.mark.parametrize("scope", ["scenario", "watch"])
@pytest.mark.parametrize("logic", ["all", "any"])
def test_compound_trigger_checks_every_window_instead_of_only_the_first_condition(scope, logic):
    facts, draft, _, trigger, _ = make_plan(scope, "open", "first_30m")
    trigger["logic"] = logic
    later = deepcopy(trigger["conditions"][0])
    later["window"] = "midday"
    trigger["conditions"].append(later)
    with pytest.raises(ContractError, match="abandon_window_precedes_trigger"):
        run(draft, facts)


@pytest.mark.parametrize("scope", ["scenario", "watch"])
@pytest.mark.parametrize("logic", ["all", "any"])
def test_compound_abandonment_cannot_smuggle_an_earlier_window_behind_a_later_one(scope, logic):
    facts, draft, _, _, abandon = make_plan(scope, "midday", "close")
    abandon["logic"] = logic
    earlier = deepcopy(abandon["conditions"][0])
    earlier["window"] = "first_30m"
    abandon["conditions"].append(earlier)
    with pytest.raises(ContractError, match="abandon_window_precedes_trigger"):
        run(draft, facts)


@pytest.mark.parametrize("scope", ["scenario", "watch"])
def test_multiple_unit_references_all_participate_in_temporal_validation(scope):
    facts, draft, plan, _, _ = make_plan(scope, "first_30m", "close")
    assert run(draft, facts)
    plan["trigger_units"].append("Cmid")
    plan["abandon_units"].append("Cearly")
    with pytest.raises(ContractError, match="abandon_window_precedes_trigger"):
        run(draft, facts)
