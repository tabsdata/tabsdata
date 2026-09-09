#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass, field
from enum import Enum

from tabsdatax.cmake.kupgrader.common.inventory import Bundle, State, Variant
from tabsdatax.cmake.kupgrader.common.naming import is_schema


class Action(Enum):
    RETAIN = "retain"
    CREATE = "create"
    REPLACE = "replace"
    DELETE = "delete"
    MODIFY = "modify"


class Form(Enum):
    GENERIC = "Generic"
    VERBOSE = "Verbose"


CHANGED_ACTIONS = {
    "assets": Action.REPLACE,
    "configs": Action.MODIFY,
    "pods": Action.REPLACE,
}


@dataclass
class Decision:
    variant: Variant
    action: Action

    @property
    def name(self) -> str:
        return self.variant.name

    @property
    def state(self) -> State:
        return self.variant.state


@dataclass
class Plan:
    bundle: Bundle
    form: Form
    reason: str
    decisions: list[Decision] = field(default_factory=list)

    @property
    def family(self) -> str:
        return self.bundle.family

    @property
    def name(self) -> str:
        return self.bundle.name


def action_of(family: str, name: str, state: State) -> Action:
    if state is State.NEW:
        return Action.CREATE
    if state is State.DELETED:
        return Action.DELETE
    if state is State.UNCHANGED:
        return Action.RETAIN
    if is_schema(name):
        return Action.REPLACE
    return CHANGED_ACTIONS[family]


def form_of(bundle: Bundle, decisions: list[Decision]) -> tuple[Form, str]:
    if not bundle.suffixed:
        return Form.VERBOSE, "The bundle holds a single unsuffixed variant"

    structural = [
        decision
        for decision in decisions
        if decision.action in (Action.CREATE, Action.DELETE)
    ]
    if structural:
        return (
            Form.VERBOSE,
            f"The bundle creates or deletes {len(structural)} variant(s)",
        )

    actions = {decision.action for decision in decisions}
    if actions != {Action.MODIFY}:
        return Form.VERBOSE, "Not every variant is modified"

    return Form.GENERIC, "Every variant is modified, pending identical changes"


def plan_bundle(bundle: Bundle) -> Plan:
    decisions = [
        Decision(
            variant=variant,
            action=action_of(bundle.family, variant.name, variant.state),
        )
        for variant in bundle.variants
    ]
    form, reason = form_of(bundle, decisions)
    return Plan(bundle=bundle, form=form, reason=reason, decisions=decisions)


def plan_bundles(bundles: list[Bundle]) -> list[Plan]:
    return [plan_bundle(bundle) for bundle in bundles if bundle.altered]
