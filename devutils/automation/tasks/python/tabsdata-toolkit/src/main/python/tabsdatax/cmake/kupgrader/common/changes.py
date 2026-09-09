#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass

import yaml

from tabsdatax.cmake.kupgrader.common.actions import Action, Plan
from tabsdatax.cmake.kupgrader.common.operations import Gap, Synthesis, synthesize
from tabsdatax.cmake.kupgrader.common.requirements import (
    is_requirements,
    synthesize_requirements,
)


@dataclass
class Change:
    family: str
    bundle: str
    name: str
    synthesis: Synthesis


def textual_gap(name: str) -> Gap:
    return Gap(
        leaf="<file>",
        shape="Text file modified",
        detail=(
            f"'{name}' is neither YAML nor a requirements file, so its change "
            "cannot be derived. Only 'modify-text-if' could express it."
        ),
    )


def synthesize_textual(name: str, previous: str, current: str) -> Synthesis:
    if is_requirements(name):
        return synthesize_requirements(previous, current)
    return Synthesis(gaps=[textual_gap(name)])


def synthesize_plans(plans: list[Plan]) -> list[Change]:
    changes = []
    for plan in plans:
        for decision in plan.decisions:
            if decision.action is not Action.MODIFY:
                continue
            variant = decision.variant
            if not variant.parsable:
                changes.append(
                    Change(
                        family=plan.family,
                        bundle=plan.name,
                        name=variant.name,
                        synthesis=synthesize_textual(
                            variant.name,
                            variant.previous.read_text(),
                            variant.current.read_text(),
                        ),
                    )
                )
                continue
            previous = yaml.safe_load(variant.previous.read_text())
            current = yaml.safe_load(variant.current.read_text())
            changes.append(
                Change(
                    family=plan.family,
                    bundle=plan.name,
                    name=variant.name,
                    synthesis=synthesize(previous, current),
                )
            )
    return changes
