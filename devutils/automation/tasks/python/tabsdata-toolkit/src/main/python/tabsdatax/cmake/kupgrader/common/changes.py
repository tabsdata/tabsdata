#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass

import yaml

from tabsdatax.cmake.kupgrader.common.actions import Action, Form, Plan
from tabsdatax.cmake.kupgrader.common.inventory import Bundle
from tabsdatax.cmake.kupgrader.common.naming import parse_name
from tabsdatax.cmake.kupgrader.common.operations import Gap, Synthesis, synthesize
from tabsdatax.cmake.kupgrader.common.renames import rename_operations, renamed
from tabsdatax.cmake.kupgrader.common.requirements import (
    is_requirements,
    synthesize_requirements,
)

RESOLVED_FAMILY = "resolved/configs"
RESOLVED_SOURCE_FAMILY = "configs"


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


def synthesize_yaml(previous: str, current: str, renames: dict[str, str]) -> Synthesis:
    synthesis = synthesize(
        yaml.safe_load(renamed(previous, renames)), yaml.safe_load(current)
    )
    synthesis.operations = [
        *rename_operations(previous, renames),
        *synthesis.operations,
    ]
    return synthesis


def synthesize_plans(plans: list[Plan], renames: dict[str, str]) -> list[Change]:
    changes = []
    for plan in plans:
        for decision in plan.decisions:
            if decision.action is not Action.MODIFY:
                continue
            variant = decision.variant
            if variant.previous is None or variant.current is None:
                raise ValueError(
                    f"The variant '{variant.name}' of bundle '{plan.name}' is modified "
                    "but holds no file on both sides"
                )
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
            changes.append(
                Change(
                    family=plan.family,
                    bundle=plan.name,
                    name=variant.name,
                    synthesis=synthesize_yaml(
                        variant.previous.read_text(),
                        variant.current.read_text(),
                        renames,
                    ),
                )
            )
    return changes


def winner(files: dict, provider: str):
    for name, path in files.items():
        if provider in parse_name(name).tokens:
            return name, path
    for name, path in files.items():
        if not parse_name(name).tokens:
            return name, path
    return None


def synthesize_resolved(bundles: list[Bundle], renames: dict[str, str]) -> list[Change]:
    changes = []
    for bundle in bundles:
        if bundle.family != RESOLVED_SOURCE_FAMILY:
            continue
        previous = {v.name: v.previous for v in bundle.variants if v.previous}
        current = {v.name: v.current for v in bundle.variants if v.current}
        providers = sorted({
            token for v in bundle.variants for token in parse_name(v.name).tokens
        })
        for provider in providers:
            old, new = winner(previous, provider), winner(current, provider)
            if old is None or new is None or old[0] == new[0]:
                continue
            changes.append(
                Change(
                    family=RESOLVED_FAMILY,
                    bundle=bundle.name,
                    name=new[0],
                    synthesis=synthesize_yaml(
                        old[1].read_text(), new[1].read_text(), renames
                    ),
                )
            )
    return changes


def settle_forms(plans: list[Plan], changes: list[Change]) -> None:
    for plan in plans:
        if plan.form is not Form.GENERIC:
            continue
        rendered = [
            [operation.rendered() for operation in change.synthesis.operations]
            for change in changes
            if change.family == plan.family and change.bundle == plan.name
        ]
        if any(operations != rendered[0] for operations in rendered):
            plan.form = Form.VERBOSE
            plan.reason = "The variants need different changes"
