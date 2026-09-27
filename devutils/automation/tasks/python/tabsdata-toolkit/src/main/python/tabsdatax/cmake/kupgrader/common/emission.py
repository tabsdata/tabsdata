#
# Copyright 2026 Tabsdata Inc.
#

import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from tabsdatax.cmake.kupgrader.common.actions import Action, Form, Plan
from tabsdatax.cmake.kupgrader.common.changes import Change
from tabsdatax.cmake.kupgrader.common.errors import KupgraderError
from tabsdatax.cmake.kupgrader.common.naming import FAMILIES

SPEC_SCHEMA = 1

HEADER = "#\n# Copyright {year} Tabsdata Inc.\n#\n\n"


class EmissionError(KupgraderError):
    pass


@dataclass
class Emission:
    spec_file: Path
    content_folder: Path
    payloads: list[Path]


class SpecDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def header_of(existing: Path | None) -> str:
    if existing is not None and existing.is_file():
        lines = []
        for line in existing.read_text().splitlines():
            if line.startswith("#"):
                lines.append(line)
                continue
            break
        if lines:
            return "\n".join(lines) + "\n\n"
    return HEADER.format(year=date.today().year)


def changes_of(changes: list[Change], family: str, name: str) -> list[dict[str, Any]]:
    for change in changes:
        if change.family == family and change.name == name:
            return [operation.rendered() for operation in change.synthesis.operations]
    return []


def entry_of(
    action: Action, changes: list[Change], family: str, name: str
) -> dict[str, Any]:
    if action is not Action.MODIFY:
        return {"action": action.value}
    return {"action": action.value, "changes": changes_of(changes, family, name)}


def resolved_of(resolved: list[Change]) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    for change in sorted(resolved, key=lambda change: (change.bundle, change.name)):
        operations = [operation.rendered() for operation in change.synthesis.operations]
        entry = (
            {"action": Action.MODIFY.value, "changes": operations}
            if operations
            else {"action": Action.RETAIN.value}
        )
        groups.setdefault(change.bundle, {})[change.name] = entry
    return {"configs": groups} if groups else {}


def document_of(
    plans: list[Plan], changes: list[Change], resolved: list[Change]
) -> dict[str, Any]:
    document: dict[str, Any] = {"schema": SPEC_SCHEMA}

    for family in FAMILIES:
        groups: dict[str, dict[str, Any]] = {}
        for plan in sorted(
            (plan for plan in plans if plan.family == family),
            key=lambda plan: plan.name,
        ):
            if plan.form is Form.GENERIC:
                decision = plan.decisions[0]
                groups[plan.name] = {
                    plan.name: entry_of(decision.action, changes, family, decision.name)
                }
                continue
            groups[plan.name] = {
                decision.name: entry_of(decision.action, changes, family, decision.name)
                for decision in sorted(
                    plan.decisions, key=lambda decision: decision.name
                )
            }
        if groups:
            document[family] = groups

    resolved_section = resolved_of(resolved)
    if resolved_section:
        document["resolved"] = resolved_section

    return document


def render(document: dict[str, Any], header: str) -> str:
    body = yaml.dump(
        document,
        Dumper=SpecDumper,
        sort_keys=False,
        default_flow_style=False,
        width=100,
    )
    return f"{header}{body}"


def emit(
    plans: list[Plan],
    changes: list[Change],
    resolved: list[Change],
    version: str,
    target_folder: Path,
    existing_spec_file: Path | None,
) -> Emission:
    if target_folder.exists():
        shutil.rmtree(target_folder)
    target_folder.mkdir(parents=True)

    document = document_of(plans, changes, resolved)
    spec_file = target_folder / f"{version}.yaml"
    spec_file.write_text(render(document, header_of(existing_spec_file)))

    content_folder = target_folder / version
    payloads = []
    for plan in plans:
        for decision in plan.decisions:
            if decision.action not in (Action.CREATE, Action.REPLACE):
                continue
            source = decision.variant.current
            if source is None:
                raise EmissionError(
                    f"'{plan.family}/{decision.name}' needs content, but the current "
                    "kprofile holds no such file."
                )
            payload = content_folder / plan.family / decision.name
            payload.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, payload)
            payloads.append(payload)

    return Emission(
        spec_file=spec_file,
        content_folder=content_folder,
        payloads=payloads,
    )
