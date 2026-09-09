#
# Copyright 2026 Tabsdata Inc.
#

import logging
from dataclasses import dataclass
from pathlib import Path

from tabsdatax.cmake.kupgrader.common.actions import Plan, plan_bundles
from tabsdatax.cmake.kupgrader.common.changes import Change, synthesize_plans
from tabsdatax.cmake.kupgrader.common.emission import Emission, emit
from tabsdatax.cmake.kupgrader.common.inventory import Bundle, collect
from tabsdatax.cmake.kupgrader.common.preparation import Preparation, prepare
from tabsdatax.cmake.kupgrader.common.rendering import (
    display_actions,
    display_bundles,
    display_emission,
    display_gaps,
    display_gate,
    display_inventory,
    display_plans,
    display_providers,
    display_synthesis,
)
from tabsdatax.cmake.kupgrader.common.workflow import Context

log = logging.getLogger(__name__)


@dataclass
class Generation:
    preparation: Preparation
    bundles: list[Bundle]
    plans: list[Plan]
    changes: list[Change]
    emission: Emission

    @property
    def version(self) -> str:
        return self.preparation.resolution.current

    @property
    def previous(self) -> str:
        return self.preparation.resolution.previous


def shipped_spec_file(context: Context, version: str) -> Path:
    return context.upgrades_folder / f"{version}.yaml"


def shipped_content_folder(context: Context, version: str) -> Path:
    return context.upgrades_folder / version


def generate(context: Context) -> Generation | None:
    preparation = prepare(context)
    if preparation is None:
        return None

    bundles = collect(
        preparation.previous_kprofile_folder,
        preparation.current_kprofile_folder,
    )
    display_inventory(bundles)
    display_gate(bundles)
    display_providers()
    display_bundles(bundles)

    plans = plan_bundles(bundles)
    display_plans(plans)
    display_actions(plans, bundles)

    changes = synthesize_plans(plans)
    display_synthesis(changes)
    display_gaps(changes)

    failing = [change for change in changes if not change.synthesis.expressible]
    if failing:
        log.error(
            f"{len(failing)} file(s) hold changes that no current operation can "
            "express. Extend the upgrade operations and run the task again."
        )
        raise SystemExit(1)

    version = preparation.resolution.current
    emission = emit(
        plans,
        changes,
        version,
        context.generated_folder,
        shipped_spec_file(context, version),
    )
    display_emission(emission, version)

    return Generation(
        preparation=preparation,
        bundles=bundles,
        plans=plans,
        changes=changes,
        emission=emission,
    )
