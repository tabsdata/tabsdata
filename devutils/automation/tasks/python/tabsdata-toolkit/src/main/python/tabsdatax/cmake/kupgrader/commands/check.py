#
# Copyright 2026 Tabsdata Inc.
#

import logging
from pathlib import Path

import rich_click as click

from tabsdatax.cmake.kupgrader.common.commit import Difference, compare
from tabsdatax.cmake.kupgrader.common.emission import Emission
from tabsdatax.cmake.kupgrader.common.pipeline import (
    generate,
    shipped_content_folder,
    shipped_spec_file,
)
from tabsdatax.cmake.kupgrader.common.rendering import (
    display_comparison,
    display_template_mismatches,
    display_verification,
)
from tabsdatax.cmake.kupgrader.common.templates import replay, version_templates
from tabsdatax.cmake.kupgrader.common.verification import locate_binary, verify
from tabsdatax.cmake.kupgrader.common.versions import declares_upgrader
from tabsdatax.cmake.kupgrader.common.workflow import context_of, display_context

log = logging.getLogger(__name__)


def perform_check(root_folder: Path) -> None:
    context = context_of("check", root_folder)
    display_context(context)

    generation = generate(context)
    if generation is None:
        return

    comparison = compare(
        generation.emission,
        shipped_spec_file(context, generation.version),
        shipped_content_folder(context, generation.version),
    )
    if not declares_upgrader(generation.preparation.resolution):
        comparison.differences.append(
            Difference(
                path=context.upgrades_file.name,
                reason="The upgrades inventory does not declare an upgrader for "
                "this version",
            )
        )
    display_comparison(comparison)

    if not comparison.matched:
        log.error(
            f"The upgrader shipped for version '{generation.version}' is not the one "
            "this repository would generate. Run 'cargo make upgrader build' to "
            "regenerate it."
        )
        raise SystemExit(1)

    shipped = Emission(
        spec_file=shipped_spec_file(context, generation.version),
        content_folder=shipped_content_folder(context, generation.version),
        payloads=[],
    )
    verification = verify(
        context.root_folder,
        shipped,
        generation.preparation.previous_kprofile_folder,
        generation.preparation.current_kprofile_folder,
        context.upgraded_folder,
        generation.version,
        generation.previous,
    )
    display_verification(verification)
    if not verification.matched:
        log.error(
            f"The upgrader shipped for version '{generation.version}' does not turn "
            f"the kprofile of '{generation.previous}' into the current one."
        )
        raise SystemExit(1)

    binary = locate_binary(context.root_folder)
    stale = version_templates(
        binary, context.kprofile_folder, context, generation.version, write=False
    )
    display_template_mismatches("Stale Head Templates", stale)
    diverged = replay(binary, context.kupgrade_folder, context)
    display_template_mismatches("Replay Mismatches", diverged)
    if stale or diverged:
        log.error(
            f"The templates of version '{generation.version}' are stale or the shipped "
            "upgraders do not reproduce every version's templates. Run 'cargo make "
            f"upgrader build' to regenerate them; diverging outcomes are kept in "
            f"{context.diverged_folder}."
        )
        raise SystemExit(1)

    log.info(f"The upgrader shipped for version '{generation.version}' is correct.")


@click.command()
@click.option(
    "--root",
    "root_folder",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    metavar="FOLDER",
    help="Repository root folder holding the resources to verify.",
)
def check(root_folder: Path) -> None:
    """Verify that the current upgrader is correct and complete."""
    perform_check(root_folder)
