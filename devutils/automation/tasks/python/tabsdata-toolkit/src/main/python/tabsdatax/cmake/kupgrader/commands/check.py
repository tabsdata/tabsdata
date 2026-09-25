#
# Copyright 2026 Tabsdata Inc.
#

import logging
from pathlib import Path

import rich_click as click

from tabsdatax.cmake.kupgrader.common.commit import Difference, compare
from tabsdatax.cmake.kupgrader.common.pipeline import (
    generate,
    shipped_content_folder,
    shipped_spec_file,
)
from tabsdatax.cmake.kupgrader.common.rendering import display_comparison
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
