#
# Copyright 2026 Tabsdata Inc.
#

import logging
from pathlib import Path

import rich_click as click

from tabsdatax.cmake.kupgrader.common.commit import commit
from tabsdatax.cmake.kupgrader.common.pipeline import (
    generate,
    shipped_content_folder,
    shipped_spec_file,
)
from tabsdatax.cmake.kupgrader.common.rendering import display_verification
from tabsdatax.cmake.kupgrader.common.verification import verify
from tabsdatax.cmake.kupgrader.common.versions import with_upgrader, write_inventory
from tabsdatax.cmake.kupgrader.common.workflow import context_of, display_context

log = logging.getLogger(__name__)


def perform_build(root_folder: Path) -> None:
    context = context_of("build", root_folder)
    display_context(context)

    generation = generate(context)
    if generation is None:
        return

    verification = verify(
        context.root_folder,
        generation.emission,
        generation.preparation.previous_kprofile_folder,
        generation.preparation.current_kprofile_folder,
        context.upgraded_folder,
        generation.version,
        generation.previous,
    )
    display_verification(verification)

    if not verification.matched:
        log.error(
            f"The generated upgrader does not reproduce the kprofile of version "
            f"'{generation.version}'. Nothing was written to the repository."
        )
        raise SystemExit(1)

    spec_file = shipped_spec_file(context, generation.version)
    content_folder = shipped_content_folder(context, generation.version)
    commit(generation.emission, spec_file, content_folder)
    write_inventory(
        context.upgrades_file, with_upgrader(generation.preparation.resolution)
    )

    log.info(
        f"Wrote the upgrader of version '{generation.version}' to {spec_file} and "
        f"{content_folder}, and declared it in {context.upgrades_file}."
    )


@click.command()
@click.option(
    "--root",
    "root_folder",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    metavar="FOLDER",
    help="Repository root folder holding the resources to generate for.",
)
def build(root_folder: Path) -> None:
    """Generate the upgrader and verify the upgraded outcome."""
    perform_build(root_folder)
