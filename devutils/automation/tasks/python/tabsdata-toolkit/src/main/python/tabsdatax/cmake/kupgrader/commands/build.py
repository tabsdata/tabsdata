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
from tabsdatax.cmake.kupgrader.common.rendering import (
    display_template_mismatches,
    display_verification,
)
from tabsdatax.cmake.kupgrader.common.templates import (
    digests,
    replay,
    stage_kupgrade,
)
from tabsdatax.cmake.kupgrader.common.verification import locate_binary, verify
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

    binary = locate_binary(context.root_folder)
    staged = stage_kupgrade(
        context, generation.emission, generation.preparation.resolution
    )
    diverged = replay(binary, context, staged)
    display_template_mismatches("Replay Mismatches", diverged)
    if diverged:
        log.error(
            f"Upgrading the templates hop by hop does not reproduce the templates of "
            f"every version up to '{generation.version}'. The diverging outcomes are "
            f"kept in {context.diverged_folder}. Nothing was written to the repository."
        )
        raise SystemExit(1)

    recorded, drifted = digests(binary, context, staged, record=True)
    display_template_mismatches("Template Digest Mismatches", drifted)
    if drifted:
        log.error(
            "Released templates no longer resolve as their recorded digests say. "
            "Explain the drift before touching the digests."
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
        f"{content_folder}, declared it in {context.upgrades_file}, and recorded "
        f"{recorded} new template digest(s)."
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
