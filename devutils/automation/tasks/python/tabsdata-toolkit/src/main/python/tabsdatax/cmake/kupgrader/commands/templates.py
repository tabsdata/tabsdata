#
# Copyright 2026 Tabsdata Inc.
#

import logging
from pathlib import Path

import rich_click as click

from tabsdatax.cmake.kupgrader.common.templates import materialize
from tabsdatax.cmake.kupgrader.common.verification import locate_binary
from tabsdatax.cmake.kupgrader.common.workflow import context_of, display_context

log = logging.getLogger(__name__)


def perform_templates(root_folder: Path) -> None:
    context = context_of("templates", root_folder)
    display_context(context)

    binary = locate_binary(context.root_folder)
    folder = materialize(binary, context, context.resolved_folder)
    log.info(f"Wrote the templates of every version to {folder}.")


@click.command()
@click.option(
    "--root",
    "root_folder",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    metavar="FOLDER",
    help="Repository root folder whose versions give the templates.",
)
def templates(root_folder: Path) -> None:
    """Write the templates of every version to the work folder.

    \b
    The newest version resolves the working kprofile; every other version
    resolves the kprofile of its git tag, with its patch applied.
    """
    perform_templates(root_folder)
