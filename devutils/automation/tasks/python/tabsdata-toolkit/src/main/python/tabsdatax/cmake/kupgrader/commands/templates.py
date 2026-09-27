#
# Copyright 2026 Tabsdata Inc.
#

import logging
from pathlib import Path

import rich_click as click

from tabsdatax.cmake.kupgrader.common.rendering import display_template_mismatches
from tabsdatax.cmake.kupgrader.common.sources import extract_kprofile
from tabsdatax.cmake.kupgrader.common.templates import (
    Mismatch,
    apply_patch,
    patch_file,
    version_templates,
)
from tabsdatax.cmake.kupgrader.common.verification import locate_binary
from tabsdatax.cmake.kupgrader.common.versions import read_current, read_inventory
from tabsdatax.cmake.kupgrader.common.workflow import context_of, display_context

log = logging.getLogger(__name__)


def perform_templates(
    root_folder: Path, versions: tuple[str, ...], write: bool
) -> None:
    context = context_of("templates", root_folder)
    display_context(context)

    binary = locate_binary(context.root_folder)
    head = read_current(context.version_file)
    known = [entry.version for entry in read_inventory(context.upgrades_file)]
    unknown = [version for version in versions if version not in known]
    if unknown:
        log.error(
            f"Version(s) {', '.join(unknown)} are not in {context.upgrades_file}."
        )
        raise SystemExit(1)

    mismatches: list[Mismatch] = []
    for version in versions or known:
        if version == head:
            kprofile_folder = context.kprofile_folder
            source = "the working tree"
        else:
            kprofile_folder = extract_kprofile(
                context.root_folder,
                version,
                context.work_folder / "templates" / version / "kprofile",
            )
            source = f"tag '{version}'"
            patch = patch_file(context, version)
            if patch.is_file():
                apply_patch(kprofile_folder, patch)
                source = f"{source} patched with {patch.name}"
        found = version_templates(binary, kprofile_folder, context, version, write)
        verb = "Wrote" if write else "Compared"
        log.info(f"{verb} the templates of '{version}' from the kprofile of {source}.")
        mismatches.extend(found)

    display_template_mismatches("Template Mismatches", mismatches)
    if mismatches:
        log.error(
            "The templates differ from what the kprofile of their version resolves to."
        )
        raise SystemExit(1)
    log.info("The templates match what the kprofile of their version resolves to.")


@click.command()
@click.option(
    "--root",
    "root_folder",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    metavar="FOLDER",
    help="Repository root folder holding the templates.",
)
@click.option(
    "--version",
    "versions",
    multiple=True,
    metavar="VERSION",
    help="Version to handle; repeat it for several. Defaults to every version in "
    "the upgrades inventory.",
)
@click.option(
    "--write",
    is_flag=True,
    help="Rewrite the templates instead of comparing them.",
)
def templates(root_folder: Path, versions: tuple[str, ...], write: bool) -> None:
    """Compare, or rewrite, the templates with each version's resolved kprofile.

    \b
    The head version resolves the working kprofile; every other version
    resolves the kprofile of its git tag.
    """
    perform_templates(root_folder, versions, write)
