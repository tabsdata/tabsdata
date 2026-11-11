#
# Copyright 2026 Tabsdata Inc.
#

import logging
from pathlib import Path

import rich_click as click

from tabsdatax.cmake.kupgrader.common.emission import SPEC_SCHEMA, header_of, render
from tabsdatax.cmake.kupgrader.common.pipeline import shipped_spec_file
from tabsdatax.cmake.kupgrader.common.rules import ensure_default_rules
from tabsdatax.cmake.kupgrader.common.versions import (
    read_current,
    read_inventory,
    resolve,
    with_upgrader,
    write_inventory,
)
from tabsdatax.cmake.kupgrader.common.workflow import context_of

log = logging.getLogger(__name__)


def perform_noop(root_folder: Path) -> None:
    context = context_of("noop", root_folder)
    current = read_current(context.version_file)
    entries = read_inventory(context.upgrades_file)
    if any(entry.version == current for entry in entries):
        log.info(
            f"The upgrades inventory already declares version '{current}'; "
            "nothing to do."
        )
        return
    resolution = resolve(current, entries)

    spec_file = shipped_spec_file(context, current)
    if not spec_file.exists():
        spec_file.parent.mkdir(parents=True, exist_ok=True)
        spec_file.write_text(
            render({"schema": SPEC_SCHEMA}, header_of(None)),
            encoding="utf-8",
            newline="\n",
        )
    write_inventory(context.upgrades_file, with_upgrader(resolution))
    ensure_default_rules(context, current)
    log.info(
        f"Declared the no-op upgrader of version '{current}' in {spec_file} and "
        f"{context.upgrades_file}. Run 'cargo make upgrader build' once the "
        "kprofile changes."
    )


@click.command(
    help="Declare an upgrader that changes nothing for the current version, "
    "unless the upgrades inventory already lists it."
)
@click.option(
    "--root",
    "root_folder",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    metavar="FOLDER",
    help="Repository root folder whose version gets the upgrader.",
)
def noop(root_folder: Path) -> None:
    perform_noop(root_folder)
