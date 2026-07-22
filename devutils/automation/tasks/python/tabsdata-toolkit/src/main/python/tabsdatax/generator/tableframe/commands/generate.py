#
# Copyright 2026 Tabsdata Inc.
#

import logging
import sys
from pathlib import Path

import rich_click as click

from tabsdatax.generator.tableframe.common.workflow import (
    MODULE,
    confirm,
    default_manifest_folder,
    default_package,
    greatest_tabsdata_manifest,
    main_folder_of,
    main_root_of,
    preview,
    resolve_manifest_file,
    utest_folder_of,
)

log = logging.getLogger(__name__)


def require_manifest(manifest: Path | None) -> Path:
    if manifest is not None:
        return resolve_manifest_file(manifest)
    folder = default_manifest_folder()
    if folder is None:
        log.error(
            "No manifest given and the TABSDATA_OS environment variable is not "
            "set. Pass --manifest with the manifest file."
        )
        sys.exit(1)
    greatest = greatest_tabsdata_manifest(folder)
    if greatest is None:
        log.error(
            f"No tabsdata manifest found in {folder}. Create a "
            "'manifest_<version>_td.json' there, or pass --manifest with a file."
        )
        sys.exit(1)
    return greatest


def require_package(package: Path | None) -> Path:
    if package is not None:
        return package
    folder = default_package()
    if folder is None:
        log.error(
            "No package given and the TABSDATA_OS environment variable is not "
            "set. Pass --package with the package folder."
        )
        sys.exit(1)
    return folder


def perform_generate(
    manifest: Path | None,
    package: Path | None,
    module: str,
    auto_accept: bool,
) -> None:
    manifest_file = require_manifest(manifest)
    package_folder = require_package(package)
    main = main_folder_of(package_folder, module)
    utest = utest_folder_of(package_folder, module)

    preview(
        "About to generate the wrapper from a manifest:",
        {
            "manifest": manifest_file,
            "package": package_folder,
            "module": module,
            "main folder": main,
            "utest folder": utest,
        },
    )
    confirm(auto_accept)

    from tabsdatax.generator.tableframe.common.render import render_generate
    from tabsdatax.generator.tableframe.engine.generate import perform
    from tabsdatax.generator.tableframe.model.model import Report

    report = Report.from_json(manifest_file.read_text())
    result = perform(report, module, str(main_root_of(package_folder)), str(utest))
    render_generate(result)


@click.command()
@click.option(
    "--manifest",
    type=click.Path(path_type=Path),
    metavar="FILE",
    default=None,
    help=(
        "Manifest file to generate from. A bare relative name resolves under the "
        "tabsdata-toolkit manifest resources folder. When omitted, the "
        "greatest-versioned tabsdata manifest in that folder is used."
    ),
)
@click.option(
    "--package",
    type=click.Path(file_okay=False, path_type=Path),
    metavar="FOLDER",
    default=None,
    help=(
        "Package folder the wrapper is generated into, under its src/main/python "
        "(and tests under src/utest/python). Defaults to the tabsdata-api "
        "package when TABSDATA_OS is set."
    ),
)
@click.option(
    "--module",
    default=MODULE,
    show_default=True,
    help="Module the generated wrapper will live under.",
)
@click.option(
    "--yes",
    "auto_accept",
    is_flag=True,
    default=False,
    help="Proceed without asking for confirmation.",
)
def generate(
    manifest: Path | None,
    package: Path | None,
    module: str,
    auto_accept: bool,
) -> None:
    """Emit the delegating wrapper from the curated manifest."""
    perform_generate(manifest, package, module, auto_accept)
