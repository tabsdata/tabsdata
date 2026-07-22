#
# Copyright 2026 Tabsdata Inc.
#

import logging
import sys
from pathlib import Path

import rich_click as click

from tabsdatax.generator.tableframe.common.workflow import (
    ManifestName,
    confirm,
    confirm_overwrite,
    default_manifest_folder,
    preview,
    resolve_manifest_file,
)

log = logging.getLogger(__name__)


def derive_output(old: Path, new: Path) -> Path | None:
    old_name = ManifestName.of(old.name)
    new_name = ManifestName.of(new.name)
    folder = default_manifest_folder()
    if old_name is None or new_name is None or folder is None:
        return None
    name = (
        f"diff_{old_name.kind}-{old_name.version}"
        f"__{new_name.kind}-{new_name.version}.json"
    )
    return folder / name


def perform_compare(
    old: Path,
    new: Path,
    output: Path | None,
    show: bool,
    auto_accept: bool,
) -> None:
    old = resolve_manifest_file(old)
    new = resolve_manifest_file(new)

    target = output or derive_output(old, new)
    if target is None:
        log.error(
            "Cannot derive an output file name because the inputs are not both "
            "named 'manifest_<version>_<pl|td>.json' or the TABSDATA_OS "
            "environment variable is not set. Pass --output with the diff file."
        )
        sys.exit(1)

    preview(
        "About to compare two manifests:",
        {
            "old": old,
            "new": new,
            "output": target,
            "show": show,
        },
    )
    confirm(auto_accept)
    confirm_overwrite([target], auto_accept)

    from tabsdatax.generator.tableframe.common.render import (
        console,
        render_compare,
        show_json,
    )
    from tabsdatax.generator.tableframe.engine.compare import perform

    diff, content = perform(old, new, target)
    if show:
        render_compare(diff)
        show_json(f"{old.name} ➜ {new.name}", content)
        console.print()
    log.info(f"Wrote {target}")


@click.command()
@click.option(
    "--old",
    type=click.Path(path_type=Path),
    required=True,
    metavar="FILE",
    help=(
        "Old manifest file. A bare relative name resolves under the "
        "tabsdata-toolkit manifest resources folder when the TABSDATA_OS "
        "environment variable is set."
    ),
)
@click.option(
    "--new",
    type=click.Path(path_type=Path),
    required=True,
    metavar="FILE",
    help=(
        "New manifest file. A bare relative name resolves under the "
        "tabsdata-toolkit manifest resources folder when the TABSDATA_OS "
        "environment variable is set."
    ),
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    metavar="FILE",
    default=None,
    help=(
        "Diff file to write. Defaults to a derived name in the manifest folder "
        "when both inputs are named manifests and TABSDATA_OS is set."
    ),
)
@click.option(
    "--show/--no-show",
    default=True,
    show_default=True,
    help="Render the diff to the console.",
)
@click.option(
    "--yes",
    "auto_accept",
    is_flag=True,
    default=False,
    help="Proceed without asking for confirmation.",
)
def compare(
    old: Path,
    new: Path,
    output: Path | None,
    show: bool,
    auto_accept: bool,
) -> None:
    """Diff two manifests across Polars versions."""
    perform_compare(old, new, output, show, auto_accept)
