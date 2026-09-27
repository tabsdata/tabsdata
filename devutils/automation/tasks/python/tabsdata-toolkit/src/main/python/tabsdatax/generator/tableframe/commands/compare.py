#
# Copyright 2026 Tabsdata Inc.
#

import logging
import sys
from pathlib import Path

import rich_click as click

from tabsdatax.generator.tableframe.common.workflow import (
    POLARS_KIND,
    TABSDATA_KIND,
    ManifestName,
    confirm,
    confirm_overwrite,
    default_manifest_folder,
    preview,
    previous_and_current_manifests,
    require_manifest_folder,
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


def resolve_inputs(old: Path | None, new: Path | None, kind: str) -> tuple[Path, Path]:
    if old is not None and new is not None:
        return resolve_manifest_file(old), resolve_manifest_file(new)
    if old is not None or new is not None:
        log.error("Pass both --old and --new, or neither to compare by --kind.")
        sys.exit(1)
    import polars as pl

    return previous_and_current_manifests(
        require_manifest_folder(None), kind, pl.__version__
    )


def perform_compare(
    old: Path | None,
    new: Path | None,
    kind: str,
    output: Path | None,
    show: bool,
    auto_accept: bool,
) -> None:
    old, new = resolve_inputs(old, new, kind)

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

    from tabsdatax.generator.tableframe.common.render import render_compare
    from tabsdatax.generator.tableframe.engine.compare import perform

    diff, _ = perform(old, new, target)
    if show:
        render_compare(diff)
    log.info(f"Wrote {target}")


@click.command()
@click.option(
    "--old",
    type=click.Path(path_type=Path),
    default=None,
    metavar="FILE",
    help=(
        "Old manifest file. A bare relative name resolves under the "
        "tabsdata-toolkit manifest resources folder when the TABSDATA_OS "
        "environment variable is set. Pass together with --new; when both are "
        "omitted, the manifests are chosen by --kind."
    ),
)
@click.option(
    "--new",
    type=click.Path(path_type=Path),
    default=None,
    metavar="FILE",
    help=(
        "New manifest file. A bare relative name resolves under the "
        "tabsdata-toolkit manifest resources folder when the TABSDATA_OS "
        "environment variable is set. Pass together with --old; when both are "
        "omitted, the manifests are chosen by --kind."
    ),
)
@click.option(
    "--kind",
    type=click.Choice([POLARS_KIND, TABSDATA_KIND]),
    default=POLARS_KIND,
    show_default=True,
    help=(
        "Manifest kind compared when --old and --new are omitted: the manifest of "
        "the installed Polars version against the one of the greatest older "
        "version in the manifest folder. Fails if either is missing."
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
    old: Path | None,
    new: Path | None,
    kind: str,
    output: Path | None,
    show: bool,
    auto_accept: bool,
) -> None:
    """Diff two manifests across Polars versions."""
    perform_compare(old, new, kind, output, show, auto_accept)
