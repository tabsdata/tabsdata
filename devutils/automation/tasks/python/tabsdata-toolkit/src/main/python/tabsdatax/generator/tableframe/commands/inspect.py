#
# Copyright 2026 Tabs Data Inc.
#

import logging
from pathlib import Path

import polars as pl
import rich_click as click

from tabsdatax.generator.tableframe.common.workflow import (
    MODULE,
    confirm,
    confirm_overwrite,
    preview,
    require_manifest_folder,
)

log = logging.getLogger(__name__)


def perform_inspect(manifest: Path | None, module: str, auto_accept: bool) -> None:
    folder = require_manifest_folder(manifest)
    polars_manifest = folder / f"manifest_{pl.__version__}_pl.json"

    preview(
        "About to scan the Polars API into a polars manifest:",
        {
            "polars version": pl.__version__,
            "module": module,
            "manifest folder": folder,
            "polars manifest": polars_manifest.name,
        },
    )
    confirm(auto_accept)
    confirm_overwrite([polars_manifest], auto_accept)

    from tabsdatax.generator.tableframe.common.render import render_inspect
    from tabsdatax.generator.tableframe.engine.inspect import perform

    report, skipped = perform(module)

    folder.mkdir(parents=True, exist_ok=True)
    polars_manifest.write_text(report.to_json())
    render_inspect(report, skipped=skipped)
    log.info(f"Wrote {polars_manifest}")

    log.info(
        f"Next, create the tabsdata manifest 'manifest_{pl.__version__}_td.json' "
        "in the same folder. For the first version, copy the polars manifest. For "
        "a Polars upgrade, carry the curation forward from the previous version's "
        "tabsdata manifest onto this new polars API -- use compare to review "
        "what changed. Do this before running generate."
    )


@click.command()
@click.option(
    "--manifest",
    type=click.Path(file_okay=False, path_type=Path),
    metavar="FOLDER",
    default=None,
    help=(
        "Manifest folder to write into. Defaults to the tabsdata-toolkit "
        "manifest resources folder when the TABSDATA_OS environment variable "
        "is set."
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
def inspect(manifest: Path | None, module: str, auto_accept: bool) -> None:
    """Scan the installed Polars API into a polars manifest."""
    perform_inspect(manifest, module, auto_accept)
