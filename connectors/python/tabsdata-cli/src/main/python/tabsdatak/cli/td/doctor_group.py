#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk doctor`` -- local development preflight."""

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._doctor import CHECKS, FAIL, Probe, run_checks
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    format_option,
    output_format,
    present,
    render,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView


def checks_view(results: list[dict]) -> TableView:
    """The table of a doctor's checks: each one's status, detail and fix."""
    return TableView(
        title="",
        columns=[
            Column("Check", Kind.ID),
            Column("Status", Kind.STATUS),
            Column("Detail"),
            Column("Fix"),
        ],
        rows=[
            [result["name"], result["status"], result["detail"], present(result["fix"])]
            for result in results
        ],
        wrap=True,
    )


@capability("development", "read")
@cli_command
@click.command()
@click.option(
    "--check",
    "checks",
    multiple=True,
    type=click.Choice(tuple(CHECKS)),
    help="Run only this check; repeat for several. Default: every check.",
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Skip the checks that need the server: server, login and connectors.",
)
@format_option
@view_options()
@click.pass_context
def doctor(ctx: click.Context, checks: tuple[str, ...], offline: bool):
    """Check that this machine is ready to develop and register functions.
    Exits 1 when any check fails; warnings do not fail"""
    names = [name for name in CHECKS if not checks or name in checks]
    probe = Probe(ctx.obj.get("tabsdataserver"), offline=offline)
    results = run_checks(probe, names)
    failed = [result["name"] for result in results if result["status"] == FAIL]
    document = {"ok": not failed, "checks": results}
    if not failed or output_format(ctx) == "table":
        render(ctx, data=document, view=lambda: checks_view(results))
    if failed:
        raise TdkException(
            "checks_failed",
            f"{len(failed)} check(s) failed: {', '.join(failed)}.",
            state=document,
        )
