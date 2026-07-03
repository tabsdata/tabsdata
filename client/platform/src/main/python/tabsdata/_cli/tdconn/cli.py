#
# Copyright 2026 Tabs Data Inc.
#
"""``tdconn`` -- Click-based CLI for the connection catalog.

Manages connections through the tabsdata apiserver: every verb maps onto the
``/collections/{name}/connection`` endpoints (the collection's name is the
connection's name). Business logic lives in
:mod:`tabsdata._cli.tdconn._internal.store.logic`; this module only wires Click
commands to the logic + UX layers, reusing the ``td`` CLI's saved login
session (``td login``).
"""

from __future__ import annotations

import os
import sys

import rich_click as click

from tabsdata._cli.cli_utils import (
    initialise_tabsdata_server_connection,
    load_pinned_objects,
)
from tabsdata._cli.tdconn._internal.interface import UXTerminal
from tabsdata._cli.tdconn._internal.store import ApiConnStore, BaseLogic
from tabsdata._cli.tdconn.conn_group import conn_commands

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option("--debug", is_flag=True, hidden=True)
@click.option(
    "--project",
    default=None,
    help=(
        "Project to operate on. Defaults to the pinned project "
        "('td pin set --proj <name>')."
    ),
)
@click.pass_context
def cli(ctx: click.Context, debug: bool, project: str | None):
    """Manage connections in a tabsdata server."""
    tabsdata_directory = os.path.join(os.path.expanduser("~"), ".tabsdata")
    os.makedirs(tabsdata_directory, exist_ok=True)
    ctx.obj = {
        "tabsdata_directory": tabsdata_directory,
        "debug": debug,
    }
    initialise_tabsdata_server_connection(ctx)
    load_pinned_objects(ctx)
    server = ctx.obj.get("tabsdataserver")
    if server is None:
        raise click.ClickException(
            "No tabsdata server session found; run 'td login' first."
        )
    # Mirror the td CLI: the active project rides on the connection — the
    # --project override wins, else the pinned project.
    server.connection.project = project or ctx.obj["pinned_objects"].get("project")
    logic = BaseLogic(ApiConnStore(server.connection))
    ctx.obj["logic"] = logic
    ctx.obj["ux"] = UXTerminal(logic, debug=debug)


# Register connection commands
for cmd in conn_commands:
    cli.add_command(cmd)


@cli.command()
@click.pass_context
def shell(ctx: click.Context):
    """Start an interactive shell session."""
    from tabsdata import __version__
    from tabsdata._cli.shell import run_shell

    run_shell(
        ctx,
        cli,
        __version__,
        banner_title="tdconn Interactive Shell",
        tool_name="tdconn",
    )


if __name__ == "__main__":
    sys.exit(cli())
