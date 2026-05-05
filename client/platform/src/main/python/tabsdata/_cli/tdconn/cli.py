#
# Copyright 2026 Tabs Data Inc.
#
"""``tdconn`` -- Click-based CLI for the connection catalog.

Manages NamedConn entries in a tabsdata instance connection store.
Business logic lives in :mod:`tabsdata._cli.tdconn._internal.os.logic`;
this module only wires Click commands to the logic + UX layers.
"""

from __future__ import annotations

import os
import sys

import rich_click as click

from tabsdata._cli.tdconn._internal.os import DEFAULT_INSTANCE, BaseLogic, FileConnStore
from tabsdata._cli.tdconn._internal.ux import UXTerminal
from tabsdata._cli.tdconn.conn_group import conn_commands

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "--instance",
    default=DEFAULT_INSTANCE,
    metavar="NAME",
    help=(
        f"tabsdata instance whose connection store to target "
        f"(default: {DEFAULT_INSTANCE})"
    ),
)
@click.option("--debug", is_flag=True, hidden=True)
@click.pass_context
def cli(ctx: click.Context, instance: str, debug: bool):
    """Manage connections in a tabsdata instance."""
    store = FileConnStore(instance=instance)
    logic = BaseLogic(store)
    tabsdata_directory = os.path.join(os.path.expanduser("~"), ".tabsdata")
    os.makedirs(tabsdata_directory, exist_ok=True)
    ctx.obj = {
        "tabsdata_directory": tabsdata_directory,
        "instance": instance,
        "debug": debug,
        "logic": logic,
        "ux": UXTerminal(logic, debug=debug),
    }


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
