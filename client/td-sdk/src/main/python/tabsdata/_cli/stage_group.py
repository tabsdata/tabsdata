#
# Copyright 2026 Tabs Data Inc.
#

import rich_click as click

import tabsdata._cli.worker_utils as worker
from tabsdata.api.status_utils.worker import (
    WORKER_VALID_USER_PROVIDED_STATUSES,
)


@click.group()
def stage():
    """Stage management commands"""


@stage.command()
@click.option(
    "--name",
    "-n",
    help="Name of the function to which the stage belongs.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.pass_context
def start(ctx: click.Context, name: str, coll: str):
    """Start a stage"""
    worker.start("ST", ctx, name, coll)


@stage.command()
@click.option(
    "--name",
    "-n",
    help="Name of the function to which the stage belongs.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.pass_context
def stop(ctx: click.Context, name: str, coll: str):
    """Stop a stage"""
    worker.stop("ST", ctx, name, coll)


@stage.command()
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter stage workers by status. The possible statuses are "
        f"'{WORKER_VALID_USER_PROVIDED_STATUSES}'. This field is "
        "case-insensitive. "
    ),
)
@click.option(
    "--coll",
    type=str,
    help="Collection to which the stage workers belong.",
)
@click.option(
    "--state",
    type=click.Choice(["enabled", "disabled"]),
    help="Whether the stage workers to be shown must be enabled or disabled.",
)
@click.pass_context
def list(
    ctx: click.Context,
    status: list[str],
    coll: str,
    state: str,
):
    """
    List all stage workers.
    """
    worker.list("ST", ctx, status, coll, state)
