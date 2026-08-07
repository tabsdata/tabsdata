#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk cron`` and ``tdk stream``: the two kinds of orchestrator-managed
trigger.

A function registered with a ``CronTrigger`` gets a ``CT`` trigger worker and a
``@stream_publisher`` gets an ``ST`` one, so a group's ``list`` is the worker
listing narrowed to that worker type, and its ``enable`` / ``disable`` are
``tdk fn enable`` / ``tdk fn disable`` restricted to functions that have such a
trigger. The trigger worker is what both read: it is created when the function is
registered and outlives a disable (the function's own ``workers`` map does not --
it empties out while the function is disabled, so it cannot tell a disabled cron
function from a plain one).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    all_projects_option,
    beautify_time,
    echo_more_results,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    make_console,
    max_result_option,
    proj_option,
    render,
    resolve_all_projects,
    take,
    verify_login_or_prompt,
)
from tabsdatak.cli.td.execution_utils import obtain_list_worker_filters
from tabsdatak.cli.td.fn_group import set_function_enabled

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


class _Kind(NamedTuple):
    """What differs between the two trigger kinds: the worker type they are
    listed by, and how they are named in output."""

    worker_type: str
    title: str
    plural: str
    # Fill the "No <trigger> found for function ... use 'tdk fn <action>' to
    # <action> <other>" refusal. Phrased so it also holds when the function does
    # not exist at all.
    trigger: str
    other: str


CRON = _Kind(
    worker_type="CT",
    title="Cron triggers",
    plural="cron triggers",
    trigger="cron trigger",
    other="a function that is not cron-triggered",
)

STREAM = _Kind(
    worker_type="ST",
    title="Stream publishers",
    plural="stream publishers",
    trigger="stream-publisher trigger",
    other="a function that is not a stream publisher",
)


def _name_option(help: str):
    return click.option("--name", "-n", help=help)


def _coll_option(help: str):
    return click.option("--coll", "-c", help=help)


@cli_command
@click.group()
def cron():
    """Cron-triggered functions"""


@cli_command
@click.group()
def stream():
    """Stream-publisher functions"""


@cron.command()
@proj_option
@_name_option("Name of the cron-triggered function to enable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def enable(ctx: click.Context, name: str, coll: str):
    """Enable a cron-triggered function

    'tdk fn enable' restricted to functions that have a cron trigger; enabling
    an already-enabled one does nothing.
    """
    _enable_function(ctx, CRON, name, coll, True)


@cron.command()
@proj_option
@_name_option("Name of the cron-triggered function to disable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def disable(ctx: click.Context, name: str, coll: str):
    """Disable a cron-triggered function

    'tdk fn disable' restricted to functions that have a cron trigger; disabling
    an already-disabled one does nothing.
    """
    _enable_function(ctx, CRON, name, coll, False)


@cron.command("list")
@proj_option
@all_projects_option
@_name_option("Only list the trigger of this function.")
@_coll_option("Only list triggers of functions in this collection.")
@format_option
@max_result_option
@click.pass_context
def list_crons(ctx: click.Context, all_projects: bool, name: str, coll: str):
    """List the cron triggers of the project"""
    _list_triggers(ctx, CRON, all_projects, name, coll)


@stream.command("enable")
@proj_option
@_name_option("Name of the stream-publisher function to enable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def enable_stream(ctx: click.Context, name: str, coll: str):
    """Enable a stream-publisher function

    'tdk fn enable' restricted to stream publishers; enabling an already-enabled
    one does nothing.
    """
    _enable_function(ctx, STREAM, name, coll, True)


@stream.command("disable")
@proj_option
@_name_option("Name of the stream-publisher function to disable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def disable_stream(ctx: click.Context, name: str, coll: str):
    """Disable a stream-publisher function

    'tdk fn disable' restricted to stream publishers; disabling an
    already-disabled one does nothing.
    """
    _enable_function(ctx, STREAM, name, coll, False)


@stream.command("list")
@proj_option
@all_projects_option
@_name_option("Only list the publisher of this function.")
@_coll_option("Only list publishers of functions in this collection.")
@format_option
@max_result_option
@click.pass_context
def list_streams(ctx: click.Context, all_projects: bool, name: str, coll: str):
    """List the stream publishers of the project"""
    _list_triggers(ctx, STREAM, all_projects, name, coll)


def _triggers_of(server: "TabsdataServer", kind: _Kind, coll: str, name: str):
    request_filter = obtain_list_worker_filters(
        fn=name, collection=coll, worker_type=kind.worker_type
    )
    return server.list_workers_generator(filter=request_filter)


def _enable_function(
    ctx: click.Context, kind: _Kind, name: str, coll: str, enabled: bool
):
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    action = "enable" if enabled else "disable"
    server: TabsdataServer = ctx.obj["tabsdataserver"]
    try:
        has_trigger = any(True for _ in _triggers_of(server, kind, coll, name))
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to {action} function: {e}")
    if not has_trigger:
        raise click.ClickException(
            f"No {kind.trigger} found for function '{name}' in collection "
            f"'{coll}'. Use 'tdk fn {action}' to {action} {kind.other}."
        )
    set_function_enabled(ctx, name, coll, enabled)


def _list_triggers(
    ctx: click.Context,
    kind: _Kind,
    all_projects: bool,
    name: str,
    coll: str,
):
    verify_login_or_prompt(ctx)
    # The pinned collection narrows the listing as it does everywhere else, but
    # is not prompted for: with no collection the listing spans the project.
    coll = coll or get_currently_pinned_object(ctx, "collection")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        spanning = resolve_all_projects(ctx, all_projects)
        triggers, truncated = take(ctx, _triggers_of(server, kind, coll, name))

        def _table():
            table = Table(title=kind.title)
            table.add_column("Function", style="cyan", no_wrap=True)
            if spanning:
                table.add_column("Project")
            table.add_column("Collection")
            table.add_column("Worker ID", no_wrap=True)
            table.add_column("Status")
            table.add_column("Started on", no_wrap=True)
            table.add_column("Ended on", no_wrap=True)
            for trigger in triggers:
                row = [trigger.function.name]
                if spanning:
                    row.append(trigger.project or "-")
                row.extend([
                    trigger.collection.name,
                    trigger.id,
                    trigger.status,
                    beautify_time(trigger.started_on_str),
                    beautify_time(trigger.ended_on_str),
                ])
                table.add_row(*row)
            click.echo()
            make_console().print(table)
            click.echo(f"Number of {kind.plural}: {len(triggers)}")
            click.echo()

        # ``kwargs`` is the row the listing already returned; ``_data`` would
        # re-fetch each worker one by one.
        render(ctx, data=lambda: [t.kwargs for t in triggers], table=_table)
        echo_more_results(ctx, truncated)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list {kind.plural}: {e}")
