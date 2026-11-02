#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk cron`` and ``tdk stream``: the two kinds of orchestrator-managed
trigger.

A function registered with a ``CronTrigger`` gets a ``CT`` trigger worker and a
``@stream_publisher`` gets an ``ST`` one, so a group's ``list`` is the worker
listing narrowed to that worker type, ``plans`` is the execution-plan listing
narrowed to the trigger type of the same name, and its ``enable`` / ``disable``
are ``tdk fn enable`` / ``tdk fn disable`` restricted to functions that have such
a trigger. The trigger worker is what both read: it is created when the function is
registered and outlives a disable (the function's own ``workers`` map does not --
it empties out while the function is disabled, so it cannot tell a disabled cron
function from a plain one).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    all_projects_option,
    ancestry,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    list_options,
    list_params,
    logical_prompt,
    max_result_option,
    present,
    proj_option,
    render_list,
    resolve_all_projects,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td.execution_utils import (
    obtain_execution_list,
    obtain_list_exec_filters,
    obtain_list_worker_filters,
    plans_view,
)
from tabsdatak.cli.td.fn_group import set_function_enabled
from tabsdatak.common._tabular import Column, Kind, TableView
from tabsdatak.sdk.status_utils.groups import Entity, annotate

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


class _Kind(NamedTuple):
    """What differs between the two trigger kinds: the code they are listed by,
    and how they are named in output.

    ``WorkerType`` and ``TriggerType`` share the ``CT``/``ST`` codes, so the one
    value selects both the trigger worker and the plans that trigger started.
    """

    code: str
    title: str
    plural: str
    # Fill the "No <trigger> found for function ... use 'tdk fn <action>' to
    # <action> <other>" refusal. Phrased so it also holds when the function does
    # not exist at all.
    trigger: str
    other: str


CRON = _Kind(
    code="CT",
    title="Cron triggers",
    plural="cron triggers",
    trigger="cron trigger",
    other="a function that is not cron-triggered",
)

STREAM = _Kind(
    code="ST",
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


@capability("operations", "write")
@cron.command()
@proj_option
@format_option
@_name_option("Name of the cron-triggered function to enable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def enable(ctx: click.Context, name: str, coll: str):
    """Enable a cron-triggered function

    'tdk fn enable' restricted to functions that have a cron trigger; enabling
    an already-enabled one does nothing.
    """
    _enable_function(ctx, CRON, name, coll, True)


@capability("operations", "write")
@cron.command()
@proj_option
@format_option
@_name_option("Name of the cron-triggered function to disable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def disable(ctx: click.Context, name: str, coll: str):
    """Disable a cron-triggered function

    'tdk fn disable' restricted to functions that have a cron trigger; disabling
    an already-disabled one does nothing.
    """
    _enable_function(ctx, CRON, name, coll, False)


@capability("operations", "read")
@cron.command("list")
@proj_option
@all_projects_option
@_name_option("Only list the trigger of this function.")
@_coll_option("Only list triggers of functions in this collection.")
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_crons(ctx: click.Context, all_projects: bool, name: str, coll: str):
    """List the cron triggers of the project"""
    _list_triggers(ctx, CRON, all_projects, name, coll)


@capability("operations", "read")
@cron.command("plans")
@proj_option
@all_projects_option
@_name_option("Only list the plans triggered for this function.")
@_coll_option("Only list the plans of functions in this collection.")
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_cron_plans(ctx: click.Context, all_projects: bool, name: str, coll: str):
    """List the execution plans the cron triggers started, newest first"""
    _list_plans(ctx, CRON, all_projects, name, coll)


@capability("operations", "write")
@stream.command("enable")
@proj_option
@format_option
@_name_option("Name of the stream-publisher function to enable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def enable_stream(ctx: click.Context, name: str, coll: str):
    """Enable a stream-publisher function

    'tdk fn enable' restricted to stream publishers; enabling an already-enabled
    one does nothing.
    """
    _enable_function(ctx, STREAM, name, coll, True)


@capability("operations", "write")
@stream.command("disable")
@proj_option
@format_option
@_name_option("Name of the stream-publisher function to disable.")
@_coll_option("Name of the collection to which the function belongs.")
@click.pass_context
def disable_stream(ctx: click.Context, name: str, coll: str):
    """Disable a stream-publisher function

    'tdk fn disable' restricted to stream publishers; disabling an
    already-disabled one does nothing.
    """
    _enable_function(ctx, STREAM, name, coll, False)


@capability("operations", "read")
@stream.command("list")
@proj_option
@all_projects_option
@_name_option("Only list the publisher of this function.")
@_coll_option("Only list publishers of functions in this collection.")
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_streams(ctx: click.Context, all_projects: bool, name: str, coll: str):
    """List the stream publishers of the project"""
    _list_triggers(ctx, STREAM, all_projects, name, coll)


@capability("operations", "read")
@stream.command("plans")
@proj_option
@all_projects_option
@_name_option("Only list the plans triggered for this function.")
@_coll_option("Only list the plans of functions in this collection.")
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_stream_plans(ctx: click.Context, all_projects: bool, name: str, coll: str):
    """List the execution plans the stream publishers started, newest first"""
    _list_plans(ctx, STREAM, all_projects, name, coll)


def _triggers_of(
    ctx: click.Context, server: "TabsdataServer", kind: _Kind, coll: str, name: str
):
    request_filter = obtain_list_worker_filters(
        fn=name, collection=coll, worker_type=kind.code
    )
    return server.list_workers_generator(**list_params(ctx, request_filter))


def _enable_function(
    ctx: click.Context, kind: _Kind, name: str, coll: str, enabled: bool
):
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function", option="--name")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(
            ctx, "Name of the collection to which the function belongs", option="--coll"
        )
    )
    action = "enable" if enabled else "disable"
    server: TabsdataServer = ctx.obj["tabsdataserver"]
    try:
        has_trigger = any(True for _ in _triggers_of(ctx, server, kind, coll, name))
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to {action} function: {e}")
    if not has_trigger:
        raise click.ClickException(
            f"No {kind.trigger} found for function '{name}' in collection "
            f"'{coll}'. Use 'tdk fn {action}' to {action} {kind.other}."
        )
    set_function_enabled(ctx, name, coll, enabled)


def _list_plans(
    ctx: click.Context,
    kind: _Kind,
    all_projects: bool,
    name: str,
    coll: str,
):
    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        resolve_all_projects(ctx, all_projects)
        request_filter = obtain_list_exec_filters(fn=name, collection=coll)
        request_filter.append(f"trigger_type:eq:{kind.code}")
        # Already newest-first and capped by --max-result.
        plans, truncated = obtain_execution_list(
            ctx, server, request_filter, last=False
        )

        def _view():
            view = plans_view(plans, title=f"Plans of the {kind.plural}")
            view.context = ancestry(ctx, collection=coll, function=name)
            return view

        # The listing already returned each row; ``_data`` would re-fetch them
        # one by one, and ``kwargs`` alone drops the id the row was built from.
        render_list(
            ctx,
            items=lambda: [
                annotate(Entity.EXECUTION, {"id": plan.id, **plan.kwargs})
                for plan in plans
            ],
            truncated=truncated,
            view=_view,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list plans of the {kind.plural}: {e}")


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
        triggers, truncated = take(ctx, _triggers_of(ctx, server, kind, coll, name))

        def _view():
            columns = [Column("Function", Kind.ID)]
            if spanning:
                columns.append(Column("Project"))
            columns += [
                Column("Collection"),
                Column("Worker ID", Kind.ID),
                Column("Status", Kind.STATUS),
                Column("Started on", Kind.TIME),
                Column("Ended on", Kind.TIME),
            ]
            rows = []
            for trigger in triggers:
                row = [trigger.function.name]
                if spanning:
                    row.append(present(trigger.project))
                row += [
                    trigger.collection.name,
                    trigger.id,
                    trigger.status,
                    present(trigger.started_on_str),
                    present(trigger.ended_on_str),
                ]
                rows.append(row)
            return TableView(
                title=kind.title,
                context=ancestry(ctx, collection=coll, function=name),
                columns=columns,
                rows=rows,
                count=kind.plural,
            )

        # ``kwargs`` is the row the listing already returned; ``_data`` would
        # re-fetch each worker one by one.
        render_list(
            ctx,
            items=lambda: [annotate(Entity.WORKER, t.kwargs) for t in triggers],
            truncated=truncated,
            view=_view,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list {kind.plural}: {e}")
