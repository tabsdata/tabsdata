#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    ancestry,
    collection_label,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    load_info_and_project_root,
    logical_prompt,
    max_result_option,
    present,
    proj_option,
    render,
    show_hint,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td.execution_utils import live_monitor
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView
from tabsdatak.sdk.status_utils.execution import EXECUTION_FINAL_STATUSES
from tabsdatak.sdk.status_utils.function_run import (
    FUNCTION_RUN_SUCCESSFUL_FINAL_STATUSES,
)
from tabsdatak.sdk.status_utils.transaction import TRANSACTION_FINAL_STATUSES

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import Execution, TabsdataServer, Transaction


_FUNCTION_STATUS_LABELS = {"A": "Active", "F": "Frozen", "D": "Deleted"}


@cli_command
@click.group()
def fn():
    """Function management commands"""


@fn.command()
@proj_option
@click.option(
    "--name",
    "-n",
    help="Name of the function to be deleted.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, coll: str, confirm: str):
    """Delete a function"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be deleted")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    click.echo(f"Deleting function '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_function(coll, name)
        click.echo("Function deleted successfully")
        show_hint(
            ctx,
            "You do not need to delete and re-register a function to change it. "
            "Use 'tdk fn update' to modify the function instead.",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete function: {e}")


@fn.command()
@proj_option
@format_option
@view_options()
@click.option(
    "--name",
    "-n",
    help="Name of the function to be displayed.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--show-history",
    is_flag=True,
    help="Show the history of the function, newest first.",
)
@click.pass_context
def get(ctx: click.Context, name: str, coll: str, show_history: bool):
    """Display a function"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be displayed")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        if show_history:
            function_list = server.list_function_history(coll, name)

            def _view():
                rows = []
                for function in function_list:
                    status = (function._data or {}).get("status")
                    rows.append([
                        function.id,
                        present(function.patch_of),
                        function.name,
                        present(function.description),
                        present(_FUNCTION_STATUS_LABELS.get(status, status)),
                        present(function.defined_by),
                        present(function.defined_on_str),
                    ])
                return TableView(
                    title="History",
                    context=ancestry(ctx, collection=coll, function=name),
                    columns=[
                        Column("ID", Kind.ID),
                        Column("Patches", Kind.ID),
                        Column("Name"),
                        Column("Description"),
                        Column("Status", Kind.STATUS),
                        Column("Created by"),
                        Column("Created on", Kind.TIME),
                    ],
                    rows=rows,
                    count="versions",
                )

            render(ctx, data=lambda: [f._data for f in function_list], view=_view)
        else:
            # noinspection PyProtectedMember
            from tabsdatak.sdk.tabsdata_server import _function_type_to_mapping

            function = server.get_function(coll, name)

            def _view():
                data = function._data or {}
                status = data.get("status")
                return RecordView(
                    title="Function",
                    fields=[
                        ("Name", function.name),
                        ("Description", present(function.description)),
                        ("Project", present(server.connection.project)),
                        ("Collection", coll),
                        ("Created by", present(function.defined_by)),
                        ("Created on", present(function.defined_on_str)),
                        (
                            "Status",
                            present(_FUNCTION_STATUS_LABELS.get(status, status)),
                        ),
                        ("Enabled", "Yes" if function.enabled else "No"),
                        ("Type", _function_type_to_mapping(function.type)),
                        ("Tables", present(", ".join(t.name for t in function.tables))),
                        ("Triggers", present(", ".join(function.triggers or []))),
                        (
                            "Dependencies",
                            present(", ".join(function.dependencies or [])),
                        ),
                        ("Snippet", present(data.get("snippet"))),
                    ],
                    kinds={"Status": Kind.STATUS},
                )

            render(ctx, data=lambda: function._data, view=_view)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display function: {e}")


@fn.command()
@proj_option
@click.option(
    "--fn-version",
    "fn_version",
    help=(
        "Function version id (fvid) whose bundle will be downloaded. Use "
        "'tdk fn get --show-history' to find available fvids."
    ),
)
@click.option(
    "--file",
    "-f",
    "file",
    help=(
        "Path to write the downloaded bundle to. The bundle is a "
        "zstandard-compressed tar archive (.tar.zst)."
    ),
)
@click.pass_context
def bundle(ctx: click.Context, fn_version: str, file: str):
    """Download the bundle of a function version as a tar.zst archive.

    The bundle is addressed by its function version id (fvid); only Active or
    Frozen versions can be downloaded (a version whose function has been
    deleted is not found).
    """
    verify_login_or_prompt(ctx)
    fn_version = fn_version or logical_prompt(
        ctx, "Function version id (fvid) whose bundle will be downloaded"
    )
    file = file or logical_prompt(
        ctx, "Path to write the downloaded bundle to (a .tar.zst archive)"
    )
    click.echo(f"Downloading bundle of function version '{fn_version}' to '{file}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.download_function_bundle(fn_version, file)
        click.echo("Bundle downloaded successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to download function bundle: {e}")


# Registered as "list" but the Python function is NOT named ``list`` to avoid
# shadowing the ``list`` builtin at module scope.
@fn.command("list")
@proj_option
@format_option
@view_options()
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the functions belong.",
)
@max_result_option
@click.pass_context
def list_functions(ctx: click.Context, coll: str):
    """List all functions in a collection"""
    # noinspection PyProtectedMember
    from tabsdatak.sdk.tabsdata_server import _function_type_to_mapping

    verify_login_or_prompt(ctx)
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the functions belong")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_functions, truncated = take(ctx, server.list_functions_generator(coll))

        def _view():
            return TableView(
                title="Functions",
                context=ancestry(ctx, collection=coll),
                columns=[
                    Column("Name", Kind.ID),
                    Column("Type"),
                    Column("Description"),
                    Column("Created by"),
                    Column("Created on", Kind.TIME),
                ],
                rows=[
                    [
                        function.name,
                        _function_type_to_mapping(function.type),
                        present(function.description),
                        present(function.defined_by),
                        present(function.defined_on_str),
                    ]
                    for function in list_of_functions
                ],
                count="functions",
            )

        render(
            ctx,
            data=lambda: [f._data for f in list_of_functions],
            view=_view,
            truncated=truncated,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list functions: {e}")


@fn.command()
@proj_option
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option("--description", help="Description of the function.")
@click.option(
    "--path",
    help=(
        "Path to the function file. Must be of the form "
        "'/path/to/file.py::function_name'. Will be prompted "
        "for it if not provided."
    ),
)
@click.option(
    "--dir-to-bundle",
    help=(
        "Path to the directory that should be stored in the bundle for "
        "execution in the backend. If not provided, the folder where the "
        "function file is will be used."
    ),
)
# --requirements and --local-pkg are temporarily disabled.
@click.option(
    "--reuse-tables",
    is_flag=True,
    help=(
        "Reuse previously created tables. "
        "If provided, any tables that the "
        "function produces that already "
        "exist in the system (but no "
        "longer have a function that "
        "generates them) will be "
        "reused, adding to their data "
        "history. If not provided, "
        "those tables will be recreated "
        "and their data history re-initialized."
    ),
)
@click.option(
    "--enabled/--disabled",
    default=True,
    help="Enable or disable the feature",
)
@click.option(
    "--update",
    is_flag=True,
    help=(
        "If provided, update the function if it already exists in the "
        "collection. If not present, the command falls through to the "
        "normal register flow."
    ),
)
@click.option(
    "--storage-path",
    "base_path",
    help=(
        "Optional storage-prefix override for this function's output table data. "
        "When omitted, the collection's base_path is used."
    ),
)
@click.pass_context
def register(
    ctx: click.Context,
    coll: str,
    description: str,
    path: str,
    dir_to_bundle: str,
    reuse_tables: bool,
    enabled: bool,
    base_path: str = None,
    update: bool = False,
):
    """Register a function.

    Dynamically imports the symbol named by ``--path`` (which must be
    a tabsdatak ``PublisherInfo`` / ``TransformerInfo`` /
    ``SubscriberInfo`` produced by a ``@publisher`` / ``@transformer``
    / ``@subscriber`` decorator) and registers it via the v2 bundle
    producer (``tabsdatak.common._archive.bundle.build_bundle``).

    With ``--update``, behaves idempotently: if the function already
    exists in the collection it is updated; otherwise it is registered.
    """
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import APIServerError

    verify_login_or_prompt(ctx)
    description = description or ""
    click.echo("Registering a function")
    click.echo("-" * 10)
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    path = path or logical_prompt(
        ctx,
        "Path to the function file. "
        "Must be of the form "
        "'/path/to/file.py::function_name'",
    )
    try:
        info, project_root = load_info_and_project_root(path, dir_to_bundle)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        if update:
            show_hint(
                ctx,
                "To update functions you can also use the dedicated command 'tdk fn "
                "update'",
                final_empty_line=True,
            )
            function_name = (
                getattr(info.func, "__name__", None) or info.__class__.__name__
            )
            try:
                server.get_function(coll, function_name)
                click.echo(
                    f"The function '{function_name}' already exists in collection "
                    f"'{coll}'. Proceeding to update it."
                )
                server.update_function(
                    coll,
                    function_name,
                    info,
                    project_root,
                    description=description,
                    local_packages=None,
                    reuse_tables=reuse_tables,
                    enabled=enabled,
                    requirements=None,
                    base_path=base_path,
                )
                click.echo("Function updated successfully")
                return
            except APIServerError:
                click.echo(
                    f"The function '{function_name}' does not exist in collection "
                    f"'{coll}'. Proceeding to register it."
                )
        server.register_function(
            coll,
            info,
            project_root,
            description=description,
            local_packages=None,
            reuse_tables=reuse_tables,
            enabled=enabled,
            requirements=None,
            base_path=base_path,
        )
        click.echo("Function registered successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to register function: {e}")


# @fn.command()
# @click.option(
#     "--name",
#     "-n",
#     help="Name of the function to be triggered.",
# )
# @click.option(
#     "--coll",
#     "-c",
#     help="Name of the collection to which the function belongs.",
# )
# @click.option(
#     "--plan",
#     help="ID of the plan to read.",
# )
# @click.pass_context
# def read_run(ctx: click.Context, name: str, coll: str, plan: str):
#     """Read the information of a function run"""
#     verify_login_or_prompt(ctx)
#     name = name or logical_prompt(ctx, "Name of the function the run belongs to")
#     coll = (
#         coll
#         or get_currently_pinned_object(ctx, "collection")
#         or logical_prompt(ctx, "Name of the collection to which the function belongs")
#     )
#     plan = plan or logical_prompt(ctx, "ID of the plan to read")
#     click.echo(
#         f"Reading information of plan '{plan}' for function '{name}' in "
#         f"collection '{coll}'"
#     )
#     click.echo("-" * 10)
#     try:
#         server: TabsdataServer = ctx.obj["tabsdataserver"]
#         response = server.read_function_run(coll, name, plan)
#         data = response.json().get("data")
#
#         table = Table(title="Run information")
#         table.add_column("Status", style="cyan", no_wrap=True)
#         table.add_column("Triggered on")
#         table.add_column("Started on")
#         table.add_column("Ended on")
#         table.add_column("Triggered by")
#
#         status = status_to_mapping(data.get("status"))
#         triggered_on = (
#             _convert_timestamp_to_string(data.get("triggered_on"))
#             if data.get("triggered_on")
#             else "-"
#         )
#         started_on = (
#             _convert_timestamp_to_string(data.get("started_on"))
#             if data.get("started_on")
#             else "-"
#         )
#         ended_on = (
#             _convert_timestamp_to_string(data.get("ended_on"))
#             if data.get("ended_on")
#             else "-"
#         )
#         triggered_by = data.get("triggered_by")
#         table.add_row(
#             status,
#             triggered_on,
#             started_on,
#             ended_on,
#             triggered_by,
#         )
#
#         click.echo()
#         console = Console()
#         console.print(table)
#
#     except Exception as e:
#         hint_common_solutions(ctx, e)
#         raise click.ClickException(f"Failed to read run information: {e}")


@fn.command()
@proj_option
@click.option(
    "--name",
    "-n",
    help="Name of the function to be triggered.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--plan-name",
    help="Name to be given to the plan that will be generated by the trigger.",
)
@click.option(
    "--detach",
    "-d",
    is_flag=True,
    default=False,
    help="Do not monitor the status of the plan.",
)
@click.pass_context
def trigger(
    ctx: click.Context,
    name: str,
    coll: str,
    plan_name: str,
    detach: bool,
):
    """Trigger a function"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be triggered")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    click.echo(f"Triggering function '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        execution = server.trigger_function(coll, name, execution_name=plan_name)
    except Exception as e:
        import requests

        if isinstance(e, requests.exceptions.ReadTimeout):
            raise click.ClickException(
                f"The server did not answer in time: {e}. The function may have "
                "been triggered; check with 'tdk plan list' before triggering it "
                "again."
            )
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to trigger function: {e}")
    click.echo("Function triggered. Execution has started.")
    click.echo(f"Plan id: {execution.id}")
    click.echo("")
    if detach:
        show_hint(
            ctx,
            "You can check the status of the plan with 'tdk plan get "
            f"--plan {execution.id}', or monitor the execution of the function "
            f"with 'tdk plan monitor --plan {execution.id}'",
        )
        return
    show_hint(
        ctx,
        "To trigger the function without monitoring the plan, "
        "use the '--debug' or '-d' option.",
        final_empty_line=True,
    )
    try:
        _monitor_execution_or_transaction(ctx, execution=execution)
    except click.ClickException:
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(
            f"Plan {execution.id} was triggered, but monitoring it failed: {e}. "
            f"Resume with 'tdk plan monitor --plan {execution.id}'"
        )


def _monitor_execution_or_transaction(
    ctx: click.Context, execution: Execution = None, transaction: Transaction = None
):
    """Monitor the execution of a plan or a transaction"""
    if execution and transaction:
        raise click.ClickException(
            "Internal error: _monitor_execution_or_transaction takes either an "
            "execution or a transaction, but not both."
        )
    if not execution and not transaction:
        raise click.ClickException(
            "Internal error: _monitor_execution_or_transaction takes either an "
            "execution or a transaction, but not neither."
        )
    if execution:
        entity_final_statuses = EXECUTION_FINAL_STATUSES
    else:
        entity_final_statuses = TRANSACTION_FINAL_STATUSES
    keyword = "plan" if execution else "transaction"
    supervised_entity = execution or transaction

    def build_view(function_runs):
        return TableView(
            title="Function runs",
            context=[(keyword.capitalize(), supervised_entity.id)],
            columns=[
                Column("Function run ID", Kind.ID),
                Column("Collection"),
                Column("Function"),
                Column("Transaction ID" if execution else "Plan ID", Kind.ID),
                Column("Started on", Kind.TIME),
                Column("Ended on", Kind.TIME),
                Column("Status", Kind.STATUS),
            ],
            rows=[
                [
                    function_run.id,
                    collection_label(function_run),
                    function_run.function.name,
                    (
                        function_run.transaction.id
                        if execution
                        else function_run.execution.id
                    ),
                    present(function_run.started_on_str),
                    present(function_run.ended_on_str),
                    present(function_run.status),
                ]
                for function_run in function_runs
            ],
        )

    def fetch():
        supervised_entity.refresh()
        return supervised_entity.function_runs

    # Finality is read from the plan or transaction itself: a run in a status
    # the client does not know must not keep the watch open.
    list_of_runs = live_monitor(
        supervised_entity.function_runs,
        fetch,
        build_view,
        lambda _: supervised_entity.status in entity_final_statuses,
    )

    click.echo(f"{keyword.capitalize()} finished.")

    failed_runs = [
        fn_run
        for fn_run in list_of_runs
        if fn_run.status not in FUNCTION_RUN_SUCCESSFUL_FINAL_STATUSES
    ]
    if failed_runs:
        click.echo("Some function runs failed:")
        for fn_run in failed_runs:
            click.echo(f"- {fn_run.id}")
        complete_command = (
            f"'tdk plan logs --plan {supervised_entity.id}'"
            if execution
            else f"'tdk trx logs --trx {supervised_entity.id}'"
        )
        show_hint(
            ctx,
            "You can check their logs with 'tdk fnrun logs --fnrun <RUN-ID>' or with "
            + complete_command,
        )
    else:
        click.echo("All function runs were successful.")


@fn.command()
@proj_option
@click.option(
    "--name",
    "-n",
    help="Name of the function to be updated.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option("--description", help="Description of the function.")
@click.option(
    "--path",
    help=(
        "Path to the function file. Must be of the form "
        "'/path/to/file.py::function_name'. Will be prompted "
        "for it if not provided."
    ),
)
@click.option(
    "--dir-to-bundle",
    help=(
        "Path to the directory that should be stored in the bundle for "
        "execution in the backend. If not provided, the folder where the "
        "function file is will be used."
    ),
)
# --requirements and --local-pkg are temporarily disabled.
@click.option(
    "--reuse-tables",
    is_flag=True,
    help=(
        "Reuse previously created tables. "
        "If provided, any tables that the "
        "function produces that already "
        "exist in the system (but no "
        "longer have a function that "
        "generates them) will be "
        "reused, adding to their data "
        "history. If not provided, "
        "those tables will be recreated "
        "and their data history re-initialized."
    ),
)
@click.option(
    "--enabled/--disabled",
    default=True,
    help="Enable or disable the feature",
)
@click.option(
    "--storage-path",
    "base_path",
    help=(
        "Optional storage-prefix override for this function's output table data. "
        "When omitted, the previous version's value is carried forward."
    ),
)
@click.option(
    "--reset-state",
    is_flag=True,
    help=(
        "Regenerate the function's state table id instead of carrying it forward, "
        "so publishers re-read their initial configuration values from scratch."
    ),
)
@click.option(
    "--reset-stage",
    is_flag=True,
    help=(
        "Regenerate the function's stage id (a fresh stage/checkpoint dir), discarding "
        "accumulated stage state. The stage's storage prefix is unchanged."
    ),
)
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    coll: str,
    description: str,
    path: str,
    dir_to_bundle: str,
    reuse_tables: bool,
    enabled: bool,
    base_path: str = None,
    reset_state: bool = False,
    reset_stage: bool = False,
):
    """Update a function.

    Dynamically imports the symbol named by ``--path`` (a tabsdatak
    ``*Info`` descriptor) and updates the existing function via the
    v2 bundle producer (``tabsdatak.common._archive.bundle.build_bundle``).

    The new version applies to runs planned from here on. Runs already
    planned keep the version they were planned with, so an update does
    not reach a stalled plan: to change the code such a run executes,
    patch its version with 'tdk fn patch'.
    """
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be updated")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    path = path or logical_prompt(
        ctx,
        "Path to the function file. "
        "Must be of the form "
        "'/path/to/file.py::function_name'",
    )
    click.echo(f"Updating function '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    try:
        info, project_root = load_info_and_project_root(path, dir_to_bundle)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_function(
            coll,
            name,
            info,
            project_root,
            description=description,
            local_packages=None,
            reuse_tables=reuse_tables,
            enabled=enabled,
            requirements=None,
            base_path=base_path,
            reset_state=reset_state,
            reset_stage=reset_stage,
        )
        click.echo("Function updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update function: {e}")


@fn.command()
@proj_option
@click.option(
    "--name",
    "-n",
    help="Name of the function to enable.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.pass_context
def enable(
    ctx: click.Context,
    name: str,
    coll: str,
):
    """Enables a function

    Enabling an already-enabled function does nothing.
    """
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be updated")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    set_function_enabled(ctx, name, coll, True)


@fn.command()
@proj_option
@click.option(
    "--name",
    "-n",
    help="Name of the function to disable.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.pass_context
def disable(
    ctx: click.Context,
    name: str,
    coll: str,
):
    """Disables a function

    Disabling an already-disabled function does nothing.
    """
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be updated")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    set_function_enabled(ctx, name, coll, False)


def set_function_enabled(ctx: click.Context, name: str, coll: str, enabled: bool):
    """Enable or disable a function, doing nothing if it is already in that
    state. Shared with the ``tdk cron`` / ``tdk stream`` enable/disable, which
    are this command restricted to a trigger kind."""
    action = "enable" if enabled else "disable"
    state = "enabled" if enabled else "disabled"
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        if server.get_function(coll, name).enabled == enabled:
            click.echo(f"Function '{name}' in collection '{coll}' is already {state}")
            return
        click.echo(
            f"{'Enabling' if enabled else 'Disabling'} function '{name}' in "
            f"collection '{coll}'"
        )
        click.echo("-" * 10)
        server.enable_function(
            collection_name=coll,
            function_name=name,
            enabled=enabled,
        )
        click.echo("Function updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to {action} function: {e}")


@fn.command()
@proj_option
@click.option(
    "--name",
    "-n",
    help="Name of the function to patch.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--fn-version",
    "fn_version",
    help=(
        "Function-version id (fvid) this patch overrides. Use 'tdk fn get "
        "--show-history' to find available fvids."
    ),
)
@click.option(
    "--path",
    help=(
        "Path to the function file. Must be of the form "
        "'/path/to/file.py::function_name'. Will be prompted "
        "for it if not provided."
    ),
)
@click.option("--description", help="Description of the patch.")
@click.option(
    "--dir-to-bundle",
    help=(
        "Path to the directory that should be stored in the bundle for "
        "execution in the backend. If not provided, the folder where the "
        "function file is will be used."
    ),
)
# --requirements and --local-pkg are temporarily disabled.
@click.pass_context
def patch(
    ctx: click.Context,
    name: str,
    coll: str,
    fn_version: str,
    path: str,
    description: str,
    dir_to_bundle: str,
):
    """Register a code-only patch of a function version.

    A patch shares the parent's topology (tables, dependencies, triggers,
    workers) — only bundle / snippet / runtime values / description can be
    overridden.

    A patch is the only way to change the code a planned run executes:
    both 'tdk fnrun replay' and 'tdk plan recover' / 'tdk fnrun recover'
    re-run the function version the plan pinned, picking up its latest
    patch. Registering a new version with 'tdk fn update' does not affect
    runs that are already planned.
    """
    verify_login_or_prompt(ctx)
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    name = name or logical_prompt(ctx, "Name of the function to patch")
    fn_version = fn_version or logical_prompt(
        ctx, "Function-version id (fvid) this patch overrides"
    )
    path = path or logical_prompt(
        ctx,
        "Path to the function file. Must be of the form "
        "'/path/to/file.py::function_name'",
    )
    click.echo(
        f"Patching function '{name}' in collection '{coll}' (fn_version={fn_version})"
    )
    click.echo("-" * 10)
    try:
        info, project_root = load_info_and_project_root(path, dir_to_bundle)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        record = server.patch_function(
            coll,
            name,
            info,
            project_root,
            patch_of=fn_version,
            description=description,
            local_packages=None,
            requirements=None,
        )
        click.echo("Patch registered successfully")
        click.echo(f"Patch id: {record.get('id')}")
        click.echo(f"Patches:  {record.get('parent_function_version_id')}")
        show_hint(
            ctx,
            "Re-run against the latest patch with 'tdk plan recover "
            "--plan <PLAN-ID>' to resume a stalled plan, or 'tdk fnrun "
            "replay --fnrun <RUN-ID>' to re-run a finished run.",
            final_empty_line=True,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to patch function: {e}")


@fn.command("patches")
@proj_option
@format_option
@view_options()
@click.option(
    "--name",
    "-n",
    help="Name of the function whose patches will be listed.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@max_result_option
@click.pass_context
def list_patches(ctx: click.Context, name: str, coll: str):
    """List patch versions of a function (latest first)."""
    verify_login_or_prompt(ctx)
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    name = name or logical_prompt(
        ctx, "Name of the function whose patches will be listed"
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        patches, truncated = take(
            ctx, server.list_function_patches_generator(coll, name)
        )

        def _defined_on(patch):
            defined_on = patch.get("defined_on")
            if isinstance(defined_on, (int, float)):
                # epoch milliseconds -> ISO-ish UTC string
                from datetime import datetime, timezone

                defined_on = datetime.fromtimestamp(
                    defined_on / 1000.0, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
            return present(defined_on)

        def _view():
            return TableView(
                title="Patches",
                context=ancestry(ctx, collection=coll, function=name),
                columns=[
                    Column("Patch id", Kind.ID),
                    Column("Patches", Kind.ID),
                    Column("Description"),
                    Column("Defined on", Kind.TIME),
                    Column("Defined by"),
                    Column("Status", Kind.STATUS),
                ],
                rows=[
                    [
                        present(p.get("id")),
                        present(p.get("parent_function_version_id")),
                        present(p.get("description")),
                        _defined_on(p),
                        present(p.get("defined_by")),
                        present(
                            _FUNCTION_STATUS_LABELS.get(
                                p.get("status"), p.get("status")
                            )
                        ),
                    ]
                    for p in patches
                ],
                count="patches",
            )

        # `patches` is already the raw list of patch dicts -> full info as-is.
        render(ctx, data=lambda: patches, view=_view, truncated=truncated)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list patches: {e}")
