#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from time import sleep
from typing import TYPE_CHECKING, Tuple

import rich_click as click
from rich.live import Live
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    beautify_list,
    beautify_time,
    get_currently_pinned_object,
    hint_common_solutions,
    load_info_and_project_root,
    logical_prompt,
    make_console,
    output_option,
    proj_option,
    render,
    show_hint,
    verify_login_or_prompt,
)
from tabsdatak.sdk.status_utils.execution import EXECUTION_FINAL_STATUSES
from tabsdatak.sdk.status_utils.function_run import (
    FUNCTION_RUN_SUCCESSFUL_FINAL_STATUSES,
)
from tabsdatak.sdk.status_utils.transaction import TRANSACTION_FINAL_STATUSES

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import Execution, TabsdataServer, Transaction


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
@output_option
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
def info(ctx: click.Context, name: str, coll: str, show_history: bool):
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

            def _table():
                table = Table(
                    title=f"History of function '{name}' in collection '{coll}'"
                )
                table.add_column("ID", style="cyan", no_wrap=True)
                table.add_column("Name")
                table.add_column("Description")
                table.add_column("Defined on")
                table.add_column("Defined by")
                table.add_column("Dependencies")
                table.add_column("Triggers (functions)")
                table.add_column("Tables")
                for function in function_list:
                    table.add_row(
                        str(function.id),
                        function.name,
                        function.description,
                        function.defined_on_str,
                        function.defined_by,
                        beautify_list(function.dependencies),
                        beautify_list(function.triggers),
                        beautify_list([t.name for t in function.tables]),
                    )
                click.echo()
                make_console().print(table)
                click.echo(f"Number of versions: {len(function_list)}")
                click.echo()

            render(
                ctx,
                data=lambda: [f._data for f in function_list],
                table=_table,
            )
        else:
            function = server.get_function(coll, name)

            def _table():
                table = Table(title=f"Function '{name}' in collection '{coll}'")
                table.add_column("ID", style="cyan", no_wrap=True)
                table.add_column("Description")
                table.add_column("Defined on")
                table.add_column("Defined by")
                table.add_column("Dependencies")
                table.add_column("Triggers (functions)")
                table.add_column("Tables")
                table.add_row(
                    str(function.id),
                    function.description,
                    function.defined_on_str,
                    function.defined_by,
                    beautify_list(function.dependencies),
                    beautify_list(function.triggers),
                    beautify_list([t.name for t in function.tables]),
                )
                click.echo()
                make_console().print(table)
                click.echo()

            render(ctx, data=lambda: function._data, table=_table)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display function: {e}")


# Registered as "list" but the Python function is NOT named ``list``: a module
# named ``list`` would shadow the ``list`` builtin, and ``register`` / ``update``
# / ``patch`` below call ``list(local_pkg)`` -- with the builtin shadowed that
# invoked this Click command instead, breaking ``--local-pkg`` at runtime.
@fn.command("list")
@proj_option
@output_option
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the functions belong.",
)
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
        list_of_functions = server.list_functions(coll)

        def _table():
            table = Table(title=f"Functions in collection '{coll}'")
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Type")
            table.add_column("Description")
            table.add_column("Defined on")
            table.add_column("Defined by")
            for function in list_of_functions:
                table.add_row(
                    function.name,
                    _function_type_to_mapping(function.type),
                    function.description,
                    function.defined_on_str,
                    function.defined_by,
                )
            click.echo()
            make_console().print(table)
            click.echo(
                f"Number of functions in collection '{coll}': {len(list_of_functions)}"
            )
            click.echo()

        render(
            ctx,
            data=lambda: [f._data for f in list_of_functions],
            table=_table,
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
@click.option(
    "--requirements",
    help=(
        "Path to a user-supplied requirements.yaml. When set, this "
        "file is embedded in the bundle verbatim, overriding the env "
        "snapshot the bundle producer would otherwise synthesise "
        "from the active Python interpreter. Use this when the build "
        "env differs from the runtime env (e.g. cross-platform "
        "deployment)."
    ),
)
@click.option(
    "--local-pkg",
    type=str,
    multiple=True,
    help="Path to a local package to include in the bundle.",
)
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
    "--base-path",
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
    requirements: str,
    local_pkg: Tuple[str, ...],
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
    local_pkg_list = list(local_pkg) if local_pkg else None
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
                    local_packages=local_pkg_list,
                    reuse_tables=reuse_tables,
                    enabled=enabled,
                    requirements=requirements,
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
            local_packages=local_pkg_list,
            reuse_tables=reuse_tables,
            enabled=enabled,
            requirements=requirements,
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
        click.echo("Function triggered. Execution has started.")
        click.echo(f"Plan id: {execution.id}")
        click.echo("")
        if not detach:
            show_hint(
                ctx,
                "To trigger the function without monitoring the plan, "
                "use the '--debug' or '-d' option.",
                final_empty_line=True,
            )
            _monitor_execution_or_transaction(ctx, execution=execution)
        else:
            show_hint(
                ctx,
                "You can check the status of the plan with 'tdk exe info "
                f"--plan {execution.id}', or monitor the execution of the function "
                f"with 'tdk exe monitor --plan {execution.id}'",
            )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to trigger function: {e}")


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

    list_of_runs = supervised_entity.function_runs

    def build_table():
        table = Table(title=f"Function Runs: {len(list_of_runs)}")
        table.add_column("Function Run ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        (
            table.add_column("Transaction ID", no_wrap=True)
            if execution
            else table.add_column("Plan ID", no_wrap=True)
        )
        table.add_column("Started on", no_wrap=True)
        table.add_column("Ended on", no_wrap=True)
        table.add_column("Status")
        for function_run in list_of_runs:
            table.add_row(
                function_run.id,
                function_run.collection.name,
                function_run.function.name,
                function_run.transaction.id if execution else function_run.execution.id,
                beautify_time(function_run.started_on_str),
                beautify_time(function_run.ended_on_str),
                function_run.status,
            )
        return table

    click.echo(f"Waiting for the {keyword} to finish...")

    refresh_rate = 1  # seconds
    with Live(
        build_table(), refresh_per_second=refresh_rate, console=make_console()
    ) as live:
        while True:
            # Note: while it would initially make more sense to write this as
            # 'if transaction.status in FAILED_FINAL_STATUSES', this approach avoids
            # the risk of ignoring failed transactions that are not in a recognized
            # status due to a mismatch between the transaction status and the
            # FINAL_STATUSES set (which ideally should not happen).
            if supervised_entity.status in entity_final_statuses:
                break
            list_of_runs = supervised_entity.function_runs
            live.update(build_table())
            supervised_entity.refresh()
            sleep(1 / refresh_rate)
        list_of_runs = supervised_entity.function_runs
        live.update(build_table())
        supervised_entity.refresh()

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
            f"'tdk exe logs --plan {supervised_entity.id}'"
            if execution
            else f"'tdk exe logs --trx {supervised_entity.id}'"
        )
        show_hint(
            ctx,
            "You can check their logs with 'tdk exe logs --fn-run <FN-RUN-ID>' or with "
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
@click.option(
    "--requirements",
    help=(
        "Path to a user-supplied requirements.yaml. When set, this "
        "file is embedded in the bundle verbatim, overriding the env "
        "snapshot the bundle producer would otherwise synthesise "
        "from the active Python interpreter."
    ),
)
@click.option(
    "--local-pkg",
    multiple=True,
    help="Path to a local package to include in the bundle.",
)
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
    "--base-path",
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
    requirements: str,
    local_pkg: Tuple[str, ...],
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
    """
    verify_login_or_prompt(ctx)
    local_pkg_list = list(local_pkg) if local_pkg else None
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
            local_packages=local_pkg_list,
            reuse_tables=reuse_tables,
            enabled=enabled,
            requirements=requirements,
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
    """Enables a function"""
    name = name or logical_prompt(ctx, "Name of the function to be updated")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )

    click.echo(f"Enabling function '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.enable_function(
            collection_name=coll,
            function_name=name,
            enabled=True,
        )
        click.echo("Function updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to enable function: {e}")


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
    """Disables a function"""
    name = name or logical_prompt(ctx, "Name of the function to be updated")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    click.echo(f"Disabling function '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.enable_function(
            collection_name=coll,
            function_name=name,
            enabled=False,
        )
        click.echo("Function updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to disable function: {e}")


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
    "--patch-of",
    help=(
        "Function-version id (fvid) this patch overrides. Use 'tdk fn info "
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
@click.option(
    "--requirements",
    help=(
        "Path to a user-supplied requirements.yaml. When set, this "
        "file is embedded in the bundle verbatim, overriding the env "
        "snapshot the bundle producer would otherwise synthesise "
        "from the active Python interpreter."
    ),
)
@click.option(
    "--local-pkg",
    type=str,
    multiple=True,
    help="Path to a local package to include in the bundle.",
)
@click.pass_context
def patch(
    ctx: click.Context,
    name: str,
    coll: str,
    patch_of: str,
    path: str,
    description: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: Tuple[str, ...],
):
    """Register a code-only patch of a function version.

    A patch shares the parent's topology (tables, dependencies, triggers,
    workers) — only bundle / snippet / runtime values / description can be
    overridden. Patches are reachable only by explicit fvid via replay.
    """
    verify_login_or_prompt(ctx)
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    name = name or logical_prompt(ctx, "Name of the function to patch")
    patch_of = patch_of or logical_prompt(
        ctx, "Function-version id (fvid) this patch overrides"
    )
    path = path or logical_prompt(
        ctx,
        "Path to the function file. Must be of the form "
        "'/path/to/file.py::function_name'",
    )
    local_pkg_list = list(local_pkg) if local_pkg else None
    click.echo(
        f"Patching function '{name}' in collection '{coll}' (patch_of={patch_of})"
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
            patch_of=patch_of,
            description=description,
            local_packages=local_pkg_list,
            requirements=requirements,
        )
        click.echo("Patch registered successfully")
        click.echo(f"Patch id (fvid): {record.get('id')}")
        click.echo(f"Patches fvid:    {record.get('parent_function_version_id')}")
        show_hint(
            ctx,
            "Replay a finished logical-function-run against the latest patch "
            "with 'tdk exe replay --coll <coll> --lfr <lfr_id>'.",
            final_empty_line=True,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to patch function: {e}")


@fn.command("list-patches")
@proj_option
@output_option
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
        patches = server.list_function_patches(coll, name)

        def _table():
            table = Table(title=f"Patches of function '{name}' in collection '{coll}'")
            table.add_column("Patch id (fvid)", style="cyan", no_wrap=True)
            table.add_column("Patches fvid")
            table.add_column("Description")
            table.add_column("Defined on")
            table.add_column("Defined by")
            table.add_column("Status")
            for p in patches:
                defined_on = p.get("defined_on")
                if isinstance(defined_on, (int, float)):
                    # epoch milliseconds -> ISO-ish UTC string
                    from datetime import datetime, timezone

                    defined_on = datetime.fromtimestamp(
                        defined_on / 1000.0, tz=timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                table.add_row(
                    p.get("id") or "",
                    p.get("parent_function_version_id") or "",
                    p.get("description") or "",
                    beautify_time(defined_on) if defined_on else "",
                    p.get("defined_by") or p.get("defined_by_id") or "",
                    p.get("status") or "",
                )
            click.echo()
            make_console().print(table)
            click.echo(f"Number of patches: {len(patches)}")
            click.echo()

        # `patches` is already the raw list of patch dicts -> full info as-is.
        render(ctx, data=lambda: patches, table=_table)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list patches: {e}")
