#
# Copyright 2024 Tabs Data Inc.
#

import datetime
import os
from time import sleep
from typing import Tuple

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.api.tabsdata_server import (
    FINAL_STATUSES,
    SUCCESSFUL_FINAL_STATUSES,
    TabsdataServer,
    convert_timestamp_to_string,
    function_type_to_mapping,
    status_to_mapping,
)
from tabsdata.cli.cli_utils import (
    DOT_FOLDER,
    beautify_list,
    cleanup_dot_files,
    generate_dot_image,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    show_hint,
    verify_login_or_prompt,
)


@click.group()
def fn():
    """Function management commands"""


@fn.command()
@click.option(
    "--name",
    "-n",
    help="Name of the function to be deleted.",
)
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, collection: str, confirm: str):
    """Delete a function"""
    verify_login_or_prompt(ctx)
    click.echo(f"Deleting function '{name}' in collection '{collection}'")
    click.echo("-" * 10)
    name = name or logical_prompt(ctx, "Name of the function to be deleted")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_function(collection, name)
        click.echo("Function deleted successfully")
        show_hint(
            ctx,
            "You do not need to delete and re-register a function to change it. "
            "Use 'td fn update' to modify the function instead.",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete function: {e}")


@fn.command()
@click.option(
    "--name",
    "-n",
    help="Name of the function to be displayed.",
)
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--show-history",
    is_flag=True,
    help="Show the history of the function, newest first.",
)
@click.pass_context
def info(ctx: click.Context, name: str, collection: str, show_history: bool):
    """Display a function"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be displayed")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    try:
        if show_history:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            function_list = server.list_function_history(collection, name)

            table = Table(
                title=f"History of function '{name}' in collection '{collection}'"
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
                    beautify_list(function.dependencies_with_names),
                    beautify_list(function.trigger_with_names),
                    beautify_list([table.name for table in function.tables]),
                )

            click.echo()
            console = Console()
            console.print(table)
            click.echo(f"Number of versions: {len(function_list)}")
            click.echo()

        else:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            function = server.function_get(collection, name)

            table = Table(title=f"Function '{name}' in collection '{collection}'")
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
                beautify_list(function.dependencies_with_names),
                beautify_list(function.trigger_with_names),
                beautify_list([table.name for table in function.tables]),
            )

            click.echo()
            console = Console()
            console.print(table)
            click.echo()

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display function: {e}")


@fn.command()
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the functions belong.",
)
@click.pass_context
def list(ctx: click.Context, collection: str):
    verify_login_or_prompt(ctx)
    """List all functions in a collection"""
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the functions belong")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_functions = server.list_functions(collection)

        table = Table(title=f"Functions in collection '{collection}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Type")
        table.add_column("Description")
        table.add_column("Defined on")
        table.add_column("Defined by")

        for function in list_of_functions:
            table.add_row(
                function.name,
                function_type_to_mapping(function.type),
                function.description,
                function.defined_on_str,
                function.defined_by,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(
            f"Number of functions in collection '{collection}':"
            f" {len(list_of_functions)}"
        )
        click.echo()

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list functions: {e}")


@fn.command()
@click.option(
    "--collection",
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
        "execution in the backed. If not provided, the folder where the "
        "function file is will be used."
    ),
)
@click.option(
    "--requirements",
    help=(
        "Path to the requirements file. If not provided, the requirements file will "
        "be generated based on your current Python environment."
    ),
)
@click.option(
    "--local-pkg",
    type=str,
    multiple=True,
    help="Path to a local package to include in the bundle.",
)
@click.option("--reuse-frozen", is_flag=True, help="Reuse frozen tables.")
@click.pass_context
def register(
    ctx: click.Context,
    collection: str,
    description: str,
    path: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: Tuple[str, ...],
    reuse_frozen: bool,
):
    """Registering a new function"""
    verify_login_or_prompt(ctx)
    description = description or ""
    if local_pkg:
        local_pkg = [element for element in local_pkg]
    else:
        local_pkg = None
    click.echo("Registering a new function")
    click.echo("-" * 10)
    collection = (
        collection
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
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.register_function(
            collection,
            path,
            description,
            dir_to_bundle,
            requirements,
            local_pkg,
            reuse_frozen_tables=reuse_frozen,
        )
        click.echo("Function registered successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to register function: {e}")


@fn.command()
@click.option(
    "--name",
    "-n",
    help="Name of the function to be triggered.",
)
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--exec",
    help="ID of the run to read.",
)
@click.pass_context
def read_run(ctx: click.Context, name: str, collection: str, exec: str):
    """Read the information of a function run"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function the run belongs to")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    exec = exec or logical_prompt(ctx, "ID of the run to read")
    click.echo(
        f"Reading information of run '{exec}' for function '{name}' in "
        f"collection '{collection}'"
    )
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        response = server.read_function_run(collection, name, exec)
        data = response.json().get("data")

        table = Table(title="Run information")
        table.add_column("Status", style="cyan", no_wrap=True)
        table.add_column("Triggered on")
        table.add_column("Started on")
        table.add_column("Ended on")
        table.add_column("Triggered by")

        status = status_to_mapping(data.get("status"))
        triggered_on = (
            convert_timestamp_to_string(data.get("triggered_on"))
            if data.get("triggered_on")
            else "-"
        )
        started_on = (
            convert_timestamp_to_string(data.get("started_on"))
            if data.get("started_on")
            else "-"
        )
        ended_on = (
            convert_timestamp_to_string(data.get("ended_on"))
            if data.get("ended_on")
            else "-"
        )
        triggered_by = data.get("triggered_by")
        table.add_row(
            status,
            triggered_on,
            started_on,
            ended_on,
            triggered_by,
        )

        click.echo()
        console = Console()
        console.print(table)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to read run information: {e}")


@fn.command()
@click.option(
    "--name",
    "-n",
    help="Name of the function to be triggered.",
)
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option(
    "--exec-name",
    help="Name to be given to the execution that will be generated by the trigger.",
)
@click.option(
    "--show-plan",
    is_flag=True,
    default=False,
    help="Show the generated execution plan.",
)
@click.option(
    "--background",
    is_flag=True,
    default=False,
    help="Do not monitor the status of the execution.",
)
@click.pass_context
def trigger(
    ctx: click.Context,
    name: str,
    collection: str,
    exec_name: str,
    show_plan: bool,
    background: bool,
):
    """Trigger a function"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the function to be triggered")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    click.echo(f"Triggering function '{name}' in collection '{collection}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        execution = server.trigger_function(collection, name, execution_name=exec_name)
        click.echo("Function triggered. Execution has started.")
        click.echo(f"Execution id: {execution.id}")
        dot = execution.dot
        execution_real_name = execution.name or execution.id
        if dot:
            os.makedirs(DOT_FOLDER, exist_ok=True)
            current_timestamp = int(
                datetime.datetime.now().replace(microsecond=0).timestamp()
            )
            file_name = f"{execution_real_name}-{current_timestamp}.dot"
            full_path = os.path.join(DOT_FOLDER, file_name)
            with open(full_path, "w") as f:
                f.write(dot)
            click.echo(f"Plan DOT at path: {full_path}")
            generate_dot_image(full_path, open_image=show_plan, ctx=ctx)
        else:
            click.echo("No DOT returned")
        cleanup_dot_files()
        if not background:
            click.echo("")
            click.echo("Waiting for the execution to finish...")
            show_hint(
                ctx,
                "To trigger the function without monitoring the execution, "
                "use the '--background' option.",
            )
            while True:
                click.echo("")
                click.echo("-" * 10)
                click.echo("")
                click.echo(f"Current execution status: {execution.status}")

                list_of_transactions = execution.transactions
                list_of_workers = execution.workers

                table = Table(title="Workers")
                table.add_column("Worker ID", style="cyan", no_wrap=True)
                table.add_column("Collection")
                table.add_column("Function")
                table.add_column("Transaction ID")
                table.add_column("Status")

                for worker in list_of_workers:
                    table.add_row(
                        worker.id,
                        worker.collection.name,
                        worker.function.name,
                        worker.transaction.id,
                        worker.status,
                    )

                click.echo()
                console = Console()
                console.print(table)
                click.echo(f"Number of workers: {len(list_of_workers)}")
                click.echo()

                # TODO: Change to loop worker status or even execution status when
                #   the API supports it.
                if all(
                    transaction.status in FINAL_STATUSES
                    for transaction in list_of_transactions
                ):
                    click.echo("")
                    click.echo("-" * 10)
                    click.echo("")
                    click.echo("Execution finished.")
                    break
                sleep(5)
                execution.refresh()
            # Note: while it would initially make more sense to write this as
            # 'if transaction.status in FAILED_FINAL_STATUSES', this approach avoids
            # the risk of ignoring failed transactions that are not in a recognized
            # status due to a mismatch between the transaction status and the
            # FINAL_STATUSES set (which ideally should not happen).
            failed_workers = [
                worker
                for worker in list_of_workers
                if worker.status not in SUCCESSFUL_FINAL_STATUSES
            ]
            if failed_workers:
                click.echo("Some workers failed:")
                for worker in failed_workers:
                    click.echo(f"- {worker.id}")
                show_hint(
                    ctx,
                    "You can check their logs with 'td exec worker-logs --worker "
                    "<worker_id>' or with 'td exec worker-logs --exec "
                    f"{execution.id}'",
                )
            else:
                click.echo("All workers were successful.")
        else:
            show_hint(
                ctx,
                "You can check the status of the execution with 'td exec info "
                f"{execution.id}'",
            )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to trigger function: {e}")


@fn.command()
@click.option(
    "--name",
    "-n",
    help="Name of the function to be updated.",
)
@click.option(
    "--collection",
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
        "execution in the backed. If not provided, the folder where the "
        "function file is will be used."
    ),
)
@click.option(
    "--requirements",
    help=(
        "Path to the requirements file. If not provided, the requirements file will "
        "be generated based on your current Python environment."
    ),
)
@click.option(
    "--local-pkg",
    multiple=True,
    help="Path to a local package to include in the bundle.",
)
@click.option("--reuse-frozen", is_flag=True, help="Reuse frozen tables.")
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    collection: str,
    description: str,
    path: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: Tuple[str, ...],
    reuse_frozen: bool,
):
    """Update a function"""
    verify_login_or_prompt(ctx)
    description = description or ""
    if local_pkg:
        local_pkg = [element for element in local_pkg]
    else:
        local_pkg = None
    name = name or logical_prompt(ctx, "Name of the function to be updated")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    path = path or logical_prompt(
        ctx,
        "Path to the function file. "
        "Must be of the form "
        "'/path/to/file.py::function_name'",
    )
    click.echo(f"Updating function '{name}' in collection '{collection}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.function_update(
            collection_name=collection,
            function_name=name,
            function_path=path,
            description=description,
            directory_to_bundle=dir_to_bundle,
            requirements=requirements,
            local_packages=local_pkg,
            reuse_frozen_tables=reuse_frozen,
        )
        click.echo("Function updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update function: {e}")
