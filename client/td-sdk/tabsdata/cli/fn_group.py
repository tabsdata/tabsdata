#
# Copyright 2024 Tabs Data Inc.
#

import datetime
import os
from typing import Tuple

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.cli.cli_utils import (
    DOT_FOLDER,
    beautify_list,
    cleanup_dot_files,
    logical_prompt,
    show_dot_file,
    verify_login_or_prompt,
)


@click.group()
@click.pass_context
def fn(ctx: click.Context):
    """Function management commands"""
    verify_login_or_prompt(ctx)


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
    """Delete a function. Currently not supported."""
    raise click.ClickException("Deleting a function is currently not supported.")
    click.echo(f"Deleting function '{name}' in collection '{collection}'")
    click.echo("-" * 10)
    name = name or logical_prompt(ctx, "Name of the function to be deleted")
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the function belongs"
    )
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        ctx.obj["tabsdataserver"].function_delete(collection, name)
        click.echo("Function deleted successfully")
    except Exception as e:
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
    name = name or logical_prompt(ctx, "Name of the function to be displayed")
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the function belongs"
    )
    try:
        if show_history:
            function_list = ctx.obj["tabsdataserver"].function_list_history(
                collection, name
            )

            table = Table(
                title=f"History of function '{name}' in collection '{collection}'"
            )
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Name")
            table.add_column("Description")
            table.add_column("Created on")
            table.add_column("Created by")
            table.add_column("Dependencies")
            table.add_column("Triggers (functions)")
            table.add_column("Tables")

            for function in function_list:
                table.add_row(
                    str(function.id),
                    function.name,
                    function.description,
                    function.created_on_string,
                    function.created_by,
                    beautify_list(function.dependencies_with_names),
                    beautify_list(function.trigger_with_names),
                    beautify_list(function.tables),
                )

            click.echo()
            console = Console()
            console.print(table)
            click.echo(f"Number of versions: {len(function_list)}")
            click.echo()

        else:
            function = ctx.obj["tabsdataserver"].function_get(collection, name)

            table = Table(title=f"Function '{name}' in collection '{collection}'")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Description")
            table.add_column("Created on")
            table.add_column("Created by")
            table.add_column("Dependencies")
            table.add_column("Triggers (functions)")
            table.add_column("Tables")

            table.add_row(
                str(function.id),
                function.description,
                function.created_on_string,
                function.created_by,
                beautify_list(function.dependencies_with_names),
                beautify_list(function.trigger_with_names),
                beautify_list(function.tables),
            )

            click.echo()
            console = Console()
            console.print(table)
            click.echo()

    except Exception as e:
        raise click.ClickException(f"Failed to display function: {e}")


@fn.command()
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the functions belong.",
)
@click.pass_context
def list(ctx: click.Context, collection: str):
    """List all functions in a collection"""
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the functions belong"
    )
    try:
        list_of_functions = ctx.obj["tabsdataserver"].collection_list_functions(
            collection
        )

        table = Table(title=f"Functions in collection '{collection}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        for function in list_of_functions:
            table.add_row(
                function.name,
                function.description,
                function.created_on_string,
                function.created_by,
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
        raise click.ClickException(f"Failed to list functions: {e}")


@fn.command()
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option("--description", help="Description of the function.")
@click.option(
    "--fn-path",
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
@click.pass_context
def register(
    ctx: click.Context,
    collection: str,
    description: str,
    fn_path: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: Tuple[str, ...],
):
    """Registering a new function"""
    description = description or ""
    if local_pkg:
        local_pkg = [element for element in local_pkg]
    else:
        local_pkg = None
    click.echo("Registering a new function")
    click.echo("-" * 10)
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the function belongs"
    )
    fn_path = fn_path or logical_prompt(
        ctx,
        "Path to the function file. "
        "Must be of the form "
        "'/path/to/file.py::function_name'",
    )
    try:
        ctx.obj["tabsdataserver"].function_create(
            collection,
            fn_path,
            description,
            dir_to_bundle,
            requirements,
            local_pkg,
        )
        click.echo("Function registered successfully")
    except Exception as e:
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
    "--execution-plan-name",
    help=(
        "Name to be given to the execution plan that will be generated by the trigger."
    ),
)
@click.pass_context
def trigger(ctx: click.Context, name: str, collection: str, execution_plan_name: str):
    """Trigger a function"""
    name = name or logical_prompt(ctx, "Name of the function to be triggered")
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the function belongs"
    )
    click.echo(f"Triggering function '{name}' in collection '{collection}'")
    click.echo("-" * 10)
    try:
        response = ctx.obj["tabsdataserver"].function_trigger(
            collection, name, execution_plan_name=execution_plan_name
        )
        click.echo("Function triggered successfully")
        dot = response.json().get("dot")
        execution_plan_real_name = response.json().get("name")
        if dot:
            os.makedirs(DOT_FOLDER, exist_ok=True)
            current_timestamp = int(
                datetime.datetime.now().replace(microsecond=0).timestamp()
            )
            file_name = f"{execution_plan_real_name}-{current_timestamp}.dot"
            full_path = os.path.join(DOT_FOLDER, file_name)
            with open(full_path, "w") as f:
                f.write(dot)
            click.echo(f"Plan DOT at path: {full_path}")
            show_dot_file(full_path)
        else:
            click.echo("No DOT returned")
        cleanup_dot_files()
    except Exception as e:
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
    "--fn-path",
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
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    collection: str,
    description: str,
    fn_path: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: Tuple[str, ...],
):
    """Update a function"""
    description = description or ""
    if local_pkg:
        local_pkg = [element for element in local_pkg]
    else:
        local_pkg = None
    name = name or logical_prompt(ctx, "Name of the function to be updated")
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the function belongs"
    )
    fn_path = fn_path or logical_prompt(
        ctx,
        "Path to the function file. "
        "Must be of the form "
        "'/path/to/file.py::function_name'",
    )
    click.echo(f"Updating function '{name}' in collection '{collection}'")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].function_update(
            collection_name=collection,
            function_name=name,
            function_path=fn_path,
            description=description,
            directory_to_bundle=dir_to_bundle,
            requirements=requirements,
            local_packages=local_pkg,
        )
        click.echo("Collection updated successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to update function: {e}")
