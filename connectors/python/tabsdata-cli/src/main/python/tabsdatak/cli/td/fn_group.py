#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._machine import machine
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    ancestry,
    bundle_confinement,
    command_failure,
    deprecated_option,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    list_options,
    list_params,
    load_info_and_project_root,
    logical_prompt,
    max_result_option,
    output_format,
    present,
    progress,
    proj_option,
    render,
    render_deleted,
    render_done,
    render_entity,
    render_list,
    require_word,
    show_hint,
    take,
    trigger_waits,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td._wait import (
    require_wait,
    timeout_option,
    wait_execution,
)
from tabsdatak.cli.td.tag_commands import PARENT_TAG, tag_group, tags_of, tags_text
from tabsdatak.common._datetime import _convert_timestamp_to_string
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView
from tabsdatak.sdk.status_utils.groups import Entity, annotate

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


_FUNCTION_STATUS_LABELS = {"A": "Active", "F": "Frozen", "D": "Deleted"}


def _environment_options(command):
    command = click.option(
        "--local-pkg",
        "local_pkg",
        multiple=True,
        type=click.Path(),
        help=(
            "Path to a local package directory to include in the bundle; "
            "repeatable. Local packages are pip-installed into the function's "
            "runtime environment, separately from the '--dir-to-bundle' source "
            "tree. Import them absolutely ('import mypkg'). If a local package "
            "shares a name with a module in that tree, the installed package "
            "wins at runtime."
        ),
    )(command)
    return click.option(
        "--requirements",
        type=click.Path(),
        help=(
            "Path to a requirements.yaml to embed in the bundle verbatim, "
            "instead of the snapshot of this machine's Python environment. "
            "Use it when the runtime environment differs from this one."
        ),
    )(command)


_dir_to_bundle_option = click.option(
    "--dir-to-bundle",
    help=(
        "Path to the directory that should be stored in the bundle for "
        "execution in the backend. If not provided, the folder where the "
        "function file is will be used."
    ),
    type=click.Path(),
)

_path_option = click.option(
    "--path",
    help=(
        "Path to the function file. Must be of the form "
        "'/path/to/file.py::function_name'. Will be prompted "
        "for it if not provided."
    ),
    type=click.Path(),
)

_dry_run_option = click.option(
    "--dry-run",
    is_flag=True,
    help=(
        "Check the function as 'tdk fn check' does and show what would be "
        "registered, without uploading anything."
    ),
)


def _collection(ctx: click.Context, coll: str | None) -> str:
    return (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(
            ctx,
            "Name of the collection to which the function belongs",
            option="--coll",
            hint=(
                "Pass --coll. 'tdk fn list --filter name:eq:<name>' shows the "
                "function's collection."
            ),
        )
    )


def _function_path(ctx: click.Context, path: str | None) -> str:
    return path or logical_prompt(
        ctx,
        "Path to the function file. Must be of the form "
        "'/path/to/file.py::function_name'",
        option="--path",
    )


def _exists(server: TabsdataServer, coll: str, name: str) -> bool:
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import APIServerError

    try:
        server.get_function(coll, name)
    except APIServerError:
        return False
    return True


def _local_packages(local_pkg: tuple[str, ...]) -> list[str] | None:
    return list(local_pkg) or None


def _checked(
    ctx: click.Context,
    path: str,
    dir_to_bundle: str | None,
    requirements: str | None,
    local_pkg: tuple[str, ...],
    coll: str | None,
    name: str | None,
) -> dict:
    from tabsdatak.cli._fn_check import check_function

    server = ctx.obj["tabsdataserver"] if coll else None
    with bundle_confinement() as confine_to:
        return check_function(
            path,
            dir_to_bundle,
            requirements=requirements,
            local_packages=_local_packages(local_pkg),
            confine_to=confine_to,
            server=server,
            collection=coll,
            name=name,
        )


def _check_table(result: dict) -> None:
    function = result["function"]
    if function is None:
        click.echo("The function could not be loaded.")
    else:
        verdict = "is valid" if result["valid"] else "is not valid"
        click.echo(f"Function '{function['name']}' ({function['kind']}) {verdict}.")
        click.echo(f"  Inputs:    {', '.join(function['inputs']) or '-'}")
        click.echo(f"  Outputs:   {', '.join(function['outputs']) or '-'}")
        click.echo(f"  Triggers:  {', '.join(function['triggers']) or '-'}")
        click.echo(f"  Connector: {function['connector'] or '-'}")
    bundle = result["bundle"]
    if bundle is not None:
        click.echo(
            f"  Bundle:    {bundle['files']} files, {bundle['bytes']} bytes, "
            f"Python {bundle['python_version'] or '-'}"
        )
    for finding in result["findings"]:
        line = f" (line {finding['line']})" if finding["line"] else ""
        click.echo(
            f"{finding['severity']}: {finding['code']}{line}: {finding['message']}"
        )
    would = result.get("would")
    if would is not None:
        target = f"function '{would['name']}' in collection '{would['collection']}'"
        click.echo(f"Would {would['action']} {target}; nothing was uploaded.")


def _render_check(ctx: click.Context, result: dict, **would) -> None:
    if would:
        result["would"] = would
    if output_format(ctx) == "table":
        _check_table(result)
    elif result["valid"]:
        render(ctx, data=result)
    if not result["valid"]:
        errors = [f for f in result["findings"] if f["severity"] == "error"]
        raise TdkException(
            "checks_failed",
            f"The function is not valid: {len(errors)} error(s).",
            hint="Fix each finding with severity 'error'.",
            state=result,
        )


@cli_command
@click.group()
def fn():
    """Function management commands"""


@capability("development", "destructive")
@fn.command()
@proj_option
@format_option
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
    name = name or logical_prompt(
        ctx, "Name of the function to be deleted", option="--name"
    )
    coll = _collection(ctx, coll)
    progress(ctx, f"Deleting function '{name}' in collection '{coll}'")
    progress(ctx, "-" * 10)
    require_word(ctx, confirm, "delete", "Please type 'delete' to confirm deletion")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_function(coll, name)
        render_deleted(
            ctx,
            kind="function",
            name=name,
            collection=coll,
            project=server.connection.project,
            message="Function deleted successfully",
        )
        show_hint(
            ctx,
            "You do not need to delete and re-register a function to change it. "
            "Use 'tdk fn update' to modify the function instead.",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete function: {e}")


@capability("analytics", "read")
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
    name = name or logical_prompt(
        ctx, "Name of the function to be displayed", option="--name"
    )
    coll = _collection(ctx, coll)
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
                        Column("Defined by"),
                        Column("Defined on", Kind.TIME),
                    ],
                    rows=rows,
                    count="versions",
                )

            render_list(
                ctx,
                items=lambda: [f._data for f in function_list],
                truncated=False,
                view=_view,
            )
        else:
            # noinspection PyProtectedMember
            from tabsdatak.sdk.tabsdata_server import _function_type_to_mapping

            function = server.get_function(coll, name)
            tags = tags_of(server, collection=coll, function=name)
            data = {**(function._data or {}), "tags": tags}
            parent = _parent(server, tags)
            if parent is not None:
                data["parent"] = parent

            def _view():
                status = data.get("status")
                return RecordView(
                    title="Function",
                    fields=[
                        ("Name", function.name),
                        ("Description", present(function.description)),
                        ("Project", present(server.connection.project)),
                        ("Collection", coll),
                        ("Defined by", present(function.defined_by)),
                        ("Defined on", present(function.defined_on_str)),
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
                        ("Tags", present(tags_text(tags))),
                        ("Parent", present(_parent_text(parent))),
                        ("Snippet", present(data.get("snippet"))),
                    ],
                    kinds={"Status": Kind.STATUS},
                )

            render(ctx, data=lambda: data, view=_view)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display function: {e}")


def _parent(server: TabsdataServer, tags: dict[str, str]) -> dict | None:
    """The function a ``_parent`` tag names, by collection and name; its id
    alone when the project no longer lists it."""
    function_id = tags.get(PARENT_TAG)
    if function_id is None:
        return None
    listing = server.list_project_functions_generator(
        filter=[f"function_id:eq:{function_id}"]
    )
    function = next(iter(listing), None)
    if function is None:
        return {"function_id": function_id}
    return {
        "function_id": function_id,
        "collection": (function._data or {}).get("collection"),
        "name": function.name,
    }


def _parent_text(parent: dict | None) -> str | None:
    if parent is None:
        return None
    if parent.get("name") is None:
        return parent["function_id"]
    return f"{parent['collection']}/{parent['name']}"


@capability("development", "read", paths={"--file": "write", "--extract": "write"})
@fn.command()
@proj_option
@format_option
@click.option(
    "--fn-version",
    "fn_version",
    help=(
        "Function version id (fvid) whose bundle will be read. Use "
        "'tdk fn get --show-history' to find available fvids."
    ),
)
@click.option(
    "--coll",
    "-c",
    help=(
        "Collection of the function whose current version is read, together "
        "with '--name'."
    ),
)
@click.option(
    "--name",
    "-n",
    help="Function whose current version is read, instead of '--fn-version'.",
)
@click.option(
    "--file",
    "-f",
    "file",
    help=(
        "Path to write the downloaded bundle to. The bundle is a "
        "zstandard-compressed tar archive (.tar.zst)."
    ),
    type=click.Path(),
)
@click.option(
    "--extract",
    help=(
        "Directory to unpack the bundle into, instead of writing the archive. "
        "It must be empty or new, unless '--force' is given."
    ),
    type=click.Path(file_okay=False),
)
@click.option(
    "--force",
    is_flag=True,
    help="With '--extract', unpack into a directory that is not empty.",
)
@click.option(
    "--print",
    "print_source",
    is_flag=True,
    help=(
        "Print the module that defines the function. With '-o json', every "
        "text file of the bundle, within a size budget."
    ),
)
@click.pass_context
def bundle(
    ctx: click.Context,
    fn_version: str,
    coll: str,
    name: str,
    file: str,
    extract: str,
    force: bool,
    print_source: bool,
):
    """Download, unpack or print the bundle of a function version.

    The bundle is addressed by its function version id (fvid), or by
    '--name' for the function's current version; only Active or Frozen
    versions can be read (a version whose function has been deleted is not
    found). It holds the code that actually runs: the whole module tree the
    function was registered with, and its requirements.
    """
    if sum(map(bool, (file, extract, print_source))) > 1:
        raise click.UsageError("Pass only one of '--file', '--extract' and '--print'.")
    if fn_version and name:
        raise click.UsageError("Pass either '--fn-version' or '--name', not both.")
    if force and not extract:
        raise click.UsageError("'--force' applies only together with '--extract'.")
    if (
        extract
        and not force
        and Path(extract).is_dir()
        and any(Path(extract).iterdir())
    ):
        raise TdkException(
            "conflict",
            f"'{extract}' is not empty.",
            hint="Extract into a new or empty directory, or pass '--force'.",
        )
    verify_login_or_prompt(ctx)
    if name:
        coll = _collection(ctx, coll)
    else:
        fn_version = fn_version or logical_prompt(
            ctx,
            "Function version id (fvid) whose bundle will be read",
            option="--fn-version",
        )
    if not (extract or print_source):
        file = file or logical_prompt(
            ctx,
            "Path to write the downloaded bundle to (a .tar.zst archive)",
            option="--file",
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        if name:
            fn_version = server.get_function(coll, name).id
        if file:
            progress(
                ctx,
                f"Downloading bundle of function version '{fn_version}' to '{file}'",
            )
            progress(ctx, "-" * 10)
            server.download_function_bundle(fn_version, file)
            render_done(
                ctx,
                action="download",
                kind="bundle",
                id=fn_version,
                details={"file": file},
                message="Bundle downloaded successfully",
            )
            return
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / "bundle.tar.zst"
            server.download_function_bundle(fn_version, str(archive))
            if extract:
                _extract_bundle(ctx, archive, fn_version, extract)
            else:
                _print_bundle(ctx, archive, fn_version)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to read function bundle: {e}")


def _extract_bundle(ctx: click.Context, archive: Path, fn_version: str, dest: str):
    # noinspection PyProtectedMember
    from tabsdatak.common._archive.bundle import extract_bundle

    extract_bundle(archive, Path(dest))
    render_done(
        ctx,
        action="extract",
        kind="bundle",
        id=fn_version,
        details={"dir": dest},
        message=f"Bundle extracted to '{dest}'",
    )


def _print_bundle(ctx: click.Context, archive: Path, fn_version: str):
    # noinspection PyProtectedMember
    from tabsdatak.common._archive.bundle import (
        read_bundle_sources,
        read_function_source,
    )

    if output_format(ctx) == "table":
        found = read_function_source(archive)
        if found is None:
            raise click.ClickException(
                "The bundle carries no module defining the function."
            )
        click.echo(found[1], nl=False)
        return
    sources = read_bundle_sources(archive)
    render(
        ctx,
        data={
            "function_version_id": fn_version,
            "defining_module": sources.defining_module,
            "files": sources.files,
            "skipped": sources.skipped,
            "truncated": sources.truncated,
        },
    )


# Registered as "list" but the Python function is NOT named ``list`` to avoid
# shadowing the ``list`` builtin at module scope.
@capability("analytics", "read")
@fn.command("list")
@proj_option
@format_option
@view_options()
@click.option(
    "--coll",
    "-c",
    help=(
        "Name of the collection to which the functions belong. Default: the "
        "pinned collection, else every collection of the project."
    ),
)
@max_result_option
@list_options
@click.pass_context
def list_functions(ctx: click.Context, coll: str):
    """List the functions of a collection, or of the whole project"""
    # noinspection PyProtectedMember
    from tabsdatak.sdk.tabsdata_server import _function_type_to_mapping

    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        listing = (
            server.list_project_functions_generator(**list_params(ctx))
            if coll is None
            else server.list_functions_generator(coll, **list_params(ctx))
        )
        list_of_functions, truncated = take(ctx, listing)

        def _view():
            return TableView(
                title="Functions",
                context=ancestry(ctx, collection=coll),
                columns=[
                    *([Column("Collection", Kind.ID)] if coll is None else []),
                    Column("Name", Kind.ID),
                    Column("Type"),
                    Column("Description"),
                    Column("Defined by"),
                    Column("Defined on", Kind.TIME),
                ],
                rows=[
                    [
                        *(
                            [present((function._data or {}).get("collection"))]
                            if coll is None
                            else []
                        ),
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

        render_list(
            ctx,
            items=lambda: [f._data for f in list_of_functions],
            truncated=truncated,
            view=_view,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list functions: {e}")


@capability(
    "development",
    "read",
    "read",
    paths={
        "--path": "read",
        "--dir-to-bundle": "read",
        "--requirements": "read",
        "--local-pkg": "read",
    },
)
@fn.command()
@proj_option
@format_option
@_path_option
@_dir_to_bundle_option
@_environment_options
@click.option(
    "--coll",
    "-c",
    help=(
        "Collection to check the function against on the server: that it "
        "exists, that the input tables exist, that no other function "
        "produces the output tables and that the server accepts the Python "
        "version. Without it the check is local."
    ),
)
@click.option(
    "--name",
    "-n",
    help=(
        "Name the function is registered under, if it differs from the "
        "decorated function's; used by the '--coll' checks."
    ),
)
@click.pass_context
def check(
    ctx: click.Context,
    path: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: tuple[str, ...],
    coll: str,
    name: str,
):
    """Check a function file before registering it. Runs the code of the
    '--path' file on this machine, since it imports it.

    Imports the function as 'tdk fn register' does, reports what it declares
    (kind, input and output tables, triggers, connector), builds its bundle
    and flags calls such as eval, exec or subprocess. Nothing is uploaded.
    Exits 1 when a finding is an error; warnings do not fail.
    """
    path = _function_path(ctx, path)
    if coll:
        verify_login_or_prompt(ctx)
    try:
        result = _checked(ctx, path, dir_to_bundle, requirements, local_pkg, coll, name)
    except Exception as e:
        raise command_failure(ctx, "check function", e)
    _render_check(ctx, result)


@capability(
    "development",
    "write",
    "read",
    paths={
        "--path": "read",
        "--dir-to-bundle": "read",
        "--requirements": "read",
        "--local-pkg": "read",
    },
    lower={"--dry-run": "read"},
)
@fn.command()
@proj_option
@format_option
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the function belongs.",
)
@click.option("--description", help="Description of the function.")
@_path_option
@_dir_to_bundle_option
@_environment_options
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
@_dry_run_option
@click.pass_context
def register(
    ctx: click.Context,
    coll: str,
    description: str,
    path: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: tuple[str, ...],
    reuse_tables: bool,
    enabled: bool,
    base_path: str = None,
    update: bool = False,
    dry_run: bool = False,
):
    """Register a function. Runs the code of the '--path' file on this
    machine, since it imports it.

    Dynamically imports the symbol named by ``--path`` (which must be
    a tabsdatak ``PublisherInfo`` / ``TransformerInfo`` /
    ``SubscriberInfo`` produced by a ``@publisher`` / ``@transformer``
    / ``@subscriber`` decorator) and registers it via the v2 bundle
    producer (``tabsdatak.common._archive.bundle.build_bundle``).

    With ``--update``, behaves idempotently: if the function already
    exists in the collection it is updated; otherwise it is registered.

    With ``--dry-run``, checks the function as 'tdk fn check --coll' does and
    reports what would be registered, uploading nothing.
    """
    verify_login_or_prompt(ctx)
    description = description or ""
    progress(ctx, "Registering a function")
    progress(ctx, "-" * 10)
    coll = _collection(ctx, coll)
    path = _function_path(ctx, path)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        if dry_run:
            result = _checked(
                ctx, path, dir_to_bundle, requirements, local_pkg, coll, None
            )
            function_name = (result["function"] or {}).get("name")
            existing = update and function_name and _exists(server, coll, function_name)
            _render_check(
                ctx,
                result,
                action="update" if existing else "register",
                collection=coll,
                name=function_name,
            )
            return
        info, project_root = load_info_and_project_root(path, dir_to_bundle)
        function_name = getattr(info.func, "__name__", None) or info.__class__.__name__
        existing = update and _exists(server, coll, function_name)
        if update:
            show_hint(
                ctx,
                "To update functions you can also use the dedicated command 'tdk fn "
                "update'",
                final_empty_line=True,
            )
            action = "updating" if existing else "registering"
            progress(
                ctx,
                f"Function '{function_name}' "
                f"{'exists' if existing else 'does not exist'} in collection "
                f"'{coll}': {action} it.",
            )
        common = {
            "description": description,
            "local_packages": _local_packages(local_pkg),
            "reuse_tables": reuse_tables,
            "enabled": enabled,
            "requirements": requirements,
            "base_path": base_path,
        }
        with bundle_confinement() as confine_to:
            if existing:
                function = server.update_function(
                    coll,
                    function_name,
                    info,
                    project_root,
                    confine_to=confine_to,
                    **common,
                )
            else:
                function = server.register_function(
                    coll, info, project_root, confine_to=confine_to, **common
                )
        render_entity(
            ctx,
            data=lambda: function._data,
            message=(
                "Function updated successfully"
                if existing
                else "Function registered successfully"
            ),
        )
    except Exception as e:
        raise command_failure(ctx, "register function", e)


@capability("operations", "write")
@fn.command()
@proj_option
@format_option
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
    "--wait/--no-wait",
    default=None,
    help=(
        "Wait until the plan reaches a final status, or return as soon as it "
        "starts. Exits 7 when '--timeout' runs out and 8 when a function run "
        "did not succeed. Default: the 'tdk set --trigger-wait' setting (true)."
    ),
)
@click.option(
    "--detach",
    "-d",
    is_flag=True,
    default=False,
    hidden=True,
    callback=deprecated_option("--detach", "'--no-wait'"),
)
@timeout_option()
@click.pass_context
def trigger(
    ctx: click.Context,
    name: str,
    coll: str,
    plan_name: str,
    wait: bool | None,
    detach: bool,
    timeout: float | None,
):
    """Trigger a function execution. It waits for the execution to finish with
    '--wait' and returns at once with '--no-wait'; with neither, the
    'tdk set --trigger-wait true|false' setting decides (default: true)."""
    if detach:
        if wait:
            raise click.UsageError("Pass '--wait' or '--no-wait', not both.")
        wait = False
    if wait is None:
        wait = trigger_waits(ctx)
    require_wait(wait, timeout)
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(
        ctx, "Name of the function to be triggered", option="--name"
    )
    coll = _collection(ctx, coll)
    progress(ctx, f"Triggering function '{name}' in collection '{coll}'")
    progress(ctx, "-" * 10)
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
    if not wait:
        render_entity(
            ctx,
            data=lambda: annotate(Entity.EXECUTION, execution._data),
            message=(
                f"Function triggered. Execution has started.\nPlan id: {execution.id}\n"
            ),
        )
        show_hint(
            ctx,
            "You can check the status of the plan with 'tdk plan get "
            f"--plan {execution.id}', or wait for it to finish with 'tdk plan "
            f"monitor --plan {execution.id}' (or trigger with '--wait').",
        )
        return
    progress(ctx, f"Function triggered. Plan id: {execution.id}")
    try:
        document = wait_execution(ctx, execution, timeout)
    except click.ClickException:
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(
            f"Plan {execution.id} was triggered, but waiting for it failed: {e}. "
            f"Resume with 'tdk plan monitor --plan {execution.id}'"
        )
    render(ctx, data=document)


@capability(
    "development",
    "write",
    "read",
    paths={
        "--path": "read",
        "--dir-to-bundle": "read",
        "--requirements": "read",
        "--local-pkg": "read",
    },
    escalate={
        "--reset-state": {"effect": "destructive"},
        "--reset-stage": {"effect": "destructive"},
    },
    lower={"--dry-run": "read"},
)
@fn.command()
@proj_option
@format_option
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
@_path_option
@_dir_to_bundle_option
@_environment_options
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
@_dry_run_option
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    coll: str,
    description: str,
    path: str,
    dir_to_bundle: str,
    requirements: str,
    local_pkg: tuple[str, ...],
    reuse_tables: bool,
    enabled: bool,
    base_path: str = None,
    reset_state: bool = False,
    reset_stage: bool = False,
    dry_run: bool = False,
):
    """Update a function. With '--path', runs the code of that file on this
    machine, since it imports it.

    Dynamically imports the symbol named by ``--path`` (a tabsdatak
    ``*Info`` descriptor) and updates the existing function via the
    v2 bundle producer (``tabsdatak.common._archive.bundle.build_bundle``).

    The new version applies to runs planned from here on. Runs already
    planned keep the version they were planned with, so an update does
    not reach a stalled plan: to change the code such a run executes,
    patch its version with 'tdk fn patch'.
    """
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(
        ctx, "Name of the function to be updated", option="--name"
    )
    coll = _collection(ctx, coll)
    path = _function_path(ctx, path)
    try:
        if dry_run:
            result = _checked(
                ctx, path, dir_to_bundle, requirements, local_pkg, coll, name
            )
            _render_check(ctx, result, action="update", collection=coll, name=name)
            return
        progress(ctx, f"Updating function '{name}' in collection '{coll}'")
        progress(ctx, "-" * 10)
        info, project_root = load_info_and_project_root(path, dir_to_bundle)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        with bundle_confinement() as confine_to:
            function = server.update_function(
                coll,
                name,
                info,
                project_root,
                description=description,
                local_packages=_local_packages(local_pkg),
                reuse_tables=reuse_tables,
                enabled=enabled,
                requirements=requirements,
                confine_to=confine_to,
                base_path=base_path,
                reset_state=reset_state,
                reset_stage=reset_stage,
            )
        render_entity(
            ctx, data=lambda: function._data, message="Function updated successfully"
        )
    except Exception as e:
        raise command_failure(ctx, "update function", e)


@capability("operations", "write")
@fn.command()
@proj_option
@format_option
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
    name = name or logical_prompt(
        ctx, "Name of the function to be updated", option="--name"
    )
    coll = _collection(ctx, coll)
    set_function_enabled(ctx, name, coll, True)


@capability("operations", "write")
@fn.command()
@proj_option
@format_option
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
    name = name or logical_prompt(
        ctx, "Name of the function to be updated", option="--name"
    )
    coll = _collection(ctx, coll)
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
            render_done(
                ctx,
                action=action,
                kind="function",
                id=name,
                details={"collection": coll, "changed": False},
                message=f"Function '{name}' in collection '{coll}' is already {state}",
            )
            return
        progress(
            ctx,
            f"{'Enabling' if enabled else 'Disabling'} function '{name}' in "
            f"collection '{coll}'",
        )
        progress(ctx, "-" * 10)
        server.enable_function(
            collection_name=coll,
            function_name=name,
            enabled=enabled,
        )
        render_done(
            ctx,
            action=action,
            kind="function",
            id=name,
            details={"collection": coll, "changed": True},
            message="Function updated successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to {action} function: {e}")


@capability(
    "development",
    "write",
    "read",
    paths={
        "--path": "read",
        "--dir-to-bundle": "read",
        "--requirements": "read",
        "--local-pkg": "read",
        "--source": "read",
    },
    lower={"--dry-run": "read"},
)
@fn.command()
@proj_option
@format_option
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
@_path_option
@click.option("--description", help="Description of the patch.")
@_dir_to_bundle_option
@click.option(
    "--source",
    help=(
        "Python file that replaces the module defining the function in the "
        "bundle of '--fn-version'; the rest of that bundle's tree and its "
        "requirements are kept. Instead of '--path' and '--dir-to-bundle'."
    ),
    type=click.Path(),
)
@click.option(
    "--add-package",
    "add_package",
    multiple=True,
    help=(
        "Package requirement (e.g. 'requests==2.32.3') added to the kept "
        "requirements, replacing any pin of the same package; repeatable. "
        "Only with '--source', and not with '--requirements'."
    ),
)
@_environment_options
@_dry_run_option
@click.pass_context
def patch(
    ctx: click.Context,
    name: str,
    coll: str,
    fn_version: str,
    path: str,
    description: str,
    dir_to_bundle: str,
    source: str,
    add_package: tuple[str, ...],
    requirements: str,
    local_pkg: tuple[str, ...],
    dry_run: bool,
):
    """Register a code-only patch of a function version. With '--path' or
    '--source', runs the code of that file on this machine, since it imports
    it.

    A patch shares the parent's topology (tables, dependencies, triggers,
    workers) — only bundle / snippet / runtime values / description can be
    overridden.

    With '--source', the patch is the bundle of '--fn-version' with only the
    module that defines the function replaced, so its sibling modules, its
    requirements and its local packages carry over ('--requirements' and
    '--local-pkg' replace them).

    A patch is the only way to change the code a planned run executes:
    both 'tdk fnrun replay' and 'tdk plan recover' / 'tdk fnrun recover'
    re-run the function version the plan pinned, picking up its latest
    patch. Registering a new version with 'tdk fn update' does not affect
    runs that are already planned.
    """
    if source and (path or dir_to_bundle):
        raise click.UsageError(
            "'--source' replaces '--path' and '--dir-to-bundle'; pass one or the other."
        )
    if add_package and not source:
        raise click.UsageError("'--add-package' applies only together with '--source'.")
    if add_package and requirements:
        raise click.UsageError(
            "'--add-package' adds to the parent bundle's requirements, which "
            "'--requirements' replaces; pass one or the other."
        )
    verify_login_or_prompt(ctx)
    coll = _collection(ctx, coll)
    name = name or logical_prompt(ctx, "Name of the function to patch", option="--name")
    fn_version = fn_version or logical_prompt(
        ctx, "Function-version id (fvid) this patch overrides", option="--fn-version"
    )
    if not source:
        path = _function_path(ctx, path)
    try:
        with _patch_tree(ctx, fn_version, source, add_package) as tree:
            if tree is not None:
                path, dir_to_bundle = tree.path, str(tree.project_root)
                if requirements is None and tree.requirements is not None:
                    requirements = str(tree.requirements)
                if not local_pkg:
                    local_pkg = tuple(str(p) for p in tree.local_packages)
            if dry_run:
                result = _checked(
                    ctx, path, dir_to_bundle, requirements, local_pkg, coll, name
                )
                _render_check(
                    ctx,
                    result,
                    action="patch",
                    collection=coll,
                    name=name,
                    fn_version=fn_version,
                )
                return
            if tree is not None:
                _require_valid_source(path, source)
            progress(
                ctx,
                f"Patching function '{name}' in collection '{coll}' "
                f"(fn_version={fn_version})",
            )
            progress(ctx, "-" * 10)
            info, project_root = load_info_and_project_root(path, dir_to_bundle)
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            with bundle_confinement() as confine_to:
                record = server.patch_function(
                    coll,
                    name,
                    info,
                    project_root,
                    patch_of=fn_version,
                    description=description,
                    local_packages=_local_packages(local_pkg),
                    requirements=requirements,
                    confine_to=confine_to,
                )
        render_entity(
            ctx,
            data=record,
            message=(
                "Patch registered successfully\n"
                f"Patch id: {record.get('id')}\n"
                f"Patches:  {record.get('parent_function_version_id')}"
            ),
        )
        show_hint(
            ctx,
            "Re-run against the latest patch with 'tdk plan recover "
            "--plan <PLAN-ID>' to resume a stalled plan, or 'tdk fnrun "
            "replay --fnrun <RUN-ID>' to re-run a finished run.",
            final_empty_line=True,
        )
    except Exception as e:
        raise command_failure(ctx, "patch function", e)


@contextmanager
def _patch_tree(
    ctx: click.Context,
    fn_version: str,
    source: str | None,
    add_package: tuple[str, ...],
) -> Iterator:
    """The bundle of ``fn_version`` with ``source`` spliced in, or ``None``
    without ``source``. It is built under the machine run's ``confine_to``
    when there is one, since the bundle made from it is confined there."""
    if not source:
        yield None
        return
    # noinspection PyProtectedMember
    from tabsdatak.common._archive.bundle import splice_function_source

    new_source = Path(source).read_text(encoding="utf-8")
    server: TabsdataServer = ctx.obj["tabsdataserver"]
    with tempfile.TemporaryDirectory(
        dir=run.confine_to if (run := machine()) else None
    ) as tmp:
        archive = Path(tmp) / "parent.tar.zst"
        server.download_function_bundle(fn_version, str(archive))
        yield splice_function_source(
            archive, new_source, Path(tmp) / "bundle", add_packages=add_package
        )


def _require_valid_source(path: str, source: str) -> None:
    from tabsdatak.cli._fn_check import check_source

    file_path, _, qualname = path.partition("::")
    errors = [
        finding["message"]
        for finding in check_source(
            Path(file_path).read_text(encoding="utf-8"), source, qualname
        )
        if finding["severity"] == "error"
    ]
    if errors:
        raise click.ClickException("; ".join(errors))


@capability("analytics", "read")
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
@list_options
@click.pass_context
def list_patches(ctx: click.Context, name: str, coll: str):
    """List patch versions of a function (latest first)."""
    verify_login_or_prompt(ctx)
    coll = _collection(ctx, coll)
    name = name or logical_prompt(
        ctx, "Name of the function whose patches will be listed", option="--name"
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        patches, truncated = take(
            ctx, server.list_function_patches_generator(coll, name, **list_params(ctx))
        )

        def _defined_on(patch):
            defined_on = patch.get("defined_on")
            if isinstance(defined_on, (int, float)):
                defined_on = _convert_timestamp_to_string(defined_on)
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

        render_list(ctx, items=lambda: patches, truncated=truncated, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list patches: {e}")


fn.add_command(tag_group("function"))
