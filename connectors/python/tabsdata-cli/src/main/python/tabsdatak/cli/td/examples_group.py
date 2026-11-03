#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk examples`` -- discover, read and extract the bundled examples, and
scaffold a function from an installed connector.

Snippets are single files (``<name>.py``) that CI imports and runs; projects
are folders with the layout ``<name>/{input-data,connections,functions,
_setup.py}``. ``extract`` writes a snippet as ``DIR/<name>.py``, and copies a
project into a new ``DIR`` (flattening every subfolder except the ``*-data``
ones) before running its ``_setup.py`` standing in that directory.
"""

from __future__ import annotations

import errno
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._examples import (
    FUNCTION_KINDS,
    KINDS,
    META_FILE,
    SNIPPET,
    Example,
    examples_root,
    find_example,
    load_examples,
    matches,
)
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    format_option,
    output_format,
    present,
    render,
    render_done,
    render_list,
    show_hint,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView

if TYPE_CHECKING:
    from tabsdatak.cli._scaffold import Scaffold

BUNDLED = "bundled"
SETUP_FILE = "_setup.py"
ERROR_FILE = "error.txt"
DATA_DIR_SUFFIX = "-data"


def _open_new(path: str | Path, mode: str, **kwargs):
    # Windows' exclusive create follows a dangling symlink and creates its target.
    if os.path.lexists(path):
        raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), str(path))
    return open(path, mode, **kwargs)


@cli_command
@click.group()
def examples():
    """Curated function examples and example projects"""


def _brief(example: Example) -> dict:
    return {**example.brief(), "source": BUNDLED}


def _examples_view(rows: list[dict]) -> TableView:
    return TableView(
        title="Available examples",
        columns=[
            Column("Name", Kind.ID),
            Column("Source"),
            Column("Kind"),
            Column("Connectors"),
            Column("Title"),
        ],
        rows=[
            [
                example["name"],
                example["source"],
                example["kind"] or example["form"],
                present(", ".join(example["connectors"])),
                present(example["title"]),
            ]
            for example in rows
        ],
        count="examples",
    )


@capability("knowledge", "read")
@examples.command("list")
@click.option("--kind", type=click.Choice(KINDS), help="Only examples of this kind.")
@click.option(
    "--connector",
    help="Only examples using this connection type, e.g. 'postgres-sql-in'.",
)
@click.option("--tag", help="Only examples with this tag.")
@format_option
@view_options()
@click.pass_context
def list_(ctx: click.Context, kind: str | None, connector: str | None, tag: str | None):
    """List the bundled examples, optionally filtered"""
    rows = [
        brief
        for brief in map(_brief, load_examples(examples_root()))
        if matches(brief, kind, connector, tag)
    ]
    render_list(
        ctx,
        items=rows,
        truncated=False,
        view=lambda: _examples_view(rows),
        compat=lambda: [{**row, "description": row["summary"]} for row in rows],
    )


def _echo_files(files: list[dict], banner: bool) -> None:
    for file in files:
        if banner:
            click.echo(f"# --- {file['path']} ---")
        click.echo(file["content"], nl=not file["content"].endswith("\n"))


@capability("knowledge", "read")
@examples.command()
@click.option(
    "--name",
    "-n",
    required=True,
    help="Name of the example to show. See 'tdk examples list'.",
)
@format_option
@click.pass_context
def show(ctx: click.Context, name: str):
    """Print an example: the source of a snippet, or the README and function
    files of a project"""
    entry = find_example(examples_root(), name)
    files = entry.read_files()
    brief = _brief(entry)
    if output_format(ctx) == "table":
        _echo_files(files, banner=brief["form"] != SNIPPET)
    render(ctx, data={**brief, "files": files})


@capability("knowledge", "read")
@examples.command()
@click.option(
    "--name",
    "-n",
    help="Name of the example whose path to print. Default: the examples directory.",
)
@format_option
@click.pass_context
def path(ctx: click.Context, name: str | None):
    """Print the absolute path of the examples directory, or of one example"""
    root = examples_root()
    resolved = (find_example(root, name).path if name else root).resolve()
    if output_format(ctx) == "table":
        click.echo(resolved)
    render(ctx, data={"path": str(resolved)})


def _skip(entry: Path) -> bool:
    name = entry.name
    return (
        name == META_FILE
        or name == "__pycache__"
        or name.startswith(".")
        or name.endswith((".pyc", ".pyo"))
    )


def _copy_flattened(src: Path, target: Path) -> None:
    """Copy ``src`` into ``target``, flattening every subfolder into the target
    root except ``*-data`` folders, which are copied verbatim. ``_meta.yaml`` and
    build artifacts are left behind."""
    for entry in sorted(src.iterdir()):
        if _skip(entry):
            continue
        if entry.is_dir():
            if entry.name.endswith(DATA_DIR_SUFFIX):
                shutil.copytree(
                    entry,
                    target / entry.name,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
                )
            else:
                for member in sorted(entry.iterdir()):
                    if member.is_file() and not _skip(member):
                        shutil.copy2(member, target / member.name)
        else:
            shutil.copy2(entry, target / entry.name)


def _run_setup(target: Path) -> None:
    """Run the example's ``_setup.py`` in ``target``. On success the script is
    removed; on failure it is kept and the full output is dumped to
    ``error.txt`` in ``target``."""
    setup_path = target / SETUP_FILE
    if not setup_path.exists():
        return
    completed = subprocess.run(
        [sys.executable, SETUP_FILE],
        cwd=target,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0:
        setup_path.unlink()
        return
    error_path = target / ERROR_FILE
    error_path.write_text(
        f"Example setup failed with exit code {completed.returncode}.\n"
        f"command: {sys.executable} {SETUP_FILE}\n"
        f"directory: {target}\n\n"
        f"--- stdout ---\n{completed.stdout}\n"
        f"--- stderr ---\n{completed.stderr}\n"
    )
    raise click.ClickException(
        f"Failed to run example setup (exit code {completed.returncode}). The "
        f"setup script was left in place and the full output was written to "
        f"'{error_path}'."
    )


def _extract_snippet(ctx: click.Context, example: Example, target: Path) -> None:
    destination = target / example.path.name
    target.mkdir(parents=True, exist_ok=True)
    try:
        with (
            open(example.path, "rb") as source,
            _open_new(destination, "xb") as written,
        ):
            shutil.copyfileobj(source, written)
    except FileExistsError:
        raise TdkException(
            "conflict", f"Failed to extract example: {destination} already exists."
        ) from None
    render_done(
        ctx,
        action="extract",
        kind="example",
        id=example.name,
        details={"dir": str(target), "file": str(destination)},
        message=f"Example '{example.name}' written to '{destination}'",
    )


def _extract_project(ctx: click.Context, example: Example, target: Path) -> None:
    try:
        target.mkdir(parents=True)
    except FileExistsError:
        raise TdkException(
            "conflict", f"Failed to extract example: {target} already exists."
        ) from None
    _copy_flattened(example.path, target)
    _run_setup(target)
    render_done(
        ctx,
        action="extract",
        kind="example",
        id=example.name,
        details={"dir": str(target)},
        message=f"Example '{example.name}' generated in '{target}'",
    )
    show_hint(
        ctx,
        "Remember that in order to run the example, tdserver must be running in "
        "the same host as the CLI.",
    )


@capability("knowledge", "read", "write", paths={"--dir": "write"})
@examples.command()
@format_option
@click.option(
    "--name",
    "-n",
    required=True,
    help="Name of the example to extract. See 'tdk examples list'.",
)
@click.option(
    "--dir",
    "-d",
    "dir_",
    required=True,
    help=(
        "Directory to extract the example into. A snippet is written as "
        "'DIR/NAME.py' (the directory may exist); a project needs a directory "
        "that does not exist yet."
    ),
    type=click.Path(),
)
@click.pass_context
def extract(ctx: click.Context, name: str, dir_: str):
    """Extract an example into a directory"""
    example = find_example(examples_root(), name)
    if example.form == SNIPPET:
        _extract_snippet(ctx, example, Path(dir_))
    else:
        _extract_project(ctx, example, Path(dir_))


@capability("development", "read", paths={"--file": "write"})
@examples.command()
@format_option
@view_options()
@click.option(
    "--kind",
    type=click.Choice(FUNCTION_KINDS),
    help="Kind of function. Defaults to the kind the '--connector' binds.",
)
@click.option(
    "--connector",
    "connector_type",
    help=(
        "Connector type the publisher reads or the subscriber writes, e.g. "
        "'mysql-sql-in'. List them with '--list-connectors'."
    ),
)
@click.option("--name", "-n", help="Name of the Python function.")
@click.option(
    "--input",
    "inputs",
    multiple=True,
    help="Input table ('collection/table', or 'table' in its own collection); "
    "repeatable. Transformers and subscribers only.",
)
@click.option(
    "--output",
    "outputs",
    multiple=True,
    help="Output table; repeatable. Publishers and transformers only.",
)
@click.option(
    "--file",
    "-f",
    help="Path to write the module to; it must not exist. Printed if not given.",
    type=click.Path(),
)
@click.option(
    "--list-connectors",
    is_flag=True,
    help="List the connector types a function can bind to, and exit.",
)
@click.pass_context
def scaffold(
    ctx: click.Context,
    kind: str,
    connector_type: str,
    name: str,
    inputs: tuple[str, ...],
    outputs: tuple[str, ...],
    file: str,
    list_connectors: bool,
):
    """Write the skeleton of a publisher, transformer or subscriber.

    The source or destination is built from the installed connector, with
    '<value>' placeholders for the values it needs; the function never names
    a connection, since the collection's connection is used on the server.
    Needs no server
    """
    from tabsdatak.cli._scaffold import ScaffoldError, build_scaffold, connectors

    if list_connectors:
        rows = [row.brief() for row in connectors()]

        def _view():
            return TableView(
                title="Connectors",
                columns=[
                    Column("Type", Kind.ID),
                    Column("Kind"),
                    Column("System"),
                    Column("Class"),
                ],
                rows=[
                    [row["type"], row["kind"], row["system"], row["class"]]
                    for row in rows
                ],
                count="connectors",
            )

        render_list(ctx, items=rows, truncated=False, view=_view)
        return
    try:
        made = build_scaffold(kind, connector_type, name, list(inputs), list(outputs))
    except ScaffoldError as e:
        raise click.UsageError(str(e))
    if file:
        try:
            with _open_new(file, "x", encoding="utf-8") as written:
                written.write(made.source)
        except FileExistsError:
            raise TdkException("conflict", f"'{file}' already exists.") from None
        except FileNotFoundError:
            raise TdkException(
                "usage", f"The directory of '{file}' does not exist."
            ) from None
    bound = made.connector
    document = {
        "path": file,
        "source": made.source,
        "connector": (
            {"type": bound.type, "system": bound.system} if bound is not None else None
        ),
        "examples": _scaffold_examples(made),
    }

    if output_format(ctx) == "table":
        if file:
            click.echo(f"Function '{made.name}' written to '{file}'")
        else:
            click.echo(made.source, nl=False)
    render(ctx, data=document)
    show_hint(
        ctx,
        "Replace the '<value>' placeholders, then check the function with "
        f"'tdk fn check --path {file or 'FILE.py'}::{made.name}'.",
    )


def _scaffold_examples(made: Scaffold) -> list[str]:
    try:
        examples = load_examples(examples_root())
    except TdkException:
        return []
    connector_type = made.connector.type if made.connector is not None else None
    return [
        example.name
        for example in examples
        if matches(example.brief(), made.kind, connector_type, None)
    ]
