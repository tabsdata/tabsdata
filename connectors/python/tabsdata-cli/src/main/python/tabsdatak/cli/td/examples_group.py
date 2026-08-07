#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk examples`` -- discover and extract bundled example use cases.

Each example is a folder bundled under ``resources/examples`` (from the
``variant/resources`` tree) with the layout
``<name>/{input-data,connections,functions,_setup.py}``, resolved through
:func:`td_resource`. ``extract`` copies the folder into a target directory
(flattening every subfolder except the ``*-data`` ones), then runs the
example's ``_setup.py`` standing in that directory to finalize it.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import format_option, make_console, render, show_hint

EXAMPLES_RESOURCE = "tdresources/examples"
META_FILE = "_meta.yaml"
SETUP_FILE = "_setup.py"
ERROR_FILE = "error.txt"
DATA_DIR_SUFFIX = "-data"


@cli_command
@click.group()
def examples():
    """Example use case commands"""


def _examples_root() -> Path:
    # noinspection PyProtectedMember
    from tabsdatak.common._resources.reader import td_resource

    try:
        return td_resource(EXAMPLES_RESOURCE)
    except FileNotFoundError as error:
        raise click.ClickException(
            "Failed to locate examples: the examples resources are not "
            "available. Reinstalling the tabsdata package is strongly "
            f"recommended. ({error})"
        )


def _example_names(root: Path) -> list[str]:
    return sorted(
        entry.name
        for entry in root.iterdir()
        if entry.is_dir() and not entry.name.startswith((".", "_"))
    )


def _read_meta(example_dir: Path) -> dict:
    meta_path = example_dir / META_FILE
    if not meta_path.exists():
        return {}
    from ruamel.yaml import YAML

    try:
        data = YAML(typ="safe").load(meta_path.read_text())
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


@examples.command("list")
@format_option
@click.pass_context
def list_(ctx: click.Context):
    """List the available examples"""
    root = _examples_root()
    rows = [
        {
            "name": name,
            "description": str(_read_meta(root / name).get("description", "")).strip(),
        }
        for name in _example_names(root)
    ]

    def _table():
        table = Table(title="Available examples")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Description")
        for row in rows:
            table.add_row(row["name"], row["description"])
        click.echo()
        make_console().print(table)
        click.echo(f"Number of examples: {len(rows)}")
        click.echo()

    render(ctx, data=rows, table=_table)


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


@examples.command()
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
        "Directory to generate the example in. The directory must not exist "
        "beforehand, as it will be created by the CLI."
    ),
)
@click.pass_context
def extract(ctx: click.Context, name: str, dir_: str):
    """Extract an example into a new directory"""
    root = _examples_root()
    available = _example_names(root)
    if name not in available:
        raise click.ClickException(
            f"Failed to extract example: unknown example '{name}'. Available "
            f"examples: {', '.join(available) if available else '(none)'}."
        )

    target = Path(dir_)
    if target.exists():
        raise click.ClickException(f"Failed to extract example: {dir_} already exists.")

    target.mkdir(parents=True)
    _copy_flattened(root / name, target)
    _run_setup(target)

    click.echo(f"Example '{name}' generated in '{dir_}'")
    show_hint(
        ctx,
        "Remember that in order to run the example, tdserver must be running in "
        "the same host as the CLI.",
    )
