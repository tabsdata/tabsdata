#
# Copyright 2026 Tabs Data Inc.
#
"""Connection management commands for ``tdconn``.

Each function is a Click command registered on the main ``cli`` group.
Commands mirror the original argparse verbs (add, update, info, list)
with identical semantics but Click-native option handling.
"""

from __future__ import annotations

import contextlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Iterator

import rich_click as click

STDIN_SENTINEL = "-"


@contextlib.contextmanager
def _resolve_file_arg(raw: str) -> Iterator[Path]:
    """Yield a real ``Path`` for *raw*, materializing stdin if ``-``."""
    if raw != STDIN_SENTINEL:
        yield Path(raw)
        return
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        tmp.write(sys.stdin.read())
        tmp_path = Path(tmp.name)
    try:
        yield tmp_path
    finally:
        tmp_path.unlink(missing_ok=True)


@click.command()
@click.option(
    "-f",
    "--file",
    "conn_file",
    required=True,
    metavar="FILE",
    help="Path to the connection JSON file, or '-' to read from stdin.",
)
@click.pass_context
def add(ctx: click.Context, conn_file: str):
    """Add a new connection from a JSON file."""
    ux = ctx.obj["ux"]
    try:
        with _resolve_file_arg(conn_file) as path:
            ux.add(path)
    except (FileNotFoundError, ValueError, json.JSONDecodeError, OSError) as exc:
        raise click.ClickException(str(exc))


@click.command()
@click.option(
    "-n",
    "--name",
    "conn_name",
    required=True,
    metavar="CONN_NAME",
    help="Name of the connection to replace.",
)
@click.option(
    "-f",
    "--file",
    "conn_file",
    required=True,
    metavar="FILE",
    help="Path to the connection JSON file, or '-' to read from stdin.",
)
@click.pass_context
def update(ctx: click.Context, conn_name: str, conn_file: str):
    """Replace an existing connection from a JSON file."""
    ux = ctx.obj["ux"]
    try:
        with _resolve_file_arg(conn_file) as path:
            ux.update(conn_name, path)
    except (FileNotFoundError, ValueError, json.JSONDecodeError, OSError) as exc:
        raise click.ClickException(str(exc))


@click.command()
@click.option(
    "-n",
    "--name",
    "conn_name",
    required=True,
    metavar="CONN_NAME",
    help="Name of the connection to show.",
)
@click.option(
    "-V",
    "--verify",
    is_flag=True,
    help="Run a live connectivity check before rendering.",
)
@click.option(
    "-d",
    "--details",
    is_flag=True,
    help="Also print the full stored JSON below the identity table.",
)
@click.pass_context
def info(ctx: click.Context, conn_name: str, verify: bool, details: bool):
    """Show a connection's identity."""
    ux = ctx.obj["ux"]
    try:
        ux.info(conn_name, verify=verify, details=details)
    except (FileNotFoundError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc))


@click.command("list")
@click.option(
    "-V",
    "--verify",
    is_flag=True,
    help="Run a live connectivity check per connection and show the result.",
)
@click.pass_context
def list_cmd(ctx: click.Context, verify: bool):
    """List all connections in the store."""
    ux = ctx.obj["ux"]
    try:
        ux.list(verify=verify)
    except (FileNotFoundError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc))


# Commands to register on the CLI group. There is no `delete`: a connection
# is 1:1 with its collection and is deleted with it (`td collection delete`).
conn_commands = [add, update, info, list_cmd]
