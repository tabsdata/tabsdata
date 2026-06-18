#
# Copyright 2026 Tabs Data Inc.
#

"""``tdk connection`` -- discover connectors and build connection documents.

These commands are offline: they introspect the connectors registered through
the SPI registry (``avail_srcs`` / ``avail_stream_srcs`` / ``avail_dests``) and
emit the connectors' own envelope dialect (``{kind, apiVersion, type, spec}``).
They do not talk to a server.
"""

import json
import sys
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.table import Table

# noinspection PyProtectedMember
from tabsdatak import _template as template
from tabsdatak.cli.cli_utils import logical_prompt


@click.group()
def connection():
    """Connection template commands"""


@connection.command()
def types():
    """List the connection types available from installed connectors"""
    rows = template.list_connectors()

    table = Table(title="Available connectors")
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Direction")
    table.add_column("Version")
    table.add_column("Kind")

    for row in rows:
        table.add_row(row.type, row.direction, row.version, row.kind)

    click.echo()
    Console().print(table)
    click.echo(f"Number of connectors: {len(rows)}")
    click.echo()


def _direction_option(fn):
    return click.option(
        "--direction",
        "-d",
        type=click.Choice(["src", "stream-src", "dest"]),
        help="Connector direction; inferred when the type is unambiguous.",
    )(fn)


def _type_option(fn):
    return click.option(
        "--type",
        "-t",
        "type_",
        help="Connector type, e.g. 'postgres', 's3'. See 'tdk connection types'.",
    )(fn)


def _resolve(ctx, type_, direction):
    type_ = type_ or logical_prompt(ctx, "Connector type (see 'tdk connection types')")
    try:
        return template.resolve_conn(type_, direction)
    except template.TemplateError as e:
        raise click.ClickException(str(e))


def _emit(document: dict, output):
    text = json.dumps(document, indent=2)
    if output:
        Path(output).write_text(text + "\n")
        click.echo(f"Wrote connection document to {output}")
    else:
        click.echo(text)


# Named "template" explicitly: the function can't be called ``template`` without
# shadowing the imported engine module.
@connection.command("template")
@_type_option
@_direction_option
@click.option(
    "--all-fields",
    is_flag=True,
    help="Include optional fields (omitted by default).",
)
@click.option(
    "--output", "-o", help="Write the template to this file instead of stdout."
)
@click.pass_context
def template_(ctx, type_, direction, all_fields, output):
    """Print a fill-in connection template for a connector type"""
    info = _resolve(ctx, type_, direction)
    _emit(template.build_template(info.conn_cls, all_fields=all_fields), output)


@connection.command()
@_type_option
@_direction_option
@click.option(
    "--output",
    "-o",
    help="Write the completed connection document to this file instead of stdout.",
)
@click.pass_context
def init(ctx, type_, direction, output):
    """Interactively build a connection document for a connector type"""
    info = _resolve(ctx, type_, direction)

    def prompt(path, *, secret, optional, default, pattern, choices):
        if choices:
            label = f"{path} -- choose one of: {', '.join(choices)}"
            return logical_prompt(ctx, label, default_value=choices[0])
        if secret:
            label = f"{path} (secret; blank = resolve from the secret store at runtime)"
            value = logical_prompt(ctx, label, default_value="", hide_input=True)
            return value or None
        label = path
        if pattern:
            label += f" (pattern: {pattern})"
        if optional:
            label += " (optional, blank to skip)"
        value = logical_prompt(
            ctx, label, default_value="" if optional else default, hide_input=False
        )
        return None if (optional and not value) else value

    document = template.build_template_interactive(info.conn_cls, prompt)
    try:
        template.validate(document)
    except template.TemplateError as e:
        raise click.ClickException(str(e))
    click.echo()
    _emit(document, output)


@connection.command()
@click.option(
    "--file",
    "-f",
    "file",
    required=True,
    help="Connection JSON file to validate, or '-' to read from stdin.",
)
def validate(file):
    """Validate a connection JSON document against its connector schema"""
    raw = sys.stdin.read() if file == "-" else Path(file).read_text()
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"not valid JSON: {e}")
    try:
        kind = template.validate(document)
    except template.TemplateError as e:
        raise click.ClickException(str(e))
    click.echo(f"Connection document is valid ({kind}).")
