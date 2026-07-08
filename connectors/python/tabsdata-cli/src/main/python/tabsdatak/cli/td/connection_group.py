#
# Copyright 2026 Tabs Data Inc.
#

"""``tdk connection`` -- discover connectors and build connection documents.

These commands are offline: they introspect the connectors registered through
the SPI registry (``avail_srcs`` / ``avail_stream_srcs`` / ``avail_dests``) and
emit the connectors' own envelope dialect (``{kind, apiVersion, type, spec}``).
They do not talk to a server.
"""

import sys
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.table import Table

# noinspection PyProtectedMember
from tabsdatak import _template as template
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import logical_prompt, output_option, render


@cli_command
@click.group()
def connection():
    """Connection template commands"""


@connection.command()
@output_option
@click.pass_context
def types(ctx):
    """List the connection types available from installed connectors"""
    rows = template.list_connectors()

    def _table():
        table = Table(title="Available connectors")
        table.add_column("Type", style="cyan", no_wrap=True)
        table.add_column("Mode")
        table.add_column("System")
        for row in rows:
            table.add_row(row.type, row.mode, row.system)
        click.echo()
        Console().print(table)
        click.echo(f"Number of connectors: {len(rows)}")
        click.echo()

    render(
        ctx,
        data=[{"type": r.type, "mode": r.mode, "system": r.system} for r in rows],
        table=_table,
    )


def _type_option(fn):
    return click.option(
        "--type",
        "-t",
        "type_",
        help="Connector type, e.g. 'mysql-sql-in', 's3-file-out'. "
        "See 'tdk connection types'.",
    )(fn)


def _resolve(ctx, type_):
    type_ = type_ or logical_prompt(ctx, "Connector type (see 'tdk connection types')")
    try:
        # Types are globally unique, so the direction is always inferred.
        return template.resolve_conn(type_, None)
    except template.TemplateError as e:
        raise click.ClickException(str(e))


def _emit(document, file):
    text = template.to_yaml(document)
    if file:
        Path(file).write_text(text)
        click.echo(f"Wrote connection document to {file}")
    else:
        click.echo(text, nl=False)


# Named "template" explicitly: the function can't be called ``template`` without
# shadowing the imported engine module.
@connection.command("template")
@_type_option
# The template is always a commented YAML document (the "# accepts:" hints are
# the point, and JSON can't carry them), so there is no output *format* to pick
# here -- ``-o/--output`` is reserved CLI-wide for that. Use ``--file``/``-f``
# for the output path, matching validate/table/exe.
@click.option("--file", "-f", help="Write the template to this file instead of stdout.")
@click.pass_context
def template_(ctx, type_, file):
    """Print a fill-in connection template for a connector type"""
    info = _resolve(ctx, type_)
    _emit(template.build_template(info.conn_cls), file)


@connection.command()
@click.option(
    "--file",
    "-f",
    "file",
    required=True,
    help="Connection YAML file to validate, or '-' to read from stdin.",
)
def validate(file):
    """Validate a connection YAML document against its connector schema"""
    raw = sys.stdin.read() if file == "-" else Path(file).read_text()
    try:
        document = template.from_yaml(raw)
    except Exception as e:  # noqa: BLE001 -- any YAML parse failure
        raise click.ClickException(f"not valid YAML: {e}")
    try:
        template.validate(document)
    except template.TemplateError as e:
        raise click.ClickException(str(e))
    click.echo("Connection document is valid")
