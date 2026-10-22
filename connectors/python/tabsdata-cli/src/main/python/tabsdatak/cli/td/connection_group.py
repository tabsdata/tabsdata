#
# Copyright 2026 Tabsdata Inc.
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

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    format_option,
    logical_prompt,
    render,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView


@cli_command
@click.group()
def connection():
    """Connection template commands"""


@connection.command()
@format_option
@view_options()
@click.pass_context
def types(ctx):
    """List the connection types available from installed connectors"""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    rows = template.list_connectors()

    def _view():
        return TableView(
            title="Available connectors",
            columns=[Column("Type", Kind.ID), Column("Mode"), Column("System")],
            rows=[[row.type, row.mode, row.system] for row in rows],
            count="connectors",
        )

    render(
        ctx,
        data=[{"type": r.type, "mode": r.mode, "system": r.system} for r in rows],
        view=_view,
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
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    type_ = type_ or logical_prompt(ctx, "Connector type (see 'tdk connection types')")
    try:
        # Types are globally unique, so the direction is always inferred.
        return template.resolve_conn(type_, None)
    except template.TemplateError as e:
        raise click.ClickException(str(e))


def _emit(document, file):
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    text = template.to_yaml(document)
    if file and file != "-":
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
# here -- ``-o/--format`` is reserved CLI-wide for that. Use ``--file``/``-f``
# for the output path, matching validate/table/exe.
@click.option(
    "--file",
    "-f",
    help="Write the template to this file. '-' (or omitting it) writes to stdout.",
)
@click.pass_context
def template_(ctx, type_, file):
    """Print a fill-in connection template for a connector type"""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    info = _resolve(ctx, type_)
    # Show every config: required fields as fill-ins, optional ones with their
    # default (default-None fields commented out so the document stays valid).
    _emit(template.build_template(info.conn_cls, all_fields=True), file)


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
    # noinspection PyProtectedMember
    from tabsdatak.cli import template
    from tabsdatak.common._error import CommonException
    from tabsdatak.common._json_serde import (
        env_secret_ref_vars,
        resolve_env_secret_refs,
    )

    raw = sys.stdin.read() if file == "-" else Path(file).read_text()
    try:
        document = template.from_yaml(raw)
    except Exception as e:  # noqa: BLE001 -- any YAML parse failure
        raise click.ClickException(f"not valid YAML: {e}")
    # Resolve any `$secret:<ENV_VAR>` references from the environment first,
    # exactly as connection create/upsert does, so a document that sources
    # secret values from the environment validates the same way it is created
    # (an unset referenced var is reported, not silently rejected as a bad
    # secret value).
    env_vars = env_secret_ref_vars(document)
    try:
        document = resolve_env_secret_refs(document)
    except CommonException as e:
        raise click.ClickException(str(e))
    if env_vars:
        click.echo(
            "Connection secrets resolved from environment variable(s): "
            + ", ".join(env_vars)
        )
    try:
        template.validate(document)
    except template.TemplateError as e:
        raise click.ClickException(str(e))
    click.echo("Connection document is valid")
