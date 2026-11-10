#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk docs`` -- reference pages for the installed ``tabsdatak`` SDK,
introspected from the live objects (no server call)."""

import rich_click as click
from click.core import ParameterSource

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._docs._catalog import (
    CATEGORIES,
    AmbiguousSymbol,
    UnknownSymbol,
    build_catalog,
)
from tabsdatak.cli._docs._render import markdown
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._machine import machine
from tabsdatak.cli._prompted import prompted
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    format_option,
    max_result_option,
    output_format,
    paged_options,
    render,
    render_list,
    take,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView

_category_option = click.option(
    "--category",
    type=click.Choice(CATEGORIES),
    help="Restrict to one category of the reference.",
)


@cli_command
@click.group(cls=TdkGroup)
def docs():
    """SDK reference: show a symbol's page, list and search the symbols"""


def _catalog(category: str | None):
    return build_catalog([category] if category else None)


def _brief_view(context, entries) -> TableView:
    return TableView(
        title="Symbols",
        context=context,
        columns=[
            Column("Symbol", Kind.ID),
            Column("Category"),
            Column("Kind"),
            Column("Summary"),
        ],
        rows=[
            [entry.symbol, entry.category, entry.kind, entry.summary]
            for entry in entries
        ],
        count="symbols",
    )


@capability("knowledge", "read")
@docs.command(login=False)
@prompted(
    "--name",
    "-n",
    "symbol",
    prompt="Symbol to show",
    help="Symbol whose page to show: a qualified or bare name, a 'Class.member', "
    "or a connection type id. Will be prompted for it if not provided.",
)
@_category_option
@format_option
@click.pass_context
def show(ctx: click.Context, symbol: str | None, category: str | None):
    """Show the reference page of a symbol"""
    try:
        entry = _catalog(category).resolve(symbol)
    except AmbiguousSymbol as error:
        candidates = [candidate.qualified_name for candidate in error.candidates]
        raise TdkException(
            "usage",
            f"'{symbol}' matches {len(candidates)} symbols: {', '.join(candidates)}.",
            hint="Pass the qualified name, or narrow with --category.",
        )
    except UnknownSymbol as error:
        suggestions = [s.symbol for s in error.suggestions]
        raise TdkException(
            "not_found",
            f"No documented symbol '{symbol}'.",
            hint=(
                f"Did you mean: {', '.join(suggestions)}?"
                if suggestions
                else f"Try 'tdk docs search --query {symbol}' or 'tdk docs list'."
            ),
        )
    if output_format(ctx) == "table":
        click.echo(markdown(entry), nl=False)
    render(ctx, data=entry.to_dict)


# Past the best hits, a program reading search results gains little.
MACHINE_SEARCH_RESULTS = 20


def _render_entries(ctx: click.Context, context, entries) -> None:
    shown, truncated = take(ctx, entries)
    render_list(
        ctx,
        items=lambda: [entry.brief() for entry in shown],
        truncated=truncated,
        view=lambda: _brief_view(context, shown),
    )


@capability("knowledge", "read")
@docs.command("list", login=False)
@_category_option
@max_result_option
@paged_options
@format_option
@view_options()
@click.pass_context
def list_(ctx: click.Context, category: str | None):
    """List the documented symbols"""
    context = [("Category", category)] if category else []
    _render_entries(ctx, context, _catalog(category).entries)


@capability("knowledge", "read")
@docs.command(login=False)
@prompted(
    "--query",
    "-q",
    prompt="Words to search for",
    help="Words to find in symbol names, qualified names and summaries. Will be "
    "prompted for it if not provided.",
)
@_category_option
@max_result_option
@paged_options
@format_option
@view_options()
@click.pass_context
def search(ctx: click.Context, query: str | None, category: str | None):
    """Search symbol names, qualified names and summaries, best first"""
    if (
        machine() is not None
        and ctx.get_parameter_source("max_result") is ParameterSource.DEFAULT
    ):
        ctx.obj["max_result"] = MACHINE_SEARCH_RESULTS
    context = [("Query", query)] + ([("Category", category)] if category else [])
    _render_entries(ctx, context, _catalog(category).search(query))
