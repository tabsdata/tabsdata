#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    hint_common_solutions,
    make_console,
    output_option,
    proj_option,
    render,
    verify_login_or_prompt,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _split(value: str | None) -> list[str]:
    """Comma-separated list parser for repeatable filter options."""
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _render_summary(data: dict) -> None:
    """Single condensed table: one row per
    `(dim_entity, group, status)`, magnitudes summed across buckets.
    """
    dim = (data.get("dim") or "").lower()
    unit = data.get("unit", "?")
    start_ms = data.get("start_ms")
    end_ms = data.get("end_ms")
    click.echo(f"dim={dim or '?'}  unit={unit}  window=[{start_ms}, {end_ms})")

    nodes = data.get("nodes") or []
    if not nodes:
        click.echo("(no activity in window)")
        return

    is_collection_dim = dim == "collection"

    agg: dict[tuple[str, str, str], dict] = {}
    for n in nodes:
        coll_id = (n.get("collection_id") or "") if is_collection_dim else ""
        coll_name = (n.get("collection_name") or "") if is_collection_dim else ""
        grp_id = n.get("group_id") or ""
        grp_name = n.get("group_name") or ""
        status = n.get("status") or ""
        dim_key = coll_id if is_collection_dim else grp_id
        key = (dim_key, grp_id, status)
        row = agg.setdefault(
            key,
            {
                "collection_name": coll_name,
                "group_name": grp_name,
                "fn_runs": 0,
                "transactions": 0,
                "tables_in": 0,
                "rows_in": 0,
                "bytes_in": 0,
                "tables_out": 0,
                "rows_out": 0,
                "bytes_out": 0,
            },
        )
        row["fn_runs"] += int(n.get("fn_runs") or 0)
        row["transactions"] += int(n.get("transactions") or 0)
        row["tables_in"] += int(n.get("table_versions_in") or 0)
        row["rows_in"] += int(n.get("row_count_in") or 0)
        row["bytes_in"] += int(n.get("data_bytes_in") or 0)
        row["tables_out"] += int(n.get("table_versions_out") or 0)
        row["rows_out"] += int(n.get("row_count_out") or 0)
        row["bytes_out"] += int(n.get("data_bytes_out") or 0)

    show_bytes = any(r["bytes_in"] or r["bytes_out"] for r in agg.values())

    table = Table(title=None, show_lines=False)
    if is_collection_dim:
        table.add_column("Collection", style="cyan", no_wrap=True)
        table.add_column("Group", no_wrap=True)
    else:
        table.add_column("Group", style="cyan", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Runs", justify="right")
    table.add_column("Txns", justify="right")
    table.add_column("Tables in", justify="right")
    table.add_column("Rows in", justify="right")
    if show_bytes:
        table.add_column("Bytes in", justify="right")
    table.add_column("Tables out", justify="right")
    table.add_column("Rows out", justify="right")
    if show_bytes:
        table.add_column("Bytes out", justify="right")

    # Stable order: by dim_id then status.
    for key in sorted(agg.keys()):
        dim_key, grp_id, status = key
        r = agg[key]
        cells: list[str] = []
        if is_collection_dim:
            cells.append(r["collection_name"] or dim_key or "-")
            cells.append(r["group_name"] or grp_id)
        else:
            cells.append(r["group_name"] or dim_key)
        cells.extend([
            status,
            str(r["fn_runs"]),
            str(r["transactions"]),
            str(r["tables_in"]),
            str(r["rows_in"]),
        ])
        if show_bytes:
            cells.append(str(r["bytes_in"]))
        cells.extend([str(r["tables_out"]), str(r["rows_out"])])
        if show_bytes:
            cells.append(str(r["bytes_out"]))
        table.add_row(*cells)

    make_console().print(table)


@cli_command
@click.group()
def metrics():
    """Operational metrics over a window."""


@metrics.command()
@proj_option
@click.option(
    "--dim",
    type=click.Choice(["collection", "group"], case_sensitive=False),
    default=None,
    help="Breakout dimension. Defaults to `collection` server-side.",
)
@click.option(
    "--unit",
    type=click.Choice(
        ["hour", "day", "week", "month", "quarter", "year"], case_sensitive=False
    ),
    default=None,
    help="Window unit. Defaults to `day` server-side.",
)
@click.option(
    "--align",
    type=click.Choice(["calendar", "rolling"], case_sensitive=False),
    default=None,
    help="`calendar` (default) snaps the start of the window to the "
    "calendar-unit boundary containing `from` (week/month/quarter/"
    "year-to-date); `rolling` is a fixed `span*unit`-long window "
    "ending exactly at `from`.",
)
@click.option(
    "--from",
    "from_ms",
    type=int,
    default=None,
    help="Right edge of the window (UTC epoch ms, exclusive). "
    "Defaults to server-now. Pass a past instant to look at a past "
    "window.",
)
@click.option(
    "--span",
    type=int,
    default=None,
    help="Window width in `unit`s. Default 1.",
)
@click.option(
    "--collection",
    default=None,
    help="Comma-separated collection ids to include.",
)
@click.option(
    "--collection-exclude",
    default=None,
    help="Comma-separated collection ids to exclude.",
)
@click.option(
    "--group",
    default=None,
    help="Comma-separated group ids to include.",
)
@click.option(
    "--group-exclude",
    default=None,
    help="Comma-separated group ids to exclude.",
)
@click.option(
    "--status",
    default=None,
    help="Comma-separated function run statuses to include (e.g. `C,D,E`).",
)
@output_option
@click.pass_context
def show(
    ctx: click.Context,
    dim: str,
    unit: str,
    align: str,
    from_ms: int,
    span: int,
    collection: str,
    collection_exclude: str,
    group: str,
    group_exclude: str,
    status: str,
):
    """Per-collection (or per-group) operational summary over the window.

    Renders one row per `(dim_entity, status)` with `Runs / Txns /
    Tables in / Rows in / Tables out / Rows out` (bytes columns added
    when any non-zero). Edges are dropped from the rendering — they
    belong in a flow visualization; use `-o json` for the raw shape.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        data = server.metrics(
            dim=dim.lower() if dim else None,
            unit=unit.lower() if unit else None,
            align=align.lower() if align else None,
            from_ms=from_ms,
            span=span,
            collection=_split(collection) or None,
            collection_exclude=_split(collection_exclude) or None,
            group=_split(group) or None,
            group_exclude=_split(group_exclude) or None,
            status=_split(status) or None,
        )
        render(ctx, data=data, table=lambda: _render_summary(data))
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to fetch metrics: {e}")


@metrics.command()
@proj_option
@click.option(
    "--collection",
    default=None,
    help="Comma-separated collection ids to include.",
)
@click.option(
    "--collection-exclude",
    default=None,
    help="Comma-separated collection ids to exclude.",
)
@click.option(
    "--group",
    default=None,
    help="Comma-separated group ids to include.",
)
@click.option(
    "--group-exclude",
    default=None,
    help="Comma-separated group ids to exclude.",
)
@click.option(
    "--status",
    default=None,
    help=(
        "Comma-separated non-terminal function run statuses to include (e.g. `S,R,H`)."
    ),
)
@output_option
@click.pass_context
def inflight(
    ctx: click.Context,
    collection: str,
    collection_exclude: str,
    group: str,
    group_exclude: str,
    status: str,
):
    """Point-in-time `/inflight` — non-terminal LFRs per
    (collection, group, status).
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        data = server.inflight(
            collection=_split(collection) or None,
            collection_exclude=_split(collection_exclude) or None,
            group=_split(group) or None,
            group_exclude=_split(group_exclude) or None,
            status=_split(status) or None,
        )
        rows = data.get("rows") or []

        def _table():
            if not rows:
                click.echo("(no inflight runs)")
                return
            table = Table(title="Inflight")
            table.add_column("Collection", style="cyan", no_wrap=True)
            table.add_column("Group", no_wrap=True)
            table.add_column("Status", no_wrap=True)
            table.add_column("Count", justify="right")
            for r in rows:
                cid = str(r.get("collection_id") or "")
                gid = str(r.get("group_id") or "")
                cname = r.get("collection_name") or cid
                gname = r.get("group_name") or gid
                table.add_row(
                    cname,
                    gname,
                    str(r.get("status", "")),
                    str(r.get("count", 0)),
                )
            make_console().print(table)

        render(ctx, data=data, table=_table)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to fetch inflight: {e}")
