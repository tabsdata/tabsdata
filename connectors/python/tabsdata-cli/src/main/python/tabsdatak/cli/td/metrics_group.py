#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    ancestry,
    format_option,
    hint_common_solutions,
    present,
    proj_option,
    render,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _split(value: str | None) -> list[str]:
    """Comma-separated list parser for repeatable filter options."""
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _summary_view(data: dict, scope: list) -> TableView:
    """Single condensed table: one row per
    `(dim_entity, group, status)`, magnitudes summed across nodes. Each node
    carries its measures per run status in `by_status`.
    """
    dim = (data.get("dim") or "").lower()
    start_ms = data.get("start_ms")
    end_ms = data.get("end_ms")
    context = [
        *scope,
        ("Dim", dim or "?"),
        ("Window", f"[{start_ms}, {end_ms})"),
    ]

    nodes = data.get("nodes") or []
    is_collection_dim = dim == "collection"

    agg: dict[tuple[str, str, str], dict] = {}
    for n in nodes:
        coll_id = (n.get("collection_id") or "") if is_collection_dim else ""
        coll_name = (n.get("collection_name") or "") if is_collection_dim else ""
        grp_id = n.get("group_id") or ""
        grp_name = n.get("group_name") or ""
        dim_key = coll_id if is_collection_dim else grp_id
        for status, measures in (n.get("by_status") or {}).items():
            row = agg.setdefault(
                (dim_key, grp_id, status),
                {
                    "collection_name": coll_name,
                    "group_name": grp_name,
                    "fn_runs": 0,
                    "tables_in": 0,
                    "rows_in": 0,
                    "bytes_in": 0,
                    "tables_out": 0,
                    "rows_out": 0,
                    "bytes_out": 0,
                },
            )
            row["fn_runs"] += int(measures.get("fn_runs") or 0)
            row["tables_in"] += int(measures.get("table_versions_in") or 0)
            row["rows_in"] += int(measures.get("row_count_in") or 0)
            row["bytes_in"] += int(measures.get("data_bytes_in") or 0)
            row["tables_out"] += int(measures.get("table_versions_out") or 0)
            row["rows_out"] += int(measures.get("row_count_out") or 0)
            row["bytes_out"] += int(measures.get("data_bytes_out") or 0)

    show_bytes = any(r["bytes_in"] or r["bytes_out"] for r in agg.values())

    if is_collection_dim:
        columns = [Column("Collection", Kind.ID), Column("Group", Kind.ID)]
    else:
        columns = [Column("Group", Kind.ID)]
    columns += [
        Column("Status", Kind.STATUS),
        Column("Runs", Kind.NUMBER),
        Column("Tables in", Kind.NUMBER),
        Column("Rows in", Kind.NUMBER),
    ]
    if show_bytes:
        columns.append(Column("Bytes in", Kind.NUMBER))
    columns += [Column("Tables out", Kind.NUMBER), Column("Rows out", Kind.NUMBER)]
    if show_bytes:
        columns.append(Column("Bytes out", Kind.NUMBER))

    rows = []
    # Stable order: by dim_id then status.
    for key in sorted(agg.keys()):
        dim_key, grp_id, status = key
        r = agg[key]
        cells: list[object] = []
        if is_collection_dim:
            cells.append(present(r["collection_name"] or dim_key))
            cells.append(present(r["group_name"] or grp_id))
        else:
            cells.append(present(r["group_name"] or dim_key))
        cells += [
            present(status),
            r["fn_runs"],
            r["tables_in"],
            r["rows_in"],
        ]
        if show_bytes:
            cells.append(r["bytes_in"])
        cells += [r["tables_out"], r["rows_out"]]
        if show_bytes:
            cells.append(r["bytes_out"])
        rows.append(cells)

    return TableView(
        title="Metrics",
        context=context,
        columns=columns,
        rows=rows,
        empty="No activity in the window.",
    )


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
@format_option
@view_options()
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

    Renders one row per `(dim_entity, status)` with `Runs /
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
        render(ctx, data=data, view=lambda: _summary_view(data, ancestry(ctx)))
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
@format_option
@view_options()
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

        def _view():
            return TableView(
                title="Inflight",
                context=ancestry(ctx),
                columns=[
                    Column("Collection", Kind.ID),
                    Column("Group", Kind.ID),
                    Column("Status", Kind.STATUS),
                    Column("Count", Kind.NUMBER),
                ],
                rows=[
                    [
                        r.get("collection_name") or str(r.get("collection_id") or ""),
                        r.get("group_name") or str(r.get("group_id") or ""),
                        str(r.get("status", "")),
                        r.get("count", 0),
                    ]
                    for r in rows
                ],
                empty="No inflight runs.",
            )

        render(ctx, data=data, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to fetch inflight: {e}")
