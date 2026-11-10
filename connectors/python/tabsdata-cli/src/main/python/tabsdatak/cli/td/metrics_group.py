#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    all_projects_option,
    ancestry,
    format_option,
    hint_common_solutions,
    present,
    proj_option,
    render,
    resolve_all_projects,
    split_csv,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


DEFAULT_UNIT = "day"
EMPTY_CALENDAR_HINT = (
    "With '--align calendar' the window starts at the boundary that holds "
    "'--from', so a '--from' on a boundary leaves it empty; for the '--span' "
    "units that end at '--from', pass '--align rolling'."
)

EXTERNAL_LABELS = {
    "ExternalSource": "(external sources)",
    "ExternalDestination": "(external destinations)",
}


def _summary_view(data: dict, scope: list, unit: str, spanning: bool) -> TableView:
    """Single condensed table: one row per
    `(project, dim_entity, group, status)`, magnitudes summed across buckets.
    """
    dim = (data.get("dim") or "").lower()
    start_ms = data.get("start_ms")
    end_ms = data.get("end_ms")
    context = [
        *scope,
        ("Dim", dim or "?"),
        ("Unit", unit),
        ("Window", f"[{start_ms}, {end_ms})"),
    ]

    nodes = data.get("nodes") or []
    is_collection_dim = dim == "collection"

    agg: dict[tuple[str, str, str, str], dict] = {}
    for n in nodes:
        project = (n.get("project_name") or "") if spanning else ""
        coll_id = (n.get("collection_id") or "") if is_collection_dim else ""
        coll_name = (n.get("collection_name") or "") if is_collection_dim else ""
        grp_id = n.get("group_id") or ""
        grp_name = n.get("group_name") or ""
        kind = n.get("kind") or ""
        if kind != "Dim":
            label = EXTERNAL_LABELS.get(kind, kind)
            dim_key, coll_name, grp_name = (
                kind,
                label,
                "" if is_collection_dim else label,
            )
        else:
            dim_key = coll_id if is_collection_dim else grp_id
        for status, measures in (n.get("by_status") or {}).items():
            row = agg.setdefault(
                (project, dim_key, grp_id, status),
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

    columns = [Column("Project", Kind.ID)] if spanning else []
    if is_collection_dim:
        columns += [Column("Collection", Kind.ID), Column("Group", Kind.ID)]
    else:
        columns += [Column("Group", Kind.ID)]
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
        project, dim_key, grp_id, status = key
        r = agg[key]
        cells: list[object] = [present(project)] if spanning else []
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
@click.group(cls=TdkGroup)
def metrics():
    """Operational metrics over a window."""


@capability("analytics", "read")
@metrics.command()
@proj_option
@all_projects_option
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
    help=f"Window unit. Defaults to `{DEFAULT_UNIT}` server-side.",
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
    all_projects: bool,
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

    Renders one row per `(dim_entity, status)` with `Runs / Tables in /
    Rows in / Tables out / Rows out` (bytes columns added
    when any non-zero). Edges are dropped from the rendering — they
    belong in a flow visualization; use `-o json` for the raw shape.
    """
    try:
        spanning = resolve_all_projects(ctx, all_projects)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        data = server.metrics(
            dim=dim.lower() if dim else None,
            unit=unit.lower() if unit else None,
            align=align.lower() if align else None,
            from_ms=from_ms,
            span=span,
            collection=split_csv(collection) or None,
            collection_exclude=split_csv(collection_exclude) or None,
            group=split_csv(group) or None,
            group_exclude=split_csv(group_exclude) or None,
            status=split_csv(status) or None,
        )
        render(
            ctx,
            data=data,
            view=lambda: _summary_view(
                data, ancestry(ctx), (unit or DEFAULT_UNIT).lower(), spanning
            ),
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        if _empty_calendar_window(e, align, from_ms):
            raise TdkException(
                "invalid_request",
                f"Failed to fetch metrics: {e}",
                hint=EMPTY_CALENDAR_HINT,
            ) from e
        raise click.ClickException(f"Failed to fetch metrics: {e}")


def _empty_calendar_window(
    e: Exception, align: str | None, from_ms: int | None
) -> bool:
    from tabsdatak.sdk._apiserver import APIServerError

    return (
        isinstance(e, APIServerError)
        and (e.code or "").startswith("TimeWindowError")
        and from_ms is not None
        and (align or "calendar").lower() == "calendar"
    )


@capability("analytics", "read")
@metrics.command()
@proj_option
@all_projects_option
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
    all_projects: bool,
    collection: str,
    collection_exclude: str,
    group: str,
    group_exclude: str,
    status: str,
):
    """Point-in-time `/inflight` — non-terminal LFRs per
    (collection, group, status).
    """
    try:
        spanning = resolve_all_projects(ctx, all_projects)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        data = server.inflight(
            collection=split_csv(collection) or None,
            collection_exclude=split_csv(collection_exclude) or None,
            group=split_csv(group) or None,
            group_exclude=split_csv(group_exclude) or None,
            status=split_csv(status) or None,
        )
        rows = data.get("rows") or []

        def _view():
            return TableView(
                title="Inflight",
                context=ancestry(ctx),
                columns=[
                    *([Column("Project", Kind.ID)] if spanning else []),
                    Column("Collection", Kind.ID),
                    Column("Group", Kind.ID),
                    Column("Status", Kind.STATUS),
                    Column("Count", Kind.NUMBER),
                ],
                rows=[
                    [
                        *([present(r.get("project_name"))] if spanning else []),
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
