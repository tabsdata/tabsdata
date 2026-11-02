#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _measures(**overrides) -> dict:
    measures = {
        "fn_runs": 0,
        "table_versions_out": 0,
        "row_count_out": 0,
        "data_bytes_out": 0,
        "table_versions_in": 0,
        "row_count_in": 0,
        "data_bytes_in": 0,
    }
    measures.update(overrides)
    return measures


def _node(kind: str, coll: str | None, grp: str | None, by_status: dict) -> dict:
    return {
        "project_id": "PID_A",
        "project_name": "proj-a",
        "kind": kind,
        "collection_id": f"CID_{coll}" if coll else None,
        "collection_name": coll,
        "group_id": f"GID_{grp}" if grp else None,
        "group_name": grp,
        "by_status": by_status,
    }


_METRICS = {
    "dim": "Collection",
    "start_ms": 1000,
    "end_ms": 2000,
    "nodes": [
        _node(
            "Dim",
            "sales",
            "sources",
            {
                "C": _measures(
                    fn_runs=3,
                    table_versions_out=6,
                    row_count_out=4321,
                    data_bytes_out=98765,
                ),
                "F": _measures(fn_runs=7),
            },
        ),
        _node(
            "Dim",
            "mart",
            "marts",
            {
                "C": _measures(
                    fn_runs=1,
                    table_versions_in=6,
                    row_count_in=1234,
                    data_bytes_in=555,
                )
            },
        ),
        _node(
            "ExternalSource",
            None,
            None,
            {"C": _measures(fn_runs=2, row_count_out=11)},
        ),
    ],
    "edges": [],
}


def _invoke(argv: list[str], data: dict, tmp_path, project: str | None = "proj-a"):
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = project
    client.metrics.return_value = data

    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(cli, argv, env={"COLUMNS": "250"})
    result.client = client
    return result


def _rows(output: str) -> list[list[str]]:
    rows = []
    for line in output.splitlines():
        cells = [c.strip() for c in line.strip().strip("│┃").split("│")]
        if len(cells) > 1:
            rows.append(cells)
    return rows


def test_show_renders_one_row_per_entity_and_status(tmp_path):
    result = _invoke(["metrics", "show"], _METRICS, tmp_path)
    assert result.exit_code == 0, result.output
    assert "Dim: collection" in result.output
    assert "Window: [1000, 2000)" in result.output
    assert "Txns" not in result.output
    assert _rows(result.output) == [
        ["mart", "marts", "C", "1", "6", "1234", "555", "0", "0", "0"],
        ["sales", "sources", "C", "3", "0", "0", "0", "6", "4321", "98765"],
        ["sales", "sources", "F", "7", "0", "0", "0", "0", "0", "0"],
        ["(external sources)", "-", "C", "2", "0", "0", "0", "0", "11", "0"],
    ]


def test_show_group_dim_has_no_collection_column(tmp_path):
    data = {
        **_METRICS,
        "dim": "Group",
        "nodes": [_node("Dim", None, "marts", {"D": _measures(fn_runs=9)})],
    }
    result = _invoke(["metrics", "show", "--dim", "group"], data, tmp_path)
    assert result.exit_code == 0, result.output
    assert "Collection" not in result.output
    assert _rows(result.output) == [
        ["marts", "D", "9", "0", "0", "0", "0"],
    ]


def test_show_without_activity(tmp_path):
    result = _invoke(["metrics", "show"], {**_METRICS, "nodes": []}, tmp_path)
    assert result.exit_code == 0, result.output
    assert "No activity in the window." in result.output


def test_show_json_is_the_server_response(tmp_path):
    result = _invoke(["metrics", "show", "-o", "json"], _METRICS, tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _METRICS


def _spanning_metrics() -> dict:
    other = _node("Dim", "sales", "sources", {"C": _measures(fn_runs=1)})
    other.update(project_id="PID_B", project_name="proj-b")
    return {**_METRICS, "nodes": [*_METRICS["nodes"], other]}


@pytest.mark.parametrize(
    ("argv", "project"),
    [(["metrics", "show"], None), (["metrics", "show", "--all-projects"], "proj-a")],
)
def test_show_spans_every_project_without_one_selected(tmp_path, argv, project):
    result = _invoke(argv, _spanning_metrics(), tmp_path, project=project)
    assert result.exit_code == 0, result.output
    assert result.client.connection.project is None
    assert "┃ Project" in result.output
    sales = [row for row in _rows(result.output) if "sales" in row]
    assert {row[0] for row in sales} == {"proj-a", "proj-b"}


def test_show_in_one_project_has_no_project_column(tmp_path):
    result = _invoke(["metrics", "show"], _METRICS, tmp_path)
    assert result.exit_code == 0, result.output
    assert result.client.connection.project == "proj-a"
    assert "┃ Project" not in result.output
