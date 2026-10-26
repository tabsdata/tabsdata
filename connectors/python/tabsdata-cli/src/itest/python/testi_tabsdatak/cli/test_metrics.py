#
# Copyright 2026 Tabsdata Inc.
#

"""`tdk metrics show` against a live instance, after a function ran."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from tabsdatax.itest.fixture.functions.harness import CollectionFactory, run_function
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.cli.td.cli import cli
from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.sdk.tabsdata_server import TabsdataServer
from testi_tabsdatak.conn.common.function_project import function_module

pytestmark = pytest.mark.cli

FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")

COMMITTED = "C"
BOX_EDGES = "│┃"

# A rolling hour ending now holds the run the test just made, whatever the
# time of day.
WINDOW = ["--unit", "hour", "--align", "rolling", "--span", "1"]


def tdk(server: TabsdataServer, argv: list[str], tmp_path: Path):
    def connect(ctx):
        ctx.obj["tabsdataserver"] = server

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", connect),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(cli, argv, env={"COLUMNS": "250"})
    assert result.exit_code == 0, result.output
    return result.stdout


def cells(line: str) -> list[str]:
    for edge in BOX_EDGES:
        line = line.replace(edge, "|")
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


@tabsdata_instance(consumer="tdtest")
def test_metrics_show_reports_the_run_of_a_function(
    instance_tdtest: Instance,
    server: TabsdataServer,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    collection = collection_factory("sources", DataGenSrcConn())
    run_function(collection, publisher.pub_metrics_publishes_three_rows, FUNCTIONS)
    show = ["metrics", "show", "--proj", server.connection.project, *WINDOW]

    payload = json.loads(tdk(server, [*show, "--format", "json"], tmp_path))
    nodes = [
        node
        for node in payload["nodes"]
        if node.get("collection_name") == collection.name
    ]
    assert len(nodes) == 1, payload
    measures = nodes[0]["by_status"][COMMITTED]
    assert measures["fn_runs"] == 1
    assert measures["row_count_out"] == len(publisher.PUBLISHED_FRAME)

    table = tdk(server, show, tmp_path).splitlines()
    header = cells(next(line for line in table if "Status" in line and "Runs" in line))
    rows = [dict(zip(header, cells(line))) for line in table if collection.name in line]
    committed = [row for row in rows if row["Status"] == COMMITTED]
    assert len(committed) == 1, "\n".join(table)
    assert committed[0]["Runs"] == str(measures["fn_runs"])
    assert committed[0]["Tables out"] == str(measures["table_versions_out"])
    assert committed[0]["Rows out"] == str(measures["row_count_out"])
    assert "Txns" not in header
