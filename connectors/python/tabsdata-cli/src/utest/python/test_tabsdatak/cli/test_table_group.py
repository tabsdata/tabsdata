#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk._apiserver import APIServerError, NotFound
from tabsdatak.sdk.tabsdata_server import Table, TabsdataServer


class _Listing:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.position = None

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._rows)


def _table(name: str, collection: str) -> MagicMock:
    table = MagicMock()
    table.name = name
    table.function.name = f"fn_{name}"
    row = {"name": name, "collection": collection, "status": "A"}
    table._data = row
    table.kwargs = row
    return table


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "proj-a"
    return client


def _invoke(argv: list[str], client: MagicMock, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env={"COLUMNS": "250"})


def test_list_without_a_collection_lists_the_project_with_a_collection_column(tmp_path):
    client = _client()
    client.list_project_tables_generator.return_value = _Listing([
        _table("orders", "sales"),
        _table("customers", "crm"),
    ])
    result = _invoke(["table", "list", "--name", "*or*"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert "Collection" in result.output
    assert "crm" in result.output and "sales" in result.output
    assert "Number of tables: 2" in result.output
    kwargs = client.list_project_tables_generator.call_args.kwargs
    assert kwargs["filter"] == ["name:lk:*or*"]
    client.list_tables_generator.assert_not_called()


def test_list_narrows_to_the_pinned_collection(tmp_path):
    (tmp_path / "options.json").write_text(
        json.dumps({"pins": {"collection": "sales"}})
    )
    client = _client()
    client.list_tables_generator.return_value = _Listing([_table("orders", "sales")])
    result = _invoke(["table", "list"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert client.list_tables_generator.call_args.args == ("sales",)
    client.list_project_tables_generator.assert_not_called()


_STATS = {
    "collection": "sales",
    "table": "orders",
    "version": {"id": "V3", "ended_on": 3000, "function": "pub"},
    "row_count": 12,
    "column_count": 1,
    "data_bytes": 1048576,
    "schema": [{"name": "id", "type": "i64"}],
}


def test_stats_emits_the_sdk_document(tmp_path):
    with patch.object(Table, "stats", return_value=_STATS) as stats:
        result = _invoke(
            ["table", "stats", "-c", "sales", "-n", "orders", "--version", "HEAD^"]
            + ["-o", "json"],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _STATS
    assert stats.call_args.kwargs == {"version": "HEAD^", "at": None, "at_trx": None}


def test_stats_table_view(tmp_path):
    with patch.object(Table, "stats", return_value=_STATS):
        result = _invoke(
            ["table", "stats", "-c", "sales", "-n", "orders"], _client(), tmp_path
        )
    assert result.exit_code == 0, result.output
    for value in ("V3", "pub", "12", "1.00", "id", "i64"):
        assert value in result.output


def test_stats_pins_are_mutually_exclusive(tmp_path):
    result = _invoke(
        [
            "table",
            "stats",
            "-c",
            "s",
            "-n",
            "t",
            "--version",
            "V",
            "--at-trx",
            "tx20260930-1",
        ],
        _client(),
        tmp_path,
    )
    assert result.exit_code == 2, result.output


_PROFILE = {
    "collection": "sales",
    "table": "orders",
    "version_id": "V3",
    "row_count": 5000,
    "approximate": True,
    "sample_size": 1000,
    "reason": "scan_cap",
    "columns": [
        {
            "name": "amount",
            "type": "f64",
            "non_null": 990,
            "null_rate": 0.01,
            "distinct": 42,
            "min": 0.5,
            "max": 9.5,
            "mean": 3.25,
        }
    ],
}


def test_profile_passes_the_options_and_renders(tmp_path):
    with patch.object(Table, "profile", return_value=_PROFILE) as profile:
        result = _invoke(
            [
                "table",
                "profile",
                "-c",
                "sales",
                "-n",
                "orders",
                "--columns",
                "amount, id",
            ]
            + ["--approx", "--sample-rows", "2000"],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 0, result.output
    assert profile.call_args.kwargs == {
        "columns": ["amount", "id"],
        "version": None,
        "at": None,
        "at_trx": None,
        "approx": True,
        "sample_rows": 2000,
    }
    for value in ("amount", "990", "1.00%", "42", "9.5", "3.25", "scan_cap", "1000"):
        assert value in result.output


def test_profile_of_an_unknown_column_is_a_usage_error(tmp_path):
    from tabsdatak.sdk._tables import UnknownColumn

    with patch.object(
        Table, "profile", side_effect=UnknownColumn("Table 'orders' has no column 'x'.")
    ):
        result = _invoke(
            ["table", "profile", "-c", "sales", "-n", "orders", "--columns", "x"]
            + ["--machine"],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 2, result.output
    assert json.loads(result.stdout)["error"]["type"] == "usage"


def test_profile_caps_the_sample(tmp_path):
    result = _invoke(
        ["table", "profile", "-c", "s", "-n", "t", "--sample-rows", "10001"],
        _client(),
        tmp_path,
    )
    assert result.exit_code == 2


_FLOW = {
    "focus": {"collection": "sales", "table": "orders", "table_version_id": "T1"},
    "direction": "both",
    "depth": None,
    "nodes": [
        {
            "id": "t:T0",
            "kind": "table",
            "collection": "raw",
            "name": "r",
            "system": False,
        },
        {"id": "f:F1", "kind": "function", "collection": "sales", "name": "clean"},
        {"id": "t:T1", "kind": "table", "collection": "sales", "name": "orders"},
    ],
    "edges": [
        {"from": "t:T0", "to": "f:F1", "kind": "dependency", "trigger": True},
        {"from": "f:F1", "to": "t:T1", "kind": "output"},
    ],
    "upstream": [{"collection": "raw", "table": "r", "distance": 2}],
    "downstream": [],
}


def test_flow_passes_the_options_and_renders(tmp_path):
    with patch.object(Table, "flow", return_value=_FLOW) as flow:
        result = _invoke(
            ["table", "flow", "-c", "sales", "-n", "orders", "--direction", "up"]
            + ["--depth", "2", "--include-system"],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 0, result.output
    assert flow.call_args.kwargs == {
        "direction": "up",
        "depth": 2,
        "at": None,
        "topology_version": None,
        "include_system": True,
    }
    assert "dependency, trigger" in result.output
    assert "raw/r (2)" in result.output
    assert "Downstream tables: -" in result.output


def test_flow_reads_a_topology_version(tmp_path):
    version = "0" * 26
    with patch.object(Table, "flow", return_value=_FLOW) as flow:
        result = _invoke(
            ["table", "flow", "-c", "sales", "-n", "orders"]
            + ["--topology-version", version],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 0, result.output
    assert flow.call_args.kwargs["topology_version"] == version
    assert flow.call_args.kwargs["at"] is None


@pytest.mark.parametrize(
    "pin",
    [
        ["--topology-version", "0" * 26, "--at", "2026-01-01"],
        ["--topology-version", "not-an-id"],
    ],
)
def test_flow_rejects_a_bad_topology_pin(pin, tmp_path):
    with patch.object(Table, "flow", return_value=_FLOW) as flow:
        result = _invoke(
            ["table", "flow", "-c", "sales", "-n", "orders", *pin],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 2, result.output
    flow.assert_not_called()


def test_flow_json_is_the_sdk_document(tmp_path):
    with patch.object(Table, "flow", return_value=_FLOW):
        result = _invoke(
            ["table", "flow", "-c", "sales", "-n", "orders", "-o", "json"],
            _client(),
            tmp_path,
        )
    assert json.loads(result.output) == _FLOW


_HISTORY = {
    "collection": "sales",
    "table": "orders",
    "current_schema_hash": "h2",
    "capped": True,
    "events": [
        {
            "version_id": "V9",
            "created_at": 1,
            "ended_on": 2,
            "function": "pub",
            "execution_id": "E",
            "added": [{"name": "discount", "type": "f64"}],
            "removed": [],
            "retyped": [{"name": "id", "from": "i32", "to": "i64"}],
        }
    ],
}


def test_diff_without_versions_lists_the_history(tmp_path):
    with patch.object(Table, "schema_history", return_value=_HISTORY) as history:
        result = _invoke(
            ["table", "diff", "-c", "sales", "-n", "orders", "--max-events", "1"],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 0, result.output
    assert history.call_args.kwargs == {"max_events": 1}
    for value in ("V9", "discount (f64)", "id (i32 -> i64)", "--max-events"):
        assert value in result.output


def test_diff_between_versions_and_against_another_table(tmp_path):
    document = {
        "from": {"collection": "sales", "table": "orders", "version_id": "V1"},
        "to": {"collection": "bi", "table": "copy", "version_id": "V7"},
        "added": [],
        "removed": [{"name": "x", "type": "str"}],
        "retyped": [],
    }
    with patch.object(Table, "schema_diff", return_value=document) as diff:
        result = _invoke(
            ["table", "diff", "-c", "sales", "-n", "orders", "--from", "V1"]
            + ["--to", "HEAD", "-o", "json"],
            _client(),
            tmp_path,
        )
        assert json.loads(result.output) == document
        assert diff.call_args.kwargs == {"from_version": "V1", "to_version": "HEAD"}
        result = _invoke(
            ["table", "diff", "-c", "sales", "-n", "orders", "--against", "bi/copy"]
            + ["--version", "V1"],
            _client(),
            tmp_path,
        )
    assert result.exit_code == 0, result.output
    [other] = diff.call_args.args
    assert (other.collection.name, other.name) == ("bi", "copy")
    assert diff.call_args.kwargs == {"from_version": "V1"}
    assert "x (str)" in result.output


def test_diff_rejects_inconsistent_options(tmp_path):
    for extra in (
        ["--to", "V2"],
        ["--version", "V2"],
        ["--from", "V", "--against", "t"],
    ):
        result = _invoke(
            ["table", "diff", "-c", "s", "-n", "t", *extra], _client(), tmp_path
        )
        assert result.exit_code == 2, (extra, result.output)


def test_list_passes_its_point_in_time(tmp_path):
    client = _client()
    client.list_tables_generator.return_value = _Listing([])
    result = _invoke(
        ["table", "list", "-c", "s", "--at", "2026-01-01"], client, tmp_path
    )
    assert result.exit_code == 0, result.output
    kwargs = client.list_tables_generator.call_args.kwargs
    assert (kwargs["at"], kwargs["at_trx"]) == ("2026-01-01", None)


def test_a_point_in_time_listing_needs_a_collection(tmp_path):
    for option, value in (("--at", "2026-01-01"), ("--at-trx", "tx20260101-1")):
        result = _invoke(["table", "list", option, value], _client(), tmp_path)
        assert result.exit_code == 2, result.output
        assert f"'{option}' lists one collection; pass '--coll'." in result.output


@pytest.mark.parametrize("command", ["data", "schema", "download", "stats"])
@pytest.mark.parametrize(
    "pins",
    [
        ["--at", "2026-01-01", "--at-trx", "tx20260930-1"],
        ["--at-trx", "tx20260930-1", "--version", "V"],
        ["--at", "2026-01-01", "--version", "V"],
    ],
)
def test_point_in_time_pins_are_mutually_exclusive(tmp_path, command, pins):
    client = _client()
    extra = ["--file", str(tmp_path / "t.parquet")] if command == "download" else []
    result = _invoke(
        ["table", command, "--coll", "s", "--name", "t", *extra, *pins],
        client,
        tmp_path,
    )
    assert result.exit_code == 2, result.output
    assert "mutually exclusive" in result.output
    assert not client.method_calls


@pytest.mark.parametrize(
    "argv",
    [
        ["table", "data", "--coll", "s", "--name", "t"],
        ["table", "schema", "--coll", "s", "--name", "t"],
        ["table", "download", "--coll", "s", "--name", "t", "--file", "t.parquet"],
        ["table", "stats", "--coll", "s", "--name", "t"],
        ["table", "profile", "--coll", "s", "--name", "t"],
    ],
)
def test_at_trx_takes_a_transaction_id(tmp_path, argv):
    client = _client()
    result = _invoke([*argv, "--at-trx", "last", "--machine"], client, tmp_path)
    assert result.exit_code == 2, result.output
    error = json.loads(result.stdout)["error"]
    assert error["message"] == "'--at-trx last' is not an id."
    assert "tdk trx list" in error["hint"]
    assert client.mock_calls == []
    assert client.connection.mock_calls == []


def _empty_page() -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"data": {"data": []}}
    return response


@pytest.mark.parametrize(
    "argv",
    [
        ["table", "stats", "-c", "s", "-n", "t", "--version", "DV_GONE"],
        ["table", "profile", "-c", "s", "-n", "t"],
        ["table", "diff", "-c", "s", "-n", "t", "--from", "DV_GONE"],
    ],
)
def test_missing_versions_are_not_found(tmp_path, argv):
    client = _client()
    client.connection.dataversion_list.return_value = _empty_page()
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == 5, result.output
    assert json.loads(result.stdout)["error"]["type"] == "not_found"


def test_missing_table_is_not_found(tmp_path):
    client = _client()
    client.get_table_schema.side_effect = NotFound("Table t not found.")
    result = _invoke(
        ["table", "schema", "--coll", "s", "--name", "t", "--machine"], client, tmp_path
    )
    assert result.exit_code == 5, result.output
    assert json.loads(result.stdout)["error"]["type"] == "not_found"


def test_download_is_capped_by_the_room_in_the_target_directory(tmp_path, monkeypatch):
    from tabsdatak.cli import _space
    from tabsdatak.sdk.tabsdata_server import DownloadTooLarge

    usage = SimpleNamespace(free=10 + _space.MARGIN_BYTES)
    monkeypatch.setattr(_space.shutil, "disk_usage", lambda path: usage)
    monkeypatch.setattr(_space, "_per_file", lambda: None)
    target = tmp_path / "t.parquet"
    target.write_bytes(b"kept")
    client = _client()
    client.download_table.side_effect = DownloadTooLarge("t", 10)
    result = _invoke(
        ["table", "download", "-c", "s", "--name", "t", "--file", str(target)]
        + ["--machine"],
        client,
        tmp_path,
    )
    assert result.exit_code == 1, result.output
    error = json.loads(result.stdout)["error"]
    assert error["type"] == "invalid_request"
    assert "larger than the 10 bytes" in error["message"]
    assert client.download_table.call_args.kwargs["max_bytes"] == 10
    assert target.read_bytes() == b"kept"


def test_sql_naming_another_relation_names_the_table_in_its_hint(tmp_path):
    client = _client()
    client.sample_table.side_effect = APIServerError(
        {
            "code": "SampleError::0000",
            "error": "invalid_request",
            "error_description": "SQL Error: relation 't' was not found",
        },
        status=400,
    )
    argv = ["table", "data", "-c", "s", "-n", "orders", "--sql", "SELECT 1 FROM t"]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == 1, result.output
    error = json.loads(result.stdout)["error"]
    assert error["status"] == 400 and error["code"] == "SampleError::0000"
    assert error["hint"].endswith("SELECT ... FROM orders")
