#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import ast
import json
import os
import platform
import textwrap
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli._fn_check import (
    check_function,
    check_source,
    dangerous_calls,
    destination_options,
)
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer

_TRANSFORMER = textwrap.dedent(
    """
    from tabsdatak.api import TableFrameSpec, transformer


    @transformer(input_tables=["sales/orders@HEAD", "items"], output_tables=["out"])
    def clean(orders: TableFrameSpec, items: TableFrameSpec) -> TableFrameSpec:
        return orders
    """
)


def _write(folder, source: str, name: str = "f.py") -> str:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(source)
    return str(folder / name)


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "p1"
    return client


def _invoke(argv, tmp_path, client=None, env=None):
    client = client or _client()

    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env=env)


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.output
    return json.loads(lines[0])


def _codes(result: dict) -> list[str]:
    return [finding["code"] for finding in result["findings"]]


def test_a_valid_function_is_described_and_bundled(tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    result = check_function(f"{path}::clean")
    assert result["valid"] is True, result
    assert result["function"] == {
        "name": "clean",
        "kind": "transformer",
        "inputs": ["sales/orders@HEAD", "items"],
        "outputs": ["out"],
        "triggers": ["sales/orders", "items"],
        "connector": None,
        "destination_options": {},
    }
    assert result["bundle"]["files"] >= 3
    assert result["bundle"]["bytes"] > 0
    assert result["bundle"]["python_version"]
    assert result["findings"] == []
    assert result["server_checks"] is None


def test_a_manual_only_function_declares_no_triggers(tmp_path):
    source = _TRANSFORMER.replace(
        "import TableFrameSpec, transformer",
        "import TableFrameSpec, TablesTrigger, transformer",
    ).replace(
        'output_tables=["out"])',
        'output_tables=["out"], trigger_by=TablesTrigger(tables=[]))',
    )
    path = _write(tmp_path / "proj", source)
    result = check_function(f"{path}::clean")
    assert result["valid"] is True, result
    assert result["function"]["triggers"] == []


def test_a_syntax_error_is_reported_without_importing(tmp_path):
    path = _write(tmp_path, "def clean(:\n    pass\n")
    result = check_function(f"{path}::clean")
    assert result["valid"] is False
    assert _codes(result) == ["syntax_error"]
    assert result["findings"][0]["line"] == 1
    assert result["function"] is None


def test_a_missing_function_is_an_error(tmp_path):
    path = _write(tmp_path, _TRANSFORMER)
    result = check_function(f"{path}::other")
    assert result["valid"] is False
    assert _codes(result) == ["import_error"]
    assert "'other' not found" in result["findings"][0]["message"]


def test_an_imported_function_is_checked_as_register_imports_it(tmp_path):
    _write(tmp_path / "proj", _TRANSFORMER, "impl.py")
    path = _write(tmp_path / "proj", "from impl import clean\n", "entry.py")
    result = check_function(f"{path}::clean")
    assert result["valid"] is True, result
    assert result["function"]["name"] == "clean"


def test_check_source_requires_the_function_to_be_defined():
    findings = check_source(_TRANSFORMER, "f.py", "other")
    assert [f["code"] for f in findings] == ["function_not_found"]
    assert "clean" in findings[0]["message"]


def test_a_failing_import_is_an_error(tmp_path):
    path = _write(tmp_path, "raise RuntimeError('boom')\n\ndef clean():\n    pass\n")
    result = check_function(f"{path}::clean")
    assert result["valid"] is False
    assert _codes(result) == ["import_error"]
    assert "boom" in result["findings"][0]["message"]


def test_an_undecorated_function_is_an_error(tmp_path):
    path = _write(tmp_path, "def clean():\n    pass\n")
    result = check_function(f"{path}::clean")
    assert _codes(result) == ["not_a_function"]


def test_an_unreadable_file_is_an_error(tmp_path):
    result = check_function(f"{tmp_path / 'missing.py'}::clean")
    assert result["valid"] is False
    assert _codes(result) == ["unreadable"]


def test_dangerous_calls_are_warnings_however_imported():
    tree = ast.parse(
        textwrap.dedent(
            """
            import subprocess as sp
            from os import system
            import pickle

            eval("1")
            sp.run(["ls"])
            system("ls")
            pickle.loads(b"")
            print("fine")
            """
        )
    )
    found = [(f["severity"], f["message"], f["line"]) for f in dangerous_calls(tree)]
    assert sorted(found, key=lambda f: f[2]) == [
        ("warning", "eval", 6),
        ("warning", "subprocess.run", 7),
        ("warning", "os.system", 8),
        ("warning", "pickle.loads", 9),
    ]


def test_warnings_leave_the_function_valid(tmp_path):
    path = _write(tmp_path, "import subprocess\n" + _TRANSFORMER)
    source = (
        open(path)
        .read()
        .replace("return orders", "subprocess.run(['ls'])\n    return orders")
    )
    _write(tmp_path, source)
    result = check_function(f"{path}::clean")
    assert result["valid"] is True
    assert _codes(result) == ["dangerous_call"]


def _table(name, function):
    table = MagicMock()
    table.name = name
    if function is None:
        table.function = None
    else:
        table.function.name = function
    return table


def _server(tables: dict, python_versions=None) -> MagicMock:
    server = _client()
    server.list_tables.side_effect = lambda coll: [
        _table(name, function) for name, function in tables.get(coll, {}).items()
    ]
    server.valid_python_versions = python_versions
    return server


def test_server_checks_pass_for_a_consistent_function(tmp_path):
    path = _write(tmp_path, _TRANSFORMER)
    local = check_function(f"{path}::clean")
    server = _server(
        {"sales": {"orders": "pub"}, "c1": {"items": "pub2", "out": "clean"}},
        python_versions=[local["bundle"]["python_version"]],
    )
    result = check_function(f"{path}::clean", server=server, collection="c1")
    assert result["valid"] is True, result
    assert result["server_checks"] == {
        "collection": "ok",
        "inputs_exist": True,
        "output_conflicts": [],
        "python_version": "ok",
    }


def test_server_checks_report_missing_inputs_conflicts_and_versions(tmp_path):
    path = _write(tmp_path, _TRANSFORMER)
    server = _server({"c1": {"out": "someone_else"}}, python_versions=["2.7"])
    result = check_function(f"{path}::clean", server=server, collection="c1")
    assert result["valid"] is False
    checks = result["server_checks"]
    assert checks["inputs_exist"] is False
    assert checks["output_conflicts"] == [{"table": "out", "function": "someone_else"}]
    assert checks["python_version"] == "rejected"
    assert sorted(_codes(result)) == [
        "input_missing",
        "input_missing",
        "output_conflict",
        "python_version",
    ]


def test_the_registered_name_owns_its_outputs(tmp_path):
    path = _write(tmp_path, _TRANSFORMER)
    server = _server({"c1": {"out": "renamed"}}, python_versions=[])
    result = check_function(
        f"{path}::clean", server=server, collection="c1", name="renamed"
    )
    assert result["server_checks"]["output_conflicts"] == []


def test_a_missing_collection_stops_the_server_checks(tmp_path):
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import APIServerError

    path = _write(tmp_path, _TRANSFORMER)
    server = _server({})
    server.get_collection.side_effect = APIServerError("not found")
    result = check_function(f"{path}::clean", server=server, collection="c1")
    assert result["server_checks"]["collection"] == "missing"
    assert _codes(result) == ["collection_missing"]
    assert server.list_tables.call_count == 0


def test_a_requirements_file_skips_the_python_version_check(tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    requirements = tmp_path / "requirements.yaml"
    requirements.write_text("pythonVersion: 3.99.0\npublicPackages: []\n")
    server = _server({"sales": {"orders": "p"}, "c1": {"items": "p"}}, ["3.12.0"])
    result = check_function(
        f"{path}::clean",
        requirements=str(requirements),
        server=server,
        collection="c1",
    )
    assert result["bundle"]["python_version"] == "3.99.0"
    assert result["server_checks"]["python_version"] is None
    assert result["valid"] is True, result


def test_fn_check_is_local_without_a_collection(tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    client = _client()
    result = _invoke(
        ["fn", "check", "--path", f"{path}::clean", "--machine"], tmp_path, client
    )
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["valid"] is True
    assert document["function"]["name"] == "clean"
    assert client.method_calls == []


def test_fn_check_fails_an_invalid_function_with_the_result(tmp_path):
    path = _write(tmp_path, "def clean(:\n")
    result = _invoke(["fn", "check", "--path", f"{path}::clean", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    error = _document(result)["error"]
    assert error["type"] == "checks_failed"
    assert error["state"]["valid"] is False
    assert [f["code"] for f in error["state"]["findings"]] == ["syntax_error"]


def test_fn_check_human_view_shows_the_findings_then_fails(tmp_path):
    path = _write(tmp_path, "def clean(:\n")
    result = _invoke(["fn", "check", "--path", f"{path}::clean"], tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    assert "The function could not be loaded." in result.stdout
    assert "error: syntax_error" in result.stdout


def test_dry_run_of_an_invalid_function_fails(tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    client = _dry_run_client()
    client.valid_python_versions = ["3.0.0"]
    argv = ["fn", "register", "-c", "c1", "--path", f"{path}::clean", "--dry-run"]
    result = _invoke([*argv, "--machine"], tmp_path, client)
    assert result.exit_code == ExitCode.FAILURE, result.output
    error = _document(result)["error"]
    assert error["type"] == "checks_failed"
    assert error["state"]["would"]["action"] == "register"
    assert client.register_function.call_count == 0


def test_fn_check_human_view(tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    result = _invoke(["fn", "check", "--path", f"{path}::clean"], tmp_path)
    assert result.exit_code == 0, result.output
    assert "Function 'clean' (transformer) is valid." in result.stdout
    assert "Outputs:   out" in result.stdout


def _dry_run_client() -> MagicMock:
    client = _client()
    client.list_tables.return_value = []
    client.valid_python_versions = [platform.python_version()]
    return client


@pytest.mark.parametrize(
    "argv, would",
    [
        (
            ["fn", "register", "-c", "c1"],
            {"action": "register", "collection": "c1", "name": "clean"},
        ),
        (
            ["fn", "update", "-c", "c1", "-n", "f1"],
            {"action": "update", "collection": "c1", "name": "f1"},
        ),
        (
            ["fn", "patch", "-c", "c1", "-n", "f1", "--fn-version", "fv1"],
            {"action": "patch", "collection": "c1", "name": "f1", "fn_version": "fv1"},
        ),
    ],
)
def test_dry_run_checks_and_never_uploads(argv, would, tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    client = _dry_run_client()
    result = _invoke(
        [*argv, "--path", f"{path}::clean", "--dry-run", "--machine"], tmp_path, client
    )
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["would"] == would
    assert document["function"]["name"] == "clean"
    assert document["server_checks"]["collection"] == "ok"
    for method in ("register_function", "update_function", "patch_function"):
        assert getattr(client, method).call_count == 0


def test_register_update_dry_run_would_update_an_existing_function(tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    client = _dry_run_client()
    argv = ["fn", "register", "-c", "c1", "--path", f"{path}::clean", "--update"]
    document = _document(_invoke([*argv, "--dry-run", "--machine"], tmp_path, client))
    assert document["would"]["action"] == "update"
    assert client.get_function.call_args == call("c1", "clean")
    assert client.update_function.call_count == 0


def test_dry_run_human_view_says_nothing_was_uploaded(tmp_path):
    path = _write(tmp_path / "proj", _TRANSFORMER)
    argv = ["fn", "register", "-c", "c1", "--path", f"{path}::clean", "--dry-run"]
    result = _invoke(argv, tmp_path, _dry_run_client())
    assert result.exit_code == 0, result.output
    assert (
        "Would register function 'clean' in collection 'c1'; nothing was uploaded."
        in result.stdout
    )


def _fn_argv(command: str) -> list[str]:
    return {
        "register": ["fn", "register", "-c", "c1"],
        "update": ["fn", "update", "-c", "c1", "-n", "f1"],
        "patch": ["fn", "patch", "-c", "c1", "-n", "f1", "--fn-version", "fv1"],
    }[command]


@pytest.mark.parametrize("command", ["register", "update", "patch"])
def test_requirements_and_local_packages_reach_the_sdk(command, tmp_path):
    client = _client()
    argv = [
        *_fn_argv(command),
        "--path",
        "/x.py::f",
        "--requirements",
        "req.yaml",
        "--local-pkg",
        "pkg1",
        "--local-pkg",
        "pkg2",
    ]
    with patch(
        "tabsdatak.cli.td.fn_group.load_info_and_project_root",
        return_value=(MagicMock(), "/root"),
    ):
        result = _invoke(argv, tmp_path, client)
    assert result.exit_code == 0, result.output
    kwargs = getattr(client, f"{command}_function").call_args.kwargs
    assert kwargs["requirements"] == "req.yaml"
    assert kwargs["local_packages"] == ["pkg1", "pkg2"]


def test_local_packages_outside_the_confinement_are_not_bundled(
    tmp_path, machine_settings
):
    workspace = tmp_path / "ws"
    path = _write(workspace, _TRANSFORMER)
    (workspace / "pkg").mkdir()
    outside = tmp_path / "pkg"
    outside.mkdir()
    machine_settings.update(confine_to=str(workspace))
    base = ["fn", "check", "--path", f"{path}::clean", "--machine"]
    inside = _invoke([*base, "--local-pkg", str(workspace / "pkg")], tmp_path)
    assert inside.exit_code == ExitCode.OK, inside.output
    refused = _invoke(
        [*base, "--local-pkg", str(workspace / "pkg"), "--local-pkg", str(outside)],
        tmp_path,
    )
    assert refused.exit_code == ExitCode.USAGE, refused.output
    assert _document(refused)["error"]["type"] == "path_not_allowed"


def test_a_check_bundling_outside_the_confinement_is_not_allowed(
    tmp_path, machine_settings
):
    workspace = tmp_path / "ws"
    path = _write(workspace, _TRANSFORMER)
    outside = tmp_path / "secret.txt"
    outside.write_text("x")
    os.symlink(outside, workspace / "link.txt")
    machine_settings.update(confine_to=str(workspace))
    result = _invoke(["fn", "check", "--path", f"{path}::clean", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    assert _document(result)["error"]["type"] == "path_not_allowed"


class _Destination:
    def _materialized(self) -> dict[str, str]:
        return {
            "if_table_exists": "append",
            "schema_evolution": "strict",
            "schema_evolution_engine": "native",
        }


def _info(kind: str, destination=None):
    from types import SimpleNamespace

    from tabsdatak._info import (
        KIND_PUBLISHER_DEF,
        KIND_SUBSCRIBER_DEF,
        KIND_TRANSFORMER_DEF,
    )

    return SimpleNamespace(
        __kind__={
            "publisher": KIND_PUBLISHER_DEF,
            "subscriber": KIND_SUBSCRIBER_DEF,
            "transformer": KIND_TRANSFORMER_DEF,
        }[kind],
        destination=destination,
    )


def test_a_subscriber_reports_what_its_destination_resolves():
    assert destination_options(_info("subscriber", _Destination())) == {
        "if_table_exists": "append",
        "schema_evolution": "strict",
        "schema_evolution_engine": "native",
    }


@pytest.mark.parametrize(
    "info",
    [
        _info("subscriber", object()),
        _info("subscriber", None),
        _info("publisher", _Destination()),
        _info("transformer", _Destination()),
    ],
    ids=["no-hook", "no-destination", "publisher", "transformer"],
)
def test_nothing_is_reported_where_no_destination_resolves_options(info):
    assert destination_options(info) == {}


def _check_result(destination_options: dict) -> dict:
    return {
        "valid": True,
        "function": {
            "name": "sub",
            "kind": "subscriber",
            "inputs": ["c/t"],
            "outputs": [],
            "triggers": ["c/t"],
            "connector": "tabsdatak.conn.postgres:PostgresDest",
            "destination_options": destination_options,
        },
        "bundle": None,
        "findings": [],
    }


def test_the_check_table_shows_what_the_destination_writes_with(capsys):
    from tabsdatak.cli.td.fn_group import _check_table

    _check_table(_check_result(_Destination()._materialized()))
    assert (
        "  Writes:    if_table_exists='append', schema_evolution='strict',"
        " schema_evolution_engine='native'"
    ) in capsys.readouterr().out


def test_the_check_table_says_nothing_of_a_destination_that_resolves_nothing(
    capsys,
):
    from tabsdatak.cli.td.fn_group import _check_table

    _check_table(_check_result({}))
    assert "Writes:" not in capsys.readouterr().out

