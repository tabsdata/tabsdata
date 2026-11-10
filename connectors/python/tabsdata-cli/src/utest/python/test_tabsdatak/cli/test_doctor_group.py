#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys
import time
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
import requests
from click.testing import CliRunner

from tabsdatak.cli import _doctor
from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk._apiserver import APIServerError
from tabsdatak.sdk.tabsdata_server import TabsdataServer

LOCAL_PYTHON = (
    f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
)


def _collection(name: str, collection_type: str) -> MagicMock:
    collection = MagicMock()
    collection._data = {"name": name, "collection_type": collection_type}
    return collection


def _client(
    python_versions=(LOCAL_PYTHON,),
    server_version="1.0.0",
    collections=(),
    connections=None,
) -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "p1"
    client.connection.refresh_token = "refresh"
    client.connection.expiration_time = None
    client.connection.runtime_info_get.return_value.json.return_value = {
        "data": {"version": server_version, "python_versions": list(python_versions)}
    }
    client.status = MagicMock(status="Running")
    client.auth_info.return_value = {"name": "admin"}
    client.list_collections_generator.side_effect = lambda: iter(collections)
    client.get_collection_connection.side_effect = lambda name: {
        "connection": (connections or {})[name]
    }
    return client


@pytest.fixture(autouse=True)
def _local_environment():
    with (
        patch.object(_doctor, "bundle_analysis", return_value=([], [])),
        patch.object(_doctor.shutil, "which", return_value="/usr/bin/uv"),
        patch.object(_doctor.sys, "prefix", "/venv"),
        patch.object(_doctor, "_client_version", return_value="1.0.0"),
        patch.object(
            _doctor,
            "_installed_connection_types",
            return_value={"postgres-sql-in", "s3-file-out"},
        ),
    ):
        yield


def _invoke(argv, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv)


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.stdout
    document = json.loads(lines[0])
    if document.get("error", {}).get("type") == "checks_failed":
        return document["error"]["state"]
    return document


def _checks(document) -> dict:
    return {check["name"]: check for check in document["checks"]}


def test_healthy_setup_passes_every_check(tmp_path):
    client = _client(
        collections=[_collection("src", "S"), _collection("fac", "F")],
        connections={"src": {"type": "postgres-sql-in"}},
    )
    result = _invoke(["doctor", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["ok"] is True
    assert [c["name"] for c in document["checks"]] == list(_doctor.CHECKS)
    assert {c["status"] for c in document["checks"]} == {"pass"}
    assert set(document["checks"][0]) == {"name", "status", "detail", "fix"}


def test_python_mismatch_fails_with_one_document_and_exit_1(tmp_path):
    result = _invoke(
        ["doctor", "--machine"], _client(python_versions=("3.99.1",)), tmp_path
    )
    assert result.exit_code == ExitCode.FAILURE, result.output
    document = _document(result)
    assert document["ok"] is False
    python = _checks(document)["python"]
    assert python["status"] == "fail"
    assert "3.99.1" in python["detail"]
    assert python["fix"] == "uv python install 3.99.1 && uv venv --python 3.99.1"


def test_offline_skips_the_server_checks(tmp_path):
    client = _client()
    result = _invoke(["doctor", "--offline", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    checks = _checks(_document(result))
    assert {checks[n]["status"] for n in ("server", "login", "connectors")} == {"skip"}
    assert checks["python"]["status"] == "warn"
    client.connection.runtime_info_get.assert_not_called()


def test_no_credentials_fails_login(tmp_path):
    result = _invoke(["doctor", "--machine"], None, tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    checks = _checks(_document(result))
    assert checks["login"]["status"] == "fail"
    assert "tdk login" in checks["login"]["fix"]
    assert checks["server"]["status"] == "skip"


def test_unreachable_server_fails_without_aborting(tmp_path):
    client = _client()
    client.connection.runtime_info_get.side_effect = (
        requests.exceptions.ConnectionError("refused")
    )
    type(client).status = PropertyMock(
        side_effect=requests.exceptions.ConnectionError("refused")
    )
    result = _invoke(["doctor", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    checks = _checks(_document(result))
    assert checks["server"]["status"] == "fail"
    assert checks["python"]["status"] == "warn"
    assert checks["venv"]["status"] == "pass"


def test_version_skew_warns(tmp_path):
    result = _invoke(
        ["doctor", "--check", "server", "--machine"],
        _client(server_version="2.0.0"),
        tmp_path,
    )
    assert result.exit_code == ExitCode.OK, result.output
    server = _checks(_document(result))["server"]
    assert server["status"] == "warn"
    assert "versions differ" in server["detail"]


def test_refused_version_fails(tmp_path):
    client = _client()
    refused = APIServerError(
        {"error": "upgrade_required"}, status=426, server_version="2.0.0"
    )
    client.connection.runtime_info_get.side_effect = refused
    type(client).status = PropertyMock(side_effect=refused)
    result = _invoke(["doctor", "--check", "server", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    server = _checks(_document(result))["server"]
    assert server["status"] == "fail"
    assert "server 2.0.0, client 1.0.0" in server["detail"]


def test_refused_version_fails_login_with_the_version_fix(tmp_path):
    client = _client()
    client.auth_info.side_effect = APIServerError(
        {"error": "upgrade_required"}, status=426, server_version="2.0.0"
    )
    result = _invoke(["doctor", "--check", "login", "--machine"], client, tmp_path)
    login = _checks(_document(result))["login"]
    assert login["status"] == "fail"
    assert login["fix"] == _doctor.VERSION_FIX


def test_expiring_session_warns(tmp_path):
    client = _client()
    client.connection.refresh_token = None
    client.connection.expiration_time = time.time() + 60
    result = _invoke(["doctor", "--check", "login", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert _checks(_document(result))["login"]["status"] == "warn"


def test_missing_connector_package_warns(tmp_path):
    client = _client(
        collections=[_collection("crm", "S"), _collection("lake", "D")],
        connections={
            "crm": {"type": "salesforce-report-in"},
            "lake": {"type": "s3-file-out"},
        },
    )
    result = _invoke(["doctor", "--check", "connectors", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    connectors = _checks(_document(result))["connectors"]
    assert connectors["status"] == "warn"
    assert "salesforce-report-in (collections crm)" in connectors["detail"]


def test_skipped_non_registry_packages_warn(tmp_path):
    with patch.object(
        _doctor, "bundle_analysis", return_value=([], ["mylib (editable)"])
    ):
        result = _invoke(
            ["doctor", "--check", "venv", "--machine"], _client(), tmp_path
        )
    assert result.exit_code == ExitCode.OK, result.output
    venv = _checks(_document(result))["venv"]
    assert venv["status"] == "warn"
    assert "mylib (editable)" in venv["detail"]


BUNDLE_ANALYSIS = _doctor.bundle_analysis


def test_bundle_analysis_bundles_index_packages_and_skips_the_rest():
    from types import SimpleNamespace

    from tabsdatak.common._interpreter.python import package, venv
    from tabsdatak.common._interpreter.python.recipe import InstallRecipe

    recipes = {
        "fromindex": SimpleNamespace(recipe=InstallRecipe.INDEX, version="1.2.3"),
        "mylib": SimpleNamespace(recipe=InstallRecipe.EDITABLE, version="0.1"),
    }
    with (
        patch.object(venv.PythonEnvironment, "locked_packages", return_value=set()),
        patch.object(
            venv.PythonEnvironment, "installed_packages", return_value=set(recipes)
        ),
        patch.object(package.Package, "recipe", lambda self: recipes[self.name]),
    ):
        assert BUNDLE_ANALYSIS() == (
            ["fromindex==1.2.3"],
            ["mylib (editable)"],
        )


def test_outside_a_venv_fails(tmp_path):
    with patch.object(_doctor.sys, "prefix", _doctor.sys.base_prefix):
        result = _invoke(
            ["doctor", "--check", "venv", "--machine"], _client(), tmp_path
        )
    assert result.exit_code == ExitCode.FAILURE, result.output
    assert _checks(_document(result))["venv"]["status"] == "fail"


def test_missing_uv_fails(tmp_path):
    with patch.object(_doctor.shutil, "which", return_value=None):
        result = _invoke(["doctor", "--check", "uv", "--machine"], _client(), tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    assert _checks(_document(result))["uv"]["status"] == "fail"


def test_a_crashing_check_is_reported_as_failed(tmp_path):
    with patch.object(_doctor, "bundle_analysis", side_effect=RuntimeError("boom")):
        result = _invoke(["doctor", "--machine"], _client(), tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    checks = _checks(_document(result))
    assert checks["venv"]["status"] == "fail"
    assert "boom" in checks["venv"]["detail"]
    assert checks["uv"]["status"] == "pass"


def test_unknown_check_is_a_usage_error(tmp_path):
    result = _invoke(["doctor", "--check", "bogus", "--machine"], _client(), tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    assert _document(result)["error"]["type"] == "usage"


def test_table_view_reports_the_failure(tmp_path):
    result = _invoke(["doctor", "--offline"], _client(), tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert "Check" in result.output
    with patch.object(_doctor.shutil, "which", return_value=None):
        failed = _invoke(["doctor", "--offline"], _client(), tmp_path)
    assert failed.exit_code == ExitCode.FAILURE
    assert "1 check(s) failed: uv." in failed.output


def test_the_checks_wrap_their_detail_and_fix_instead_of_cutting_them():
    from tabsdatak.cli.td.doctor_group import checks_view

    view = checks_view([
        {"name": "search", "status": "warn", "detail": "d" * 200, "fix": "f" * 200}
    ])

    assert view.wrap


def test_connectors_without_a_project_asks_a_program_for_dash_p(tmp_path):
    client = _client()
    client.connection.project = None
    result = _invoke(["doctor", "--check", "connectors", "--machine"], client, tmp_path)
    connectors = _checks(_document(result))["connectors"]
    assert connectors["status"] == "skip"
    assert connectors["fix"] == "Pass '-P NAME'."


def test_connectors_take_the_project_from_dash_p(tmp_path):
    client = _client()
    client.connection.project = None
    client.list_collections_generator.return_value = iter([])
    argv = ["doctor", "--check", "connectors", "-P", "p2", "--machine"]
    connectors = _checks(_document(_invoke(argv, client, tmp_path)))["connectors"]
    assert connectors["status"] == "pass"
    assert client.connection.project == "p2"
