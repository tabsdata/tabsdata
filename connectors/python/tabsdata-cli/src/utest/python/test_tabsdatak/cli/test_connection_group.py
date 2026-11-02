#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk connection`` CLI: the offline template/validate/check surface, which
speaks YAML, and the catalog commands, mocked at the SDK seam."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli.td.cli import cli
from tabsdatak.cli.td.connection_group import connector_kind, connector_types

# noinspection PyProtectedMember
from tabsdatak.sdk._apiserver import APIServerError
from tabsdatak.sdk.tabsdata_server import TabsdataServer, WaitTimeout


def _installed(type_: str, direction: str) -> bool:
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    try:
        template.resolve_conn(type_, direction)
        return True
    except template.TemplateError:
        return False


def test_types_lists_mode_and_system():
    result = CliRunner().invoke(cli, ["connection", "types"])
    assert result.exit_code == 0, result.output
    # New columns + a known reference connector (localfile always installed).
    assert "Mode" in result.output
    assert "System" in result.output
    assert "local-file-in" in result.output
    assert "Source" in result.output


def test_connector_types_names_the_types_of_a_conn_class():
    kind = connector_kind("local-file-in")
    assert "local-file-in" in connector_types(kind).split(", ")
    assert connector_types("no.such:Conn") == "no.such:Conn"


def test_template_emits_yaml():
    result = CliRunner().invoke(cli, ["connection", "template", "-t", "local-file-in"])
    assert result.exit_code == 0, result.output
    assert "kind: connectionDef" in result.output
    # YAML, not JSON -- no leading brace.
    assert not result.output.lstrip().startswith("{")


def test_template_file_writes_document_to_path(tmp_path):
    # ``--file``/``-f`` writes the template to a file instead of stdout. The
    # template's output path is ``--file`` (the CLI-wide name), NOT ``-o`` --
    # ``-o/--format`` is reserved for the table/json/yaml format contract.
    out = tmp_path / "conn.yaml"
    result = CliRunner().invoke(
        cli, ["connection", "template", "-t", "local-file-in", "--file", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert f"Wrote connection document to {out}" in result.output
    assert "kind: connectionDef" in out.read_text()
    # The short form -f is the same option.
    out2 = tmp_path / "conn2.yaml"
    result = CliRunner().invoke(
        cli, ["connection", "template", "-t", "local-file-in", "-f", str(out2)]
    )
    assert result.exit_code == 0, result.output
    assert out2.read_text() == out.read_text()


def test_template_file_dash_writes_to_stdout():
    # ``--file -`` means stdout, not a file literally named "-". The output is
    # identical to omitting ``--file``, with no "Wrote ..." status line.
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli, ["connection", "template", "-t", "local-file-in", "--file", "-"]
        )
        assert result.exit_code == 0, result.output
        assert "kind: connectionDef" in result.output
        assert "Wrote connection document" not in result.output
        assert not Path("-").exists()
    plain = CliRunner().invoke(cli, ["connection", "template", "-t", "local-file-in"])
    assert result.output == plain.output


def test_template_dash_o_is_the_output_format_not_a_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli, ["connection", "template", "-t", "local-file-in", "-o", "x"]
        )
        assert result.exit_code == 2
        assert not Path("x").exists()


def test_template_json_wraps_the_yaml_text():
    plain = CliRunner().invoke(cli, ["connection", "template", "-t", "local-file-in"])
    result = CliRunner().invoke(
        cli, ["connection", "template", "-t", "local-file-in", "-o", "json"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "type": "local-file-in",
        "file": None,
        "template": plain.output,
    }


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_template_yaml_carries_accepts_comments():
    result = CliRunner().invoke(cli, ["connection", "template", "-t", "mysql-sql-in"])
    assert result.exit_code == 0, result.output
    # uri (str|Secret) carries an "# accepts:" note in union order; the plain
    # arm shows the ``str:`` prefix that keeps it distinct from a secret.
    assert "# accepts: str:<value> | secret:<value>" in result.output


def test_validate_reads_yaml(tmp_path):
    # Generate a template (YAML), write it, validate it through the CLI.
    gen = CliRunner().invoke(cli, ["connection", "template", "-t", "local-file-in"])
    assert gen.exit_code == 0, gen.output
    path = tmp_path / "conn.yaml"
    path.write_text(gen.output)

    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code == 0, result.output
    assert "Connection document is valid" in result.output


def test_validate_rejects_malformed_yaml(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("kind: : :\n  - not valid")
    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code != 0
    assert "invalid connection YAML" in result.output


_ENV_REF_DOC = (
    "kind: connectionDef\n"
    "apiVersion: '1.0'\n"
    "type: tabsdatak.conn.localfile:LocalFileSrcConn\n"
    "spec:\n"
    "  base_path: '$secret:MY_BP'\n"
)


def test_validate_resolves_env_secret_refs(tmp_path, monkeypatch):
    # validate resolves `$secret:<ENV_VAR>` from the environment first, exactly
    # as create does -- so a document sourcing a secret from the environment
    # validates instead of being rejected as a bad secret value.
    path = tmp_path / "conn.yaml"
    path.write_text(_ENV_REF_DOC)
    monkeypatch.setenv("MY_BP", "/data/in")
    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code == 0, result.output
    assert "Connection document is valid" in result.output
    assert "MY_BP" in result.output  # echoes the env var it read


def test_validate_json_reports_the_env_vars_on_stdout_only(tmp_path, monkeypatch):
    path = tmp_path / "conn.yaml"
    path.write_text(_ENV_REF_DOC)
    monkeypatch.setenv("MY_BP", "/data/in")
    result = CliRunner().invoke(
        cli, ["connection", "validate", "-f", str(path), "-o", "json"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "valid": True,
        "type": "tabsdatak.conn.localfile:LocalFileSrcConn",
        "secret_env_vars": ["MY_BP"],
    }
    assert "MY_BP" in result.stderr


def test_validate_reports_unset_env_secret_ref(tmp_path, monkeypatch):
    # An unset referenced var is reported by name, not silently rejected as a
    # malformed secret value.
    path = tmp_path / "conn.yaml"
    path.write_text(_ENV_REF_DOC)
    monkeypatch.delenv("MY_BP", raising=False)
    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code != 0
    assert "MY_BP" in result.output


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "p1"
    return client


def _machine(argv, client, tmp_path, env=None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, [*argv, "--machine"], env=env)


def _human(argv, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv)


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.output
    return json.loads(lines[0])


def _probe(status: str, kind: str = "check", **extra) -> dict:
    return {
        "probe_id": "PR1",
        "connection_id": "C1",
        "collection": "crm",
        "kind": kind,
        "status": status,
        **extra,
    }


def _api_error(status: int, message: str) -> APIServerError:
    return APIServerError({"error_description": message}, status=status)


def test_list_emits_the_envelope_with_its_filters(tmp_path):
    client = _client()
    rows = [{"collection": "crm", "kind": "m:PgConn", "compatible": {"types": []}}]
    client.list_connections_generator.return_value = iter(rows)
    result = _machine(
        ["connection", "list", "--mode", "src", "--status", "error"], client, tmp_path
    )
    assert result.exit_code == 0, result.output
    assert _document(result) == {"items": rows, "truncated": False, "next": None}
    kwargs = client.list_connections_generator.call_args.kwargs
    assert kwargs["filter"] == ["collection_type:eq:S", "check_status:eq:error"]
    assert client.connection.project == "p1"


def test_list_all_projects_spans_every_project(tmp_path):
    client = _client()
    client.list_connections_generator.return_value = iter([])
    result = _machine(["connection", "list", "--all-projects"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert client.connection.project is None


def test_list_shows_no_annotation_column(tmp_path):
    client = _client()
    client.list_connections_generator.return_value = iter(
        [{"collection": "crm", "kind": "m:PgConn"}]
    )
    result = _human(["connection", "list"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert "Annotated" not in result.output


@pytest.mark.skipif(
    not _installed("local-file-in", "src"), reason="localfile connector not installed"
)
def test_list_type_filters_on_the_conn_class(tmp_path):
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    conn_cls = template.resolve_conn("local-file-in", None).conn_cls
    client = _client()
    client.list_connections_generator.return_value = iter([])
    result = _machine(["connection", "list", "--type", "local-file-in"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert client.list_connections_generator.call_args.kwargs["filter"] == [
        f"kind:eq:{conn_cls.__module__}:{conn_cls.__qualname__}"
    ]


def test_list_unknown_type_is_a_usage_error(tmp_path):
    result = _machine(["connection", "list", "--type", "nope"], _client(), tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output


def test_get_emits_the_catalog(tmp_path):
    client = _client()
    catalog = {"collection": "crm", "check_status": "ok", "compatible": {}}
    client.get_connection_catalog.return_value = catalog
    result = _machine(["connection", "get", "-n", "crm"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert _document(result) == catalog
    client.get_collection_connection.assert_not_called()


def test_get_definition_never_shows_secret_names(tmp_path):
    client = _client()
    client.get_connection_catalog.return_value = {"collection": "crm"}
    client.get_collection_connection.return_value = {
        "id": "C1",
        "connection": {"type": "m:PgConn"},
        "secret_names": ["S1"],
    }
    result = _machine(
        ["connection", "get", "-n", "crm", "--definition"], client, tmp_path
    )
    assert result.exit_code == 0, result.output
    assert _document(result)["definition"] == {
        "id": "C1",
        "connection": {"type": "m:PgConn"},
    }
    client.get_collection_connection.assert_called_once_with("crm")


def test_collection_connection_is_a_deprecated_alias(tmp_path):
    client = _client()
    client.get_collection_connection.return_value = {
        "connection": {"system": "postgres"},
        "secret_names": ["pw"],
    }
    argv = ["collection", "connection", "-n", "crm"]

    result = _human(argv, client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert json.loads(result.stdout) == {"connection": {"system": "postgres"}}
    assert "'tdk collection connection' is deprecated" in result.stderr
    assert "'tdk connection get --definition'" in result.stderr

    result = _machine(argv, client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result) == {"connection": {"system": "postgres"}}


def test_collection_connection_is_hidden(tmp_path):
    result = _human(["collection", "--help"], _client(), tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert not re.search(r"^\s*connection\b", result.output, re.MULTILINE)


def test_check_waits_for_the_probe(tmp_path):
    client = _client()
    client.submit_connection_probe.return_value = _probe("queued")
    done = _probe("succeeded", result={"ok": False, "error": "refused"})
    client.wait_connection_probe.return_value = done
    result = _machine(["connection", "check", "-n", "crm"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["result"] == {"ok": False, "error": "refused"}
    assert "waited_ms" in document
    client.submit_connection_probe.assert_called_once_with("crm", "check")
    assert client.wait_connection_probe.call_args.kwargs["timeout"] == 60


def test_a_joined_probe_is_waited_for_like_a_new_one(tmp_path):
    client = _client()
    client.submit_connection_probe.return_value = _probe("running", kind="metadata")
    client.wait_connection_probe.return_value = _probe("succeeded", kind="metadata")
    result = _machine(["connection", "refresh", "-n", "crm"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    client.submit_connection_probe.assert_called_once_with("crm", "metadata")
    assert client.wait_connection_probe.call_args.args[0] == "PR1"


def test_no_wait_returns_the_submitted_probe(tmp_path):
    client = _client()
    client.submit_connection_probe.return_value = _probe("queued")
    result = _machine(["connection", "check", "-n", "crm", "--no-wait"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result)["status"] == "queued"
    client.wait_connection_probe.assert_not_called()


def test_no_wait_takes_no_timeout(tmp_path):
    result = _machine(
        ["connection", "refresh", "-n", "crm", "--no-wait", "--timeout", "5"],
        _client(),
        tmp_path,
    )
    assert result.exit_code == ExitCode.USAGE


def test_a_wait_past_its_budget_exits_7_with_the_resume(tmp_path):
    client = _client()
    client.submit_connection_probe.return_value = _probe("queued")

    def _wait(probe_id, timeout, on_update):
        on_update(_probe("running"))
        raise WaitTimeout(_probe("running"), timeout)

    client.wait_connection_probe.side_effect = _wait
    result = _machine(
        ["connection", "check", "-n", "crm", "--timeout", "5"], client, tmp_path
    )
    assert result.exit_code == ExitCode.WAIT_TIMEOUT, result.output
    error = _document(result)["error"]
    assert error["type"] == "wait_timeout"
    assert error["resume"] == [
        "tdk",
        "connection",
        "probe",
        "--probe",
        "PR1",
        "--wait",
        "--proj",
        "p1",
    ]
    assert error["state"]["status"] == "running"
    assert client.wait_connection_probe.call_args.kwargs["timeout"] == 5


def test_max_wait_caps_the_probe_budget(tmp_path, machine_settings):
    machine_settings.update(max_wait=(20.0, "a 20 s cap"))
    client = _client()
    client.submit_connection_probe.return_value = _probe("queued")
    client.wait_connection_probe.return_value = _probe("succeeded")
    result = _machine(["connection", "check", "-n", "crm"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert client.wait_connection_probe.call_args.kwargs["timeout"] == 20


@pytest.mark.parametrize("status", ["failed", "timeout", "cancelled"])
def test_a_probe_that_did_not_succeed_exits_8(status, tmp_path):
    client = _client()
    client.submit_connection_probe.return_value = _probe("queued")
    ended = _probe(status, error={"code": "x", "message": "no route to host"})
    client.wait_connection_probe.return_value = ended
    result = _machine(["connection", "check", "-n", "crm"], client, tmp_path)
    assert result.exit_code == ExitCode.RUN_FAILED, result.output
    error = _document(result)["error"]
    assert error["type"] == "run_failed"
    assert "no route to host" in error["message"]
    assert error["state"] == ended


def test_a_rate_limited_submission_exits_1(tmp_path):
    client = _client()
    client.submit_connection_probe.side_effect = _api_error(
        429, "rate_limited: too many probe submissions, retry after 50s"
    )
    result = _machine(["connection", "check", "-n", "crm"], client, tmp_path)
    assert result.exit_code == ExitCode.FAILURE, result.output
    error = _document(result)["error"]
    assert error["type"] == "rate_limited"
    assert "retry after 50s" in error["message"]


def test_a_sample_of_an_entity_not_in_the_catalog_exits_5(tmp_path):
    client = _client()
    client.submit_connection_probe.side_effect = _api_error(
        404, "entity_not_in_catalog: 'db.t' is not in the catalog"
    )
    result = _machine(
        ["connection", "sample", "-n", "crm", "--entity", "db.t"], client, tmp_path
    )
    assert result.exit_code == ExitCode.NOT_FOUND, result.output
    assert _document(result)["error"]["type"] == "not_found"


def test_sample_emits_the_table_data_envelope(tmp_path):
    client = _client()
    client.submit_connection_probe.return_value = _probe("queued", kind="sample")
    client.wait_connection_probe.return_value = _probe(
        "succeeded",
        kind="sample",
        result={
            "entity": "db.t",
            "columns": ["id", "name"],
            "rows": [[1, "Acme"]],
            "truncated": True,
        },
        expires_on=123,
    )
    result = _machine(
        ["connection", "sample", "-n", "crm", "--entity", "db.t", "--len", "5"],
        client,
        tmp_path,
    )
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document.pop("waited_ms") >= 0
    assert document == {
        "probe_id": "PR1",
        "expires_on": 123,
        "entity": "db.t",
        "columns": [{"name": "id"}, {"name": "name"}],
        "rows": [[1, "Acme"]],
        "row_count": 1,
        "truncated": True,
    }
    client.submit_connection_probe.assert_called_once_with(
        "crm", "sample", entity="db.t", limit=5
    )


def test_sample_of_a_connector_without_rows_says_so(tmp_path):
    client = _client()
    client.submit_connection_probe.return_value = _probe("queued", kind="sample")
    client.wait_connection_probe.return_value = _probe(
        "succeeded", kind="sample", result={"supported": False}
    )
    result = _machine(
        ["connection", "sample", "-n", "crm", "--entity", "db.t"], client, tmp_path
    )
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result)["supported"] is False


def test_sample_caps_its_rows():
    result = CliRunner().invoke(
        cli, ["connection", "sample", "-n", "crm", "--entity", "t", "--len", "101"]
    )
    assert result.exit_code == ExitCode.USAGE


def test_entities_pages_with_the_list_options(tmp_path):
    client = _client()
    client.list_connection_entities_generator.return_value = iter(
        [{"fqn": "db.a"}, {"fqn": "db.b"}]
    )
    result = _machine(
        [
            "connection",
            "entities",
            "-n",
            "crm",
            "--fields",
            "--filter",
            "fqn:lk:db.*",
            "--max-result",
            "1",
        ],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    document = _document(result)
    assert document["items"] == [{"fqn": "db.a"}]
    assert document["truncated"] is True
    args, kwargs = client.list_connection_entities_generator.call_args
    assert args == ("crm",)
    assert kwargs["fields"] is True
    assert kwargs["filter"] == ["fqn:lk:db.*"]


_PROBE_ID = "00000000000000000000000001"


def test_probe_reads_and_optionally_waits(tmp_path):
    client = _client()
    client.get_connection_probe.return_value = _probe("running")
    result = _machine(["connection", "probe", "--probe", _PROBE_ID], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert _document(result)["status"] == "running"
    client.wait_connection_probe.assert_not_called()

    client.wait_connection_probe.return_value = _probe("succeeded")
    result = _machine(
        ["connection", "probe", "--probe", _PROBE_ID, "--wait"], client, tmp_path
    )
    assert result.exit_code == 0, result.output
    assert _document(result)["status"] == "succeeded"


def test_probe_refuses_a_name_before_asking_the_server(tmp_path):
    client = _client()
    result = _machine(["connection", "probe", "--probe", "iot_sensors"], client, tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    error = _document(result)["error"]
    assert error["type"] == "usage"
    assert "'--probe iot_sensors' is not an id" in error["message"]
    assert "tdk connection check|refresh|sample --name NAME" in error["hint"]
    client.get_connection_probe.assert_not_called()


def test_probe_timeout_needs_wait(tmp_path):
    result = _machine(
        ["connection", "probe", "--probe", _PROBE_ID, "--timeout", "5"], _client(), tmp_path
    )
    assert result.exit_code == ExitCode.USAGE


class _Entity:
    def __init__(self, fqn):
        self._fqn = fqn

    def fqn(self):
        return self._fqn


class _Explorer:
    connects = True

    def check_connect(self, conn):
        if not self.connects:
            raise ConnectionError("refused")

    def metadata(self, conn):
        return [_Entity("b"), _Entity("a")]

    def data(self, conn, entity, limit):
        return [{"id": i} for i in range(limit + 1)]


@pytest.fixture
def conn_file(tmp_path, monkeypatch):
    if not _installed("local-file-in", "src"):
        pytest.skip("localfile connector not installed")
    # noinspection PyProtectedMember
    from tabsdatak import _connectors

    monkeypatch.setattr(_connectors, "explorer_for_conn", lambda conn: _Explorer)
    generated = CliRunner().invoke(
        cli, ["connection", "template", "-t", "local-file-in"]
    )
    path = tmp_path / "conn.yaml"
    path.write_text(generated.output)
    return str(path)


def test_check_conn_file_runs_the_local_explorer(conn_file, tmp_path):
    client = _client()
    result = _machine(
        ["connection", "check", "--conn-file", conn_file, "--metadata"],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert _document(result) == {"ok": True, "entity_count": 2, "entities": ["a", "b"]}
    client.submit_connection_probe.assert_not_called()


def test_check_conn_file_samples_an_entity(conn_file, tmp_path):
    result = _machine(
        ["connection", "check", "--conn-file", conn_file, "--sample", "a", "--len", "2"],
        _client(),
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert _document(result)["sample"] == {
        "entity": "a",
        "columns": [{"name": "id"}],
        "rows": [[0], [1]],
        "row_count": 2,
        "truncated": True,
    }


def test_check_conn_file_reports_a_failed_connect(conn_file, tmp_path, monkeypatch):
    monkeypatch.setattr(_Explorer, "connects", False)
    result = _machine(["connection", "check", "--conn-file", conn_file], _client(), tmp_path)
    assert result.exit_code == 0, result.output
    assert _document(result) == {"ok": False, "error": "ConnectionError: refused"}


def test_check_conn_file_of_a_missing_connector_exits_2(tmp_path):
    path = tmp_path / "conn.yaml"
    path.write_text("kind: connectionDef\napiVersion: '1.0'\ntype: nope.missing:Conn\n")
    result = _machine(["connection", "check", "--conn-file", str(path)], _client(), tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    assert "not installed" in _document(result)["error"]["message"]


@pytest.mark.parametrize(
    "argv",
    [
        ["connection", "check", "-n", "crm", "--metadata"],
        ["connection", "check", "-n", "crm", "--conn-file", "c.yaml"],
        ["connection", "check", "--conn-file", "c.yaml", "--len", "5"],
    ],
)
def test_check_modes_do_not_mix(argv, tmp_path):
    result = _machine(argv, _client(), tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
