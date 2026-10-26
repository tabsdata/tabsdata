#
# Copyright 2026 Tabsdata Inc.
#

import json
import subprocess

import pytest
from click.testing import CliRunner

from tabsdatax.itest.tdkinstance.cli import main
from tabsdatax.itest.tdkinstance.commands import storage as storage_module
from tabsdatax.itest.tdkinstance.commands.storage import (
    ENV_INSTANCE,
    ENV_URI,
    parse_exports,
    storage_environment,
)

AWSE_STUB = """\
files:
  apiserver.storage:
    storage.mounts[id=ROOT]:
      uri: env:${TD_STORAGE__URI}/${TD_STORAGE__INSTANCE}/data/
      options.aws_access_key_id: env:TD_STORAGE__OPTIONS__AWS_ACCESS_KEY_ID
      options.aws_endpoint_url: env:TD_STORAGE__OPTIONS__AWS_ENDPOINT_URL
"""
OPTIONS = {"aws_access_key_id": "key", "aws_endpoint_url": "http://127.0.0.1:9000"}


def export_listing(options: dict) -> str:
    return "\n".join(
        [
            "export TD_STORAGE__URI='s3://bucket/d_user_0_instances_awse/'",
            f"export TD_STORAGE__STORAGE_OPTIONS='{json.dumps(options)}'",
            "export TD_STORAGE__PASSWORD='never-forwarded'",
        ]
    )


@pytest.fixture
def config(tmp_path):
    (tmp_path / "stubs" / "storages").mkdir(parents=True)
    (tmp_path / "stubs" / "commons.yaml").write_text("tdk_jwt_expiry: '10'\n")
    (tmp_path / "stubs" / "storages" / "awse.yaml").write_text(AWSE_STUB)
    config = tmp_path / "kubernetes.yaml"
    config.write_text("tdtest:\n  stubs:\n    - stubs/commons.yaml\n")
    return config


@pytest.fixture
def tdd_calls(monkeypatch):
    calls = []

    def run_tdd(tdd, arguments):
        calls.append(arguments)
        return subprocess.CompletedProcess(arguments, 0, export_listing(OPTIONS), "")

    monkeypatch.setattr(storage_module, "run_tdd", run_tdd)
    return calls


@pytest.fixture
def child(monkeypatch):
    seen = {}

    def run(command, env):
        seen["command"] = command
        seen["env"] = env
        return subprocess.CompletedProcess(command, 7)

    monkeypatch.setattr(storage_module.subprocess, "run", run)
    return seen


def test_a_name_carries_the_storage_after_the_role():
    result = CliRunner().invoke(main, ["name", "tdtest", "--storage", "awse"])
    assert result.exit_code == 0
    assert result.output.startswith("tdtest-awse-")


def test_a_name_defaults_to_the_file_storage():
    result = CliRunner().invoke(main, ["name", "tdtest"])
    assert result.output.startswith("tdtest-file-")


def test_an_unknown_storage_is_rejected():
    result = CliRunner().invoke(main, ["name", "tdtest", "--storage", "aws"])
    assert result.exit_code != 0


def test_the_storage_stub_comes_after_the_instance_type_stubs(config):
    result = CliRunner().invoke(
        main, ["arguments", str(config), "tdtest", "--storage", "awse"]
    )
    assert result.exit_code == 0
    folder = config.parent.resolve()
    assert result.output.splitlines() == [
        "--config",
        f"@{folder / 'stubs' / 'commons.yaml'}",
        "--config",
        f"@{folder / 'stubs' / 'storages' / 'awse.yaml'}",
    ]


def test_the_file_storage_adds_no_stub(config):
    result = CliRunner().invoke(main, ["arguments", str(config), "tdtest"])
    assert result.output.count("--config") == 1


def test_only_the_uri_and_the_storage_options_are_read():
    assert set(parse_exports(export_listing(OPTIONS))) == {
        ENV_URI,
        "TD_STORAGE__STORAGE_OPTIONS",
    }


def test_each_storage_option_becomes_its_own_variable():
    environment = storage_environment(
        parse_exports(export_listing(OPTIONS)), "tdtest-awse-1"
    )
    assert environment == {
        ENV_URI: "s3://bucket/d_user_0_instances_awse",
        ENV_INSTANCE: "tdtest-awse-1",
        "TD_STORAGE__OPTIONS__AWS_ACCESS_KEY_ID": "key",
        "TD_STORAGE__OPTIONS__AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
    }


def test_the_domain_of_a_storage_is_created_on_its_system(tdd_calls):
    result = CliRunner().invoke(
        main, ["storage", "create", "--storage", "gcpe", "--tdd", "tdd"]
    )
    assert result.exit_code == 0
    assert tdd_calls == [
        [
            "domain",
            "create",
            "--sys",
            "gcslocal",
            "--domain",
            "instances_gcpe",
            "--force",
        ]
    ]


def test_the_file_storage_creates_no_domain(tdd_calls):
    CliRunner().invoke(main, ["storage", "create", "--tdd", "tdd"])
    assert tdd_calls == []


def test_the_command_runs_with_the_storage_environment(config, tdd_calls, child):
    result = CliRunner().invoke(
        main,
        [
            "storage",
            "exec",
            "--config",
            str(config),
            "--storage",
            "awse",
            "--instance",
            "tdtest-awse-1",
            "--tdd",
            "tdd",
            "tdkserver",
            "quickstart",
            "--yes",
        ],
    )
    assert result.exit_code == 7
    assert child["command"] == ["tdkserver", "quickstart", "--yes"]
    assert child["env"][ENV_INSTANCE] == "tdtest-awse-1"
    assert child["env"]["TD_STORAGE__OPTIONS__AWS_ACCESS_KEY_ID"] == "key"
    assert "key" not in result.output


def test_a_variable_the_stub_reads_must_come_from_the_domain(
    config, monkeypatch, child
):
    def run_tdd(tdd, arguments):
        listing = export_listing({"aws_access_key_id": "key"})
        return subprocess.CompletedProcess(arguments, 0, listing, "")

    monkeypatch.setattr(storage_module, "run_tdd", run_tdd)
    result = CliRunner().invoke(
        main,
        [
            "storage",
            "exec",
            "--config",
            str(config),
            "--storage",
            "awse",
            "--instance",
            "tdtest-awse-1",
            "--tdd",
            "tdd",
            "tdkserver",
        ],
    )
    assert result.exit_code != 0
    assert "TD_STORAGE__OPTIONS__AWS_ENDPOINT_URL" in result.output
    assert child == {}
