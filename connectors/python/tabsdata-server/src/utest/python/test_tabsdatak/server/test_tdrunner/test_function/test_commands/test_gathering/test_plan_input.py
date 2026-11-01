#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.gathering.plan_input import main
from tabsdatak.server._tdrunner.function.commands.gathering.plan_input.error import (
    BadRequestError,
    InputsManifestWriteFailedError,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    info_payload as _info,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _table(entry: dict, index: int) -> dict:
    table = {
        "collection_id": 1,
        "collection": "c",
        "table_id": "T-1",
        "table_version_id": "TV-1",
        "input_idx": index,
        "table_pos": index,
        "version_pos": 0,
        **entry,
    }
    return {"Table": table}


def _write_request(
    path: Path,
    *,
    system_input: list | None = None,
    inputs: list | None = None,
    bundle_uri: str = "file:///bundle.tar.zst",
) -> Path:
    body = {
        "Function": {
            "info": _info(bundle_uri),
            "system_input": [
                _table(entry, index) for index, entry in enumerate(system_input or [])
            ],
            "input": [_table(entry, index) for index, entry in enumerate(inputs or [])],
        }
    }
    path.write_text(yaml.safe_dump(body))
    return path


def main_with_args(
    monkeypatch,
    request_path: Path,
    inputs_manifest: Path,
    liveness: Path,
    *,
    responder_url: str,
) -> int:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "plan-input",
            "--request",
            str(request_path),
            "--inputs-manifest",
            str(inputs_manifest),
            "--liveness",
            str(liveness),
            "--responder-url",
            responder_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
            "--journal-file",
            "/tmp/em.yaml",
        ],
    )
    return main()


# --- happy path ---


def test_main_writes_only_the_inputs_manifest(monkeypatch, tmp_path, http_ok_url):
    """State is read where it is stored, so a request with system_input
    produces nothing besides the inputs manifest."""
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_input=[
            {
                "table": "td.fn_state__F-1",
                "location": {"uri": "s3://bucket/state.t"},
            },
        ],
    )
    out = tmp_path / "out"
    out.mkdir()
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        out / "inputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert (tmp_path / "live").exists()
    assert [path.name for path in out.iterdir()] == ["inputs.yaml"]
    assert yaml.safe_load((out / "inputs.yaml").read_text()) == {"inputs": []}


def test_main_inputs_manifest_lists_input_tables(monkeypatch, tmp_path, http_ok_url):
    """One inputs-manifest entry per request.input table, each record
    pointing at the table's storage location."""
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        inputs=[
            {
                "table": "t_a",
                "location": {"uri": "s3://bucket/a.parquet"},
            },
            {
                "table": "t_b",
                "location": {"uri": "s3://bucket/b.parquet"},
            },
        ],
    )
    inputs_manifest = tmp_path / "inputs.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        inputs_manifest,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    data = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in data["inputs"]] == ["t_a", "t_b"]
    assert [t["location"] for t in data["inputs"][0]["tables"]] == [
        {"uri": "s3://bucket/a.parquet", "env_prefix": None},
    ]
    assert [t["location"] for t in data["inputs"][1]["tables"]] == [
        {"uri": "s3://bucket/b.parquet", "env_prefix": None},
    ]
    assert [t["file"] for e in data["inputs"] for t in e["tables"]] == [None, None]


def test_main_inputs_manifest_empty_files_for_null_location(
    monkeypatch, tmp_path, http_ok_url
):
    """A request.input entry with `location is None` produces a manifest
    entry that is not empty but whose single table record carries neither
    `file` nor `location` (so downstream materialisation preserves the
    slot position as a `None` value)."""
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        inputs=[{"table": "t_x"}],  # no location
    )
    inputs_manifest = tmp_path / "inputs.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        inputs_manifest,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    data = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in data["inputs"]] == ["t_x"]
    entry = data["inputs"][0]
    assert entry["is_empty"] is False
    assert [(t["file"], t["location"]) for t in entry["tables"]] == [(None, None)]


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path, http_ok_url):
    rc = main_with_args(
        monkeypatch,
        tmp_path / "no-request.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == BadRequestError.exit_code


def test_main_malformed_request_returns_bad_request(monkeypatch, tmp_path, http_ok_url):
    bad = tmp_path / "request.yaml"
    bad.write_text("info: { unclosed")
    rc = main_with_args(
        monkeypatch,
        bad,
        tmp_path / "inputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == BadRequestError.exit_code


def test_main_inputs_manifest_write_failure_maps_to_inputs_manifest_failed(
    monkeypatch, tmp_path, http_ok_url
):
    """Pointing --inputs-manifest at an existing directory makes the write fail."""
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad_target = tmp_path / "i-am-a-dir"
    bad_target.mkdir()
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        bad_target,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == InputsManifestWriteFailedError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["plan-input", "--request", "x"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td-plan-input"
