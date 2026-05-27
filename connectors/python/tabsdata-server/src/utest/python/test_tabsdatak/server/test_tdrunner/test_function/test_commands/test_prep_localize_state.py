#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.prep_localize_state import (
    EXIT_BAD_REQUEST,
    EXIT_INPUTS_MANIFEST_WRITE_FAILED,
    EXIT_STATE_MANIFEST_WRITE_FAILED,
    EXIT_UNSUPPORTED_SCHEME,
    EXIT_WRITE_FAILED,
    main,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _set_argv(
    monkeypatch,
    request_path,
    in_dir,
    state_dir,
    out,
    inputs_manifest,
    state_manifest,
    liveness,
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prep-localize-state",
            "--request",
            str(request_path),
            "--in-dir",
            str(in_dir),
            "--state-dir",
            str(state_dir),
            "--out",
            str(out),
            "--inputs-manifest",
            str(inputs_manifest),
            "--state-manifest",
            str(state_manifest),
            "--liveness",
            str(liveness),
        ],
    )


def _write_request(
    path: Path,
    *,
    system_input: list | None = None,
    inputs: list | None = None,
    bundle_uri: str = "file:///bundle.tgz",
) -> Path:
    body = {
        "info": {
            "collection": "c",
            "collection_id": 1,
            "execution_id": 2,
            "transaction_id": 3,
            "function_run_id": 4,
            "function_bundle": {"uri": bundle_uri},
        },
        "system_input": system_input or [],
        "input": inputs or [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


# --- happy path ---


def test_main_writes_transporter_yaml(monkeypatch, tmp_path):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_input=[
            {
                "name": "td-initial-values",
                "location": {"uri": "s3://bucket/state.parquet"},
            },
        ],
    )
    out = tmp_path / "transporter.yaml"
    inputs_manifest = tmp_path / "inputs.yaml"
    state_manifest = tmp_path / "state.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        out,
        inputs_manifest,
        state_manifest,
        tmp_path / "live",
    )
    assert rc == 0
    text = out.read_text()
    assert text.startswith("!CopyV1")
    assert "!S3" in text
    assert "s3://bucket/state.parquet" in text
    assert (tmp_path / "live").exists()
    # An inputs manifest is always written (empty when no request.input).
    data = yaml.safe_load(inputs_manifest.read_text())
    assert data == {"inputs": []}


def test_main_handles_empty_system_input(monkeypatch, tmp_path):
    """A request with no system_input still produces a valid (empty) request."""
    request_yaml = _write_request(tmp_path / "request.yaml")
    out = tmp_path / "transporter.yaml"
    inputs_manifest = tmp_path / "inputs.yaml"
    state_manifest = tmp_path / "state.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        out,
        inputs_manifest,
        state_manifest,
        tmp_path / "live",
    )
    assert rc == 0
    assert "source_target_pairs: []" in out.read_text()


def test_main_inputs_manifest_lists_input_tables(monkeypatch, tmp_path):
    """One inputs-manifest entry per request.input table, files pointing
    at <in_dir>/<name>.parquet."""
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        inputs=[
            {
                "name": "t_a",
                "location": {"uri": "s3://bucket/a.parquet"},
            },
            {
                "name": "t_b",
                "location": {"uri": "s3://bucket/b.parquet"},
            },
        ],
    )
    in_dir = tmp_path / "in"
    inputs_manifest = tmp_path / "inputs.yaml"
    state_manifest = tmp_path / "state.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        in_dir,
        tmp_path / "state",
        tmp_path / "transporter.yaml",
        inputs_manifest,
        state_manifest,
        tmp_path / "live",
    )
    assert rc == 0
    data = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in data["inputs"]] == ["t_a", "t_b"]
    assert data["inputs"][0]["files"] == [
        str((in_dir / "t_a.parquet").absolute()),
    ]
    assert data["inputs"][1]["files"] == [
        str((in_dir / "t_b.parquet").absolute()),
    ]


def test_main_inputs_manifest_empty_files_for_null_location(monkeypatch, tmp_path):
    """A request.input entry with `location is None` produces a manifest
    entry with an empty `files` list."""
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        inputs=[{"name": "t_x"}],  # no location
    )
    inputs_manifest = tmp_path / "inputs.yaml"
    state_manifest = tmp_path / "state.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "transporter.yaml",
        inputs_manifest,
        state_manifest,
        tmp_path / "live",
    )
    assert rc == 0
    data = yaml.safe_load(inputs_manifest.read_text())
    assert data["inputs"] == [{"name": "t_x", "files": []}]


def test_main_state_manifest_lists_system_input_tables(monkeypatch, tmp_path):
    """One state-manifest entry per request.system_input table, files
    pointing at <state_dir>/<name>.parquet."""
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_input=[
            {
                "name": "td-initial-values",
                "location": {"uri": "s3://bucket/state.parquet"},
            },
            {
                "name": "td-cursor",
                "location": {"uri": "s3://bucket/cursor.parquet"},
            },
        ],
    )
    state_dir = tmp_path / "state"
    state_manifest = tmp_path / "state.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        state_dir,
        tmp_path / "transporter.yaml",
        tmp_path / "inputs.yaml",
        state_manifest,
        tmp_path / "live",
    )
    assert rc == 0
    data = yaml.safe_load(state_manifest.read_text())
    assert [e["name"] for e in data["state"]] == [
        "td-initial-values",
        "td-cursor",
    ]
    assert data["state"][0]["files"] == [
        str((state_dir / "td-initial-values.parquet").absolute()),
    ]
    assert data["state"][1]["files"] == [
        str((state_dir / "td-cursor.parquet").absolute()),
    ]


def test_main_state_manifest_empty_when_no_system_input(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    state_manifest = tmp_path / "state.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "transporter.yaml",
        tmp_path / "inputs.yaml",
        state_manifest,
        tmp_path / "live",
    )
    assert rc == 0
    assert yaml.safe_load(state_manifest.read_text()) == {"state": []}


def test_main_state_manifest_empty_files_for_null_location(monkeypatch, tmp_path):
    """A system_input entry with `location is None` produces a state
    manifest entry with an empty `files` list."""
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_input=[{"name": "td-ephemeral"}],
    )
    state_manifest = tmp_path / "state.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "transporter.yaml",
        tmp_path / "inputs.yaml",
        state_manifest,
        tmp_path / "live",
    )
    assert rc == 0
    data = yaml.safe_load(state_manifest.read_text())
    assert data["state"] == [{"name": "td-ephemeral", "files": []}]


def main_with_args(
    monkeypatch,
    request_path,
    in_dir,
    state_dir,
    out,
    inputs_manifest,
    state_manifest,
    liveness,
):
    _set_argv(
        monkeypatch,
        request_path,
        in_dir,
        state_dir,
        out,
        inputs_manifest,
        state_manifest,
        liveness,
    )
    return main()


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path):
    rc = main_with_args(
        monkeypatch,
        tmp_path / "no-request.yaml",
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "state.yaml",
        tmp_path / "live",
    )
    assert rc == EXIT_BAD_REQUEST


def test_main_malformed_request_returns_bad_request(monkeypatch, tmp_path):
    bad = tmp_path / "request.yaml"
    bad.write_text("info: { unclosed")
    rc = main_with_args(
        monkeypatch,
        bad,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "state.yaml",
        tmp_path / "live",
    )
    assert rc == EXIT_BAD_REQUEST


def test_main_unsupported_scheme_returns_unsupported_scheme(monkeypatch, tmp_path):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_input=[
            {
                "name": "x",
                "location": {"uri": "ftp://server/x.parquet"},
            },
        ],
    )
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "state.yaml",
        tmp_path / "live",
    )
    assert rc == EXIT_UNSUPPORTED_SCHEME


def test_main_write_error_returns_write_failed(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "no" / "such" / "out.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "state.yaml",
        tmp_path / "live",
    )
    assert rc == EXIT_WRITE_FAILED


def test_main_inputs_manifest_write_failure_maps_to_inputs_manifest_failed(
    monkeypatch, tmp_path
):
    """Pointing --inputs-manifest at an existing directory makes the write fail."""
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad_target = tmp_path / "i-am-a-dir"
    bad_target.mkdir()
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "transporter.yaml",
        bad_target,
        tmp_path / "state.yaml",
        tmp_path / "live",
    )
    assert rc == EXIT_INPUTS_MANIFEST_WRITE_FAILED


def test_main_state_manifest_write_failure_maps_to_state_manifest_failed(
    monkeypatch, tmp_path
):
    """Pointing --state-manifest at an existing directory makes the write fail."""
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad_target = tmp_path / "i-am-a-dir"
    bad_target.mkdir()
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "transporter.yaml",
        tmp_path / "inputs.yaml",
        bad_target,
        tmp_path / "live",
    )
    assert rc == EXIT_STATE_MANIFEST_WRITE_FAILED


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prep-localize-state", "--request", "x"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_prep_localize_state"
