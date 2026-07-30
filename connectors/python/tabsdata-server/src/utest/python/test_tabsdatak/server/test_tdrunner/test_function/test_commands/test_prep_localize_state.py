#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.prep_localize_state import main
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.error import (
    BadEndpointSchemeError,
    BadRequestError,
    InputsManifestWriteFailedError,
    StateManifestWriteFailedError,
    WriteFailedError,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    info_payload as _info,
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
    *,
    responder_url,
    responder_method="POST",
    responder_authorization="Bearer tok",
):
    storage_config = Path(out).parent / "storage.yaml"
    storage_config.parent.mkdir(parents=True, exist_ok=True)
    storage_config.write_text("storage:\n  mounts:\n    - id: ROOT\n      options: {}\n")
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
            "--storage-config",
            str(storage_config),
            "--responder-url",
            responder_url,
            "--responder-method",
            responder_method,
            "--responder-authorization",
            responder_authorization,
            "--journal-file",
            "/tmp/em.yaml",
        ],
    )


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


# --- happy path ---


def test_main_writes_transporter_yaml(monkeypatch, tmp_path, http_ok_url):
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
        responder_url=http_ok_url,
    )
    assert rc == 0
    text = out.read_text()
    assert text.startswith("pairs:")
    assert "s3://bucket/state.parquet" in text
    assert (tmp_path / "live").exists()
    # An inputs manifest is always written (empty when no request.input).
    data = yaml.safe_load(inputs_manifest.read_text())
    assert data == {"inputs": []}


def test_main_handles_empty_system_input(monkeypatch, tmp_path, http_ok_url):
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
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert "pairs: []" in out.read_text()


def test_main_inputs_manifest_lists_input_tables(monkeypatch, tmp_path, http_ok_url):
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
        responder_url=http_ok_url,
    )
    assert rc == 0
    data = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in data["inputs"]] == ["t_a", "t_b"]
    assert [t["file"] for t in data["inputs"][0]["tables"]] == [
        str((in_dir / "t_a.parquet").absolute()),
    ]
    assert [t["file"] for t in data["inputs"][1]["tables"]] == [
        str((in_dir / "t_b.parquet").absolute()),
    ]


def test_main_inputs_manifest_empty_files_for_null_location(
    monkeypatch, tmp_path, http_ok_url
):
    """A request.input entry with `location is None` produces a manifest
    entry that is not empty but whose single table record carries a null
    `file` (so downstream materialisation preserves the slot position as
    a `None` value)."""
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
        responder_url=http_ok_url,
    )
    assert rc == 0
    data = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in data["inputs"]] == ["t_x"]
    entry = data["inputs"][0]
    assert entry["is_empty"] is False
    assert [t["file"] for t in entry["tables"]] == [None]


def test_main_state_manifest_lists_system_input_tables(
    monkeypatch, tmp_path, http_ok_url
):
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
        responder_url=http_ok_url,
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


def test_main_state_manifest_empty_when_no_system_input(
    monkeypatch, tmp_path, http_ok_url
):
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
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert yaml.safe_load(state_manifest.read_text()) == {"state": []}


def test_main_state_manifest_empty_files_for_null_location(
    monkeypatch, tmp_path, http_ok_url
):
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
        responder_url=http_ok_url,
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
    *,
    responder_url,
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
        responder_url=responder_url,
    )
    return main()


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path, http_ok_url):
    rc = main_with_args(
        monkeypatch,
        tmp_path / "no-request.yaml",
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "state.yaml",
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
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "state.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == BadRequestError.exit_code


@pytest.mark.skip(
    reason="v1-only: the td_transporter2 path builds the Location from the URI "
    "without client-side scheme validation, so an unsupported scheme is no longer "
    "rejected here (the binary handles/validates it). Kept for v1 reference."
)
def test_main_unsupported_scheme_returns_unsupported_scheme(
    monkeypatch, tmp_path, http_ok_url
):
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
        responder_url=http_ok_url,
    )
    assert rc == BadEndpointSchemeError.exit_code


def test_main_write_error_returns_write_failed(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(tmp_path / "request.yaml")
    out_dir = tmp_path / "out-is-a-dir"
    out_dir.mkdir()
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "in",
        tmp_path / "state",
        out_dir,
        tmp_path / "inputs.yaml",
        tmp_path / "state.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == WriteFailedError.exit_code


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
        tmp_path / "in",
        tmp_path / "state",
        tmp_path / "transporter.yaml",
        bad_target,
        tmp_path / "state.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == InputsManifestWriteFailedError.exit_code


def test_main_state_manifest_write_failure_maps_to_state_manifest_failed(
    monkeypatch, tmp_path, http_ok_url
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
        responder_url=http_ok_url,
    )
    assert rc == StateManifestWriteFailedError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prep-localize-state", "--request", "x"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_prep_localize_state"
