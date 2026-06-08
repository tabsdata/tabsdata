#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.prep_upload_state import (
    EXIT_BAD_REQUEST,
    EXIT_UNSUPPORTED_SCHEME,
    EXIT_WRITE_FAILED,
    main,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _set_argv(
    monkeypatch,
    request_path,
    state_dir,
    out,
    liveness,
    *,
    responder_url,
    responder_method="POST",
    responder_authorization="Bearer tok",
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prep-upload-state",
            "--request",
            str(request_path),
            "--state-dir",
            str(state_dir),
            "--out",
            str(out),
            "--liveness",
            str(liveness),
            "--responder-url",
            responder_url,
            "--responder-method",
            responder_method,
            "--responder-authorization",
            responder_authorization,
        ],
    )


def _write_request(
    path: Path,
    *,
    system_output: list | None = None,
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
        "system_output": system_output or [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


def main_with_args(
    monkeypatch, request_path, state_dir, out, liveness, *, responder_url
):
    _set_argv(
        monkeypatch,
        request_path,
        state_dir,
        out,
        liveness,
        responder_url=responder_url,
    )
    return main()


# --- happy path ---


def test_main_writes_transporter_yaml(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_output=[
            {
                "name": "td-initial-values",
                "location": {"uri": "s3://bucket/state-out.parquet"},
            },
        ],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "state",
        out,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    text = out.read_text()
    assert text.startswith("!CopyV1")
    assert "!LocalFile" in text
    assert "!S3" in text
    assert "s3://bucket/state-out.parquet" in text
    assert (tmp_path / "live").exists()


def test_main_handles_empty_system_output(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(tmp_path / "request.yaml")
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "state",
        out,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert "source_target_pairs: []" in out.read_text()


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path, http_ok_url):
    rc = main_with_args(
        monkeypatch,
        tmp_path / "no-request.yaml",
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_BAD_REQUEST


def test_main_malformed_request_returns_bad_request(
    monkeypatch, tmp_path, http_ok_url
):
    bad = tmp_path / "request.yaml"
    bad.write_text("info: { unclosed")
    rc = main_with_args(
        monkeypatch,
        bad,
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_BAD_REQUEST


def test_main_unsupported_scheme_returns_unsupported_scheme(
    monkeypatch, tmp_path, http_ok_url
):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        system_output=[
            {"name": "x", "location": {"uri": "ftp://server/x.parquet"}},
        ],
    )
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "state",
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_UNSUPPORTED_SCHEME


def test_main_write_error_returns_write_failed(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(tmp_path / "request.yaml")
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "state",
        tmp_path / "no" / "such" / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_WRITE_FAILED


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prep-upload-state", "--request", "x"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_prep_upload_state"
