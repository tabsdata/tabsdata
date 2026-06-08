#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.prep_upload_tables import (
    EXIT_BAD_MANIFEST,
    EXIT_BAD_REQUEST,
    EXIT_MANIFEST_MISMATCH,
    EXIT_UNSUPPORTED_SCHEME,
    EXIT_WRITE_FAILED,
    main,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _set_argv(
    monkeypatch, request_path, manifest_path, out, liveness, *, responder_url
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prep-upload-tables",
            "--request",
            str(request_path),
            "--outputs-manifest",
            str(manifest_path),
            "--out",
            str(out),
            "--liveness",
            str(liveness),
            "--responder-url",
            responder_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer wtok",
        ],
    )


def _write_request(
    path: Path,
    *,
    output: list | None = None,
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
        "output": output or [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


def _write_manifest(path: Path, entries: list) -> Path:
    path.write_text(yaml.safe_dump({"outputs": entries}))
    return path


def main_with_args(
    monkeypatch, request_path, manifest_path, out, liveness, *, responder_url
):
    _set_argv(
        monkeypatch,
        request_path,
        manifest_path,
        out,
        liveness,
        responder_url=responder_url,
    )
    return main()


# --- happy path -----------------------------------------------------------


def test_main_writes_transporter_yaml(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        output=[
            {"name": "t1", "location": {"uri": "s3://bucket/t1.parquet"}},
        ],
    )
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [{"name": "t1", "file": "/data/slot0/t1.parquet"}],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    text = out.read_text()
    assert text.startswith("!CopyV1")
    assert "!LocalFile" in text
    assert "!S3" in text
    assert "s3://bucket/t1.parquet" in text
    assert "/data/slot0/t1.parquet" in text
    assert (tmp_path / "live").exists()


def test_main_skips_null_file(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        output=[
            {"name": "t1", "location": {"uri": "s3://bucket/t1.parquet"}},
            {"name": "t2", "location": {"uri": "s3://bucket/t2.parquet"}},
        ],
    )
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [
            {"name": "t1", "file": "/data/t1.parquet"},
            {"name": "t2", "file": None},
        ],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    text = out.read_text()
    assert "/data/t1.parquet" in text
    # t2 has file: null → should not be uploaded
    assert "s3://bucket/t2.parquet" not in text


def test_main_skips_location_none(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        output=[
            {"name": "t1"},  # no location → apiserver didn't allocate
        ],
    )
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [{"name": "t1", "file": "/data/t1.parquet"}],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert "source_target_pairs: []" in out.read_text()


def test_main_handles_empty_output_and_manifest(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert "source_target_pairs: []" in out.read_text()


def test_main_preserves_request_order(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        output=[
            {"name": "t2", "location": {"uri": "s3://bucket/t2.parquet"}},
            {"name": "t1", "location": {"uri": "s3://bucket/t1.parquet"}},
        ],
    )
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [
            {"name": "t1", "file": "/data/t1.parquet"},
            {"name": "t2", "file": "/data/t2.parquet"},
        ],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == 0
    text = out.read_text()
    # t2 must appear before t1 — the upload order follows request.output
    assert text.index("t2.parquet") < text.index("t1.parquet")


# --- error paths ----------------------------------------------------------


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path, http_ok_url):
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    rc = main_with_args(
        monkeypatch,
        tmp_path / "no.yaml",
        manifest,
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_BAD_REQUEST


def test_main_malformed_request_returns_bad_request(monkeypatch, tmp_path, http_ok_url):
    bad = tmp_path / "request.yaml"
    bad.write_text("info: { unclosed")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    rc = main_with_args(
        monkeypatch,
        bad,
        manifest,
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_BAD_REQUEST


def test_main_missing_manifest_returns_bad_manifest(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(tmp_path / "request.yaml")
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "no.yaml",
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_BAD_MANIFEST


def test_main_malformed_manifest_returns_bad_manifest(
    monkeypatch, tmp_path, http_ok_url
):
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad = tmp_path / "outputs_manifest.yaml"
    bad.write_text("outputs: [{unclosed")
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        bad,
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_BAD_MANIFEST


def test_main_table_missing_from_manifest_returns_mismatch(
    monkeypatch, tmp_path, http_ok_url
):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        output=[
            {"name": "t1", "location": {"uri": "s3://bucket/t1.parquet"}},
        ],
    )
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_MANIFEST_MISMATCH


def test_main_unsupported_scheme_returns_unsupported_scheme(
    monkeypatch, tmp_path, http_ok_url
):
    request_yaml = _write_request(
        tmp_path / "request.yaml",
        output=[
            {"name": "t1", "location": {"uri": "ftp://server/t1.parquet"}},
        ],
    )
    manifest = _write_manifest(
        tmp_path / "outputs_manifest.yaml",
        [{"name": "t1", "file": "/data/t1.parquet"}],
    )
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        tmp_path / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_UNSUPPORTED_SCHEME


def test_main_write_error_returns_write_failed(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "outputs_manifest.yaml", [])
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        tmp_path / "no" / "such" / "out.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert rc == EXIT_WRITE_FAILED


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prep-upload-tables", "--request", "x"])
    assert main() == 1


# --- decorator metadata ---------------------------------------------------


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_prep_upload_tables"
