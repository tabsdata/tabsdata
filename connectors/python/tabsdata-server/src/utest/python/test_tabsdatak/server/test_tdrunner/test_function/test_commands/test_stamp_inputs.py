#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the stamp-inputs command.

Covers:
  * `_load_inputs_manifest_or_fail` exception-mapping shim
  * `_stamp_file` (read parquet → stamp → write parquet)
  * `main()` error paths (bad request, bad manifest)
  * end-to-end happy path: write a raw parquet + inputs manifest by
    hand, then invoke `main()`, verify enriched parquet has the five
    system columns with the right values
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest
import yaml

from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    write_inputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.system_columns import (
    EMPTY_TIMESTAMP,
    EMPTY_VERSION,
    SYSTEM_COLUMNS,
    TD_IDENTIFIER,
    TD_VER_EXECUTION,
    TD_VER_TIMESTAMP,
    TD_VER_TRANSACTION,
    TD_VER_VERSION,
)
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    load_inputs_manifest_or_fail,
)
from tabsdatak.server._tdrunner.function.commands.stamp_inputs import (
    EXIT_BAD_INPUT_PARQUET,
    EXIT_BAD_MANIFEST,
    EXIT_BAD_REQUEST,
    EXIT_MANIFEST_WRITE_FAILED,
    EXIT_STAMP_FAILED,
    _stamp_file,
    main,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers -------------------------------------------------------------


def _write_request(path: Path) -> Path:
    """Minimal request.yaml — stamp-inputs only reads execution_id and
    transaction_id from info."""
    path.write_text(
        yaml.safe_dump(
            {
                "FunctionV2": {
                    "info": {
                        "collection": "c",
                        "collection_id": 1,
                        "function_version_id": "FV-1",
                        "function_defined_on": 1780000000000,
                        "function": "fn",
                        "function_run_id": 7,
                        "function_bundle": {"uri": "file:///bundle.tgz"},
                        "function_stage": {"uri": "file:///stage"},
                        "triggered_on": 1780000000100,
                        "transaction_id": "tx-42",
                        "execution_id": "exec-42",
                        "function_data": {"uri": "file:///data"},
                        "scheduled_on": 1780000000200,
                    },
                }
            }
        )
    )
    return path


def _write_user_parquet(path: Path) -> Path:
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def _set_argv(
    monkeypatch,
    request_path: Path,
    inputs_manifest: Path,
    enriched_dir: Path,
    enriched_manifest: Path,
    liveness: Path,
    *,
    responder_url: str,
    responder_method: str = "POST",
    responder_authorization: str = "Bearer tok",
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stamp-inputs",
            "--request",
            str(request_path),
            "--inputs-manifest",
            str(inputs_manifest),
            "--enriched-dir",
            str(enriched_dir),
            "--enriched-inputs-manifest",
            str(enriched_manifest),
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


# --- unit: _load_inputs_manifest_or_fail ---------------------------------


def test_load_manifest_shim_happy_path(tmp_path):
    m = tmp_path / "m.yaml"
    write_inputs_manifest(
        [{MANIFEST_NAME_KEY: "in_1", MANIFEST_FILES_KEY: ["/x.parquet"]}], m
    )
    out = load_inputs_manifest_or_fail(m, EXIT_BAD_MANIFEST)
    assert out == [{"name": "in_1", "files": ["/x.parquet"]}]


def test_load_manifest_shim_missing_file_maps_to_bad_manifest(tmp_path):
    with pytest.raises(CommandSpecificError) as exc_info:
        load_inputs_manifest_or_fail(tmp_path / "nope.yaml", EXIT_BAD_MANIFEST)
    assert exc_info.value.exit_code == EXIT_BAD_MANIFEST


def test_load_manifest_shim_malformed_maps_to_bad_manifest(tmp_path):
    m = tmp_path / "m.yaml"
    m.write_text("inputs: [{unclosed")
    with pytest.raises(CommandSpecificError) as exc_info:
        load_inputs_manifest_or_fail(m, EXIT_BAD_MANIFEST)
    assert exc_info.value.exit_code == EXIT_BAD_MANIFEST


def test_load_manifest_shim_bad_shape_maps_to_bad_manifest(tmp_path):
    m = tmp_path / "m.yaml"
    m.write_text(yaml.safe_dump({"other": []}))
    with pytest.raises(CommandSpecificError) as exc_info:
        load_inputs_manifest_or_fail(m, EXIT_BAD_MANIFEST)
    assert exc_info.value.exit_code == EXIT_BAD_MANIFEST


# --- unit: _stamp_file --------------------------------------------------


def test_stamp_file_writes_enriched_parquet(tmp_path):
    src = _write_user_parquet(tmp_path / "src.parquet")
    dest = tmp_path / "enriched" / "out.parquet"

    _stamp_file(
        src,
        dest,
        execution="exec-1",
        transaction="tx-1",
        version=EMPTY_VERSION,
        timestamp=EMPTY_TIMESTAMP,
    )

    stamped = pl.read_parquet(dest)
    for col in SYSTEM_COLUMNS:
        assert col in stamped.columns
    assert stamped[TD_VER_EXECUTION].to_list() == ["exec-1"] * 3
    assert stamped[TD_VER_TRANSACTION].to_list() == ["tx-1"] * 3
    # User columns survive.
    assert stamped["id"].to_list() == [1, 2, 3]


def test_stamp_file_creates_missing_dest_parent(tmp_path):
    src = _write_user_parquet(tmp_path / "src.parquet")
    dest = tmp_path / "a" / "b" / "c" / "out.parquet"
    _stamp_file(
        src,
        dest,
        execution="e",
        transaction="t",
        version=EMPTY_VERSION,
        timestamp=EMPTY_TIMESTAMP,
    )
    assert dest.is_file()


def test_stamp_file_unreadable_src_raises_bad_input_parquet(tmp_path):
    bogus = tmp_path / "not-a-parquet.parquet"
    bogus.write_bytes(b"this is not parquet")
    dest = tmp_path / "out.parquet"
    with pytest.raises(CommandSpecificError) as exc_info:
        _stamp_file(
            bogus,
            dest,
            execution="e",
            transaction="t",
            version=EMPTY_VERSION,
            timestamp=EMPTY_TIMESTAMP,
        )
    assert exc_info.value.exit_code == EXIT_BAD_INPUT_PARQUET


def test_stamp_file_write_to_directory_raises_stamp_failed(tmp_path):
    src = _write_user_parquet(tmp_path / "src.parquet")
    a_dir = tmp_path / "i-am-a-dir"
    a_dir.mkdir()
    with pytest.raises(CommandSpecificError) as exc_info:
        _stamp_file(
            src,
            a_dir,  # writing to a directory triggers OSError
            execution="e",
            transaction="t",
            version=EMPTY_VERSION,
            timestamp=EMPTY_TIMESTAMP,
        )
    assert exc_info.value.exit_code == EXIT_STAMP_FAILED


# --- main: error paths --------------------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch, http_ok_url):
    _set_argv(
        monkeypatch,
        tmp_path / "no-such-request.yaml",
        tmp_path / "inputs.yaml",
        tmp_path / "enriched",
        tmp_path / "enriched.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == EXIT_BAD_REQUEST


def test_main_missing_inputs_manifest_returns_bad_manifest(
    tmp_path, monkeypatch, http_ok_url
):
    req = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        req,
        tmp_path / "no-manifest.yaml",
        tmp_path / "enriched",
        tmp_path / "enriched.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == EXIT_BAD_MANIFEST


def test_main_invalid_options_returns_1(monkeypatch):
    """argparse missing-required-arg → rc 1."""
    monkeypatch.setattr(sys, "argv", ["stamp-inputs", "--request", "x"])
    assert main() == 1


# --- main: end-to-end ---------------------------------------------------


def test_main_end_to_end_happy_path(tmp_path, monkeypatch, http_ok_url):
    """Lay down a raw parquet + inputs manifest, invoke main(), verify
    the enriched parquet has the five system columns and that the
    enriched manifest points to it."""
    request_path = _write_request(tmp_path / "request.yaml")

    raw_parquet = _write_user_parquet(tmp_path / "raw" / "rows.parquet")
    inputs_manifest = tmp_path / "inputs.yaml"
    write_inputs_manifest(
        [
            {
                MANIFEST_NAME_KEY: "in_1",
                MANIFEST_FILES_KEY: [str(raw_parquet)],
            }
        ],
        inputs_manifest,
    )

    enriched_dir = tmp_path / "enriched"
    enriched_manifest = tmp_path / "enriched.yaml"
    liveness = tmp_path / "live"

    _set_argv(
        monkeypatch,
        request_path,
        inputs_manifest,
        enriched_dir,
        enriched_manifest,
        liveness,
        responder_url=http_ok_url,
    )
    rc = main()
    assert rc == 0, f"main() returned {rc}"

    # One enriched parquet under slot0/.
    enriched_parquets = list(enriched_dir.rglob("*.parquet"))
    assert len(enriched_parquets) == 1
    assert enriched_parquets[0].parent.name == "slot0"

    # Schema has all five system columns.
    stamped = pl.read_parquet(enriched_parquets[0])
    for col in SYSTEM_COLUMNS:
        assert col in stamped.columns

    # Stamped values are the request-derived ones.
    assert stamped[TD_VER_EXECUTION].to_list() == ["exec-42"] * 3
    assert stamped[TD_VER_TRANSACTION].to_list() == ["tx-42"] * 3
    assert stamped[TD_VER_VERSION].to_list() == [EMPTY_VERSION] * 3
    # Timestamp dtype + value.
    dtype = stamped.schema[TD_VER_TIMESTAMP]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_unit == "us"
    assert dtype.time_zone == "UTC"
    # idx column is per-row unique UUIDs.
    ids = stamped[TD_IDENTIFIER].to_list()
    assert len(ids) == 3
    assert len(set(ids)) == 3

    # Enriched manifest schema mirrors the input one, with rewritten paths.
    data = yaml.safe_load(enriched_manifest.read_text())
    assert list(data.keys()) == ["inputs"]
    assert len(data["inputs"]) == 1
    entry = data["inputs"][0]
    assert entry["name"] == "in_1"
    assert len(entry["files"]) == 1
    assert Path(entry["files"][0]).samefile(enriched_parquets[0])

    assert liveness.exists()


def test_main_empty_slot_passes_through(tmp_path, monkeypatch, http_ok_url):
    """If a slot has zero files, the enriched manifest mirrors that:
    same slot name, empty files list. No parquet rewritten."""
    request_path = _write_request(tmp_path / "request.yaml")

    inputs_manifest = tmp_path / "inputs.yaml"
    write_inputs_manifest(
        [{MANIFEST_NAME_KEY: "in_1", MANIFEST_FILES_KEY: []}],
        inputs_manifest,
    )

    enriched_dir = tmp_path / "enriched"
    enriched_manifest = tmp_path / "enriched.yaml"
    liveness = tmp_path / "live"

    _set_argv(
        monkeypatch,
        request_path,
        inputs_manifest,
        enriched_dir,
        enriched_manifest,
        liveness,
        responder_url=http_ok_url,
    )
    assert main() == 0

    data = yaml.safe_load(enriched_manifest.read_text())
    assert data == {"inputs": [{"name": "in_1", "files": []}]}


# --- decorator metadata -------------------------------------------------


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_stamp_inputs"
