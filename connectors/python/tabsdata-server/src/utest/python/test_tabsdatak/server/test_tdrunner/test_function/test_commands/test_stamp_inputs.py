#
# Copyright 2026 Tabsdata Inc.
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

from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    build_entry,
    build_table_record,
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
from tabsdatak.server._tdrunner.function.commands.stamp_inputs import _stamp_file, main
from tabsdatak.server._tdrunner.function.commands.stamp_inputs.error import (
    BadManifestError,
    BadRequestError,
    StampFailedError,
)
from tabsdatak.tableframe._engine.model.frame import DataVersion
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers -------------------------------------------------------------


def _write_request(path: Path) -> Path:
    """Minimal request.yaml -- stamp-inputs only reads execution_id and
    transaction_id from info. ``exec-42`` / ``tx-42`` are asserted on
    in the stamped-parquet test, so they're passed as explicit
    overrides instead of letting the helper's defaults through."""
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(
                    function_run_id=7,
                    transaction_id="tx-42",
                    execution_id="exec-42",
                ),
            }
        })
    )
    return path


def _write_user_parquet(path: Path) -> Path:
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def _data_version(
    *,
    execution: str = "exec-1",
    transaction: str = "tx-1",
    version: str = EMPTY_VERSION,
    timestamp=EMPTY_TIMESTAMP,
) -> DataVersion:
    return (
        DataVersion
        .builder()
        .with_execution(execution)
        .with_transaction(transaction)
        .with_version(version)
        .with_timestamp(timestamp)
        .build()
    )


def _slot_entry(name: str, table_pos: int, files: list[str]) -> dict:
    """Build an inputs-manifest entry from a list of parquet paths.

    One table record per file, each with a sequential ``input_idx`` and
    the single-version sentinel ``version_pos`` (-1).
    """
    return build_entry(
        name=name,
        table_pos=table_pos,
        tables=[
            build_table_record(
                file=file, metadata=None, input_idx=index, version_pos=-1
            )
            for index, file in enumerate(files)
        ],
    )


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
            "--journal-file",
            "/tmp/em.yaml",
        ],
    )


# --- unit: _stamp_file --------------------------------------------------


def test_stamp_file_writes_enriched_parquet(tmp_path):
    src = _write_user_parquet(tmp_path / "src.parquet")
    dest = tmp_path / "enriched" / "out.parquet"

    _stamp_file(
        src,
        dest,
        version=_data_version(execution="exec-1", transaction="tx-1"),
        idx=0,
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
        version=_data_version(execution="e", transaction="t"),
        idx=0,
    )
    assert dest.is_file()


def test_stamp_file_unreadable_src_raises_stamp_failed(tmp_path):
    # `pl.scan_parquet` is lazy, so a garbage file slips past the scan
    # and the parquet error surfaces when the TableFrame build reads the
    # schema -> mapped to StampFailedError (SI_4), not BadInputParquetError.
    bogus = tmp_path / "not-a-parquet.parquet"
    bogus.write_bytes(b"this is not parquet")
    dest = tmp_path / "out.parquet"
    with pytest.raises(StampFailedError):
        _stamp_file(
            bogus,
            dest,
            version=_data_version(execution="e", transaction="t"),
            idx=0,
        )


def test_stamp_file_write_to_directory_raises_stamp_failed(tmp_path):
    src = _write_user_parquet(tmp_path / "src.parquet")
    a_dir = tmp_path / "i-am-a-dir"
    a_dir.mkdir()
    with pytest.raises(StampFailedError):
        _stamp_file(
            src,
            a_dir,  # writing to a directory triggers OSError
            version=_data_version(execution="e", transaction="t"),
            idx=0,
        )


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
    assert main() == BadRequestError.exit_code


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
    assert main() == BadManifestError.exit_code


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
        [_slot_entry("in_1", 0, [str(raw_parquet)])],
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
    # The version is the empty sentinel broadcast across all rows; the
    # TableFrame engine re-encodes the UUID, so assert on the broadcast
    # invariant (one identical, non-null value per row) rather than the
    # raw EMPTY_VERSION literal.
    versions = stamped[TD_VER_VERSION].to_list()
    assert len(versions) == 3
    assert len(set(versions)) == 1
    assert versions[0] is not None
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
    assert len(entry["tables"]) == 1
    record = entry["tables"][0]
    assert Path(record["file"]).samefile(enriched_parquets[0])
    # stamp-inputs assigns each stamped record a unique input_idx.
    assert record["input_idx"] == 0

    assert liveness.exists()


def test_main_empty_slot_passes_through(tmp_path, monkeypatch, http_ok_url):
    """If a slot has zero files, the enriched manifest mirrors that:
    same slot name, empty files list. No parquet rewritten."""
    request_path = _write_request(tmp_path / "request.yaml")

    inputs_manifest = tmp_path / "inputs.yaml"
    write_inputs_manifest(
        [_slot_entry("in_1", 0, [])],
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
    assert data == {
        "inputs": [{"name": "in_1", "table_pos": 0, "is_empty": False, "tables": []}]
    }


# --- decorator metadata -------------------------------------------------


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_stamp_inputs"
