#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for `tabsdatak.server._tdrunner.function._lib.inputs_manifest`.

Covers the shared inputs-manifest reader/writer used by
`run-source-plugin`, `stamp-inputs`, and (eventually) `run-function`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_IS_EMPTY_KEY,
    MANIFEST_NAME_KEY,
    MANIFEST_TABLE_POS_KEY,
    MANIFEST_TABLES_KEY,
    InvalidManifestError,
    build_entry,
    build_table_record,
    load_inputs_manifest,
    write_inputs_manifest,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- constants -----------------------------------------------------------


def test_public_constants_match_yaml_schema():
    assert MANIFEST_NAME_KEY == "name"
    assert MANIFEST_TABLE_POS_KEY == "table_pos"
    assert MANIFEST_IS_EMPTY_KEY == "is_empty"
    assert MANIFEST_TABLES_KEY == "tables"
    assert MANIFEST_FILE_KEY == "file"


# --- load_inputs_manifest ------------------------------------------------


def _write_yaml(path: Path, data) -> Path:
    path.write_text(yaml.safe_dump(data))
    return path


def _entry(name: str, table_pos: int, files: list[str]) -> dict:
    return build_entry(
        name=name,
        table_pos=table_pos,
        tables=[
            build_table_record(file=file, input_idx=index, version_pos=-1)
            for index, file in enumerate(files)
        ],
    )


def test_load_happy_path(tmp_path):
    entries = [_entry("in_1", 0, ["/a.parquet"])]
    p = _write_yaml(tmp_path / "m.yaml", {"inputs": entries})
    assert load_inputs_manifest(p) == entries


def test_load_multiple_slots_preserves_order(tmp_path):
    entries = [
        _entry("in_1", 0, ["/a.parquet"]),
        _entry("in_2", 1, []),
        _entry("in_3", 2, ["/b.parquet", "/c.parquet"]),
    ]
    p = _write_yaml(tmp_path / "m.yaml", {"inputs": entries})
    assert load_inputs_manifest(p) == entries


def test_load_empty_inputs_list(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"inputs": []})
    assert load_inputs_manifest(p) == []


def test_load_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_inputs_manifest(tmp_path / "nope.yaml")


def test_load_malformed_yaml(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("inputs: [{unclosed")
    with pytest.raises(InvalidManifestError, match="malformed YAML"):
        load_inputs_manifest(p)


def test_load_top_level_not_mapping(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(InvalidManifestError, match="inputs"):
        load_inputs_manifest(p)


def test_load_missing_inputs_key(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"other": []})
    with pytest.raises(InvalidManifestError, match="inputs"):
        load_inputs_manifest(p)


def test_load_inputs_not_a_list(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"inputs": "not-a-list"})
    with pytest.raises(InvalidManifestError, match="must be a list"):
        load_inputs_manifest(p)


# --- write_inputs_manifest -----------------------------------------------


def test_write_then_load_roundtrip(tmp_path):
    entries = [_entry("in_1", 0, ["/x.parquet"])]
    p = tmp_path / "m.yaml"
    write_inputs_manifest(entries, p)
    assert load_inputs_manifest(p) == entries


def test_write_preserves_entry_order(tmp_path):
    entries = [
        _entry("in_2", 1, ["/b.parquet"]),
        _entry("in_1", 0, ["/a.parquet"]),
    ]
    p = tmp_path / "m.yaml"
    write_inputs_manifest(entries, p)
    data = yaml.safe_load(p.read_text())
    assert [e["name"] for e in data["inputs"]] == ["in_2", "in_1"]


def test_write_wraps_under_inputs_top_level_key(tmp_path):
    p = tmp_path / "m.yaml"
    write_inputs_manifest([], p)
    data = yaml.safe_load(p.read_text())
    assert "inputs" in data
    assert data["inputs"] == []


def test_write_creates_missing_parent_dir(tmp_path):
    nested = tmp_path / "a" / "b" / "m.yaml"
    write_inputs_manifest([_entry("x", 0, [])], nested)
    assert nested.is_file()


def test_write_to_directory_path_raises_oserror(tmp_path):
    d = tmp_path / "i-am-a-dir"
    d.mkdir()
    with pytest.raises(OSError):
        write_inputs_manifest([], d)
