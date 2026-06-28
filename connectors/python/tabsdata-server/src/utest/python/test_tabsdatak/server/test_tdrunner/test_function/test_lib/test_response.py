#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function._lib.response import (
    InvalidResponseError,
    Response,
    TableEntry,
    TableInfo,
    load_response,
    write_response,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _write(tmp_path: Path, body: str, name: str = "response.yaml") -> Path:
    p = tmp_path / name
    p.write_text(body)
    return p


# --- roundtrip ---


def test_roundtrip_empty_output(tmp_path):
    resp = Response(output=())
    out = tmp_path / "response.yaml"
    write_response(resp, out)
    assert load_response(out) == resp


def test_roundtrip_data_only(tmp_path):
    resp = Response(
        output=(
            TableEntry(
                table="customers",
                info=TableInfo(column_count=5, row_count=1000, schema_hash="abc"),
            ),
        )
    )
    out = tmp_path / "response.yaml"
    write_response(resp, out)
    assert load_response(out) == resp


def test_roundtrip_no_data_only(tmp_path):
    resp = Response(output=(TableEntry(table="orders"),))
    out = tmp_path / "response.yaml"
    write_response(resp, out)
    assert load_response(out) == resp


def test_roundtrip_mixed(tmp_path):
    resp = Response(
        output=(
            TableEntry(
                table="customers",
                info=TableInfo(column_count=5, row_count=1000, schema_hash="abc"),
            ),
            TableEntry(table="orders"),
            TableEntry(
                table="td-initial-values",
                info=TableInfo(column_count=2, row_count=1, schema_hash="xyz"),
            ),
        )
    )
    out = tmp_path / "response.yaml"
    write_response(resp, out)
    assert load_response(out) == resp


def test_write_produces_plain_yaml(tmp_path):
    """Output should not contain custom YAML tags (forward-compat)."""
    resp = Response(
        output=(
            TableEntry(
                table="t",
                info=TableInfo(column_count=1, row_count=1, schema_hash="h"),
            ),
            TableEntry(table="u"),
        )
    )
    out = tmp_path / "response.yaml"
    write_response(resp, out)
    text = out.read_text()
    assert "!Data" not in text
    assert "!NoData" not in text
    assert "!Function" not in text
    # safe_load (no custom constructors) must accept it
    parsed = yaml.safe_load(text)
    assert parsed == {
        "output": [
            {
                "table": "t",
                "info": {"column_count": 1, "row_count": 1, "schema_hash": "h"},
            },
            {"table": "u"},
        ]
    }


# --- write_response error paths ---


def test_write_creates_missing_parent_dirs(tmp_path):
    # Under the publisher chain `report-completion` writes
    # `shared/response/response.yaml` on the shared emptyDir which
    # starts out empty (the image-template subdir is overlaid by the
    # mount); the writer creates the chain on demand rather than
    # raising OSError.
    target = tmp_path / "no" / "such" / "dir" / "response.yaml"
    write_response(Response(output=()), target)
    assert target.exists()


def test_write_to_directory_path_raises_oserror(tmp_path):
    """Writing to a path that's a directory should raise OSError."""
    with pytest.raises(OSError):
        write_response(Response(output=()), tmp_path)


# --- load_response error paths ---


def test_load_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_response(tmp_path / "nope.yaml")


def test_load_malformed_yaml(tmp_path):
    p = _write(tmp_path, "output: { unclosed")
    with pytest.raises(InvalidResponseError, match="malformed YAML"):
        load_response(p)


def test_load_top_level_not_a_mapping(tmp_path):
    p = _write(tmp_path, "- a\n- b\n")
    with pytest.raises(InvalidResponseError):
        load_response(p)


def test_load_missing_output(tmp_path):
    p = _write(tmp_path, "other: 1\n")
    with pytest.raises(InvalidResponseError, match="output"):
        load_response(p)


def test_load_output_not_a_list(tmp_path):
    p = _write(tmp_path, "output: not-a-list\n")
    with pytest.raises(InvalidResponseError, match="output"):
        load_response(p)


def test_load_entry_not_a_mapping(tmp_path):
    p = _write(tmp_path, "output:\n  - just-a-string\n")
    with pytest.raises(InvalidResponseError, match="output"):
        load_response(p)


def test_load_entry_missing_table(tmp_path):
    p = _write(
        tmp_path,
        "output:\n  - info: {column_count: 1, row_count: 1, schema_hash: h}\n",
    )
    with pytest.raises(InvalidResponseError, match="table"):
        load_response(p)


def test_load_table_wrong_type(tmp_path):
    p = _write(tmp_path, "output:\n  - table: [1, 2, 3]\n")
    with pytest.raises(InvalidResponseError, match="table"):
        load_response(p)


def test_load_info_wrong_type(tmp_path):
    p = _write(tmp_path, "output:\n  - table: t\n    info: not-a-mapping\n")
    with pytest.raises(InvalidResponseError, match="info"):
        load_response(p)


def test_load_info_explicit_null_yields_no_data(tmp_path):
    p = _write(tmp_path, "output:\n  - table: t\n    info: null\n")
    resp = load_response(p)
    assert resp == Response(output=(TableEntry(table="t", info=None),))


@pytest.mark.parametrize(
    "missing_field", ["column_count", "row_count", "schema_hash"]
)
def test_load_info_missing_required_field(tmp_path, missing_field):
    fields = {"column_count": 1, "row_count": 1, "schema_hash": "h"}
    fields.pop(missing_field)
    body = "output:\n  - table: t\n    info:\n"
    for k, v in fields.items():
        body += f"      {k}: {v if not isinstance(v, str) else repr(v)}\n"
    with pytest.raises(InvalidResponseError, match=missing_field):
        load_response(_write(tmp_path, body))


def test_load_info_column_count_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        "output:\n  - table: t\n    info:\n      column_count: 'oops'\n      row_count: 1\n      schema_hash: h\n",
    )
    with pytest.raises(InvalidResponseError, match="column_count"):
        load_response(p)


def test_load_info_row_count_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        "output:\n  - table: t\n    info:\n      column_count: 1\n      row_count: 'oops'\n      schema_hash: h\n",
    )
    with pytest.raises(InvalidResponseError, match="row_count"):
        load_response(p)


def test_load_info_schema_hash_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        "output:\n  - table: t\n    info:\n      column_count: 1\n      row_count: 1\n      schema_hash: [1, 2]\n",
    )
    with pytest.raises(InvalidResponseError, match="schema_hash"):
        load_response(p)


# --- extra="forbid": unknown fields are rejected ---


def test_load_extra_field_at_top_level_is_forbidden(tmp_path):
    p = _write(tmp_path, "output: []\nunknown_top_level: hi\n")
    with pytest.raises(InvalidResponseError, match="unknown_top_level"):
        load_response(p)


def test_load_extra_field_in_entry_is_forbidden(tmp_path):
    p = _write(tmp_path, "output:\n  - table: t\n    surprise: 1\n")
    with pytest.raises(InvalidResponseError, match="surprise"):
        load_response(p)


def test_load_extra_field_in_info_is_forbidden(tmp_path):
    p = _write(
        tmp_path,
        "output:\n  - table: t\n    info:\n      column_count: 1\n      row_count: 1\n      schema_hash: h\n      surprise: 1\n",
    )
    with pytest.raises(InvalidResponseError, match="surprise"):
        load_response(p)


# --- frozen ---


def test_response_is_frozen():
    resp = Response(output=())
    with pytest.raises(Exception):
        resp.output = (TableEntry(table="x"),)  # type: ignore[misc]


def test_table_entry_is_frozen():
    e = TableEntry(table="x")
    with pytest.raises(Exception):
        e.table = "y"  # type: ignore[misc]


def test_table_info_is_frozen():
    info = TableInfo(column_count=1, row_count=1, schema_hash="h")
    with pytest.raises(Exception):
        info.column_count = 2  # type: ignore[misc]
