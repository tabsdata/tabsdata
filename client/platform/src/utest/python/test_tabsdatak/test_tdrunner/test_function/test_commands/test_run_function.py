#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the run-function command.

Covers:
  * Helper-function unit tests (`_load_function_def`,
    `_load_inputs_manifest_or_fail`, `_resolve_src_def`,
    `_materialise_input_slot`, `_invoke_user_function`,
    `_sink_outputs`).
  * `main()` error paths and end-to-end happy path: bundle a real
    publisher, write an inputs manifest pointing at a real parquet,
    invoke `main()`, verify the output parquet contents + the
    outputs manifest.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import polars as pl
import pytest
import yaml

from tabsdatak._bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak._info import PublisherInfo
from tabsdatak._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY as INPUTS_NAME_KEY,
    write_inputs_manifest,
)
from tabsdatak._tdrunner.function.commands.run_function import (
    EXIT_BAD_BUNDLE,
    EXIT_BAD_INPUT_PARQUET,
    EXIT_BAD_MANIFEST,
    EXIT_BAD_REQUEST,
    EXIT_BAD_USER_CODE,
    EXIT_NO_PLUGIN,
    EXIT_OUTPUT_SHAPE_MISMATCH,
    EXIT_OUTPUT_WRITE_FAILED,
    EXIT_OUTPUTS_MANIFEST_WRITE_FAILED,
    EXIT_USER_FUNCTION_FAILED,
    _invoke_user_function,
    _load_function_def,
    _load_inputs_manifest_or_fail,
    _materialise_input_slot,
    _resolve_src_def,
    _scan_parquet_or_fail,
    _sink_outputs,
    main,
)
from tabsdatak.spi import TableMode

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak._tdrunner.function.commands.run_function"


# --- shared fixtures ----------------------------------------------------


_USER_PUBLISHER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileConn, LocalFileSrc

_SRC = LocalFileSrc(
    conn=LocalFileConn(base_path={base_path!r}),
    paths=["*.parquet"],
)


@publisher(source=_SRC, output_tables=["my_table", "untouched"])
def my_publisher(in_1: TableFramesSpec) -> tuple[TableFrameSpec, TableFrameSpec]:
    # Append a marker column to the first input frame; second output
    # is None to exercise the "not modified" branch of the outputs
    # manifest.
    import polars as pl
    if in_1 and in_1[0] is not None:
        first = in_1[0].with_columns(pl.lit("from_user_fn").alias("marker"))
    else:
        first = None
    return (first, None)
"""


def _write_sample_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).write_parquet(path)


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """Materialise a user project on disk with a real @publisher
    function, make it importable, return the project root."""
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()

    project = tmp_path / "project"
    pkg = project / "my_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "publisher.py").write_text(
        _USER_PUBLISHER_SRC.format(base_path=str(srcdata))
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    yield project
    for name in list(sys.modules):
        if name == "my_pkg" or name.startswith("my_pkg."):
            del sys.modules[name]


@pytest.fixture
def fixture_publisher(user_project) -> PublisherInfo:
    from my_pkg.publisher import my_publisher  # noqa: E402

    assert isinstance(my_publisher, PublisherInfo)
    return my_publisher


@pytest.fixture
def built_bundle(tmp_path, user_project, fixture_publisher) -> Path:
    bundle_path = tmp_path / "bundle.tar.gz"
    build_bundle(fixture_publisher, user_project, bundle_path)
    extracted = tmp_path / "extracted"
    extract_bundle(bundle_path, extracted)
    return extracted


def _write_request(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "info": {
                    "collection": "c",
                    "collection_id": 1,
                    "execution_id": 42,
                    "transaction_id": 99,
                    "function_run_id": 7,
                    "function_bundle": {"uri": "file:///bundle.tgz"},
                },
            }
        )
    )
    return path


def _set_argv(
    monkeypatch,
    request_path: Path,
    bundle_dir: Path,
    inputs_manifest: Path,
    out_dir: Path,
    outputs_manifest: Path,
    liveness: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-function",
            "--request",
            str(request_path),
            "--bundle-dir",
            str(bundle_dir),
            "--inputs-manifest",
            str(inputs_manifest),
            "--out-dir",
            str(out_dir),
            "--outputs-manifest",
            str(outputs_manifest),
            "--liveness",
            str(liveness),
        ],
    )


# --- unit: _load_function_def -------------------------------------------


def test_load_function_def_happy(built_bundle):
    info = _load_function_def(built_bundle)
    assert isinstance(info, PublisherInfo)
    assert list(info.output_tables) == ["my_table", "untouched"]


def test_load_function_def_missing_manifest_raises_bad_bundle(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(CommandSpecificError) as ei:
        _load_function_def(empty)
    assert ei.value.exit_code == EXIT_BAD_BUNDLE


def test_load_function_def_malformed_json_raises_bad_bundle(tmp_path):
    bad = tmp_path / "bad-bundle"
    bad.mkdir()
    (bad / BUNDLE_MANIFEST_FILENAME).write_text("{ not json")
    with pytest.raises(CommandSpecificError) as ei:
        _load_function_def(bad)
    assert ei.value.exit_code == EXIT_BAD_BUNDLE


def test_load_function_def_unresolvable_func_raises_bad_user_code(built_bundle):
    """Edit the manifest's func reference to point at a missing module.
    The import failure surfaces via pydantic ValidationError → mapped
    to EXIT_BAD_USER_CODE."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))
    with pytest.raises(CommandSpecificError) as ei:
        _load_function_def(built_bundle)
    assert ei.value.exit_code == EXIT_BAD_USER_CODE


# --- unit: _load_inputs_manifest_or_fail --------------------------------


def test_load_inputs_manifest_happy(tmp_path):
    m = tmp_path / "m.yaml"
    write_inputs_manifest(
        [{INPUTS_NAME_KEY: "in_1", MANIFEST_FILES_KEY: ["/x.parquet"]}], m
    )
    assert _load_inputs_manifest_or_fail(m) == [
        {"name": "in_1", "files": ["/x.parquet"]}
    ]


def test_load_inputs_manifest_missing_maps_to_bad_manifest(tmp_path):
    with pytest.raises(CommandSpecificError) as ei:
        _load_inputs_manifest_or_fail(tmp_path / "nope.yaml")
    assert ei.value.exit_code == EXIT_BAD_MANIFEST


# --- unit: _resolve_src_def ---------------------------------------------


def test_resolve_src_def_happy(fixture_publisher):
    src_def = _resolve_src_def(fixture_publisher)
    # LocalFileSrc is configured ARRAY mode in LOCALFILE_SRC.
    assert src_def.table_mode is TableMode.ARRAY


def test_resolve_src_def_unknown_source_raises_no_plugin(
    fixture_publisher, monkeypatch
):
    def _raise(_):
        raise KeyError("no connector registered")

    monkeypatch.setattr(f"{_MOD}.get_src", _raise)
    with pytest.raises(CommandSpecificError) as ei:
        _resolve_src_def(fixture_publisher)
    assert ei.value.exit_code == EXIT_NO_PLUGIN


# --- unit: _scan_parquet_or_fail ----------------------------------------


def test_scan_parquet_happy(tmp_path):
    p = tmp_path / "x.parquet"
    _write_sample_parquet(p)
    lf = _scan_parquet_or_fail(p)
    assert isinstance(lf, pl.LazyFrame)


def test_scan_parquet_garbage_raises_bad_input_parquet(tmp_path):
    p = tmp_path / "x.parquet"
    p.write_bytes(b"not a parquet")
    with pytest.raises(CommandSpecificError) as ei:
        _scan_parquet_or_fail(p)
    assert ei.value.exit_code == EXIT_BAD_INPUT_PARQUET


# --- unit: _materialise_input_slot --------------------------------------


def test_materialise_single_zero_files_is_none(tmp_path):
    assert _materialise_input_slot([], table_mode=TableMode.SINGLE) is None


def test_materialise_single_one_file_is_lazyframe(tmp_path):
    p = tmp_path / "x.parquet"
    _write_sample_parquet(p)
    out = _materialise_input_slot([str(p)], table_mode=TableMode.SINGLE)
    assert isinstance(out, pl.LazyFrame)


def test_materialise_single_two_files_raises_bad_manifest(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write_sample_parquet(a)
    _write_sample_parquet(b)
    with pytest.raises(CommandSpecificError) as ei:
        _materialise_input_slot([str(a), str(b)], table_mode=TableMode.SINGLE)
    assert ei.value.exit_code == EXIT_BAD_MANIFEST


def test_materialise_array_returns_list_of_lazyframes(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write_sample_parquet(a)
    _write_sample_parquet(b)
    out = _materialise_input_slot(
        [str(a), str(b)], table_mode=TableMode.ARRAY
    )
    assert isinstance(out, list)
    assert len(out) == 2
    assert all(isinstance(x, pl.LazyFrame) for x in out)


def test_materialise_array_empty_is_empty_list():
    out = _materialise_input_slot([], table_mode=TableMode.ARRAY)
    assert out == []


# --- unit: _invoke_user_function ----------------------------------------


def _fake_lf() -> pl.LazyFrame:
    return pl.DataFrame({"x": [1]}).lazy()


def test_invoke_user_function_happy():
    def fn(in_1):
        return (in_1, None)

    result = _invoke_user_function(
        fn, [_fake_lf()], ["t1", "t2"]
    )
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], pl.LazyFrame)
    assert result[1] is None


def test_invoke_user_function_raises_maps_to_user_function_failed():
    def fn(in_1):
        raise RuntimeError("boom")

    with pytest.raises(CommandSpecificError) as ei:
        _invoke_user_function(fn, [_fake_lf()], ["t1"])
    assert ei.value.exit_code == EXIT_USER_FUNCTION_FAILED


def test_invoke_user_function_non_tuple_return_raises_shape_mismatch():
    def fn(in_1):
        return _fake_lf()  # missing tuple wrap

    with pytest.raises(CommandSpecificError) as ei:
        _invoke_user_function(fn, [_fake_lf()], ["t1"])
    assert ei.value.exit_code == EXIT_OUTPUT_SHAPE_MISMATCH


def test_invoke_user_function_wrong_length_raises_shape_mismatch():
    def fn(in_1):
        return (_fake_lf(), _fake_lf())  # returns 2, expects 1

    with pytest.raises(CommandSpecificError) as ei:
        _invoke_user_function(fn, [_fake_lf()], ["t1"])
    assert ei.value.exit_code == EXIT_OUTPUT_SHAPE_MISMATCH


def test_invoke_user_function_wrong_element_type_raises_shape_mismatch():
    def fn(in_1):
        return ("not a lazyframe",)

    with pytest.raises(CommandSpecificError) as ei:
        _invoke_user_function(fn, [_fake_lf()], ["t1"])
    assert ei.value.exit_code == EXIT_OUTPUT_SHAPE_MISMATCH


# --- unit: _sink_outputs ------------------------------------------------


def test_sink_outputs_writes_each_lazyframe(tmp_path):
    result = (_fake_lf(), _fake_lf())
    entries = _sink_outputs(result, ["t1", "t2"], tmp_path)
    assert len(entries) == 2
    for i, entry in enumerate(entries):
        assert entry["name"] == f"t{i + 1}"
        assert entry["file"] is not None
        assert Path(entry["file"]).is_file()
        # File path layout matches the documented slot<i>/<name>.parquet.
        assert Path(entry["file"]).parent.name == f"slot{i}"
        assert Path(entry["file"]).name == f"t{i + 1}.parquet"


def test_sink_outputs_none_value_records_null_file(tmp_path):
    result = (None, _fake_lf())
    entries = _sink_outputs(result, ["t1", "t2"], tmp_path)
    assert entries[0] == {"name": "t1", "file": None}
    assert entries[1]["file"] is not None
    # No slot0 dir should have been created (we skipped the sink).
    assert not (tmp_path / "slot0").exists()
    assert (tmp_path / "slot1").is_dir()


def test_sink_outputs_write_failure_maps_to_output_write_failed(
    tmp_path, monkeypatch
):
    def _broken_sink(self, dest):
        raise RuntimeError("disk full")

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", _broken_sink)
    with pytest.raises(CommandSpecificError) as ei:
        _sink_outputs((_fake_lf(),), ["t1"], tmp_path)
    assert ei.value.exit_code == EXIT_OUTPUT_WRITE_FAILED


# --- main: error paths --------------------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch):
    _set_argv(
        monkeypatch,
        tmp_path / "no-such.yaml",
        tmp_path / "bundle",
        tmp_path / "inputs.yaml",
        tmp_path / "out",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_REQUEST


def test_main_missing_bundle_returns_bad_bundle(tmp_path, monkeypatch):
    req = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        req,
        tmp_path / "no-bundle",
        tmp_path / "inputs.yaml",
        tmp_path / "out",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_BUNDLE


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run-function", "--request", "x"])
    assert main() == 1


# --- main: end-to-end ---------------------------------------------------


def test_main_end_to_end_happy_path(tmp_path, built_bundle, monkeypatch):
    """Real publisher, real bundle, real inputs manifest pointing at a
    real parquet. Invoke main(); verify outputs."""
    # Write the request.
    request_path = _write_request(tmp_path / "request.yaml")

    # Write a sample parquet and an inputs manifest pointing at it.
    raw_parquet = tmp_path / "raw" / "rows.parquet"
    _write_sample_parquet(raw_parquet)
    inputs_manifest = tmp_path / "inputs.yaml"
    write_inputs_manifest(
        [{INPUTS_NAME_KEY: "in_1", MANIFEST_FILES_KEY: [str(raw_parquet)]}],
        inputs_manifest,
    )

    out_dir = tmp_path / "fn_out"
    outputs_manifest = tmp_path / "outputs.yaml"
    liveness = tmp_path / "live"

    _set_argv(
        monkeypatch,
        request_path,
        built_bundle,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        liveness,
    )
    rc = main()
    assert rc == 0, f"main() returned {rc}"

    # One output parquet was written for `my_table` (the function
    # returned a LazyFrame), nothing for `untouched` (function
    # returned None).
    my_table_file = out_dir / "slot0" / "my_table.parquet"
    assert my_table_file.is_file()
    assert not (out_dir / "slot1" / "untouched.parquet").exists()

    # The user function added a `marker` column; the underlying data
    # comes through unchanged.
    df = pl.read_parquet(my_table_file)
    assert "marker" in df.columns
    assert df["marker"].to_list() == ["from_user_fn"] * 3
    assert df["id"].to_list() == [1, 2, 3]
    assert df["name"].to_list() == ["a", "b", "c"]

    # Outputs manifest shape: my_table → file, untouched → null.
    data = yaml.safe_load(outputs_manifest.read_text())
    assert list(data.keys()) == ["outputs"]
    assert len(data["outputs"]) == 2
    e1, e2 = data["outputs"]
    assert e1["name"] == "my_table"
    assert e1["file"] is not None
    assert Path(e1["file"]).samefile(my_table_file)
    assert e2 == {"name": "untouched", "file": None}

    assert liveness.exists()


def test_main_outputs_manifest_write_failure(tmp_path, built_bundle, monkeypatch):
    """If the outputs-manifest write fails, surface EXIT_OUTPUTS_MANIFEST_WRITE_FAILED."""
    request_path = _write_request(tmp_path / "request.yaml")
    raw_parquet = tmp_path / "raw" / "rows.parquet"
    _write_sample_parquet(raw_parquet)
    inputs_manifest = tmp_path / "inputs.yaml"
    write_inputs_manifest(
        [{INPUTS_NAME_KEY: "in_1", MANIFEST_FILES_KEY: [str(raw_parquet)]}],
        inputs_manifest,
    )

    # Point --outputs-manifest at an existing directory → write fails.
    bad_target = tmp_path / "i-am-a-dir"
    bad_target.mkdir()

    _set_argv(
        monkeypatch,
        request_path,
        built_bundle,
        inputs_manifest,
        tmp_path / "fn_out",
        bad_target,
        tmp_path / "live",
    )
    assert main() == EXIT_OUTPUTS_MANIFEST_WRITE_FAILED


# --- decorator metadata -------------------------------------------------


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_run_function"
