#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the run-destination-plugin command.

Covers:
  * Helper-function unit tests (`_load_function_def`,
    `_resolve_plugin`, `_tables_from_manifest`, `_invoke_write_out`,
    `_DestContext`).
  * `main()` error paths and end-to-end happy path against the real
    `tabsdata-conn-localfile` destination.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from tabsdatak._bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak._info import SubscriberInfo
from tabsdatak._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    write_outputs_manifest,
)
from tabsdatak._tdrunner.function.commands.run_destination_plugin import (
    EXIT_BAD_BUNDLE,
    EXIT_BAD_MANIFEST,
    EXIT_BAD_REQUEST,
    EXIT_BAD_USER_CODE,
    EXIT_NO_PLUGIN,
    EXIT_PLUGIN_FAILED,
    EXIT_WRONG_KIND,
    _DestContext,
    _invoke_write_out,
    _load_function_def,
    _resolve_plugin,
    _tables_from_manifest,
    main,
)
from tabsdatak.spi import ParquetFile

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak._tdrunner.function.commands.run_destination_plugin"


# --- shared fixtures ----------------------------------------------------


_USER_SUBSCRIBER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileDest

_DEST = LocalFileDest(
    conn=LocalFileDestConn(base_path={base_path!r}),
    paths=["sink.parquet"],
)


@subscriber(destination=_DEST, input_tables=["c1/t_in"])
def my_subscriber(in_1: TableFrameSpec) -> tuple[TableFrameSpec]:
    return (in_1,)
"""


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """User project carrying a @subscriber. The destination writes to
    a sibling `dest_out/` dir under tmp_path."""
    dest_out = tmp_path / "dest_out"
    dest_out.mkdir()
    project = tmp_path / "project"
    pkg = project / "sub_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "subscriber.py").write_text(
        _USER_SUBSCRIBER_SRC.format(base_path=str(dest_out))
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    yield project
    for name in list(sys.modules):
        if name == "sub_pkg" or name.startswith("sub_pkg."):
            del sys.modules[name]


@pytest.fixture
def fixture_subscriber(user_project) -> SubscriberInfo:
    from sub_pkg.subscriber import my_subscriber  # noqa: E402

    assert isinstance(my_subscriber, SubscriberInfo)
    return my_subscriber


@pytest.fixture
def built_bundle(tmp_path, user_project, fixture_subscriber) -> Path:
    bundle_path = tmp_path / "bundle.tar.zst"
    build_bundle(fixture_subscriber, user_project, bundle_path)
    extracted = tmp_path / "extracted"
    extract_bundle(bundle_path, extracted)
    return extracted


def _write_request(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump({
            "info": {
                "collection": "c",
                "collection_id": 1,
                "execution_id": 42,
                "transaction_id": 99,
                "function_run_id": 7,
                "function_bundle": {"uri": "file:///bundle.tgz"},
            },
        })
    )
    return path


def _set_argv(
    monkeypatch,
    request_path: Path,
    bundle_dir: Path,
    outputs_manifest: Path,
    liveness: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-destination-plugin",
            "--request",
            str(request_path),
            "--bundle-dir",
            str(bundle_dir),
            "--outputs-manifest",
            str(outputs_manifest),
            "--liveness",
            str(liveness),
        ],
    )


# --- unit: _DestContext --------------------------------------------------


def _dest_ctx(**kwargs) -> _DestContext:
    """Default-fill the new DestContext fields so each test only has to
    override what it's exercising."""
    defaults = dict(
        work_dir=Path("/work"),
        exec_plan_id="exec1",
        trx_id="tx1",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn_run_42",
    )
    defaults.update(kwargs)
    return _DestContext(**defaults)


def test_dest_context_exposes_fields():
    ctx = _dest_ctx()
    assert ctx.work_dir == Path("/work")
    assert ctx.exec_plan_id == "exec1"
    assert ctx.trx_id == "tx1"
    assert ctx.collection_id == "c_id"
    assert ctx.collection_name == "c_name"
    assert ctx.function_id == "fn_run_42"


def test_dest_context_function_name_raises_notimplemented():
    with pytest.raises(NotImplementedError, match="function_name"):
        _ = _dest_ctx().function_name


def test_dest_context_exec_plan_ts_raises_notimplemented():
    with pytest.raises(NotImplementedError, match="exec_plan_ts"):
        _ = _dest_ctx().exec_plan_ts


def test_dest_context_get_attr_returns_none_no_state():
    """No state store wired through tdrunner yet -- ``get_attr``
    honours the documented ``returns None if unset`` contract."""
    assert _dest_ctx().get_attr("k") is None


def test_dest_context_store_attr_is_noop():
    """No state store wired yet -- ``store_attr`` swallows the write."""
    assert _dest_ctx().store_attr("k", "v") is None


def test_dest_context_add_metadata_is_noop():
    """No observability sink wired yet -- ``add_metadata`` is a no-op."""
    assert _dest_ctx().add_metadata("k", "v") is None


# --- unit: _load_function_def -------------------------------------------


def test_load_function_def_happy(built_bundle):
    info = _load_function_def(built_bundle)
    assert isinstance(info, SubscriberInfo)


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


def test_load_function_def_wrong_kind_raises_wrong_kind(built_bundle):
    """A bundle whose kind is not subscriberDef must surface as
    EXIT_WRONG_KIND so the orchestrator routes it elsewhere."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["kind"] = "publisherDef"
    manifest.write_text(json.dumps(data))
    with pytest.raises(CommandSpecificError) as ei:
        _load_function_def(built_bundle)
    assert ei.value.exit_code == EXIT_WRONG_KIND


def test_load_function_def_unresolvable_func_raises_bad_user_code(built_bundle):
    """A `func` pointing at a missing module surfaces as
    EXIT_BAD_USER_CODE via pydantic's ValidationError chain."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))
    with pytest.raises(CommandSpecificError) as ei:
        _load_function_def(built_bundle)
    assert ei.value.exit_code == EXIT_BAD_USER_CODE


# --- unit: _resolve_plugin ----------------------------------------------


def test_resolve_plugin_happy(fixture_subscriber):
    plugin = _resolve_plugin(fixture_subscriber)
    # LocalFileDestPlugin is what's registered for LocalFileDest.
    assert plugin.__class__.__name__ == "LocalFileDestPlugin"


def test_resolve_plugin_unknown_dest_raises_no_plugin(fixture_subscriber, monkeypatch):
    def _raise(_cls):
        raise KeyError("no DestDef registered")

    monkeypatch.setattr(f"{_MOD}.get_dest", _raise)
    with pytest.raises(CommandSpecificError) as ei:
        _resolve_plugin(fixture_subscriber)
    assert ei.value.exit_code == EXIT_NO_PLUGIN


# --- unit: _tables_from_manifest ---------------------------------------


def test_tables_from_manifest_happy():
    entries = [
        {MANIFEST_NAME_KEY: "out_0", MANIFEST_FILE_KEY: "/a.parquet"},
        {MANIFEST_NAME_KEY: "out_1", MANIFEST_FILE_KEY: "/b.parquet"},
    ]
    tables = _tables_from_manifest(entries)
    assert tables == [ParquetFile("/a.parquet"), ParquetFile("/b.parquet")]
    assert all(isinstance(t, ParquetFile) for t in tables)


def test_tables_from_manifest_empty_entries_yields_empty_list():
    assert _tables_from_manifest([]) == []


def test_tables_from_manifest_null_entry_raises_bad_manifest():
    """Subscribers must return concrete frames for every slot — a
    `file: null` entry is a contract violation here."""
    entries = [
        {MANIFEST_NAME_KEY: "out_0", MANIFEST_FILE_KEY: "/a.parquet"},
        {MANIFEST_NAME_KEY: "out_1", MANIFEST_FILE_KEY: None},
    ]
    with pytest.raises(CommandSpecificError) as ei:
        _tables_from_manifest(entries)
    assert ei.value.exit_code == EXIT_BAD_MANIFEST


# --- unit: _invoke_write_out -------------------------------------------


def test_invoke_write_out_returns_normally(tmp_path):
    calls = []

    class _Plugin:
        def write_out(self, ctx, dest, tables):
            calls.append((ctx, dest, tables))

    ctx = _DestContext(
        work_dir=tmp_path,
        exec_plan_id="e",
        trx_id="t",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn",
    )
    _invoke_write_out(_Plugin(), ctx, object(), [Path("/x.parquet")])
    assert len(calls) == 1


def test_invoke_write_out_wraps_plugin_exception(tmp_path):
    class _Plugin:
        def write_out(self, ctx, dest, tables):
            raise RuntimeError("boom")

    ctx = _DestContext(
        work_dir=tmp_path,
        exec_plan_id="e",
        trx_id="t",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn",
    )
    with pytest.raises(CommandSpecificError) as ei:
        _invoke_write_out(_Plugin(), ctx, object(), [])
    assert ei.value.exit_code == EXIT_PLUGIN_FAILED


# --- main: error paths --------------------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch):
    _set_argv(
        monkeypatch,
        tmp_path / "no.yaml",
        tmp_path / "bundle",
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
        tmp_path / "outputs.yaml",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_BUNDLE


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run-destination-plugin"])
    assert main() == 1


def test_main_missing_outputs_manifest_returns_bad_manifest(
    tmp_path, built_bundle, monkeypatch
):
    req = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        req,
        built_bundle,
        tmp_path / "no-outputs.yaml",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_MANIFEST


def test_main_null_file_in_manifest_returns_bad_manifest(
    tmp_path, built_bundle, monkeypatch
):
    req = _write_request(tmp_path / "request.yaml")
    outputs_manifest = tmp_path / "outputs.yaml"
    write_outputs_manifest(
        [{MANIFEST_NAME_KEY: "out_0", MANIFEST_FILE_KEY: None}],
        outputs_manifest,
    )
    _set_argv(
        monkeypatch,
        req,
        built_bundle,
        outputs_manifest,
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_MANIFEST


# --- main: end-to-end --------------------------------------------------


def test_main_end_to_end_happy_path(tmp_path, built_bundle, monkeypatch):
    """Real subscriber, real LocalFileDestPlugin. Write a parquet on
    disk, point the outputs manifest at it, invoke main(); verify the
    destination plugin actually copied the file into the dest dir.
    """
    request_path = _write_request(tmp_path / "request.yaml")

    # The user function's "output" parquet, which the dest plugin
    # should consume and ship to the destination's base_path.
    raw_parquet = tmp_path / "raw" / "to_ship.parquet"
    raw_parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}),
        raw_parquet,
    )

    outputs_manifest = tmp_path / "outputs.yaml"
    write_outputs_manifest(
        [{MANIFEST_NAME_KEY: "out_0", MANIFEST_FILE_KEY: str(raw_parquet)}],
        outputs_manifest,
    )

    liveness = tmp_path / "live"
    _set_argv(monkeypatch, request_path, built_bundle, outputs_manifest, liveness)

    rc = main()
    assert rc == 0, f"main() returned {rc}"

    # LocalFileDest wrote sink.parquet under tmp_path/dest_out (the
    # base_path baked into the user project's destination).
    shipped = tmp_path / "dest_out" / "sink.parquet"
    assert shipped.is_file()
    df = pl.read_parquet(shipped)
    assert df["id"].to_list() == [1, 2, 3]
    assert df["name"].to_list() == ["a", "b", "c"]

    assert liveness.exists()


# --- decorator metadata ------------------------------------------------


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_run_destination_plugin"
