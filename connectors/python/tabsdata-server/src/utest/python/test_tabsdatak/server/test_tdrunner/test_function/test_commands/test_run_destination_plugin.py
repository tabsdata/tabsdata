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

from tabsdatak._info import SubscriberInfo
from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    write_outputs_manifest,
)
from tabsdatak.server._tdrunner.function.commands.run_destination_plugin import (
    _DestContext,
    _invoke_write_out,
    _load_function_def,
    _resolve_plugin,
    _tables_from_manifest,
    main,
)
from tabsdatak.server._tdrunner.function.commands.run_destination_plugin.error import (
    BadBundleError,
    BadManifestError,
    BadRequestError,
    BadUserCodeError,
    NoPluginError,
    PluginFailedError,
    WrongKindError,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak.server._tdrunner.function.commands.run_destination_plugin"


# --- shared fixtures ----------------------------------------------------


_USER_SUBSCRIBER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileDest

_DEST = LocalFileDest(
    paths=["sink.parquet"],
)


@subscriber(
    conn=LocalFileDestConn(base_path={base_path!r}),
    destination=_DEST,
    input_tables=["c1/t_in"],
)
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


def _write_request(path: Path, bundle_uri: str = "file:///bundle.tgz") -> Path:
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(
                    bundle_uri,
                    function_run_id=7,
                    transaction_id=99,
                    execution_id=42,
                ),
            }
        })
    )
    return path


def _write_connection_file(path: Path, base_path: Path) -> Path:
    """Write the connection.yaml the command loads via --connection-file.

    The connection is no longer carried on the function descriptor; the
    runtime reads it from this file (the collection's connection)."""
    from tabsdatak.conn.localfile import LocalFileDestConn

    path.write_text(
        yaml.safe_dump(LocalFileDestConn(base_path=str(base_path)).to_dict())
    )
    return path


def _set_argv(
    monkeypatch,
    request_path: Path,
    bundle_dir: Path,
    outputs_manifest: Path,
    liveness: Path,
    *,
    connection_file: Path,
    responder_url: str,
    responder_method: str = "POST",
    responder_authorization: str = "Bearer tok",
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
            "--connection-file",
            str(connection_file),
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


# --- unit: _DestContext --------------------------------------------------


def _dest_ctx(**kwargs) -> _DestContext:
    """Default-fill the new DestContext fields so each test only has to
    override what it's exercising."""
    defaults = dict(
        work_dir=Path("/work"),
        exec_plan_id="exec1",
        trx_id="tx1",
        exec_plan_ts="2026-06-04T07:14:28.376000+00:00",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn_run_42",
        function_name="publish_people",
    )
    defaults.update(kwargs)
    return _DestContext(**defaults)


def test_dest_context_exposes_fields():
    ctx = _dest_ctx()
    assert ctx.work_dir == Path("/work")
    assert ctx.exec_plan_id == "exec1"
    assert ctx.trx_id == "tx1"
    assert ctx.exec_plan_ts == "2026-06-04T07:14:28.376000+00:00"
    assert ctx.collection_id == "c_id"
    assert ctx.collection_name == "c_name"
    assert ctx.function_id == "fn_run_42"
    assert ctx.function_name == "publish_people"


def test_dest_context_get_attr_returns_none_for_unset_key():
    """``get_attr`` honours the ``returns None if unset`` contract on
    a fresh ctx."""
    assert _dest_ctx().get_attr("k") is None


def test_dest_context_set_attr_round_trips_via_get_attr():
    """``set_attr`` writes through; a subsequent ``get_attr`` sees
    the value."""
    c = _dest_ctx()
    c.set_attr("k", "v")
    assert c.get_attr("k") == "v"


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
    with pytest.raises(BadBundleError):
        _load_function_def(empty)


def test_load_function_def_malformed_json_raises_bad_bundle(tmp_path):
    bad = tmp_path / "bad-bundle"
    bad.mkdir()
    (bad / BUNDLE_MANIFEST_FILENAME).write_text("{ not json")
    with pytest.raises(BadBundleError):
        _load_function_def(bad)


def test_load_function_def_wrong_kind_raises_wrong_kind(built_bundle):
    """A bundle whose kind is not subscriberDef must surface as
    WrongKindError so the orchestrator routes it elsewhere."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["kind"] = "publisherDef"
    manifest.write_text(json.dumps(data))
    with pytest.raises(WrongKindError):
        _load_function_def(built_bundle)


def test_load_function_def_unresolvable_func_raises_bad_user_code(built_bundle):
    """A `func` pointing at a missing module surfaces as
    BadUserCodeError via pydantic's ValidationError chain."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadUserCodeError):
        _load_function_def(built_bundle)


# --- unit: _resolve_plugin ----------------------------------------------


def test_resolve_plugin_happy(fixture_subscriber):
    plugin = _resolve_plugin(fixture_subscriber)
    # LocalFileDestPlugin is what's registered for LocalFileDest.
    assert plugin.__class__.__name__ == "LocalFileDestPlugin"


def test_resolve_plugin_unknown_dest_raises_no_plugin(fixture_subscriber, monkeypatch):
    def _raise(_cls):
        raise KeyError("no DestDef registered")

    monkeypatch.setattr(f"{_MOD}.get_dest", _raise)
    with pytest.raises(NoPluginError):
        _resolve_plugin(fixture_subscriber)


# --- unit: _tables_from_manifest ---------------------------------------


def test_tables_from_manifest_happy():
    entries = [
        {MANIFEST_NAME_KEY: "out_0", MANIFEST_FILE_KEY: "/a.parquet"},
        {MANIFEST_NAME_KEY: "out_1", MANIFEST_FILE_KEY: "/b.parquet"},
    ]
    tables = _tables_from_manifest(entries)
    assert tables == [Path("/a.parquet"), Path("/b.parquet")]
    assert all(isinstance(t, Path) for t in tables)


def test_tables_from_manifest_empty_entries_yields_empty_list():
    assert _tables_from_manifest([]) == []


def test_tables_from_manifest_null_entry_raises_bad_manifest():
    """Subscribers must return concrete frames for every slot — a
    `file: null` entry is a contract violation here."""
    entries = [
        {MANIFEST_NAME_KEY: "out_0", MANIFEST_FILE_KEY: "/a.parquet"},
        {MANIFEST_NAME_KEY: "out_1", MANIFEST_FILE_KEY: None},
    ]
    with pytest.raises(BadManifestError):
        _tables_from_manifest(entries)


# --- unit: _invoke_write_out -------------------------------------------


def test_invoke_write_out_returns_normally(tmp_path):
    calls = []

    class _Plugin:
        def write_out(self, ctx, conn, dest, tables):
            calls.append((ctx, conn, dest, tables))

    ctx = _DestContext(
        work_dir=tmp_path,
        exec_plan_id="e",
        trx_id="t",
        exec_plan_ts="2026-06-04T07:14:28.376000+00:00",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn",
        function_name="publish_people",
    )
    _invoke_write_out(_Plugin(), ctx, object(), object(), [Path("/x.parquet")])
    assert len(calls) == 1


def test_invoke_write_out_wraps_plugin_exception(tmp_path):
    class _Plugin:
        def write_out(self, ctx, conn, dest, tables):
            raise RuntimeError("boom")

    ctx = _DestContext(
        work_dir=tmp_path,
        exec_plan_id="e",
        trx_id="t",
        exec_plan_ts="2026-06-04T07:14:28.376000+00:00",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn",
        function_name="publish_people",
    )
    with pytest.raises(PluginFailedError):
        _invoke_write_out(_Plugin(), ctx, object(), object(), [])


# --- main: error paths --------------------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch, http_ok_url):
    _set_argv(
        monkeypatch,
        tmp_path / "no.yaml",
        tmp_path / "bundle",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        connection_file=tmp_path / "connection.yaml",
        responder_url=http_ok_url,
    )
    assert main() == BadRequestError.exit_code


def test_main_missing_bundle_returns_bad_bundle(tmp_path, monkeypatch, http_ok_url):
    req = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        req,
        tmp_path / "no-bundle",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        connection_file=tmp_path / "connection.yaml",
        responder_url=http_ok_url,
    )
    assert main() == BadBundleError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run-destination-plugin"])
    assert main() == 1


def test_main_missing_outputs_manifest_returns_bad_manifest(
    tmp_path, built_bundle, monkeypatch, http_ok_url
):
    req = _write_request(tmp_path / "request.yaml")
    conn_file = _write_connection_file(
        tmp_path / "connection.yaml", tmp_path / "dest_out"
    )
    _set_argv(
        monkeypatch,
        req,
        built_bundle,
        tmp_path / "no-outputs.yaml",
        tmp_path / "live",
        connection_file=conn_file,
        responder_url=http_ok_url,
    )
    assert main() == BadManifestError.exit_code


def test_main_null_file_in_manifest_returns_bad_manifest(
    tmp_path, built_bundle, monkeypatch, http_ok_url
):
    req = _write_request(tmp_path / "request.yaml")
    conn_file = _write_connection_file(
        tmp_path / "connection.yaml", tmp_path / "dest_out"
    )
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
        connection_file=conn_file,
        responder_url=http_ok_url,
    )
    assert main() == BadManifestError.exit_code


# --- main: end-to-end --------------------------------------------------


def test_main_end_to_end_happy_path(tmp_path, built_bundle, monkeypatch, http_ok_url):
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

    # The connection (with the dest base_path) now comes from the
    # connection file the runtime loads, not from the function descriptor.
    conn_file = _write_connection_file(
        tmp_path / "connection.yaml", tmp_path / "dest_out"
    )

    liveness = tmp_path / "live"
    _set_argv(
        monkeypatch,
        request_path,
        built_bundle,
        outputs_manifest,
        liveness,
        connection_file=conn_file,
        responder_url=http_ok_url,
    )

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
