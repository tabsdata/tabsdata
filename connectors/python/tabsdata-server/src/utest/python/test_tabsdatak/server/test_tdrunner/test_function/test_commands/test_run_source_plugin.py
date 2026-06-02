#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the run-source-plugin command.

Covers:
  * Helper-function unit tests (`_normalise_table_file`, `_SrcContext`,
    `_load_function_def`, `_resolve_plugin`, `_invoke_read_in`,
    `_write_inputs_manifest`).
  * End-to-end happy path via `main()`, using a real LocalFileSrc
    publisher. Same dependency profile as `donotmerge.py`: assumes
    `td_transporter` is on PATH and `tabsdata-conn-localfile` is
    installed; no plugin mocking.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from tabsdatak._bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak._info import PublisherInfo
from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak.server._tdrunner.function.commands.run_source_plugin import (
    EXIT_BAD_BUNDLE,
    EXIT_BAD_INITIAL_VALUES,
    EXIT_BAD_REQUEST,
    EXIT_BAD_USER_CODE,
    EXIT_MANIFEST_WRITE_FAILED,
    EXIT_NO_PLUGIN,
    EXIT_PLUGIN_FAILED,
    _SrcContext,
    _invoke_read_in,
    _load_function_def,
    _normalise_table_file,
    _resolve_plugin,
    _write_inputs_manifest,
    main,
)
from tabsdatak.server._tdrunner.function._lib.initial_values import (
    drain_ctx_to_state,
    hydrate_ctx_from_state,
    read_initial_values,
    write_initial_values,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    Info,
    Location,
    Request,
    Table,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak.server._tdrunner.function.commands.run_source_plugin"


# --- fixtures and helpers --------------------------------------------------


_USER_PUBLISHER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrcConn, LocalFileSrc

_SRC = LocalFileSrc(
    conn=LocalFileSrcConn(base_path={base_path!r}),
    paths=["*.parquet"],
)


@publisher(source=_SRC, output_tables=["my_table"])
def my_publisher(in_1: TableFramesSpec) -> tuple[TableFrameSpec]:
    return (None,)
"""


def _write_sample_parquet(path: Path) -> None:
    """Write a tiny three-row parquet for the source plugin to import."""
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
    })
    pq.write_table(table, path)


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """Materialise a user project on disk with a @publisher-decorated
    function pointing at a real `srcdata` dir.

    Returns the project root and makes `my_pkg.publisher` importable by
    prepending the project to `sys.path`. Cleans up cached modules on
    teardown so each test starts with a fresh import.
    """
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    _write_sample_parquet(srcdata / "rows.parquet")

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
    """Import and return the decorated `my_publisher` (a `PublisherInfo`)."""
    from my_pkg.publisher import my_publisher  # noqa: E402  (lazy by design)

    assert isinstance(my_publisher, PublisherInfo)
    return my_publisher


@pytest.fixture
def built_bundle(tmp_path, user_project, fixture_publisher) -> Path:
    """Build and extract the bundle. Returns the extracted directory."""
    bundle_path = tmp_path / "bundle.tar.zst"
    build_bundle(fixture_publisher, user_project, bundle_path)
    extracted = tmp_path / "extracted"
    extract_bundle(bundle_path, extracted)
    return extracted


def _write_request(path: Path, bundle_uri: str = "file:///bundle.tgz") -> Path:
    path.write_text(
        yaml.safe_dump({
            "info": {
                "collection": "c",
                "collection_id": 1,
                "execution_id": 2,
                "transaction_id": 3,
                "function_run_id": 4,
                "function_bundle": {"uri": bundle_uri},
            },
            "output": [{"name": "my_table"}],
        })
    )
    return path


def _set_argv(
    monkeypatch,
    request_path: Path,
    bundle_dir: Path,
    out_dir: Path,
    inputs_manifest: Path,
    liveness: Path,
    state_dir: Path | None = None,
) -> None:
    if state_dir is None:
        # Default to a sibling dir of out_dir so callers that don't care
        # about state lifecycle don't have to plumb a path.
        state_dir = out_dir.parent / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-source-plugin",
            "--request",
            str(request_path),
            "--bundle-dir",
            str(bundle_dir),
            "--out-dir",
            str(out_dir),
            "--inputs-manifest",
            str(inputs_manifest),
            "--liveness",
            str(liveness),
            "--state-dir",
            str(state_dir),
        ],
    )


# --- unit: _normalise_table_file ------------------------------------------


def test_normalise_triple_slash_uri():
    assert _normalise_table_file("file:///tmp/x.parquet") == "/tmp/x.parquet"


def test_normalise_single_slash_uri():
    # `Path("file:///x")` collapses to `"file:/x"` via `str()`; the
    # function has to handle both shapes.
    assert _normalise_table_file("file:/tmp/x.parquet") == "/tmp/x.parquet"


def test_normalise_path_object_wrapping_uri():
    assert _normalise_table_file(Path("file:///tmp/x.parquet")) == "/tmp/x.parquet"


def test_normalise_uri_with_percent_encoding():
    assert (
        _normalise_table_file("file:///tmp/with%20space.parquet")
        == "/tmp/with space.parquet"
    )


def test_normalise_uri_scheme_is_case_insensitive():
    assert _normalise_table_file("FILE:///tmp/x.parquet") == "/tmp/x.parquet"
    assert _normalise_table_file("File:/tmp/x.parquet") == "/tmp/x.parquet"


def test_normalise_bare_absolute_path():
    assert _normalise_table_file("/tmp/x.parquet") == "/tmp/x.parquet"


def test_normalise_bare_relative_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert _normalise_table_file("x.parquet") == str(tmp_path / "x.parquet")


# --- unit: _SrcContext ----------------------------------------------------


def _ctx(**kwargs) -> _SrcContext:
    """Default-fill the new SrcPluginCtx fields so each test only has
    to override what it's exercising."""
    defaults = dict(
        work_dir=Path("/work"),
        exec_plan_id="exec1",
        trx_id="tx1",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn_run_42",
    )
    defaults.update(kwargs)
    return _SrcContext(**defaults)


def test_src_context_returns_work_dir():
    assert _ctx().work_dir == Path("/work")


def test_src_context_exposes_exec_plan_id():
    assert _ctx().exec_plan_id == "exec1"


def test_src_context_exposes_trx_id():
    assert _ctx().trx_id == "tx1"


def test_src_context_exposes_collection_id():
    assert _ctx().collection_id == "c_id"


def test_src_context_exposes_collection_name():
    assert _ctx().collection_name == "c_name"


def test_src_context_exposes_function_id():
    assert _ctx().function_id == "fn_run_42"


def test_src_context_function_name_raises_notimplemented():
    """``function_name`` is declared but intentionally unimplemented
    until ``request.yaml`` carries it."""
    with pytest.raises(NotImplementedError, match="function_name"):
        _ = _ctx().function_name


def test_src_context_exec_plan_ts_raises_notimplemented():
    """``exec_plan_ts`` is declared but intentionally unimplemented
    until ``request.yaml`` carries a timestamp."""
    with pytest.raises(NotImplementedError, match="exec_plan_ts"):
        _ = _ctx().exec_plan_ts


def test_src_context_get_attr_returns_none_for_unset_key():
    """``get_attr`` honours the ``returns None if unset`` contract
    on a fresh ctx."""
    assert _ctx().get_attr("k") is None


def test_src_context_store_attr_round_trips_via_get_attr():
    """``store_attr`` writes through; a subsequent ``get_attr`` sees
    the value."""
    c = _ctx()
    c.store_attr("k", "v")
    assert c.get_attr("k") == "v"


def test_src_context_add_metadata_is_noop():
    """No observability sink wired yet -- ``add_metadata`` is a no-op."""
    assert _ctx().add_metadata("k", "v") is None


# --- unit: _load_function_def --------------------------------------------


def test_load_function_def_happy(built_bundle):
    info = _load_function_def(built_bundle)
    assert isinstance(info, PublisherInfo)
    assert list(info.output_tables) == ["my_table"]


def test_load_function_def_missing_manifest_raises_bad_bundle(tmp_path):
    empty = tmp_path / "empty-bundle"
    empty.mkdir()
    with pytest.raises(CommandSpecificError) as exc_info:
        _load_function_def(empty)
    assert exc_info.value.exit_code == EXIT_BAD_BUNDLE


def test_load_function_def_malformed_json_raises_bad_bundle(tmp_path):
    bad = tmp_path / "bad-bundle"
    bad.mkdir()
    (bad / BUNDLE_MANIFEST_FILENAME).write_text("{ not json")
    with pytest.raises(CommandSpecificError) as exc_info:
        _load_function_def(bad)
    assert exc_info.value.exit_code == EXIT_BAD_BUNDLE


def test_load_function_def_unresolvable_func_raises_bad_user_code(
    built_bundle,
):
    """Edit the manifest to point at a non-existent module; the import
    failure surfaces via the pydantic validator as `ValidationError`,
    which the command maps to `EXIT_BAD_USER_CODE`."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))

    with pytest.raises(CommandSpecificError) as exc_info:
        _load_function_def(built_bundle)
    assert exc_info.value.exit_code == EXIT_BAD_USER_CODE


# --- unit: _resolve_plugin -----------------------------------------------


def test_resolve_plugin_happy(fixture_publisher):
    from tabsdatak.spi import SrcPlugin

    plugin = _resolve_plugin(fixture_publisher)
    assert isinstance(plugin, SrcPlugin)


def test_resolve_plugin_unknown_source_raises_no_plugin(fixture_publisher, monkeypatch):
    """When `get_src` raises `KeyError`, the runner surfaces
    `EXIT_NO_PLUGIN`."""

    def _raise(_):
        raise KeyError("no connector registered")

    monkeypatch.setattr(f"{_MOD}.get_src", _raise)
    with pytest.raises(CommandSpecificError) as exc_info:
        _resolve_plugin(fixture_publisher)
    assert exc_info.value.exit_code == EXIT_NO_PLUGIN


# --- unit: _invoke_read_in ----------------------------------------------


def test_invoke_read_in_returns_plugin_result():
    class _FakePlugin:
        def read_in(self, ctx, src):
            return [[Path("/x.parquet")]]

    out = _invoke_read_in(_FakePlugin(), object(), object())
    assert out == [[Path("/x.parquet")]]


def test_invoke_read_in_wraps_exceptions_as_plugin_failed():
    class _BrokenPlugin:
        def read_in(self, ctx, src):
            raise RuntimeError("boom")

    with pytest.raises(CommandSpecificError) as exc_info:
        _invoke_read_in(_BrokenPlugin(), object(), object())
    assert exc_info.value.exit_code == EXIT_PLUGIN_FAILED


# --- unit: _write_inputs_manifest ----------------------------------------


def test_write_inputs_manifest_happy(tmp_path, fixture_publisher):
    out = tmp_path / "manifest.yaml"
    produced = [[Path("/tmp/a.parquet"), Path("/tmp/b.parquet")]]
    _write_inputs_manifest(fixture_publisher, produced, out)
    data = yaml.safe_load(out.read_text())
    assert data == {
        "inputs": [
            {
                "name": "in_1",
                "files": ["/tmp/a.parquet", "/tmp/b.parquet"],
            }
        ],
    }


def test_write_inputs_manifest_normalises_file_uris(tmp_path, fixture_publisher):
    """The plugin may return `Path("file:///...")` URI-shaped values
    rather than bare paths; the manifest must always carry plain
    absolute filesystem paths."""
    out = tmp_path / "manifest.yaml"
    produced = [[Path("file:///tmp/uri-shaped.parquet")]]
    _write_inputs_manifest(fixture_publisher, produced, out)
    data = yaml.safe_load(out.read_text())
    assert data["inputs"][0]["files"] == ["/tmp/uri-shaped.parquet"]


def test_write_inputs_manifest_cardinality_mismatch_raises(tmp_path, fixture_publisher):
    """Source cardinality is 1 (`LocalFileSrc` with one path); a plugin
    that returns two slots is a contract violation."""
    out = tmp_path / "manifest.yaml"
    produced = [[Path("/x.parquet")], [Path("/y.parquet")]]
    with pytest.raises(CommandSpecificError) as exc_info:
        _write_inputs_manifest(fixture_publisher, produced, out)
    assert exc_info.value.exit_code == EXIT_PLUGIN_FAILED


def test_write_inputs_manifest_oserror_raises_manifest_write_failed(
    tmp_path, fixture_publisher
):
    """Writing to a path that's actually a directory triggers
    `IsADirectoryError` (a subclass of `OSError`)."""
    a_dir = tmp_path / "i-am-a-dir"
    a_dir.mkdir()
    produced = [[Path("/x.parquet")]]
    with pytest.raises(CommandSpecificError) as exc_info:
        _write_inputs_manifest(fixture_publisher, produced, a_dir)
    assert exc_info.value.exit_code == EXIT_MANIFEST_WRITE_FAILED


# --- main: error paths via argparse ---------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch):
    _set_argv(
        monkeypatch,
        tmp_path / "no-such.yaml",
        tmp_path / "bundle",
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_REQUEST


def test_main_missing_bundle_returns_bad_bundle(tmp_path, monkeypatch):
    req = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        req,
        tmp_path / "missing-bundle",
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_BUNDLE


def test_main_invalid_options_returns_1(monkeypatch):
    """argparse missing-required-arg → rc 1."""
    monkeypatch.setattr(sys, "argv", ["run-source-plugin", "--request", "x"])
    assert main() == 1


# --- main: end-to-end happy path -----------------------------------------


def test_main_end_to_end_happy_path(tmp_path, built_bundle, monkeypatch):
    """Real LocalFile publisher, real plugin, real td_transporter.

    Mirrors `donotmerge.py`. Requires `td_transporter` on PATH and
    `tabsdata-conn-localfile` installed (entry-point group
    `tabsdatak.connectors`).
    """
    req = _write_request(tmp_path / "request.yaml")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest = tmp_path / "manifest.yaml"
    liveness = tmp_path / "live"

    _set_argv(monkeypatch, req, built_bundle, out_dir, manifest, liveness)

    rc = main()
    assert rc == 0, f"main() returned {rc} (non-zero)"

    # Exactly one parquet should have been produced.
    parquets = list(out_dir.rglob("*.parquet"))
    assert len(parquets) == 1, f"expected one parquet output, got: {parquets}"

    # The parquet round-trips the input rows.
    table = pq.read_table(parquets[0])
    assert table.num_rows == 3
    assert "id" in table.column_names
    assert "name" in table.column_names
    assert table.to_pydict()["id"] == [1, 2, 3]
    assert table.to_pydict()["name"] == ["alice", "bob", "carol"]

    # The inputs manifest was written with the expected schema.
    assert manifest.is_file()
    data = yaml.safe_load(manifest.read_text())
    assert list(data.keys()) == ["inputs"]
    assert len(data["inputs"]) == 1
    entry = data["inputs"][0]
    assert entry["name"] == "in_1"
    assert len(entry["files"]) == 1
    # Resolve symlinks (e.g. macOS /tmp -> /private/tmp) on both sides
    # before comparing — `samefile` handles that.
    assert Path(entry["files"][0]).samefile(parquets[0])

    # Liveness file was created by the context manager.
    assert liveness.exists()


# --- decorator metadata -------------------------------------------------


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_run_source_plugin"


# --- unit: initial_values lifecycle helpers -----------------------------


def _bare_ctx() -> _SrcContext:
    return _SrcContext(
        work_dir=Path("/work"),
        exec_plan_id="ep",
        trx_id="tx",
        collection_id="cid",
        collection_name="c",
        function_id="fn",
    )


def _bare_request(
    *,
    system_input=(),
    system_output=(),
) -> Request:
    info = Info(
        collection="c",
        collection_id=1,
        execution_id=2,
        transaction_id=3,
        function_run_id=4,
        function_bundle=Location(uri="file:///bundle.tgz"),
    )
    return Request(
        info=info,
        system_input=system_input,
        system_output=system_output,
    )


def test_hydrate_noop_when_system_input_empty(tmp_path):
    ctx = _bare_ctx()
    hydrate_ctx_from_state(
        ctx, _bare_request(), tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES
    )
    assert ctx.drain() == {}


def test_hydrate_noop_when_slot_has_no_location(tmp_path):
    ctx = _bare_ctx()
    req = _bare_request(system_input=(Table(name="iv", location=None),))
    hydrate_ctx_from_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    assert ctx.drain() == {}


def test_hydrate_noop_when_state_file_missing(tmp_path):
    """First run for a flow: the slot has a location (the URI in the
    request), but the transporter never materialised it locally —
    e.g. first time the flow runs. Start with empty scratch."""
    ctx = _bare_ctx()
    req = _bare_request(
        system_input=(Table(name="iv", location=Location(uri="file:///x")),),
    )
    hydrate_ctx_from_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    assert ctx.drain() == {}


def test_hydrate_loads_values_when_state_file_present(tmp_path):
    state_file = tmp_path / "iv.parquet"
    write_initial_values({"last-modified": "2026-05-27T00:00:00Z"}, state_file)
    ctx = _bare_ctx()
    req = _bare_request(
        system_input=(Table(name="iv", location=Location(uri="file:///x")),),
    )
    hydrate_ctx_from_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    assert ctx.drain() == {"last-modified": "2026-05-27T00:00:00Z"}


def test_hydrate_malformed_state_file_raises_bad_initial_values(tmp_path):
    state_file = tmp_path / "iv.parquet"
    state_file.write_bytes(b"not parquet")
    ctx = _bare_ctx()
    req = _bare_request(
        system_input=(Table(name="iv", location=Location(uri="file:///x")),),
    )
    with pytest.raises(CommandSpecificError) as exc_info:
        hydrate_ctx_from_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    assert exc_info.value.exit_code == EXIT_BAD_INITIAL_VALUES


def test_drain_noop_when_system_output_empty(tmp_path):
    ctx = _bare_ctx()
    ctx.store_attr("foo", "bar")  # has state but nowhere to send it.
    drain_ctx_to_state(
        ctx, _bare_request(), tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES
    )
    assert list(tmp_path.iterdir()) == []


def test_drain_noop_when_slot_has_no_location(tmp_path):
    ctx = _bare_ctx()
    ctx.store_attr("foo", "bar")
    req = _bare_request(system_output=(Table(name="iv", location=None),))
    drain_ctx_to_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    assert list(tmp_path.iterdir()) == []


def test_drain_writes_state_file_when_slot_present(tmp_path):
    ctx = _bare_ctx()
    ctx.store_attr("last-modified", "2026-05-28T00:00:00Z")
    req = _bare_request(
        system_output=(Table(name="iv", location=Location(uri="file:///x")),),
    )
    drain_ctx_to_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    written = tmp_path / "iv.parquet"
    assert written.is_file()
    assert read_initial_values(written) == {"last-modified": "2026-05-28T00:00:00Z"}


def test_drain_writes_empty_file_when_ctx_empty(tmp_path):
    """An empty scratch store still drains to an empty Parquet — the
    plugin explicitly didn't want to persist anything this run."""
    ctx = _bare_ctx()
    req = _bare_request(
        system_output=(Table(name="iv", location=Location(uri="file:///x")),),
    )
    drain_ctx_to_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    written = tmp_path / "iv.parquet"
    assert written.is_file()
    assert read_initial_values(written) == {}


def test_drain_creates_state_dir_if_missing(tmp_path):
    state_dir = tmp_path / "fresh"
    assert not state_dir.exists()
    ctx = _bare_ctx()
    ctx.store_attr("k", "v")
    req = _bare_request(
        system_output=(Table(name="iv", location=Location(uri="file:///x")),),
    )
    drain_ctx_to_state(ctx, req, state_dir, exit_code=EXIT_BAD_INITIAL_VALUES)
    assert (state_dir / "iv.parquet").is_file()


def test_drain_bad_value_raises_bad_initial_values(tmp_path, monkeypatch):
    """If something poisons the ctx with a non-ContextValueSpec value,
    the codec rejects it and the command surfaces EXIT_BAD_INITIAL_VALUES."""
    ctx = _bare_ctx()
    # Bypass store_attr's type-friendly surface to inject a bad value
    # directly into the underlying scratch dict.
    ctx._scratch["k"] = object()  # type: ignore[assignment]
    req = _bare_request(
        system_output=(Table(name="iv", location=Location(uri="file:///x")),),
    )
    with pytest.raises(CommandSpecificError) as exc_info:
        drain_ctx_to_state(ctx, req, tmp_path, exit_code=EXIT_BAD_INITIAL_VALUES)
    assert exc_info.value.exit_code == EXIT_BAD_INITIAL_VALUES


# --- main: end-to-end with initial_values round-trip --------------------


def test_main_hydrates_and_drains_initial_values(tmp_path, built_bundle, monkeypatch):
    """Wire a previous-run state file into system_input[0], let main()
    run the plugin (which is unaware of state), and verify drain
    produced an output state file. The publisher plugin doesn't itself
    mutate ctx, so the drained dict equals the hydrated one."""
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    # Seed: previous run's state.
    write_initial_values(
        {"last-modified": "2026-05-27T00:00:00Z"},
        state_dir / "iv-in.parquet",
    )

    # Request declares system_input[0] AND system_output[0] both named
    # after the local state files.
    req_path = tmp_path / "request.yaml"
    req_path.write_text(
        yaml.safe_dump({
            "info": {
                "collection": "c",
                "collection_id": 1,
                "execution_id": 2,
                "transaction_id": 3,
                "function_run_id": 4,
                "function_bundle": {"uri": "file:///bundle.tgz"},
            },
            "output": [{"name": "my_table"}],
            "system_input": [
                {"name": "iv-in", "location": {"uri": "file:///irrelevant"}}
            ],
            "system_output": [
                {"name": "iv-out", "location": {"uri": "file:///irrelevant"}}
            ],
        })
    )
    manifest = tmp_path / "manifest.yaml"
    liveness = tmp_path / "live"
    _set_argv(
        monkeypatch,
        req_path,
        built_bundle,
        out_dir,
        manifest,
        liveness,
        state_dir=state_dir,
    )

    rc = main()
    assert rc == 0, f"main() returned {rc}"

    # iv-out.parquet now holds whatever the ctx had when drained.
    # The publisher's plugin doesn't store anything, so the drained
    # dict equals the hydrated dict.
    out_state = state_dir / "iv-out.parquet"
    assert out_state.is_file()
    assert read_initial_values(out_state) == {"last-modified": "2026-05-27T00:00:00Z"}
