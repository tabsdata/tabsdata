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

from tabsdatak._info import PublisherInfo

# Imported at module level so that string-form type annotations (under
# ``from __future__ import annotations``) on locally-defined
# @stream_publisher fixtures resolve via ``get_type_hints``.
from tabsdatak.api import TableFrameSpec, TableFramesSpec  # noqa: F401
from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function._lib.initial_values import (
    drain_ctx_to_state_or_raise,
    hydrate_ctx_from_state_or_raise,
    read_initial_values,
    write_initial_values,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    Info,
    InputTable,
    Location,
    OutputTable,
    Request,
)
from tabsdatak.server._tdrunner.function.commands.run_source_plugin import (
    _build_manifest_stage_conn,
    _invoke_read_in,
    _load_function_def,
    _normalise_table_file,
    _resolve_plugin_and_io,
    _SrcContext,
    _write_inputs_manifest,
    main,
)
from tabsdatak.server._tdrunner.function.commands.run_source_plugin.error import (
    BadBundleError,
    BadRequestError,
    BadStateInError,
    BadStateOutError,
    BadUserCodeError,
    ManifestWriteFailedError,
    NoPluginError,
    PluginFailedError,
    RunSourcePluginErrorCode,
    UnsupportedStageSchemeError,
)
from tabsdatak.spi import TableFileInput
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    info_payload as _info_payload,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak.server._tdrunner.function.commands.run_source_plugin"


# --- module-level stream-publisher fixture --------------------------------
#
# Defined at module top level (not inside a test) because the bundle
# serialiser stores ``func`` as ``"module:qualname"`` and rejects
# locally-defined callables (those have ``<locals>`` in their qualname).
# Used by ``test_load_function_def_accepts_stream_publisher_kind``.

from tabsdatak.api import stream_publisher as _stream_publisher_decorator  # noqa: E402
from tabsdatak.conn.kafka import (  # noqa: E402
    KafkaStreamSrc as _KafkaStreamSrc,
    KafkaStreamSrcConn as _KafkaStreamSrcConn,
)


@_stream_publisher_decorator(
    conn=_KafkaStreamSrcConn(servers="x:9092", group_id="g"),
    source=_KafkaStreamSrc(data_format="json", schema="{}", topic="t"),
    flush_interval_mins=1,
    output_tables=["t"],
)
def _module_level_stream_pub(t: TableFramesSpec) -> tuple[TableFrameSpec]:
    return (None,)


# --- fixtures and helpers --------------------------------------------------


_USER_PUBLISHER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrcConn, LocalFileSrc

_SRC = LocalFileSrc(
    paths=["*.parquet"],
)


@publisher(
    conn=LocalFileSrcConn(base_path={base_path!r}),
    source=_SRC,
    output_tables=["my_table"],
)
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


def _input_table_payload(name: str, *, location: dict | None = None) -> dict:
    return {
        "Table": {
            "name": name,
            "collection_id": 1,
            "collection": "c",
            "table_id": f"T-{name}",
            "table_version_id": f"TV-{name}",
            "location": location,
            "input_idx": 0,
            "table_pos": -1,
            "version_pos": -1,
        }
    }


def _output_table_payload(name: str, *, location: dict) -> dict:
    return {
        "Table": {
            "name": name,
            "collection_id": 1,
            "collection": "c",
            "table_id": f"T-{name}",
            "table_version_id": f"TV-{name}",
            "table_data_version_id": f"PDV-{name}",
            "location": location,
            "table_pos": 0,
        }
    }


def _write_request(path: Path, bundle_uri: str = "file:///bundle.tgz") -> Path:
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": _info_payload(bundle_uri=bundle_uri),
                "output": [
                    _output_table_payload(
                        "my_table", location={"uri": "file:///out/my_table"}
                    )
                ],
            }
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
    *,
    responder_url: str,
    responder_method: str = "POST",
    responder_authorization: str = "Bearer tok",
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
        exec_plan_ts="2026-06-04T07:14:28.376000+00:00",
        collection_id="c_id",
        collection_name="c_name",
        function_id="fn_run_42",
        function_name="publish_people",
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


def test_src_context_exposes_function_name():
    assert _ctx().function_name == "publish_people"


def test_src_context_exposes_exec_plan_ts():
    assert _ctx().exec_plan_ts == "2026-06-04T07:14:28.376000+00:00"


def test_src_context_get_attr_returns_none_for_unset_key():
    """``get_attr`` honours the ``returns None if unset`` contract
    on a fresh ctx."""
    assert _ctx().get_attr("k") is None


def test_src_context_set_attr_round_trips_via_get_attr():
    """``set_attr`` writes through; a subsequent ``get_attr`` sees
    the value."""
    c = _ctx()
    c.set_attr("k", "v")
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
    with pytest.raises(BadBundleError):
        _load_function_def(empty)


def test_load_function_def_malformed_json_raises_bad_bundle(tmp_path):
    bad = tmp_path / "bad-bundle"
    bad.mkdir()
    (bad / BUNDLE_MANIFEST_FILENAME).write_text("{ not json")
    with pytest.raises(BadBundleError):
        _load_function_def(bad)


def test_load_function_def_unresolvable_func_raises_bad_user_code(
    built_bundle,
):
    """Edit the manifest to point at a non-existent module; the import
    failure surfaces via the pydantic validator as `ValidationError`,
    which the command maps to `BadUserCodeError`."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))

    with pytest.raises(BadUserCodeError):
        _load_function_def(built_bundle)


# --- unit: _resolve_plugin -----------------------------------------------


def test_resolve_plugin_and_io_publisher_passthrough(fixture_publisher):
    """For a regular ``@publisher``, ``_resolve_plugin_and_io`` returns
    the plugin + the user-declared conn/source verbatim."""
    from tabsdatak.spi import SrcPlugin

    # function_stage is unused on the PublisherInfo path; pass a stub.
    stub_stage = Location(uri="file:///unused")
    plugin, conn, src, cardinality = _resolve_plugin_and_io(
        fixture_publisher, stub_stage
    )
    assert isinstance(plugin, SrcPlugin)
    assert conn is fixture_publisher.conn
    assert src is fixture_publisher.source
    assert cardinality == 1  # LocalFileSrc with one path


def test_resolve_plugin_and_io_unknown_source_raises_no_plugin(
    fixture_publisher, monkeypatch
):
    """When `get_src` raises `KeyError`, the runner surfaces
    `NoPluginError`."""

    def _raise(_):
        raise KeyError("no connector registered")

    monkeypatch.setattr(f"{_MOD}.get_src", _raise)
    with pytest.raises(NoPluginError):
        _resolve_plugin_and_io(fixture_publisher, Location(uri="file:///unused"))


# --- unit: _build_manifest_stage_conn (stream-publisher substitution) --


def test_build_manifest_stage_conn_file_scheme_resolves_localfile():
    """file:// keeps the connector-conn form: a LocalFileSrcConn in the
    `local` slot and no prebuilt `stage_backend`. ManifestStageSrcPlugin's
    backend_for_conn reads the slot. The integration tests use the same
    SimpleNamespace idiom."""
    from tabsdatak.conn.localfile import LocalFileSrcConn

    conn = _build_manifest_stage_conn(Location(uri="file:///tmp/stage"))
    assert conn.stage_backend is None
    assert isinstance(conn.local, LocalFileSrcConn)
    assert str(conn.local.base_path) == "/tmp/stage"
    assert conn.s3 is None
    assert conn.az is None
    assert conn.gcs is None


def test_build_manifest_stage_conn_s3_resolves_to_object_store_backend():
    """Cloud schemes resolve (via endpoint.py) to a prebuilt StageBackend
    carried on `stage_backend`; backend_for_conn returns it as-is. The
    stage-relative path is appended to the stage-root URL."""
    conn = _build_manifest_stage_conn(Location(uri="s3://bucket/stage"))
    assert conn.local is None
    backend = conn.stage_backend
    assert backend is not None
    assert backend.is_object_store is True
    loc = backend.build_location("manifests/stream-manifest-*.json")
    assert loc.url == "s3://bucket/stage/manifests/stream-manifest-*.json"


def test_build_manifest_stage_conn_az_resolves():
    conn = _build_manifest_stage_conn(Location(uri="az://account/container/stage"))
    assert conn.stage_backend is not None
    assert conn.stage_backend.is_object_store is True


def test_build_manifest_stage_conn_gs_resolves():
    """GCS uses the ``gs://`` scheme -- endpoint.py's registered builder."""
    conn = _build_manifest_stage_conn(Location(uri="gs://bucket/stage"))
    assert conn.stage_backend is not None
    assert conn.stage_backend.is_object_store is True


def test_build_manifest_stage_conn_rejects_unsupported_scheme():
    """An unknown scheme still surfaces RSP_11 with a clean exit code.
    Note: GCS stages must use ``gs://`` -- ``gcs://`` has no endpoint
    builder and is rejected here."""
    with pytest.raises(UnsupportedStageSchemeError):
        _build_manifest_stage_conn(Location(uri="gcs://bucket/stage"))


def test_resolve_plugin_and_io_stream_publisher_substitutes_to_manifest_stage():
    """For a ``@stream_publisher``, ``_resolve_plugin_and_io`` swaps
    the user-declared Kafka source/conn for the publisher-side
    StreamStageSrc + a SimpleNamespace duck-type of ManifestStageConn
    that ``ManifestStageSrcPlugin`` knows how to read. Cardinality
    drops to ``-1`` because the manifest decides slot count at read
    time."""
    from tabsdatak.api import stream_publisher
    from tabsdatak.conn.kafka import KafkaStreamSrc, KafkaStreamSrcConn
    from tabsdatak.conn.localfile import LocalFileSrcConn
    from tabsdatak.spi import StreamStageSrc, StreamStageSrcPlugin

    @stream_publisher(
        conn=KafkaStreamSrcConn(servers="example:9092", group_id="g"),
        source=KafkaStreamSrc(data_format="json", schema="{}", topic="t"),
        flush_interval_mins=1,
        output_tables=["t"],
    )
    def _my_stream_pub(t: TableFramesSpec) -> tuple[TableFrameSpec]:
        return (None,)

    plugin, conn, src, cardinality = _resolve_plugin_and_io(
        _my_stream_pub, Location(uri="file:///stage/dir")
    )
    # Plugin substituted to StreamStageSrcPlugin (which delegates to
    # ManifestStageSrcPlugin internally on read_in).
    assert isinstance(plugin, StreamStageSrcPlugin)
    # Conn substituted to a duck-typed namespace with file:// backend.
    assert isinstance(conn.local, LocalFileSrcConn)
    assert str(conn.local.base_path) == "/stage/dir"
    assert conn.s3 is None
    assert conn.az is None
    assert conn.gcs is None
    # Source substituted to a StreamStageSrc instance.
    assert isinstance(src, StreamStageSrc)
    # Cardinality is -1 (dynamic, from the StreamStageSrc registration).
    assert cardinality == -1


def test_load_function_def_accepts_stream_publisher_kind(tmp_path):
    """`_load_function_def`'s allowed_kinds now includes
    ``streamPublisherDef``. Synthesise a minimal function_def.json by
    hand (no full bundle build) and verify the loader returns a
    StreamPublisherInfo.

    Uses the module-level ``_module_level_stream_pub`` fixture below
    so the bundle serialiser can resolve ``module:qualname`` for the
    ``func`` field (locally-defined callables would fail the
    "must be importable at module top level" check)."""
    import json

    from tabsdatak._info import KIND_STREAM_PUBLISHER_DEF, StreamPublisherInfo
    from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME

    bundle_dir = tmp_path / "extracted"
    bundle_dir.mkdir()
    (bundle_dir / BUNDLE_MANIFEST_FILENAME).write_text(
        json.dumps(_module_level_stream_pub.to_dict())
    )

    info = _load_function_def(bundle_dir)
    assert isinstance(info, StreamPublisherInfo)
    assert info.__kind__ == KIND_STREAM_PUBLISHER_DEF


# --- unit: _invoke_read_in ----------------------------------------------


def test_invoke_read_in_returns_plugin_result():
    class _FakePlugin:
        def read_in(self, ctx, conn, src):
            return [[TableFileInput(file=Path("/x.parquet"))]]

    out = _invoke_read_in(_FakePlugin(), object(), object(), object())
    assert out == [[TableFileInput(file=Path("/x.parquet"))]]


def test_invoke_read_in_wraps_exceptions_as_plugin_failed():
    class _BrokenPlugin:
        def read_in(self, ctx, conn, src):
            raise RuntimeError("boom")

    with pytest.raises(PluginFailedError):
        _invoke_read_in(_BrokenPlugin(), object(), object(), object())


# --- unit: _write_inputs_manifest ----------------------------------------


def test_write_inputs_manifest_happy(tmp_path, fixture_publisher):
    out = tmp_path / "manifest.yaml"
    produced = [
        [
            TableFileInput(file=Path("/tmp/a.parquet")),
            TableFileInput(file=Path("/tmp/b.parquet")),
        ]
    ]
    _write_inputs_manifest(fixture_publisher, produced, out, cardinality=1)
    data = yaml.safe_load(out.read_text())

    def _record(file):
        return {
            "file": file,
            "input_idx": None,
            "version_pos": -1,
            "execution_id": None,
            "transaction_id": None,
            "table_data_version_id": None,
            "triggered_on": None,
        }

    assert data == {
        "inputs": [
            {
                "name": "in_1",
                "table_pos": 0,
                "is_empty": False,
                "tables": [
                    _record("/tmp/a.parquet"),
                    _record("/tmp/b.parquet"),
                ],
            }
        ],
    }


def test_write_inputs_manifest_normalises_file_uris(tmp_path, fixture_publisher):
    """The plugin may return `Path("file:///...")` URI-shaped values
    rather than bare paths; the manifest must always carry plain
    absolute filesystem paths."""
    out = tmp_path / "manifest.yaml"
    produced = [[TableFileInput(file=Path("file:///tmp/uri-shaped.parquet"))]]
    _write_inputs_manifest(fixture_publisher, produced, out, cardinality=1)
    data = yaml.safe_load(out.read_text())
    assert [t["file"] for t in data["inputs"][0]["tables"]] == [
        "/tmp/uri-shaped.parquet"
    ]


def test_write_inputs_manifest_cardinality_mismatch_raises(tmp_path, fixture_publisher):
    """Source cardinality is 1 (`LocalFileSrc` with one path); a plugin
    that returns two slots is a contract violation."""
    out = tmp_path / "manifest.yaml"
    produced = [
        [TableFileInput(file=Path("/x.parquet"))],
        [TableFileInput(file=Path("/y.parquet"))],
    ]
    with pytest.raises(PluginFailedError):
        _write_inputs_manifest(fixture_publisher, produced, out, cardinality=1)


def test_write_inputs_manifest_negative_cardinality_pads_to_param_count(
    tmp_path, fixture_publisher
):
    """``cardinality=-1`` (dynamic slot count -- e.g. a stream-stage
    manifest source) pads the manifest up to the function-declared
    input-slot count with empty entries when the plugin returned
    fewer slots. This is what makes a ``td fn trigger`` against a
    ``@stream_publisher`` work when no manifest has been staged: the
    user function is invoked with ``t=[]`` (one empty slot per
    declared parameter) instead of bailing on the input-count
    mismatch in ``run-function``.

    ``fixture_publisher`` has one input parameter (``in_1``), so 0
    produced slots → 1 padded entry whose ``tables`` is empty."""
    out = tmp_path / "manifest.yaml"
    _write_inputs_manifest(fixture_publisher, [], out, cardinality=-1)
    data = yaml.safe_load(out.read_text())
    assert len(data["inputs"]) == 1
    assert data["inputs"][0]["name"] == "in_1"
    assert data["inputs"][0]["tables"] == []


def test_write_inputs_manifest_negative_cardinality_does_not_pad_when_matched(
    tmp_path, fixture_publisher
):
    """When the dynamic source produced exactly the right number of
    slots, the manifest writes them verbatim — no padding needed."""
    out = tmp_path / "manifest.yaml"
    _write_inputs_manifest(
        fixture_publisher,
        [[TableFileInput(file=Path("/tmp/a.parquet"))]],
        out,
        cardinality=-1,
    )
    data = yaml.safe_load(out.read_text())
    assert len(data["inputs"]) == 1
    assert data["inputs"][0]["name"] == "in_1"
    assert [t["file"] for t in data["inputs"][0]["tables"]] == ["/tmp/a.parquet"]


def test_write_inputs_manifest_negative_cardinality_overproduction_raises(
    tmp_path, fixture_publisher
):
    """Over-production (source returned more slots than the function
    declares) is still a contract violation even under dynamic
    cardinality — we can't index past ``parameters[i]``."""
    out = tmp_path / "manifest.yaml"
    with pytest.raises(PluginFailedError):
        _write_inputs_manifest(
            fixture_publisher,
            [
                [TableFileInput(file=Path("/x.parquet"))],
                [TableFileInput(file=Path("/y.parquet"))],
            ],
            out,
            cardinality=-1,
        )


def test_write_inputs_manifest_oserror_raises_manifest_write_failed(
    tmp_path, fixture_publisher
):
    """Writing to a path that's actually a directory triggers
    `IsADirectoryError` (a subclass of `OSError`)."""
    a_dir = tmp_path / "i-am-a-dir"
    a_dir.mkdir()
    produced = [[TableFileInput(file=Path("/x.parquet"))]]
    with pytest.raises(ManifestWriteFailedError):
        _write_inputs_manifest(fixture_publisher, produced, a_dir, cardinality=1)


# --- main: error paths via argparse ---------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch, http_ok_url):
    _set_argv(
        monkeypatch,
        tmp_path / "no-such.yaml",
        tmp_path / "bundle",
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadRequestError.exit_code


def test_main_missing_bundle_returns_bad_bundle(tmp_path, monkeypatch, http_ok_url):
    req = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        req,
        tmp_path / "missing-bundle",
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadBundleError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    """argparse missing-required-arg → rc 1."""
    monkeypatch.setattr(sys, "argv", ["run-source-plugin", "--request", "x"])
    assert main() == 1


# --- main: end-to-end happy path -----------------------------------------


def test_main_end_to_end_happy_path(tmp_path, built_bundle, monkeypatch, http_ok_url):
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

    _set_argv(
        monkeypatch,
        req,
        built_bundle,
        out_dir,
        manifest,
        liveness,
        responder_url=http_ok_url,
    )

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
    assert len(entry["tables"]) == 1
    # Resolve symlinks (e.g. macOS /tmp -> /private/tmp) on both sides
    # before comparing — `samefile` handles that.
    assert Path(entry["tables"][0]["file"]).samefile(parquets[0])

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
        exec_plan_ts="2026-06-04T07:14:28.376000+00:00",
        collection_id="cid",
        collection_name="c",
        function_id="fn",
        function_name="publish_people",
    )


def _bare_info() -> Info:
    return Info(
        project_id="P-1",
        collection_id=1,
        collection="c",
        function_version_id="FV-1",
        function_defined_on=1780000000000,
        function="fn",
        function_run_id=4,
        function_bundle=Location(uri="file:///bundle.tgz"),
        function_stage=Location(uri="file:///stage"),
        triggered_on=1780000000100,
        transaction_id=3,
        execution_id=2,
        execution_name=None,
        function_data=Location(uri="file:///data"),
        scheduled_on=1780000000200,
    )


def _input_table(name: str, *, location: Location | None) -> InputTable:
    return InputTable(
        name=name,
        collection_id=1,
        collection="c",
        table_id=f"T-{name}",
        table_version_id=f"TV-{name}",
        location=location,
        input_idx=0,
        table_pos=-1,
        version_pos=-1,
    )


def _output_table(name: str, *, location: Location) -> OutputTable:
    return OutputTable(
        name=name,
        collection_id=1,
        collection="c",
        table_id=f"T-{name}",
        table_version_id=f"TV-{name}",
        table_data_version_id=f"PDV-{name}",
        location=location,
        table_pos=-1,
    )


def _bare_request(
    *,
    system_input=(),
    system_output=(),
) -> Request:
    return Request(
        info=_bare_info(),
        system_input=system_input,
        system_output=system_output,
    )


def test_hydrate_noop_when_system_input_empty(tmp_path):
    ctx = _bare_ctx()
    hydrate_ctx_from_state_or_raise(
        ctx, _bare_request(), tmp_path, error_code=RunSourcePluginErrorCode.RSP_9
    )
    assert ctx.drain() == {}


def test_hydrate_noop_when_slot_has_no_location(tmp_path):
    ctx = _bare_ctx()
    req = _bare_request(system_input=(_input_table("iv", location=None),))
    hydrate_ctx_from_state_or_raise(
        ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_9
    )
    assert ctx.drain() == {}


def test_hydrate_noop_when_state_file_missing(tmp_path):
    """First run for a flow: the slot has a location (the URI in the
    request), but the transporter never materialised it locally —
    e.g. first time the flow runs. Start with empty scratch."""
    ctx = _bare_ctx()
    req = _bare_request(
        system_input=(_input_table("iv", location=Location(uri="file:///x")),),
    )
    hydrate_ctx_from_state_or_raise(
        ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_9
    )
    assert ctx.drain() == {}


def test_hydrate_loads_values_when_state_file_present(tmp_path):
    state_file = tmp_path / "iv.parquet"
    write_initial_values({"last-modified": "2026-05-27T00:00:00Z"}, state_file)
    ctx = _bare_ctx()
    req = _bare_request(
        system_input=(_input_table("iv", location=Location(uri="file:///x")),),
    )
    hydrate_ctx_from_state_or_raise(
        ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_9
    )
    assert ctx.drain() == {"last-modified": "2026-05-27T00:00:00Z"}


def test_hydrate_malformed_state_file_raises_bad_initial_values(tmp_path):
    state_file = tmp_path / "iv.parquet"
    state_file.write_bytes(b"not parquet")
    ctx = _bare_ctx()
    req = _bare_request(
        system_input=(_input_table("iv", location=Location(uri="file:///x")),),
    )
    with pytest.raises(BadStateInError):
        hydrate_ctx_from_state_or_raise(
            ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_9
        )


def test_drain_noop_when_system_output_empty(tmp_path):
    ctx = _bare_ctx()
    ctx.set_attr("foo", "bar")  # has state but nowhere to send it.
    drain_ctx_to_state_or_raise(
        ctx, _bare_request(), tmp_path, error_code=RunSourcePluginErrorCode.RSP_10
    )
    assert list(tmp_path.iterdir()) == []


def test_drain_noop_when_slot_has_no_location(tmp_path):
    ctx = _bare_ctx()
    ctx.set_attr("foo", "bar")
    # OutputTable.location is required by pydantic; bypass validation
    # with model_construct to exercise the drain function's defensive
    # ``if slot.location is None: return`` branch.
    slot = OutputTable.model_construct(
        name="iv",
        collection_id=1,
        collection="c",
        table_id="T-iv",
        table_version_id="TV-iv",
        table_data_version_id="PDV-iv",
        location=None,
        table_pos=-1,
    )
    req = _bare_request(system_output=(slot,))
    drain_ctx_to_state_or_raise(
        ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_10
    )
    assert list(tmp_path.iterdir()) == []


def test_drain_writes_state_file_when_slot_present(tmp_path):
    ctx = _bare_ctx()
    ctx.set_attr("last-modified", "2026-05-28T00:00:00Z")
    req = _bare_request(
        system_output=(_output_table("iv", location=Location(uri="file:///x")),),
    )
    drain_ctx_to_state_or_raise(
        ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_10
    )
    written = tmp_path / "iv.parquet"
    assert written.is_file()
    assert read_initial_values(written) == {"last-modified": "2026-05-28T00:00:00Z"}


def test_drain_writes_empty_file_when_ctx_empty(tmp_path):
    """An empty scratch store still drains to an empty Parquet — the
    plugin explicitly didn't want to persist anything this run."""
    ctx = _bare_ctx()
    req = _bare_request(
        system_output=(_output_table("iv", location=Location(uri="file:///x")),),
    )
    drain_ctx_to_state_or_raise(
        ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_10
    )
    written = tmp_path / "iv.parquet"
    assert written.is_file()
    assert read_initial_values(written) == {}


def test_drain_creates_state_dir_if_missing(tmp_path):
    state_dir = tmp_path / "fresh"
    assert not state_dir.exists()
    ctx = _bare_ctx()
    ctx.set_attr("k", "v")
    req = _bare_request(
        system_output=(_output_table("iv", location=Location(uri="file:///x")),),
    )
    drain_ctx_to_state_or_raise(
        ctx, req, state_dir, error_code=RunSourcePluginErrorCode.RSP_10
    )
    assert (state_dir / "iv.parquet").is_file()


def test_drain_bad_value_raises_bad_initial_values(tmp_path, monkeypatch):
    """If something poisons the ctx with a non-BasicTypeSpec value,
    the codec rejects it and the command surfaces BadStateOutError."""
    ctx = _bare_ctx()
    # Bypass set_attr's type-friendly surface to inject a bad value
    # directly into the underlying scratch dict.
    ctx._scratch["k"] = object()  # type: ignore[assignment]
    req = _bare_request(
        system_output=(_output_table("iv", location=Location(uri="file:///x")),),
    )
    with pytest.raises(BadStateOutError):
        drain_ctx_to_state_or_raise(
            ctx, req, tmp_path, error_code=RunSourcePluginErrorCode.RSP_10
        )


# --- main: end-to-end with initial_values round-trip --------------------


def test_main_hydrates_and_drains_initial_values(
    tmp_path, built_bundle, monkeypatch, http_ok_url
):
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
            "Function": {
                "info": _info_payload(),
                "output": [
                    _output_table_payload(
                        "my_table", location={"uri": "file:///out/my_table"}
                    )
                ],
                "system_input": [
                    _input_table_payload(
                        "iv-in", location={"uri": "file:///irrelevant"}
                    )
                ],
                "system_output": [
                    _output_table_payload(
                        "iv-out", location={"uri": "file:///irrelevant"}
                    )
                ],
            }
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
        responder_url=http_ok_url,
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
