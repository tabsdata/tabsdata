#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime
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
from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.server._tdrunner.function._commons.document.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function._handlers.state import (
    STATE_FILE,
    read_state,
    write_state,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin import main
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.error import (
    BadBundleError,
    BadRequestError,
    BadUserCodeError,
    ManifestWriteFailedError,
    NoPluginError,
    PluginFailedError,
    UnsupportedStageSchemeError,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.runner import (  # noqa: E501
    SourceBinding,
    SourceContext,
    bind_source,
    build_manifest_stage_connection,
    load_function_definition,
    read_in,
    table_file_path,
    write_inputs_manifest,
)
from tabsdatak.spi import TableFileInput
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

HANDLER = TransporterHandler1()


RUNNER_MODULE = (
    "tabsdatak.server._tdrunner.function.commands.runners.source_plugin.runner"
)


# --- module-level stream-publisher fixture --------------------------------
#
# Defined at module top level (not inside a test) because the bundle
# serialiser stores ``func`` as ``"module:qualname"`` and rejects
# locally-defined callables (those have ``<locals>`` in their qualname).
# Used by ``test_load_function_def_accepts_stream_publisher_kind``.

from tabsdatak.api import stream_publisher as _stream_publisher_decorator  # noqa: E402
from tabsdatak.conn.kafka import KafkaStreamSrc as _KafkaStreamSrc  # noqa: E402


@_stream_publisher_decorator(
    source=_KafkaStreamSrc(data_format="json", schema="{}", topic="t"),
    batch_max_secs=60,
    output_tables=["t"],
)
def module_level_stream_publisher(tables: TableFramesSpec) -> TableFrameSpec:
    return None


# --- fixtures and helpers --------------------------------------------------


USER_PUBLISHER_SOURCE = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc

_SRC = LocalFileSrc(
    paths=["*.parquet"],
)


@publisher(
    source=_SRC,
    output_tables=["my_table"],
)
def my_publisher(in_1: TableFramesSpec) -> TableFrameSpec:
    return None
"""


def write_sample_parquet(path: Path) -> None:
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
    write_sample_parquet(srcdata / "rows.parquet")

    project = tmp_path / "project"
    package = project / "my_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "publisher.py").write_text(USER_PUBLISHER_SOURCE)
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


def input_table_payload(name: str, *, location: dict | None = None) -> dict:
    return {
        "Table": {
            "collection_id": 1,
            "collection": "c",
            "table_id": f"T-{name}",
            "table": name,
            "table_version_id": f"TV-{name}",
            "location": location,
            "input_idx": 0,
            "table_pos": -1,
            "version_pos": -1,
        }
    }


def output_table_payload(name: str, *, location: dict) -> dict:
    return {
        "Table": {
            "collection_id": 1,
            "collection": "c",
            "table_id": f"T-{name}",
            "table": name,
            "table_version_id": f"TV-{name}",
            "logical_table_data_version_id": f"LDV-{name}",
            "physical_table_data_version_id": f"PDV-{name}",
            "location": location,
            "table_pos": 0,
        }
    }


def write_request(path: Path, bundle_uri: str = "file:///bundle.tar.zst") -> Path:
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(bundle_uri=bundle_uri),
                "output": [
                    output_table_payload(
                        "my_table", location={"uri": "file:///out/my_table"}
                    )
                ],
            }
        })
    )
    return path


def write_connection_file(path: Path, base_path: Path) -> Path:
    """Write the connection.yaml the command loads via --connection-file.

    The connection is no longer carried on the function descriptor; the
    runtime reads it from this file (the collection's connection)."""
    from tabsdatak.conn.localfile import LocalFileSrcConn

    path.write_text(
        yaml.safe_dump(LocalFileSrcConn(base_path=str(base_path)).to_dict())
    )
    return path


def set_arguments(
    monkeypatch,
    request_path: Path,
    bundle_folder: Path,
    output_folder: Path,
    inputs_manifest: Path,
    liveness: Path,
    *,
    connection_file: Path,
    responder_url: str,
    responder_method: str = "POST",
    responder_authorization: str = "Bearer tok",
    state_dir: Path | None = None,
) -> None:
    if state_dir is None:
        # Default to a sibling dir of out_dir so callers that don't care
        # about state lifecycle don't have to plumb a path.
        state_dir = output_folder.parent / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-source-plugin",
            "--request",
            str(request_path),
            "--bundle-folder",
            str(bundle_folder),
            "--output-folder",
            str(output_folder),
            "--inputs-manifest",
            str(inputs_manifest),
            "--connection-file",
            str(connection_file),
            "--liveness",
            str(liveness),
            "--state-folder",
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


# --- unit: table_file_path ------------------------------------------


def test_normalise_triple_slash_uri():
    assert table_file_path("file:///tmp/x.parquet") == "/tmp/x.parquet"


def test_normalise_single_slash_uri():
    # `Path("file:///x")` collapses to `"file:/x"` via `str()`; the
    # function has to handle both shapes.
    assert table_file_path("file:/tmp/x.parquet") == "/tmp/x.parquet"


def test_normalise_path_object_wrapping_uri():
    assert table_file_path(Path("file:///tmp/x.parquet")) == "/tmp/x.parquet"


def test_normalise_uri_with_percent_encoding():
    assert (
        table_file_path("file:///tmp/with%20space.parquet") == "/tmp/with space.parquet"
    )


def test_normalise_uri_scheme_is_case_insensitive():
    assert table_file_path("FILE:///tmp/x.parquet") == "/tmp/x.parquet"
    assert table_file_path("File:/tmp/x.parquet") == "/tmp/x.parquet"


def test_normalise_bare_absolute_path():
    # An already-absolute path is returned posix-canonical and unchanged
    # (on POSIX; on Windows `Path.absolute()` anchors it to the cwd drive,
    # so compare against the same resolution the function performs).
    assert (
        table_file_path("/tmp/x.parquet")
        == Path("/tmp/x.parquet").absolute().as_posix()
    )


def test_normalise_bare_relative_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # Manifest paths are posix-canonical, so the resolved absolute path
    # comes back with forward slashes on every OS.
    assert table_file_path("x.parquet") == (tmp_path / "x.parquet").as_posix()


# --- unit: SourceContext ----------------------------------------------------


def source_context(**overrides) -> SourceContext:
    """Default-fill the new SrcPluginCtx fields so each test only has
    to override what it's exercising."""
    defaults = dict(
        work_folder=Path("/work"),
        project_id="p-1",
        project="p",
        collection_id="c_id",
        collection="c_name",
        function_id="fn_run_42",
        function="publish_people",
        function_version_id="fv-1",
        execution_id="exec1",
        execution=None,
        transaction_id="tx1",
        logical_function_run_id="lfr-1",
        physical_function_run_id="pfr-1",
        triggered_on=datetime.fromisoformat("2026-06-04T07:14:28.376000+00:00"),
    )
    defaults.update(overrides)
    return SourceContext(**defaults)


def test_src_context_returns_work_dir():
    assert source_context().work_dir == Path("/work")


def test_src_context_exposes_execution_id():
    assert source_context().execution_id == "exec1"


def test_src_context_exposes_transaction_id():
    assert source_context().transaction_id == "tx1"


def test_src_context_exposes_collection_id():
    assert source_context().collection_id == "c_id"


def test_src_context_exposes_collection():
    assert source_context().collection == "c_name"


def test_src_context_exposes_function_id():
    assert source_context().function_id == "fn_run_42"


def test_src_context_exposes_function():
    assert source_context().function == "publish_people"


def test_src_context_exposes_triggered_on():
    assert source_context().triggered_on == datetime.fromisoformat(
        "2026-06-04T07:14:28.376000+00:00"
    )


def test_src_context_get_attr_returns_none_for_unset_key():
    """``get_attr`` honours the ``returns None if unset`` contract
    on a fresh ctx."""
    assert source_context().get_attr("k") is None


def test_src_context_set_attr_round_trips_via_get_attr():
    """``set_attr`` writes through; a subsequent ``get_attr`` sees
    the value."""
    context = source_context()
    context.set_attr("k", "v")
    assert context.get_attr("k") == "v"


def test_src_context_add_metadata_is_noop():
    """No observability sink wired yet -- ``add_metadata`` is a no-op."""
    assert source_context().add_metadata("k", "v") is None


# --- unit: load_function_definition --------------------------------------------


def test_load_function_def_happy(built_bundle):
    publisher = load_function_definition(built_bundle)
    assert isinstance(publisher, PublisherInfo)
    assert list(publisher.output_tables) == ["my_table"]


def test_load_function_def_missing_manifest_raises_bad_bundle(tmp_path):
    empty = tmp_path / "empty-bundle"
    empty.mkdir()
    with pytest.raises(BadBundleError):
        load_function_definition(empty)


def test_load_function_def_malformed_json_raises_bad_bundle(tmp_path):
    bad = tmp_path / "bad-bundle"
    bad.mkdir()
    (bad / BUNDLE_MANIFEST_FILENAME).write_text("{ not json")
    with pytest.raises(BadBundleError):
        load_function_definition(bad)


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
        load_function_definition(built_bundle)


# --- unit: _resolve_plugin -----------------------------------------------


def test_resolve_plugin_and_io_publisher_passthrough(tmp_path, fixture_publisher):
    """For a regular ``@publisher``, ``bind_source`` returns
    the plugin + user-declared source verbatim, and the conn loaded from
    the connection file."""
    from tabsdatak.conn.localfile import LocalFileSrcConn
    from tabsdatak.spi import SrcPlugin

    # function_stage is unused on the PublisherInfo path; pass a stub.
    stub_stage = Location(uri="file:///unused")
    connection_file = write_connection_file(
        tmp_path / "connection.yaml", tmp_path / "srcdata"
    )
    binding = bind_source(fixture_publisher, stub_stage, connection_file)
    assert isinstance(binding.plugin, SrcPlugin)
    assert isinstance(binding.connection, LocalFileSrcConn)
    assert binding.source is fixture_publisher.source
    assert binding.cardinality == 1  # LocalFileSrc with one path


def test_resolve_plugin_and_io_unknown_source_raises_no_plugin(
    tmp_path, fixture_publisher, monkeypatch
):
    """When `get_src` raises `KeyError`, the runner surfaces
    `NoPluginError`."""

    def missing_source(_):
        raise KeyError("no connector registered")

    monkeypatch.setattr(f"{RUNNER_MODULE}.get_src", missing_source)
    with pytest.raises(NoPluginError):
        # `get_src` raises before the connection file is read, so a
        # non-existent path is fine for the 3rd arg.
        bind_source(
            fixture_publisher,
            Location(uri="file:///unused"),
            tmp_path / "no-connection.yaml",
        )


# --- unit: build_manifest_stage_connection (stream-publisher substitution) --


def test_build_manifest_stage_conn_file_scheme_resolves_localfile():
    """file:// keeps the connector-conn form: a LocalFileSrcConn in the
    `local` slot and no prebuilt `stage_backend`. ManifestStageSrcPlugin's
    backend_for_conn reads the slot. The integration tests use the same
    StageConnection idiom."""
    from tabsdatak.conn.localfile import LocalFileSrcConn

    connection = build_manifest_stage_connection(Location(uri="file:///tmp/stage"))
    assert connection.stage_backend is None
    assert isinstance(connection.local, LocalFileSrcConn)
    assert str(connection.local.base_path) == "/tmp/stage"
    assert connection.s3 is None
    assert connection.az is None
    assert connection.gcs is None


def test_build_manifest_stage_conn_s3_resolves_to_object_store_backend(monkeypatch):
    """Cloud schemes resolve (via endpoint.py) to a prebuilt StageBackend
    carried on `stage_backend`; backend_for_conn returns it as-is. The
    stage-relative path is appended to the stage-root URL."""
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"):
        monkeypatch.setenv(f"SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__{key}", "v")
    connection = build_manifest_stage_connection(
        Location(uri="s3://bucket/stage", env_prefix="ROOT")
    )
    assert connection.local is None
    backend = connection.stage_backend
    assert backend is not None
    assert backend.is_object_store is True
    loc = backend.build_location("manifests/stream-manifest-*.json", HANDLER)
    assert loc.url == "s3://bucket/stage/manifests/stream-manifest-*.json"


def test_build_manifest_stage_conn_az_resolves():
    connection = build_manifest_stage_connection(
        Location(uri="az://account/container/stage")
    )
    assert connection.stage_backend is not None
    assert connection.stage_backend.is_object_store is True


def test_build_manifest_stage_conn_gs_resolves():
    """GCS uses the ``gs://`` scheme -- endpoint.py's registered builder."""
    connection = build_manifest_stage_connection(Location(uri="gs://bucket/stage"))
    assert connection.stage_backend is not None
    assert connection.stage_backend.is_object_store is True


def test_build_manifest_stage_conn_rejects_unsupported_scheme():
    """An unknown scheme still surfaces RSP_11 with a clean exit code.
    Note: GCS stages must use ``gs://`` -- ``gcs://`` has no endpoint
    builder and is rejected here."""
    with pytest.raises(UnsupportedStageSchemeError):
        build_manifest_stage_connection(Location(uri="gcs://bucket/stage"))


def test_resolve_plugin_and_io_stream_publisher_substitutes_to_manifest_stage(
    tmp_path,
):
    """For a ``@stream_publisher``, ``bind_source`` swaps
    the user-declared Kafka source for the publisher-side
    StreamStageSrc + a StageConnection shaped like ManifestStageConn
    that ``ManifestStageSrcPlugin`` knows how to read. Cardinality
    drops to ``-1`` because the manifest decides slot count at read
    time."""
    from tabsdatak.api import stream_publisher
    from tabsdatak.conn.kafka import KafkaStreamSrc
    from tabsdatak.conn.localfile import LocalFileSrcConn
    from tabsdatak.spi import StreamStageSrc, StreamStageSrcPlugin

    @stream_publisher(
        source=KafkaStreamSrc(data_format="json", schema="{}", topic="t"),
        batch_max_secs=60,
        output_tables=["t"],
    )
    def my_stream_publisher(tables: TableFramesSpec) -> TableFrameSpec:
        return None

    # The stream-publisher path substitutes the conn from function_stage
    # and never reads the connection file, so a stub path is fine here.
    binding = bind_source(
        my_stream_publisher, Location(uri="file:///stage/dir"), tmp_path / "unused.yaml"
    )
    # Plugin substituted to StreamStageSrcPlugin (which delegates to
    # ManifestStageSrcPlugin internally on read_in).
    assert isinstance(binding.plugin, StreamStageSrcPlugin)
    # Conn substituted to a duck-typed namespace with file:// backend.
    assert isinstance(binding.connection.local, LocalFileSrcConn)
    assert str(binding.connection.local.base_path) == "/stage/dir"
    assert binding.connection.s3 is None
    assert binding.connection.az is None
    assert binding.connection.gcs is None
    # Source substituted to a StreamStageSrc instance.
    assert isinstance(binding.source, StreamStageSrc)
    # Cardinality is -1 (dynamic, from the StreamStageSrc registration).
    assert binding.cardinality == -1


def test_load_function_def_accepts_stream_publisher_kind(tmp_path):
    """`load_function_definition`'s allowed_kinds now includes
    ``streamPublisherDef``. Synthesise a minimal function_def.json by
    hand (no full bundle build) and verify the loader returns a
    StreamPublisherInfo.

    Uses the module-level ``module_level_stream_publisher`` fixture below
    so the bundle serialiser can resolve ``module:qualname`` for the
    ``func`` field (locally-defined callables would fail the
    "must be importable at module top level" check)."""
    import json

    from tabsdatak._info import KIND_STREAM_PUBLISHER_DEF, StreamPublisherInfo
    from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME

    bundle_folder = tmp_path / "extracted"
    bundle_folder.mkdir()
    (bundle_folder / BUNDLE_MANIFEST_FILENAME).write_text(
        json.dumps(module_level_stream_publisher.to_dict())
    )

    publisher = load_function_definition(bundle_folder)
    assert isinstance(publisher, StreamPublisherInfo)
    assert publisher.__kind__ == KIND_STREAM_PUBLISHER_DEF


# --- unit: read_in ----------------------------------------------


def test_invoke_read_in_returns_plugin_result():
    class FakePlugin:
        def read_in(self, context, connection, source):
            return [[TableFileInput(file=Path("/x.parquet"))]]

    output = read_in(SourceBinding(FakePlugin(), object(), object(), 1), object())
    assert output == [[TableFileInput(file=Path("/x.parquet"))]]


def test_invoke_read_in_wraps_exceptions_as_plugin_failed():
    class BrokenPlugin:
        def read_in(self, context, connection, source):
            raise RuntimeError("boom")

    with pytest.raises(PluginFailedError):
        read_in(SourceBinding(BrokenPlugin(), object(), object(), 1), object())


# --- unit: write_inputs_manifest ----------------------------------------


def test_write_inputs_manifest_happy(tmp_path, fixture_publisher):
    output = tmp_path / "manifest.yaml"
    metadata = {
        "@td.file.path": "s3://bucket/in/a.csv",
        "@td.file.rows": 2,
        "@td.file.seconds": 1.5,
        "@td.file.truncated": True,
    }
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    produced = [
        [
            TableFileInput(file=a, metadata=metadata),
            TableFileInput(file=b),
        ]
    ]
    write_inputs_manifest(fixture_publisher, produced, output, cardinality=1)
    data = yaml.safe_load(output.read_text())

    def record(file, metadata=None):
        return {
            "file": file,
            "location": None,
            "metadata": metadata,
            "input_idx": None,
            "version_pos": -1,
            "execution_id": None,
            "transaction_id": None,
            "triggered_on": None,
            "physical_table_data_version_id": None,
        }

    # Each metadata value type round-trips through the manifest YAML.
    assert data == {
        "inputs": [
            {
                "name": "in_1",
                "table_pos": 0,
                "is_empty": False,
                "tables": [
                    record(a.as_posix(), metadata),
                    record(b.as_posix()),
                ],
            }
        ],
    }


def test_write_inputs_manifest_normalises_file_uris(tmp_path, fixture_publisher):
    """The plugin may return `Path("file:///...")` URI-shaped values
    rather than bare paths; the manifest must always carry plain
    absolute filesystem paths."""
    output = tmp_path / "manifest.yaml"
    produced = [[TableFileInput(file=Path("file:///tmp/uri-shaped.parquet"))]]
    write_inputs_manifest(fixture_publisher, produced, output, cardinality=1)
    data = yaml.safe_load(output.read_text())
    assert [tables["file"] for tables in data["inputs"][0]["tables"]] == [
        "/tmp/uri-shaped.parquet"
    ]


def test_write_inputs_manifest_cardinality_mismatch_raises(tmp_path, fixture_publisher):
    """Source cardinality is 1 (`LocalFileSrc` with one path); a plugin
    that returns two slots is a contract violation."""
    output = tmp_path / "manifest.yaml"
    produced = [
        [TableFileInput(file=Path("/x.parquet"))],
        [TableFileInput(file=Path("/y.parquet"))],
    ]
    with pytest.raises(PluginFailedError):
        write_inputs_manifest(fixture_publisher, produced, output, cardinality=1)


def test_write_inputs_manifest_negative_cardinality_pads_to_param_count(
    tmp_path, fixture_publisher
):
    """``cardinality=-1`` (dynamic slot count -- e.g. a stream-stage
    manifest source) pads the manifest up to the function-declared
    input-slot count with empty entries when the plugin returned
    fewer slots. This is what makes a ``tdk fn trigger`` against a
    ``@stream_publisher`` work when no manifest has been staged: the
    user function is invoked with ``t=[]`` (one empty slot per
    declared parameter) instead of bailing on the input-count
    mismatch in ``run-function``.

    ``fixture_publisher`` has one input parameter (``in_1``), so 0
    produced slots → 1 padded entry whose ``tables`` is empty."""
    output = tmp_path / "manifest.yaml"
    write_inputs_manifest(fixture_publisher, [], output, cardinality=-1)
    data = yaml.safe_load(output.read_text())
    assert len(data["inputs"]) == 1
    assert data["inputs"][0]["name"] == "in_1"
    assert data["inputs"][0]["tables"] == []


def test_write_inputs_manifest_negative_cardinality_does_not_pad_when_matched(
    tmp_path, fixture_publisher
):
    """When the dynamic source produced exactly the right number of
    slots, the manifest writes them verbatim — no padding needed."""
    output = tmp_path / "manifest.yaml"
    a = tmp_path / "a.parquet"
    write_inputs_manifest(
        fixture_publisher,
        [[TableFileInput(file=a)]],
        output,
        cardinality=-1,
    )
    data = yaml.safe_load(output.read_text())
    assert len(data["inputs"]) == 1
    assert data["inputs"][0]["name"] == "in_1"
    assert [tables["file"] for tables in data["inputs"][0]["tables"]] == [a.as_posix()]


def test_write_inputs_manifest_negative_cardinality_overproduction_raises(
    tmp_path, fixture_publisher
):
    """Over-production (source returned more slots than the function
    declares) is still a contract violation even under dynamic
    cardinality — we can't index past ``parameters[i]``."""
    output = tmp_path / "manifest.yaml"
    with pytest.raises(PluginFailedError):
        write_inputs_manifest(
            fixture_publisher,
            [
                [TableFileInput(file=Path("/x.parquet"))],
                [TableFileInput(file=Path("/y.parquet"))],
            ],
            output,
            cardinality=-1,
        )


def test_write_inputs_manifest_oserror_raises_manifest_write_failed(
    tmp_path, fixture_publisher
):
    """Writing to a path that's actually a directory triggers
    `IsADirectoryError` (a subclass of `OSError`)."""
    a_folder = tmp_path / "i-am-a-dir"
    a_folder.mkdir()
    produced = [[TableFileInput(file=Path("/x.parquet"))]]
    with pytest.raises(ManifestWriteFailedError):
        write_inputs_manifest(fixture_publisher, produced, a_folder, cardinality=1)


# --- main: error paths via argparse ---------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch, http_ok_url):
    set_arguments(
        monkeypatch,
        tmp_path / "no-such.yaml",
        tmp_path / "bundle",
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
        connection_file=tmp_path / "connection.yaml",
        responder_url=http_ok_url,
    )
    assert main() == BadRequestError.exit_code


def test_main_missing_bundle_returns_bad_bundle(tmp_path, monkeypatch, http_ok_url):
    request_path = write_request(tmp_path / "request.yaml")
    set_arguments(
        monkeypatch,
        request_path,
        tmp_path / "missing-bundle",
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
        connection_file=tmp_path / "connection.yaml",
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
    request_path = write_request(tmp_path / "request.yaml")
    output_folder = tmp_path / "out"
    output_folder.mkdir()
    manifest = tmp_path / "manifest.yaml"
    liveness = tmp_path / "live"
    # The source connection's base_path is the srcdata dir the
    # `user_project` fixture wrote `rows.parquet` into.
    connection_file = write_connection_file(
        tmp_path / "connection.yaml", tmp_path / "srcdata"
    )

    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        output_folder,
        manifest,
        liveness,
        connection_file=connection_file,
        responder_url=http_ok_url,
    )

    status = main()
    assert status == 0, f"main() returned {status} (non-zero)"

    # Exactly one parquet should have been produced.
    parquets = list(output_folder.rglob("*.parquet"))
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


# --- main: end-to-end with initial_values round-trip --------------------


def test_main_hydrates_and_drains_initial_values(
    tmp_path, built_bundle, monkeypatch, http_ok_url
):
    """Store a previous-run state at system_input[0]'s location, let
    main() run the plugin (which is unaware of state), and verify drain
    produced this run's local state file. The publisher plugin doesn't
    itself mutate ctx, so the drained dict equals the hydrated one."""
    output_folder = tmp_path / "out"
    output_folder.mkdir()
    state_dir = tmp_path / "state"
    previous = tmp_path / "storage" / "state-in.t"
    previous.parent.mkdir()
    write_state({"last-modified": "2026-05-27T00:00:00Z"}, previous)

    req_path = tmp_path / "request.yaml"
    req_path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(),
                "output": [
                    output_table_payload(
                        "my_table", location={"uri": "file:///out/my_table"}
                    )
                ],
                "system_input": [
                    input_table_payload(
                        "td.fn_state__F-1", location={"uri": previous.as_uri()}
                    )
                ],
                "system_output": [
                    output_table_payload(
                        "td.fn_state__F-1",
                        location={"uri": (tmp_path / "storage/state-out.t").as_uri()},
                    )
                ],
            }
        })
    )
    manifest = tmp_path / "manifest.yaml"
    liveness = tmp_path / "live"
    # base_path is the srcdata dir the `user_project` fixture populated.
    connection_file = write_connection_file(
        tmp_path / "connection.yaml", tmp_path / "srcdata"
    )
    set_arguments(
        monkeypatch,
        req_path,
        built_bundle,
        output_folder,
        manifest,
        liveness,
        connection_file=connection_file,
        responder_url=http_ok_url,
        state_dir=state_dir,
    )

    status = main()
    assert status == 0, f"main() returned {status}"

    assert read_state(TableFile.local(state_dir / STATE_FILE)) == {
        "last-modified": "2026-05-27T00:00:00Z"
    }
    assert not (tmp_path / "storage" / "state-out.t").exists()
