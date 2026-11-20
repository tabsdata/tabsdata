#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import io
import json
import sys
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import obstore
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain
from tabsdatax.itest.fixture.emulators import EMULATORS, emulated

from tabsdatak._info import PublisherInfo

# Imported at module level so that string-form type annotations (under
# ``from __future__ import annotations``) on locally-defined
# @stream_publisher fixtures resolve via ``get_type_hints``.
from tabsdatak.api import (  # noqa: F401
    TableFrameSpec,
    TableFramesSpec,
    TransactionCtx,
)
from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak.common._storage.transfer import OBJECT_STORES
from tabsdatak.server._tdrunner.function._commons.context.source import SourceContext
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
    Location,
)
from tabsdatak.server._tdrunner.function._commons.document.state.function import (
    FunctionState,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputManifest,
)
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    FunctionBundle,
)
from tabsdatak.server._tdrunner.function._handlers.binding import (
    SourceBinding,
    StageConnection,
    UnknownSourceError,
    UnsupportedStageError,
)
from tabsdatak.server._tdrunner.function._handlers.bundle import BundleArchive
from tabsdatak.server._tdrunner.function._handlers.handoff import Handoff
from tabsdatak.server._tdrunner.function._handlers.landing import LandedFile
from tabsdatak.server._tdrunner.function._handlers.state import (
    STATE_FILE,
    State,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin import main
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.error import (
    BadBundleError,
    BadConnectionError,
    BadRequestError,
    BadUserCodeError,
    ManifestWriteFailedError,
    NoPluginError,
    PluginFailedError,
    RunSourcePluginErrorCode,
    UnsupportedStageSchemeError,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.options import (  # noqa: E501
    RunSourcePluginArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.runner import (  # noqa: E501
    DEFINITION_ERRORS,
    KINDS,
    SourcePlugin,
)
from tabsdatak.spi import LazyFrameInput, SrcPlugin, TableFileInput
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    emulated_run,
    info_payload,
    run_location,
    stored,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


BINDING_MODULE = "tabsdatak.server._tdrunner.function._handlers.binding"


def source_plugin(tmp_path: Path, **state) -> SourcePlugin:
    command = SourcePlugin(
        RunSourcePluginArguments(
            request=tmp_path / "request.yaml",
            bundle_folder=tmp_path / "bundle",
            context_folder=tmp_path / "output",
            inputs_manifest=tmp_path / "inputs.yaml",
            state_folder=tmp_path / "state",
            connection_file=tmp_path / "connection.yaml",
            liveness=tmp_path / "liveness",
            responder_url="http://127.0.0.1:1",
            responder_method="POST",
            responder_authorization="Bearer tok",
            journal_file=tmp_path / "journal.yaml",
        )
    )
    for name, value in state.items():
        setattr(command, name, value)
    return command


def load_function_definition(bundle_folder: Path):
    return FunctionBundle(bundle_folder).definition(KINDS, DEFINITION_ERRORS)


# --- module-level stream-publisher fixture --------------------------------
#
# Defined at module top level (not inside a test) because the bundle
# serialiser stores ``func`` as ``"module:qualname"`` and rejects
# locally-defined callables (those have ``<locals>`` in their qualname).
# Used by ``test_load_function_def_accepts_stream_publisher_kind``.

from tabsdatak.api import stream_publisher as _stream_publisher_decorator  # noqa: E402
from tabsdatak.conn.kafka import KafkaStreamSrc as _KafkaStreamSrc  # noqa: E402


@_stream_publisher_decorator(
    source=_KafkaStreamSrc(
        data_format="json",
        schema="{}",
        topic="t",
    ),
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
def built_bundle(
    tmp_path,
    user_project,
    fixture_publisher,
) -> Path:
    """Build and extract the bundle. Returns the extracted directory."""
    bundle_path = tmp_path / "bundle.tar.zst"
    build_bundle(
        fixture_publisher,
        user_project,
        bundle_path,
    )
    extracted = tmp_path / "extracted"
    BundleArchive(bundle_path).extract(extracted)
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


def write_request(
    path: Path,
    bundle_uri: str = "file:///bundle.tar.zst",
    scratch: dict[str, str] | None = None,
    landing: dict[str, str] | None = None,
) -> Path:
    if landing is None:
        landing = {"uri": (path.parent / "landing").as_uri(), "env_prefix": "ROOT"}
    if scratch is None:
        scratch = {"uri": (path.parent / "scratch").as_uri(), "env_prefix": "ROOT"}
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(
                    bundle_uri=bundle_uri,
                    function_landing=landing,
                    function_scratch=scratch,
                ),
                "output": [
                    output_table_payload(
                        "my_table",
                        location={"uri": "file:///out/my_table"},
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
    context_folder: Path,
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
        state_dir = context_folder.parent / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "source-plugin",
            "--request",
            str(request_path),
            "--bundle-folder",
            str(bundle_folder),
            "--context-folder",
            str(context_folder),
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


# --- unit: SourceBinding.of ---------------------------------------------


def test_a_publisher_binds_its_own_source(tmp_path, fixture_publisher):
    """For a regular ``@publisher``, ``SourceBinding.of`` returns
    the plugin + user-declared source verbatim, and the conn loaded from
    the connection file."""
    from tabsdatak.conn.localfile import LocalFileSrcConn
    from tabsdatak.spi import SrcPlugin

    # function_stage is unused on the PublisherInfo path; pass a stub.
    stub_stage = Location(uri="file:///unused")
    connection_file = write_connection_file(
        tmp_path / "connection.yaml",
        tmp_path / "srcdata",
    )
    binding = SourceBinding.of(
        fixture_publisher,
        stub_stage,
        connection_file,
    )
    assert isinstance(binding.plugin, SrcPlugin)
    assert isinstance(binding.connection, LocalFileSrcConn)
    assert binding.source is fixture_publisher.source
    assert binding.cardinality == 1  # LocalFileSrc with one path


def test_an_unknown_source_cannot_be_bound(
    tmp_path,
    fixture_publisher,
    monkeypatch,
):
    """When `get_src` raises `KeyError`, the binding raises
    `UnknownSourceError`, which the command reports as `NoPluginError`."""

    def missing_source(_):
        raise KeyError("no connector registered")

    monkeypatch.setattr(f"{BINDING_MODULE}.get_src", missing_source)
    with pytest.raises(UnknownSourceError):
        # `get_src` raises before the connection file is read, so a
        # non-existent path is fine for the 3rd arg.
        SourceBinding.of(
            fixture_publisher,
            Location(uri="file:///unused"),
            tmp_path / "no-connection.yaml",
        )


# --- unit: StageConnection.of (stream-publisher substitution) --------------


def test_a_local_stage_connection_carries_the_stage_location():
    connection = StageConnection.of(Location(uri="file:///tmp/stage"))
    assert connection.location.uri == "file:///tmp/stage"


def test_a_cloud_stage_connection_carries_the_mount_options(monkeypatch):
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"):
        monkeypatch.setenv(f"SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__{key}", "v")
    connection = StageConnection.of(
        Location(uri="s3://bucket/stage", env_prefix="ROOT")
    )
    assert connection.location.uri == "s3://bucket/stage"
    assert connection.location.options


@pytest.mark.parametrize(
    "uri",
    ["az://account/container/stage", "gs://bucket/stage"],
    ids=["azure", "gcs"],
)
def test_a_cloud_stage_connection_opens(uri):
    assert StageConnection.of(Location(uri=uri)).location.uri == uri


def test_a_stage_connection_rejects_an_unsupported_scheme():
    """An unknown scheme surfaces RSP_11 with a clean exit code."""
    with pytest.raises(UnsupportedStageError):
        StageConnection.of(Location(uri="ftp://bucket/stage"))


def test_a_stream_publisher_binds_the_manifest_stage(
    tmp_path,
):
    """For a ``@stream_publisher``, ``SourceBinding.of`` swaps
    the user-declared Kafka source for the publisher-side
    StreamStageSrc + a StageConnection carrying the function stage's
    location, which ``ManifestStageSrcPlugin`` reads. Cardinality
    drops to ``-1`` because the manifest decides slot count at read
    time."""
    from tabsdatak.api import stream_publisher
    from tabsdatak.conn.kafka import KafkaStreamSrc
    from tabsdatak.spi import StreamStageSrc, StreamStageSrcPlugin

    @stream_publisher(
        source=KafkaStreamSrc(
            data_format="json",
            schema="{}",
            topic="t",
        ),
        batch_max_secs=60,
        output_tables=["t"],
    )
    def my_stream_publisher(tables: TableFramesSpec) -> TableFrameSpec:
        return None

    # The stream-publisher path substitutes the conn from function_stage
    # and never reads the connection file, so a stub path is fine here.
    binding = SourceBinding.of(
        my_stream_publisher,
        Location(uri="file:///stage/dir"),
        tmp_path / "unused.yaml",
    )
    # Plugin substituted to StreamStageSrcPlugin (which delegates to
    # ManifestStageSrcPlugin internally on read_in).
    assert isinstance(binding.plugin, StreamStageSrcPlugin)
    # Conn substituted to the function stage's location.
    assert binding.connection.location.uri == "file:///stage/dir"
    # Source substituted to a StreamStageSrc instance.
    assert isinstance(binding.source, StreamStageSrc)
    # Cardinality is -1 (dynamic, from the StreamStageSrc registration).
    assert binding.cardinality == -1


def test_load_function_def_accepts_stream_publisher_kind(tmp_path):
    """The source plugin's definition kinds include
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


def binding_request(tmp_path: Path, stage_uri: str) -> SimpleNamespace:
    return SimpleNamespace(info=SimpleNamespace(function_stage=Location(uri=stage_uri)))


def test_an_unknown_source_is_reported_as_no_plugin(
    tmp_path,
    fixture_publisher,
    monkeypatch,
):
    def missing_source(_):
        raise KeyError("no connector registered")

    monkeypatch.setattr(f"{BINDING_MODULE}.get_src", missing_source)
    command = source_plugin(
        tmp_path,
        publisher=fixture_publisher,
        request=binding_request(tmp_path, "file:///unused"),
    )
    with pytest.raises(NoPluginError):
        command.bind()


def test_a_connection_that_cannot_be_loaded_is_reported(tmp_path, fixture_publisher):
    command = source_plugin(
        tmp_path,
        publisher=fixture_publisher,
        request=binding_request(tmp_path, "file:///unused"),
    )
    with pytest.raises(BadConnectionError) as raised:
        command.bind()
    assert raised.value.api_error is RunSourcePluginErrorCode.RSP_12


def test_an_unsupported_stage_is_reported_as_such(tmp_path):
    command = source_plugin(
        tmp_path,
        publisher=module_level_stream_publisher,
        request=binding_request(tmp_path, "ftp://bucket/stage"),
    )
    with pytest.raises(UnsupportedStageSchemeError):
        command.bind()


# --- unit: landed inputs ------------------------------------------------


def binding_of(cardinality: int) -> SourceBinding:
    return SourceBinding(object(), object(), object(), cardinality)


def test_inputs_are_named_after_the_publisher_parameters(fixture_publisher):
    assert binding_of(1).input_names(fixture_publisher) == ["in_1"]
    assert binding_of(-1).input_names(fixture_publisher) == ["in_1"]
    assert binding_of(0).input_names(fixture_publisher) == []


def function_with_ctx(in_1: TableFramesSpec, ctx: TransactionCtx) -> TableFrameSpec:
    return None


def test_the_ctx_parameter_is_not_an_input():
    publisher = SimpleNamespace(func=function_with_ctx)
    assert binding_of(1).input_names(publisher) == ["in_1"]
    assert binding_of(-1).input_names(publisher) == ["in_1"]


def test_the_inputs_manifest_records_each_landed_file(tmp_path):
    def landed(position: int, index: int) -> LandedFile:
        return LandedFile(
            position=position,
            index=index,
            data=Location(uri=f"file:///r/{position}.{index}.t", env_prefix="ROOT"),
            metadata=Location(
                uri=f"file:///r/{position}.{index}.m",
                env_prefix="ROOT",
            ),
            rows=1,
            bytes=1,
        )

    path = tmp_path / "manifest.yaml"
    source_plugin(
        tmp_path,
        names=["orders", "customers"],
        landed=[[landed(0, 0), landed(0, 1)], []],
        inputs_manifest_file=path,
    ).write_manifest()
    orders, customers = InputManifest.load(path).inputs
    assert (orders.name, orders.table_pos, customers.tables) == ("orders", 0, ())
    assert [record.location.uri for record in orders.tables] == [
        "file:///r/0.0.t",
        "file:///r/0.1.t",
    ]
    assert orders.tables[1].metadata_location == Location(
        uri="file:///r/0.1.m",
        env_prefix="ROOT",
    )
    assert all(record.location is not None for record in orders.tables)


# --- unit: write_in -------------------------------------------------------


class LegacyFiles(SrcPlugin):
    def __init__(self, produced) -> None:
        self.produced = produced

    def read_in(
        self,
        ctx,
        conn,
        src,
    ):
        if isinstance(self.produced, Exception):
            raise self.produced
        return self.produced


def write_legacy(
    tmp_path: Path,
    produced,
    names: list[str],
    *,
    variable: bool = False,
):
    plugin = LegacyFiles(produced)
    handoff = Handoff.of(plugin, tmp_path)
    info = Info.model_validate(
        info_payload(
            function_landing={
                "uri": (tmp_path / "landing").as_uri(),
                "env_prefix": "ROOT",
            },
        ),
    )
    handoff.seed(
        info,
        names,
        variable=variable,
    )
    return source_plugin(
        tmp_path,
        handoff=handoff,
        binding=SourceBinding(
            plugin,
            object(),
            object(),
            len(names),
        ),
        context=object(),
    ).write_in()


def parquet_of(path: Path, values: list[int]) -> Path:
    pl.DataFrame({"id": values}).write_parquet(path)
    return path


def test_write_in_lands_the_files_a_legacy_plugin_returns(tmp_path):
    metadata = {"@td.file.path": "s3://bucket/in/a.csv", "@td.file.rows": 2}
    a = parquet_of(tmp_path / "a.parquet", [1, 2])
    b = parquet_of(tmp_path / "b.parquet", [3])
    [[first, second]] = write_legacy(
        tmp_path,
        [
            [
                TableFileInput(file=a, metadata=metadata),
                TableFileInput(file=Path(b.as_uri())),
            ],
        ],
        ["in_1"],
    )
    assert (first.index, first.rows) == (0, 2)
    assert first.table.read_metadata().columns == metadata
    assert (second.index, second.rows) == (1, 1)
    assert second.table.read_metadata().columns is None
    assert first.data.uri.endswith("/landing/0.0.t")


def test_write_in_refuses_another_number_of_input_slots(tmp_path):
    a = parquet_of(tmp_path / "a.parquet", [1])
    with pytest.raises(PluginFailedError, match="returned 2 input slot"):
        write_legacy(
            tmp_path,
            [[TableFileInput(file=a)], [TableFileInput(file=a)]],
            ["in_1"],
        )


def test_write_in_pads_a_variable_source_to_the_function_inputs(tmp_path):
    assert write_legacy(
        tmp_path,
        [],
        ["in_1"],
        variable=True,
    ) == [[]]


def test_write_in_refuses_too_many_slots_from_a_variable_source(tmp_path):
    a = parquet_of(tmp_path / "a.parquet", [1])
    with pytest.raises(PluginFailedError, match="returned 2 input slot"):
        write_legacy(
            tmp_path,
            [[TableFileInput(file=a)], [TableFileInput(file=a)]],
            ["in_1"],
            variable=True,
        )


def test_write_in_reports_a_failing_plugin(tmp_path):
    with pytest.raises(PluginFailedError, match="boom"):
        write_legacy(tmp_path, RuntimeError("boom"), ["in_1"])


def test_an_unwritable_inputs_manifest_is_reported(tmp_path):
    folder = tmp_path / "i-am-a-folder"
    folder.mkdir()
    command = source_plugin(
        tmp_path,
        names=["in_1"],
        landed=[[]],
        inputs_manifest_file=folder,
    )
    with pytest.raises(ManifestWriteFailedError):
        command.write_manifest()


# --- main: error paths via argparse ---------------------------------------


def test_main_missing_request_returns_bad_request(
    tmp_path,
    monkeypatch,
    http_ok_url,
):
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


def test_main_missing_bundle_returns_bad_bundle(
    tmp_path,
    monkeypatch,
    http_ok_url,
):
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
    monkeypatch.setattr(
        sys,
        "argv",
        ["source-plugin", "--request", "x"],
    )
    assert main() == 1


# --- main: end-to-end happy path -----------------------------------------


def test_main_end_to_end_happy_path(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    """Real LocalFile publisher and plugin, landing straight from the
    source file. Requires `tabsdata-conn-localfile` installed (entry-point
    group `tabsdatak.connectors`).
    """
    request_path = write_request(tmp_path / "request.yaml")
    context_folder = tmp_path / "out"
    context_folder.mkdir()
    manifest = tmp_path / "manifest.yaml"
    liveness = tmp_path / "live"
    # The source connection's base_path is the srcdata dir the
    # `user_project` fixture wrote `rows.parquet` into.
    connection_file = write_connection_file(
        tmp_path / "connection.yaml",
        tmp_path / "srcdata",
    )

    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        context_folder,
        manifest,
        liveness,
        connection_file=connection_file,
        responder_url=http_ok_url,
    )

    status = main()
    assert status == 0, f"main() returned {status} (non-zero)"

    # Nothing is copied to a local parquet on the way: the source file lands.
    assert list(context_folder.rglob("*.parquet")) == []

    # The landed table holds the input rows.
    table = pq.read_table(tmp_path / "landing" / "0.0.t")
    assert table.num_rows == 3
    assert "id" in table.column_names
    assert "name" in table.column_names
    assert table.to_pydict()["id"] == [1, 2, 3]
    assert table.to_pydict()["name"] == ["alice", "bob", "carol"]

    # The inputs manifest records the file landed from it, and its metadata.
    [entry] = InputManifest.load(manifest).inputs
    assert entry.name == "in_1"
    [record] = entry.tables
    assert record.location.uri.endswith("/landing/0.0.t")
    assert record.metadata_location.uri.endswith("/landing/0.0.m")

    # Liveness file was created by the context manager.
    assert liveness.exists()


def scratch_read_in(self, ctx, conn, src) -> list[Iterable[LazyFrameInput]]:
    ctx._scratch.write("staged.bin", io.BytesIO(b"staged"))
    return [[LazyFrameInput(pl.LazyFrame({"id": [1]}))]]


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
def test_main_clears_what_the_plugin_left_in_the_scratch(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
    emulator,
):
    from tabsdatak.conn.localfile._plugin import LocalFileSrcPlugin

    domain = locals()[f"domain_{emulator}"]
    monkeypatch.setattr(LocalFileSrcPlugin, "read_in", scratch_read_in)
    run = emulated_run(domain, monkeypatch)
    landing, scratch = run_location(run, "r"), run_location(run, "s")
    set_arguments(
        monkeypatch,
        write_request(tmp_path / "request.yaml", scratch=scratch, landing=landing),
        built_bundle,
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
        connection_file=write_connection_file(
            tmp_path / "connection.yaml",
            tmp_path / "srcdata",
        ),
        responder_url=http_ok_url,
    )
    try:
        assert main() == 0
        assert stored(scratch, domain) == []
        assert sorted(stored(landing, domain)) == ["0.0.m", "0.0.t"]
    finally:
        names = stored({"uri": run}, domain)
        if names:
            obstore.delete(
                OBJECT_STORES.store(run, dict(domain.storage_options)), names
            )


def legacy_read_in(self, ctx, conn, src) -> list[list[TableFileInput]]:
    target = ctx.work_dir / "rows.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(target)
    return [[TableFileInput(file=target)]]


def test_main_logs_that_a_legacy_plugin_is_deprecated(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
    capfd,
):
    from tabsdatak.conn.localfile._plugin import LocalFileSrcPlugin

    monkeypatch.setattr(LocalFileSrcPlugin, "read_in", legacy_read_in)
    set_arguments(
        monkeypatch,
        write_request(tmp_path / "request.yaml"),
        built_bundle,
        tmp_path / "out",
        tmp_path / "manifest.yaml",
        tmp_path / "live",
        connection_file=write_connection_file(
            tmp_path / "connection.yaml",
            tmp_path / "srcdata",
        ),
        responder_url=http_ok_url,
    )
    assert main() == 0
    output = capfd.readouterr()
    assert (
        "LocalFileSrcPlugin.read_in returns the deprecated "
        "list[list[TableFileInput]]" in output.out + output.err
    )


# --- decorator metadata -------------------------------------------------


def test_main_carries_tabsdata_command_metadata():
    assert (
        getattr(
            main,
            "tabsdata_command",
            False,
        )
        is True
    )
    assert (
        getattr(
            main,
            "tabsdata_command_name",
            None,
        )
        == "td-p-src-regular"
    )


# --- main: end-to-end with initial_values round-trip --------------------


def test_main_hydrates_and_drains_initial_values(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    """Store a previous-run state at system_input[0]'s location, let
    main() run the plugin (which is unaware of state), and verify drain
    produced this run's local state file. The publisher plugin adds only
    the schema it served its slot with, so the drained dict is the
    hydrated one plus that."""
    context_folder = tmp_path / "out"
    context_folder.mkdir()
    state_dir = tmp_path / "state"
    previous = tmp_path / "storage" / "state-in.t"
    previous.parent.mkdir()
    State.save(FunctionState({"last-modified": "2026-05-27T00:00:00Z"}), previous)

    req_path = tmp_path / "request.yaml"
    landing = {"uri": (tmp_path / "landing").as_uri(), "env_prefix": "ROOT"}
    req_path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(function_landing=landing),
                "output": [
                    output_table_payload(
                        "my_table",
                        location={"uri": "file:///out/my_table"},
                    )
                ],
                "system_input": [
                    input_table_payload(
                        "td.fn_state__F-1",
                        location={"uri": previous.as_uri()},
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
        tmp_path / "connection.yaml",
        tmp_path / "srcdata",
    )
    set_arguments(
        monkeypatch,
        req_path,
        built_bundle,
        context_folder,
        manifest,
        liveness,
        connection_file=connection_file,
        responder_url=http_ok_url,
        state_dir=state_dir,
    )

    status = main()
    assert status == 0, f"main() returned {status}"

    assert State.load(TableFile.local(state_dir / STATE_FILE)).values == {
        "last-modified": "2026-05-27T00:00:00Z",
        "@td.schema.0": '{"id": "Int64", "name": "String"}',
    }
    assert not (tmp_path / "storage" / "state-out.t").exists()
