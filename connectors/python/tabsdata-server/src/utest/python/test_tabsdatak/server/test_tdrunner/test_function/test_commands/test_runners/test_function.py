#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
import yaml

from tabsdatak._info import (
    PublisherInfo,
    StreamPublisherInfo,
    SubscriberInfo,
    TransformerInfo,
)
from tabsdatak.api import FunctionCtx
from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak.server._tdrunner.function._commons.context.function import (
    FunctionContext,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.document.state.function import (
    FunctionState,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._commons.solver.secrets import ProjectSecret
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    FunctionBundle,
    FunctionInfo,
)
from tabsdatak.server._tdrunner.function._handlers.bundle import BundleArchive
from tabsdatak.server._tdrunner.function._handlers.function import (
    FunctionFailedError,
    InputModeError,
    NotATupleError,
    ResultCountError,
    ResultTypeError,
    TypeHintsError,
    UnknownDestinationError,
    UserFunction,
)
from tabsdatak.server._tdrunner.function._handlers.inputs import (
    Inputs,
    InputScanError,
    SingleSlotError,
)
from tabsdatak.server._tdrunner.function._handlers.outputs import (
    SinkError,
    StatsError,
)
from tabsdatak.server._tdrunner.function._handlers.state import (
    STATE_FILE,
    State,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function.commands.runners.function import main
from tabsdatak.server._tdrunner.function.commands.runners.function.error import (
    BadBundleError,
    BadInputParquetError,
    BadManifestError,
    BadRequestError,
    BadUserCodeError,
    NoPluginError,
    OutputShapeMismatchError,
    OutputsManifestWriteFailedError,
    OutputWriteFailedError,
    RunFunctionErrorCode,
    UserFunctionFailedError,
)
from tabsdatak.server._tdrunner.function.commands.runners.function.options import (
    RunFunctionArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.function.runner import (
    DEFINITION_ERRORS,
    KINDS,
    Function,
)
from tabsdatak.spi import DestContext, SrcPluginCtx, TableMode
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode
from test_tabsdatak.server.conftest import TESTING_RESOURCES_FOLDER
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


FUNCTION_HANDLER_MODULE = "tabsdatak.server._tdrunner.function._handlers.function"


# --- shared fixtures ----------------------------------------------------


def write_sample_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).write_parquet(path)


def slot_entry(
    name: str,
    table_pos: int,
    files: list[str],
) -> InputEntry:
    return InputEntry(
        name=name,
        table_pos=table_pos,
        tables=tuple(
            InputRecord(file=file, input_idx=index) for index, file in enumerate(files)
        ),
    )


PROJECTS = Path(TESTING_RESOURCES_FOLDER) / "run_function"


def copy_project(
    tmp_path: Path,
    monkeypatch,
    name: str,
) -> Path:
    """Copy a user project from the test resources into ``tmp_path`` and
    make it importable."""
    project = tmp_path / name
    shutil.copytree(
        PROJECTS / name,
        project,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    return project


def build_test_bundle(
    tmp_path: Path,
    project: Path,
    function,
) -> Path:
    bundle_path = tmp_path / f"{project.name}.tar.zst"
    build_bundle(
        function,
        project,
        bundle_path,
    )
    extracted = tmp_path / f"{project.name}_extracted"
    BundleArchive(bundle_path).extract(extracted)
    return extracted


def forget(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            del sys.modules[name]


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """Materialise a user project on disk with a real @publisher
    function, make it importable, return the project root."""
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    yield copy_project(
        tmp_path,
        monkeypatch,
        "publisher_project",
    )
    forget("my_pkg")


@pytest.fixture
def fixture_publisher(user_project) -> PublisherInfo:
    from my_pkg.publisher import my_publisher  # noqa: E402

    assert isinstance(my_publisher, PublisherInfo)
    return my_publisher


@pytest.fixture
def built_bundle(
    tmp_path,
    user_project,
    fixture_publisher,
) -> Path:
    return build_test_bundle(
        tmp_path,
        user_project,
        fixture_publisher,
    )


# A trivial @transformer source used to build a transformer bundle.
# Just enough to round-trip through `load_function_definition` and verify
# the kind-dispatch picks TransformerInfo. The body never runs.
@pytest.fixture
def transformer_bundle(tmp_path, monkeypatch) -> Path:
    """Materialise a tiny @transformer-decorated function in a fresh
    project, build + extract its bundle, return the extracted dir."""
    project = copy_project(
        tmp_path,
        monkeypatch,
        "tx_project",
    )
    from tx_pkg.transformer import my_transformer  # noqa: E402

    yield build_test_bundle(
        tmp_path,
        project,
        my_transformer,
    )

    forget("tx_pkg")


# A trivial @subscriber-decorated function used to verify kind dispatch
# routes correctly to SubscriberInfo and that output table names are
# synthesised (LocalFileDest with one path → cardinality 1 → one
# synthetic `out_0`).
@pytest.fixture
def subscriber_bundle(tmp_path, monkeypatch) -> Path:
    """Materialise a tiny @subscriber-decorated function in a fresh
    project, build + extract its bundle, return the extracted dir."""
    project = copy_project(
        tmp_path,
        monkeypatch,
        "sub_project",
    )
    from sub_pkg.subscriber import my_subscriber  # noqa: E402

    yield build_test_bundle(
        tmp_path,
        project,
        my_subscriber,
    )

    forget("sub_pkg")


# A trivial @stream_publisher-decorated function used to verify kind
# dispatch routes correctly to StreamPublisherInfo. By the time
# function executes the function-publisher chain has already landed
# its inputs (via run_source_plugin's stream-publisher substitution) so the
# call shape here is identical to a regular publisher's.
@pytest.fixture
def stream_publisher_bundle(tmp_path, monkeypatch) -> Path:
    """Materialise a tiny @stream_publisher-decorated function in a
    fresh project, build + extract its bundle, return the extracted
    dir."""
    project = copy_project(
        tmp_path,
        monkeypatch,
        "sp_project",
    )
    from sp_pkg.stream_publisher import my_stream_publisher  # noqa: E402

    yield build_test_bundle(
        tmp_path,
        project,
        my_stream_publisher,
    )

    forget("sp_pkg")


# A 2-path publisher (cardinality 2). The source plugin produces one
# slot per path; the function receives one TableFramesSpec parameter
# per slot. Concatenates the two inputs into one output.
@pytest.fixture
def multi_publisher_bundle(tmp_path, monkeypatch) -> Path:
    """A publisher whose source has 2 paths → cardinality 2 → user
    function has 2 input parameters."""
    srcdata = tmp_path / "mp_srcdata"
    srcdata.mkdir()

    project = copy_project(
        tmp_path,
        monkeypatch,
        "mp_project",
    )
    from mp_pkg.publisher import multi_publisher  # noqa: E402

    yield build_test_bundle(
        tmp_path,
        project,
        multi_publisher,
    )

    forget("mp_pkg")


# A 3-input transformer. Mixes SINGLE and ARRAY modes to also exercise
# the per-slot mode inference. Concatenates everything.
@pytest.fixture
def multi_transformer_bundle(tmp_path, monkeypatch) -> Path:
    """A transformer with 3 input_tables → 3 input parameters,
    mixing SINGLE and ARRAY slot modes."""
    project = copy_project(
        tmp_path,
        monkeypatch,
        "mtx_project",
    )
    from mtx_pkg.transformer import multi_transformer  # noqa: E402

    yield build_test_bundle(
        tmp_path,
        project,
        multi_transformer,
    )

    forget("mtx_pkg")


@pytest.fixture(
    params=["udf_transformer", "broken_udf_transformer"],
    ids=["valid", "broken"],
)
def udf_transformer_bundle(
    request,
    tmp_path,
    monkeypatch,
) -> tuple[Path, bool]:
    """A transformer applying a UDF; the broken one returns a row short."""
    project = copy_project(
        tmp_path,
        monkeypatch,
        "udf_project",
    )
    function = getattr(importlib.import_module("udf_pkg.transformer"), request.param)

    yield (
        build_test_bundle(
            tmp_path,
            project,
            function,
        ),
        request.param == "broken_udf_transformer",
    )

    forget("udf_pkg")


@pytest.fixture(
    params=["quality_udf_transformer", "failing_quality_udf_transformer"],
    ids=["passing", "failing"],
)
def quality_udf_transformer_bundle(
    request,
    tmp_path,
    monkeypatch,
):
    """A transformer applying a UDF with data quality on its output; the
    failing one does not tolerate the row its Fail operator flags."""
    project = copy_project(
        tmp_path,
        monkeypatch,
        "udf_project",
    )
    module = importlib.import_module("udf_pkg.transformer")

    yield (
        build_test_bundle(
            tmp_path,
            project,
            getattr(module, request.param),
        ),
        module,
        request.param == "failing_quality_udf_transformer",
    )

    forget("udf_pkg")


def input_table(entry: dict, index: int) -> dict:
    table = {
        "collection_id": 1,
        "collection": "c",
        "table_id": "T-1",
        "table_version_id": "TV-1",
        "input_idx": index,
        "table_pos": index,
        "version_pos": 0,
        **entry,
    }
    return {"Table": table}


def output_table(entry: dict, index: int) -> dict:
    table = {
        "collection_id": 1,
        "collection": "c",
        "table_id": "T-1",
        "table_version_id": "TV-1",
        "logical_table_data_version_id": "LDV-1",
        "physical_table_data_version_id": "PDV-1",
        "location": {"uri": "file:///irrelevant", "env_prefix": "ROOT"},
        "table_pos": index,
        **entry,
    }
    return {"Table": table}


def stored_path(root: Path, table: str) -> Path:
    return root / "storage" / f"{table}.t"


def landing_path(root: Path) -> Path:
    return root / "landing"


def stored_output(
    root: Path,
    table: str,
    index: int,
) -> dict:
    location = {"uri": stored_path(root, table).as_uri(), "env_prefix": "ROOT"}
    return output_table({"table": table, "location": location}, index)


def write_request(path: Path, *outputs: str) -> Path:
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(
                    function_landing={
                        "uri": landing_path(path.parent).as_uri(),
                        "env_prefix": "ROOT",
                    }
                ),
                "output": [
                    stored_output(
                        path.parent,
                        table,
                        index,
                    )
                    for index, table in enumerate(outputs)
                ],
            }
        })
    )
    return path


def set_arguments(
    monkeypatch,
    request_path: Path,
    bundle_dir: Path,
    inputs_manifest: Path,
    out_dir: Path,
    outputs_manifest: Path,
    liveness: Path,
    state_dir: Path | None = None,
    *,
    responder_url: str,
    responder_method: str = "POST",
    responder_authorization: str = "Bearer tok",
) -> None:
    if state_dir is None:
        state_dir = out_dir.parent / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "function",
            "--request",
            str(request_path),
            "--bundle-folder",
            str(bundle_dir),
            "--inputs-manifest",
            str(inputs_manifest),
            "--output-folder",
            str(out_dir),
            "--outputs-manifest",
            str(outputs_manifest),
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


def function_command(tmp_path: Path, **state) -> Function:
    command = Function(
        RunFunctionArguments(
            request=tmp_path / "request.yaml",
            bundle_folder=tmp_path / "bundle",
            inputs_manifest=tmp_path / "inputs.yaml",
            output_folder=tmp_path / "out",
            outputs_manifest=tmp_path / "outputs.yaml",
            state_folder=tmp_path / "state",
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


def load_function_definition(bundle_folder: Path) -> FunctionInfo:
    return FunctionBundle(bundle_folder).definition(KINDS, DEFINITION_ERRORS)


def plain_function(function) -> UserFunction:
    return UserFunction(SimpleNamespace(func=function))


def fake_table_frame() -> TableFrame:
    return TableFrame._build(pl.DataFrame({"x": [1]}).lazy(), CreationMode.MUT)


def build_request(tmp_path: Path, *outputs: str) -> Request:
    return Request.load(
        write_request(tmp_path / "request.yaml", *outputs),
        RunFunctionErrorCode.RF_1,
    )


# --- unit: load_function_definition -------------------------------------


def test_load_function_def_happy(built_bundle):
    definition = load_function_definition(built_bundle)
    assert isinstance(definition, PublisherInfo)
    assert list(definition.output_tables) == ["my_table", "untouched"]


def test_load_function_def_missing_manifest_raises_bad_bundle(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(BadBundleError) as error:
        load_function_definition(empty)
    assert error.value.api_error is RunFunctionErrorCode.RF_2


def test_load_function_def_malformed_json_raises_bad_bundle(tmp_path):
    bad = tmp_path / "bad-bundle"
    bad.mkdir()
    (bad / BUNDLE_MANIFEST_FILENAME).write_text("{ not json")
    with pytest.raises(BadBundleError) as error:
        load_function_definition(bad)
    assert error.value.api_error is RunFunctionErrorCode.RF_2


def test_load_function_def_unresolvable_func_raises_bad_user_code(built_bundle):
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadUserCodeError) as error:
        load_function_definition(built_bundle)
    assert error.value.api_error is RunFunctionErrorCode.RF_3


def test_load_function_def_unknown_kind_raises_bad_bundle(built_bundle):
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["kind"] = "autoPublisherDef"
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadBundleError) as error:
        load_function_definition(built_bundle)
    assert error.value.api_error is RunFunctionErrorCode.RF_4


def test_load_function_def_missing_kind_raises_bad_bundle(built_bundle):
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    del data["kind"]
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadBundleError) as error:
        load_function_definition(built_bundle)
    assert error.value.api_error is RunFunctionErrorCode.RF_4


def test_load_function_def_transformer_kind_loads_transformer_info(
    transformer_bundle,
):
    definition = load_function_definition(transformer_bundle)
    assert isinstance(definition, TransformerInfo)
    assert list(definition.output_tables) == ["t_out"]


def test_load_function_def_subscriber_kind_loads_subscriber_info(
    subscriber_bundle,
):
    definition = load_function_definition(subscriber_bundle)
    assert isinstance(definition, SubscriberInfo)
    assert not hasattr(definition, "output_tables")


def test_load_function_def_stream_publisher_kind_loads_stream_publisher_info(
    stream_publisher_bundle,
):
    """Without this, `tdk fn trigger` on a `@stream_publisher` fails at
    the `function` step with WrongKindError (RF_4) — even though
    upstream steps (`source-plugin`'s substitution) have done their
    job materialising the staged inputs."""
    definition = load_function_definition(stream_publisher_bundle)
    assert isinstance(definition, StreamPublisherInfo)
    assert list(definition.output_tables) == ["people_streamed"]


# --- unit: load_inputs --------------------------------------------------


def test_load_inputs_maps_an_unreadable_manifest_to_bad_manifest(tmp_path):
    (tmp_path / "inputs.yaml").write_text("{ not yaml")
    with pytest.raises(BadManifestError) as error:
        function_command(tmp_path).load_inputs()
    assert error.value.api_error is RunFunctionErrorCode.RF_5


# --- unit: infer_table_modes --------------------------------------------


def unannotated_function(in_1):
    pass


def unresolvable_function(in_1: NoSuchType):  # noqa: F821
    pass


def test_infer_table_modes_maps_type_hints_to_bad_user_code(tmp_path):
    command = function_command(
        tmp_path,
        user_function=plain_function(unresolvable_function),
    )
    with pytest.raises(BadUserCodeError) as error:
        command.infer_table_modes()
    assert error.value.api_error is RunFunctionErrorCode.RF_6
    assert isinstance(error.value.__cause__, TypeHintsError)


def test_infer_table_modes_maps_input_mode_to_bad_user_code(tmp_path):
    command = function_command(
        tmp_path,
        user_function=plain_function(unannotated_function),
    )
    with pytest.raises(BadUserCodeError, match="'in_1'") as error:
        command.infer_table_modes()
    assert error.value.api_error is RunFunctionErrorCode.RF_7
    assert isinstance(error.value.__cause__, InputModeError)


# --- unit: input_slot ---------------------------------------------------


def no_landing_inputs() -> Inputs:
    return Inputs(Info.model_validate(info_payload()), publisher=False)


def test_input_slot_maps_input_scan_to_bad_input_parquet(tmp_path):
    parquet_path = tmp_path / "x.parquet"
    parquet_path.write_bytes(b"not a parquet")
    with pytest.raises(BadInputParquetError) as error:
        function_command(tmp_path, inputs=no_landing_inputs()).input_slot(
            slot_entry("in_1", 0, [str(parquet_path)]),
            TableMode.SINGLE,
        )
    assert error.value.api_error is RunFunctionErrorCode.RF_9
    assert isinstance(error.value.__cause__, InputScanError)


def test_input_slot_maps_single_slot_to_bad_manifest(tmp_path):
    first = tmp_path / "a.parquet"
    second = tmp_path / "b.parquet"
    write_sample_parquet(first)
    write_sample_parquet(second)
    with pytest.raises(BadManifestError) as error:
        function_command(tmp_path, inputs=no_landing_inputs()).input_slot(
            slot_entry("in_1", 0, [str(first), str(second)]),
            TableMode.SINGLE,
        )
    assert error.value.api_error is RunFunctionErrorCode.RF_11
    assert isinstance(error.value.__cause__, SingleSlotError)


# --- unit: resolve_output_tables ----------------------------------------


def test_resolve_output_tables_maps_unknown_destination_to_no_plugin(
    tmp_path,
    subscriber_bundle,
    monkeypatch,
):
    def missing_destination(_destination_class):
        raise KeyError("no DestDef registered")

    monkeypatch.setattr(f"{FUNCTION_HANDLER_MODULE}.get_dest", missing_destination)
    command = function_command(
        tmp_path,
        user_function=UserFunction(load_function_definition(subscriber_bundle)),
    )
    with pytest.raises(NoPluginError) as error:
        command.resolve_output_tables()
    assert error.value.api_error is RunFunctionErrorCode.RF_8
    assert isinstance(error.value.__cause__, UnknownDestinationError)


# --- unit: call ---------------------------------------------------------


def calling_command(tmp_path: Path, function, *tables: str) -> Function:
    return function_command(
        tmp_path,
        user_function=plain_function(function),
        call_arguments=[fake_table_frame()],
        output_tables=list(tables),
    )


def raising_function(in_1):
    raise RuntimeError("boom")


def bare_function(in_1):
    return in_1


def two_results_function(in_1):
    return (in_1, in_1)


def wrong_type_function(in_1):
    return ("not a table frame",)


@pytest.mark.parametrize(
    ("function", "tables", "exception", "code", "cause"),
    [
        (
            raising_function,
            ["t1"],
            UserFunctionFailedError,
            RunFunctionErrorCode.RF_12,
            FunctionFailedError,
        ),
        (
            bare_function,
            ["t1", "t2"],
            OutputShapeMismatchError,
            RunFunctionErrorCode.RF_13,
            NotATupleError,
        ),
        (
            two_results_function,
            ["t1"],
            OutputShapeMismatchError,
            RunFunctionErrorCode.RF_14,
            ResultCountError,
        ),
        (
            wrong_type_function,
            ["t1"],
            OutputShapeMismatchError,
            RunFunctionErrorCode.RF_15,
            ResultTypeError,
        ),
    ],
    ids=["failed", "not-a-tuple", "count", "type"],
)
def test_call_maps_the_user_function_errors(
    tmp_path,
    function,
    tables,
    exception,
    code,
    cause,
):
    with pytest.raises(exception) as error:
        calling_command(tmp_path, function, *tables).call()
    assert error.value.api_error is code
    assert isinstance(error.value.__cause__, cause)


def test_call_returns_the_results(tmp_path):
    results = calling_command(tmp_path, bare_function, "t1").call()
    assert len(results) == 1
    assert isinstance(results[0], TableFrame)


# --- unit: targets ------------------------------------------------------


def test_targets_of_a_publisher_are_their_stored_locations(
    tmp_path,
    fixture_publisher,
):
    command = function_command(
        tmp_path,
        request=build_request(
            tmp_path,
            "untouched",
            "my_table",
        ),
        user_function=UserFunction(fixture_publisher),
        output_tables=["my_table", "untouched"],
    )
    assert [target.uri for target in command.targets()] == [
        stored_path(tmp_path, "my_table").as_uri(),
        stored_path(tmp_path, "untouched").as_uri(),
    ]


def test_targets_reject_a_table_without_location(tmp_path, fixture_publisher):
    command = function_command(
        tmp_path,
        request=build_request(tmp_path, "my_table"),
        user_function=UserFunction(fixture_publisher),
        output_tables=["my_table", "untouched"],
    )
    with pytest.raises(BadRequestError, match="untouched") as error:
        command.targets()
    assert error.value.api_error is RunFunctionErrorCode.RF_20


def test_targets_of_a_subscriber_are_its_landing_files(
    tmp_path,
    subscriber_bundle,
):
    command = function_command(
        tmp_path,
        request=build_request(tmp_path),
        user_function=UserFunction(load_function_definition(subscriber_bundle)),
        output_tables=["out_0", "out_1"],
    )
    assert [target.uri for target in command.targets()] == [
        f"{landing_path(tmp_path).as_uri()}/0.t",
        f"{landing_path(tmp_path).as_uri()}/1.t",
    ]


# --- unit: sink ---------------------------------------------------------


def sinking_command(tmp_path: Path, fixture_publisher) -> Function:
    return function_command(
        tmp_path,
        request=build_request(tmp_path, "my_table"),
        user_function=UserFunction(fixture_publisher),
        output_tables=["my_table"],
        results=[fake_table_frame()],
        checks=[],
        state=SimpleNamespace(output=lambda: None),
    )


def test_sink_writes_the_outputs_manifest_entries(tmp_path, fixture_publisher):
    command = sinking_command(tmp_path, fixture_publisher)
    command.sink()
    (entry,) = command.outputs_manifest.outputs
    assert entry.name == "my_table"
    assert entry.uri == stored_path(tmp_path, "my_table").as_uri()
    assert entry.stats.row_count == 1


def test_sink_maps_a_write_failure_to_output_write_failed(
    tmp_path,
    fixture_publisher,
    monkeypatch,
):
    def broken_sink(self, *arguments):
        raise RuntimeError("disk full")

    monkeypatch.setattr(
        TableFrame,
        "_observed",
        broken_sink,
    )
    with pytest.raises(OutputWriteFailedError, match="my_table.t") as error:
        sinking_command(tmp_path, fixture_publisher).sink()
    assert error.value.api_error is RunFunctionErrorCode.RF_16
    assert isinstance(error.value.__cause__, SinkError)


def test_sink_maps_unreadable_stats_to_output_write_failed(
    tmp_path,
    fixture_publisher,
    monkeypatch,
):
    def unreadable(self):
        raise OSError("gone")

    monkeypatch.setattr(
        TableFile,
        "stats",
        unreadable,
    )
    with pytest.raises(
        OutputWriteFailedError,
        match="stats of output 'my_table'",
    ) as error:
        sinking_command(tmp_path, fixture_publisher).sink()
    assert error.value.api_error is RunFunctionErrorCode.RF_21
    assert isinstance(error.value.__cause__, StatsError)


# --- unit: write_manifest -----------------------------------------------


def test_write_manifest_maps_an_os_error_to_outputs_manifest_write_failed(
    tmp_path,
):
    (tmp_path / "outputs.yaml").mkdir()
    command = function_command(
        tmp_path,
        outputs_manifest=OutputManifest(outputs=(OutputEntry(name="t1"),)),
    )
    with pytest.raises(OutputsManifestWriteFailedError) as error:
        command.write_manifest()
    assert error.value.api_error is RunFunctionErrorCode.RF_17


# --- main: error paths --------------------------------------------------


def test_main_missing_request_returns_bad_request(
    tmp_path,
    monkeypatch,
    http_ok_url,
):
    set_arguments(
        monkeypatch,
        tmp_path / "no-such.yaml",
        tmp_path / "bundle",
        tmp_path / "inputs.yaml",
        tmp_path / "out",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
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
        tmp_path / "no-bundle",
        tmp_path / "inputs.yaml",
        tmp_path / "out",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadBundleError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["function", "--request", "x"],
    )
    assert main() == 1


# --- main: end-to-end ---------------------------------------------------


def test_main_end_to_end_happy_path(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    """Real publisher, real bundle, real inputs manifest pointing at a
    real parquet. Invoke main(); verify outputs."""
    # Write the request.
    request_path = write_request(
        tmp_path / "request.yaml",
        "my_table",
        "untouched",
    )

    # Write a sample parquet and an inputs manifest pointing at it.
    raw_parquet = tmp_path / "raw" / "rows.parquet"
    write_sample_parquet(raw_parquet)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            slot_entry(
                "in_1",
                0,
                [str(raw_parquet)],
            ),
        )
    ).write(inputs_manifest)

    out_dir = tmp_path / "fn_out"
    outputs_manifest = tmp_path / "outputs.yaml"
    liveness = tmp_path / "live"

    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        liveness,
        responder_url=http_ok_url,
    )
    status = main()
    assert status == 0, f"main() returned {status}"

    # One output was written to its stored location for `my_table` (the
    # function returned a LazyFrame), nothing for `untouched` (function
    # returned None), and nothing locally.
    my_table_file = stored_path(tmp_path, "my_table")
    assert my_table_file.is_file()
    assert not stored_path(tmp_path, "untouched").exists()
    assert not list(out_dir.rglob("*.parquet"))

    # The user function added a `marker` column; the underlying data
    # comes through unchanged.
    frame = pl.read_parquet(my_table_file)
    assert "marker" in frame.columns
    assert frame["marker"].to_list() == ["from_user_fn"] * 3
    assert frame["id"].to_list() == [1, 2, 3]
    assert frame["name"].to_list() == ["a", "b", "c"]

    # Outputs manifest shape: my_table → uri and stats, untouched → null,
    # and no state (the request declares no system output).
    data = yaml.safe_load(outputs_manifest.read_text())
    assert list(data.keys()) == ["outputs", "state"]
    assert data["state"] is None
    assert len(data["outputs"]) == 2
    e1, e2 = data["outputs"]
    assert e1["name"] == "my_table"
    assert e1["uri"] == my_table_file.as_uri()
    assert e1["stats"] == {
        "column_count": len(pl.read_parquet_schema(my_table_file)),
        "row_count": 3,
        "schema_hash": e1["stats"]["schema_hash"],
        "data_bytes": my_table_file.stat().st_size,
    }
    assert e2 == {"name": "untouched", "uri": None, "stats": None}

    assert liveness.exists()


def test_main_outputs_manifest_write_failure(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    """If the outputs-manifest write fails, surface OutputsManifestWriteFailedError."""
    request_path = write_request(
        tmp_path / "request.yaml",
        "my_table",
        "untouched",
    )
    raw_parquet = tmp_path / "raw" / "rows.parquet"
    write_sample_parquet(raw_parquet)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            slot_entry(
                "in_1",
                0,
                [str(raw_parquet)],
            ),
        )
    ).write(inputs_manifest)

    # Point --outputs-manifest at an existing directory → write fails.
    bad_target = tmp_path / "i-am-a-dir"
    bad_target.mkdir()

    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        inputs_manifest,
        tmp_path / "fn_out",
        bad_target,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == OutputsManifestWriteFailedError.exit_code


# --- main: multi-cardinality (regression — see commit history) -----------


def test_main_multi_cardinality_publisher_concatenates_two_slots(
    tmp_path,
    multi_publisher_bundle,
    monkeypatch,
    http_ok_url,
):
    """A publisher whose source has 2 paths produces 2 input slots, and
    the user function declares 2 parameters. Exercises the full run()
    flow with input_count > 1 on the publisher path — guards against
    the regression where _input_count was hardcoded to 1 for
    publishers."""
    request_path = write_request(tmp_path / "request.yaml", "concat")

    # One parquet per slot.
    a_parquet = tmp_path / "raw" / "a.parquet"
    b_parquet = tmp_path / "raw" / "b.parquet"
    a_parquet.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2], "name": ["a1", "a2"]}).write_parquet(a_parquet)
    pl.DataFrame({"id": [10, 11], "name": ["b1", "b2"]}).write_parquet(b_parquet)

    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            slot_entry(
                "in_a",
                0,
                [str(a_parquet)],
            ),
            slot_entry(
                "in_b",
                1,
                [str(b_parquet)],
            ),
        )
    ).write(inputs_manifest)

    out_dir = tmp_path / "fn_out"
    outputs_manifest = tmp_path / "outputs.yaml"
    set_arguments(
        monkeypatch,
        request_path,
        multi_publisher_bundle,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    status = main()
    assert status == 0, f"main() returned {status}"

    concat_file = stored_path(tmp_path, "concat")
    assert concat_file.is_file()
    frame = pl.read_parquet(concat_file)
    # Both inputs got concatenated (2 + 2 rows).
    assert frame.height == 4
    assert sorted(frame["id"].to_list()) == [1, 2, 10, 11]


def test_main_multi_cardinality_transformer_with_mixed_modes(
    tmp_path,
    multi_transformer_bundle,
    monkeypatch,
    http_ok_url,
):
    """A transformer with 3 input_tables (mixed SINGLE/ARRAY/SINGLE) and
    one output. Exercises the full run() flow with input_count > 1 on
    the transformer path AND with mixed slot modes — guards against
    regressions in both per-slot mode inference and the slot count."""
    request_path = write_request(tmp_path / "request.yaml", "merged")

    # Three input parquets: a (SINGLE), b (ARRAY, one file), c (SINGLE).
    first = tmp_path / "raw" / "a.parquet"
    second = tmp_path / "raw" / "b.parquet"
    third = tmp_path / "raw" / "c.parquet"
    first.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1], "name": ["a"]}).write_parquet(first)
    pl.DataFrame({"id": [2], "name": ["b"]}).write_parquet(second)
    pl.DataFrame({"id": [3], "name": ["c"]}).write_parquet(third)

    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            slot_entry(
                "a",
                0,
                [str(first)],
            ),
            slot_entry(
                "b",
                1,
                [str(second)],
            ),
            slot_entry(
                "c",
                2,
                [str(third)],
            ),
        )
    ).write(inputs_manifest)

    out_dir = tmp_path / "fn_out"
    outputs_manifest = tmp_path / "outputs.yaml"
    set_arguments(
        monkeypatch,
        request_path,
        multi_transformer_bundle,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    status = main()
    assert status == 0, f"main() returned {status}"

    merged_file = stored_path(tmp_path, "merged")
    assert merged_file.is_file()
    frame = pl.read_parquet(merged_file)
    assert frame.height == 3
    assert sorted(frame["id"].to_list()) == [1, 2, 3]


def test_main_transformer_with_a_udf(
    tmp_path,
    udf_transformer_bundle,
    monkeypatch,
    http_ok_url,
):
    bundle, broken = udf_transformer_bundle
    request_path = write_request(tmp_path / "request.yaml", "t_out")
    source = tmp_path / "raw" / "in.parquet"
    write_sample_parquet(source)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            slot_entry(
                "t_in",
                0,
                [str(source)],
            ),
        )
    ).write(inputs_manifest)
    out_dir = tmp_path / "fn_out"
    set_arguments(
        monkeypatch,
        request_path,
        bundle,
        inputs_manifest,
        out_dir,
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )

    exit_code = main()

    if broken:
        assert exit_code == OutputWriteFailedError.exit_code
        return
    assert exit_code == 0, f"main() returned {exit_code}"
    frame = pl.read_parquet(stored_path(tmp_path, "t_out"))
    assert frame.select(
        "id",
        "name",
        "doubled",
        "label",
    ).to_dicts() == [
        {"id": 1, "name": "a", "doubled": 2, "label": "A"},
        {"id": 2, "name": "b", "doubled": 4, "label": "B"},
        {"id": 3, "name": "c", "doubled": 6, "label": "C"},
    ]
    assert "$td.id" in frame.columns


def test_main_evaluates_a_udf_once_per_row_with_data_quality(
    tmp_path,
    quality_udf_transformer_bundle,
    monkeypatch,
    http_ok_url,
):
    bundle, module, failing = quality_udf_transformer_bundle
    request_path = write_request(
        tmp_path / "request.yaml",
        "t_out",
        "t_out_quality",
        "t_out_quality_summary",
    )
    source = tmp_path / "raw" / "in.parquet"
    write_sample_parquet(source)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            slot_entry(
                "t_in",
                0,
                [str(source)],
            ),
        )
    ).write(inputs_manifest)
    out_dir = tmp_path / "fn_out"
    set_arguments(
        monkeypatch,
        request_path,
        bundle,
        inputs_manifest,
        out_dir,
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )

    exit_code = main()

    assert sorted(module.CALLS) == [1, 2, 3]
    if failing:
        assert exit_code != 0
        return
    assert exit_code == 0, f"main() returned {exit_code}"
    stored = {
        path.stem: pl.read_parquet(path) for path in (tmp_path / "storage").glob("*.t")
    }
    assert set(stored) == {"t_out", "t_out_quality", "t_out_quality_summary"}
    assert stored["t_out"]["doubled"].to_list() == [2, 4, 6]
    summary = stored["t_out_quality_summary"]
    assert summary.select(
        "dq.total",
        "dq.true",
        "dq.false",
    ).row(0) == (3, 2, 1)


def test_main_manifest_length_mismatch_returns_bad_manifest(
    tmp_path,
    multi_publisher_bundle,
    monkeypatch,
    http_ok_url,
):
    """The multi-cardinality publisher declares 2 input slots; feeding
    it a 1-entry manifest must surface BadManifestError rather than
    silently dropping the second slot."""
    request_path = write_request(tmp_path / "request.yaml")
    a_parquet = tmp_path / "raw" / "a.parquet"
    a_parquet.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1]}).write_parquet(a_parquet)

    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            slot_entry(
                "in_a",
                0,
                [str(a_parquet)],
            ),
        )
    ).write(inputs_manifest)

    set_arguments(
        monkeypatch,
        request_path,
        multi_publisher_bundle,
        inputs_manifest,
        tmp_path / "fn_out",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadManifestError.exit_code


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
        == "td-f-regular"
    )


# --- T5: FunctionCtx + initial_values lifecycle -------------------------


def test_function_context_is_a_function_ctx_subclass():
    """``FunctionContext`` must be a concrete FunctionCtx so that
    ``isinstance(ctx, FunctionCtx)`` holds at the call boundary, and
    must NOT also satisfy the SrcPluginCtx / DestContext role tags."""
    third = FunctionContext(
        work_folder=Path("/work"),
        project_id="p-1",
        project="p",
        collection_id="cid",
        collection="c",
        function_id="fn",
        function="publish_people",
        function_version_id="fv-1",
        execution_id="ep",
        execution=None,
        transaction_id="tx",
        logical_function_run_id="lfr-1",
        physical_function_run_id="pfr-1",
        triggered_on=datetime.fromisoformat("2026-06-04T07:14:28.376000+00:00"),
    )
    assert isinstance(third, FunctionCtx)
    assert not isinstance(third, SrcPluginCtx)
    assert not isinstance(third, DestContext)


def test_function_context_serves_the_project_secret():
    third = FunctionContext(
        work_folder=Path("/work"),
        project_id="p-1",
        project="p",
        collection_id="cid",
        collection="c",
        function_id="fn",
        function="publish_people",
        function_version_id="fv-1",
        execution_id="ep",
        execution=None,
        transaction_id="tx",
        logical_function_run_id="lfr-1",
        physical_function_run_id="pfr-1",
        triggered_on=datetime.fromisoformat("2026-06-04T07:14:28.376000+00:00"),
    ).with_project_secret(ProjectSecret({"aws": {"key1": "mykey1"}}))
    assert third.secret_names == ["aws"]
    assert third.secret_keys("aws") == ["key1"]
    assert third.secret("aws", "key1") == "mykey1"
    assert third.secret("aws.key1") == "mykey1"


# Helper to build a self-contained user project with a ctx-aware
# publisher whose user function reads / writes initial_values through
# the trailing ctx parameter.
@pytest.fixture
def ctx_aware_built_bundle(tmp_path, monkeypatch) -> Path:
    """Build + extract a bundle whose @publisher function declares a
    trailing ``ctx: TransactionCtx`` parameter."""
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    write_sample_parquet(srcdata / "rows.parquet")

    project = copy_project(
        tmp_path,
        monkeypatch,
        "ctx_project",
    )
    from my_pkg.publisher import my_publisher  # noqa: E402

    yield build_test_bundle(
        tmp_path,
        project,
        my_publisher,
    )

    forget("my_pkg")


def write_inputs_manifest_for(input_files: list[str], path: Path) -> None:
    """Helper: write a minimal inputs manifest mapping ``in_1`` to
    ``input_files`` (ARRAY mode → list)."""
    InputManifest(
        inputs=(
            slot_entry(
                "in_1",
                0,
                input_files,
            ),
        )
    ).write(path)


STATE_TABLE = "td.fn_state__F-1"


def previous_state_path(root: Path) -> Path:
    return root / "storage" / "previous-state.t"


def request_with_state_slots(
    path: Path,
    *,
    outputs: tuple[str, ...] = ("my_table",),
    previous: dict | None = None,
) -> Path:
    definition = info_payload()
    definition["execution_id"] = 2
    definition["transaction_id"] = 3
    definition["physical_function_run_id"] = 4
    definition["function_landing"] = {
        "uri": landing_path(path.parent).as_uri(),
        "env_prefix": "ROOT",
    }
    state_in = None
    if previous is not None:
        previous_state_path(path.parent).parent.mkdir(parents=True, exist_ok=True)
        State.save(FunctionState(previous), previous_state_path(path.parent))
        state_in = {
            "uri": previous_state_path(path.parent).as_uri(),
            "env_prefix": "ROOT",
        }
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": definition,
                "output": [
                    stored_output(
                        path.parent,
                        table,
                        index,
                    )
                    for index, table in enumerate(outputs)
                ],
                "system_input": [
                    input_table({"table": STATE_TABLE, "location": state_in}, 0)
                ],
                "system_output": [
                    stored_output(
                        path.parent,
                        STATE_TABLE,
                        0,
                    )
                ],
            }
        })
    )
    return path


def stored_state(root: Path) -> dict:
    return State.load(TableFile.local(stored_path(root, STATE_TABLE))).values


def run_with_state(
    tmp_path,
    bundle,
    monkeypatch,
    http_ok_url,
    request_path,
    *,
    local=None,
) -> OutputManifest:
    inputs_manifest = tmp_path / "inputs.yaml"
    write_inputs_manifest_for([str(seed_input_parquet(tmp_path))], inputs_manifest)
    outputs_manifest = tmp_path / "outputs.yaml"
    state_folder = tmp_path / "state"
    state_folder.mkdir()
    if local is not None:
        State.save(FunctionState(local), state_folder / STATE_FILE)
    set_arguments(
        monkeypatch,
        request_path,
        bundle,
        inputs_manifest,
        tmp_path / "out",
        outputs_manifest,
        tmp_path / "live",
        state_dir=state_folder,
        responder_url=http_ok_url,
    )
    status = main()
    assert status == 0, status
    return OutputManifest.load(outputs_manifest)


def test_main_ctx_aware_function_first_run_writes_its_state(
    tmp_path,
    ctx_aware_built_bundle,
    monkeypatch,
    http_ok_url,
):
    """First run for the flow: no previous state. The user function
    sees ``None`` for ``get_attr``, sets a value, and function writes
    the drained state to system_output[0] with its stats."""
    request_path = request_with_state_slots(tmp_path / "request.yaml")
    manifest = run_with_state(
        tmp_path,
        ctx_aware_built_bundle,
        monkeypatch,
        http_ok_url,
        request_path,
    )
    assert stored_state(tmp_path) == {"my-key": "from-user-fn"}
    state_file = TableFile.local(tmp_path / "state" / STATE_FILE)
    assert State.load(state_file).values == {"my-key": "from-user-fn"}
    assert manifest.state == OutputEntry(
        name=STATE_TABLE,
        uri=stored_path(tmp_path, STATE_TABLE).as_uri(),
        stats=TableFile.local(stored_path(tmp_path, STATE_TABLE)).stats(),
    )
    assert manifest.state.stats.row_count == 1


def test_main_ctx_aware_function_prefers_this_run_state(
    tmp_path,
    ctx_aware_built_bundle,
    monkeypatch,
    http_ok_url,
):
    """When source-plugin already drained this run's state, the user
    function reads it, not the previous run's."""
    request_path = request_with_state_slots(
        tmp_path / "request.yaml",
        previous={"my-key": "from-state-in"},
    )
    run_with_state(
        tmp_path,
        ctx_aware_built_bundle,
        monkeypatch,
        http_ok_url,
        request_path,
        local={"my-key": "preset"},
    )
    assert stored_state(tmp_path) == {"my-key": "again-after-preset"}


def test_main_ctx_aware_function_reads_the_previous_state_where_stored(
    tmp_path,
    ctx_aware_built_bundle,
    monkeypatch,
    http_ok_url,
):
    """Without this run's state (transformer / subscriber chains), the
    user function hydrates from the previous run's stored state."""
    request_path = request_with_state_slots(
        tmp_path / "request.yaml",
        previous={"my-key": "from-state-in"},
    )
    run_with_state(
        tmp_path,
        ctx_aware_built_bundle,
        monkeypatch,
        http_ok_url,
        request_path,
    )
    assert stored_state(tmp_path) == {"my-key": "again-after-from-state-in"}


def test_main_function_without_ctx_writes_no_state(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    """A function with no ctx drains nothing; without a state drained by
    source-plugin there is no state to write."""
    request_path = request_with_state_slots(
        tmp_path / "request.yaml",
        outputs=("my_table", "untouched"),
    )
    manifest = run_with_state(
        tmp_path,
        built_bundle,
        monkeypatch,
        http_ok_url,
        request_path,
    )
    assert not (tmp_path / "state" / STATE_FILE).exists()
    assert not stored_path(tmp_path, STATE_TABLE).exists()
    assert manifest.state is None


def test_main_function_without_ctx_writes_the_source_plugin_state(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    """The state source-plugin drained is written even when the user
    function has no ctx."""
    request_path = request_with_state_slots(
        tmp_path / "request.yaml",
        outputs=("my_table", "untouched"),
    )
    manifest = run_with_state(
        tmp_path,
        built_bundle,
        monkeypatch,
        http_ok_url,
        request_path,
        local={"cursor": 250},
    )
    assert stored_state(tmp_path) == {"cursor": 250}
    assert manifest.state.name == STATE_TABLE


def seed_input_parquet(tmp_path: Path) -> Path:
    """Write a simple parquet so the inputs manifest references a real
    file (function won't fail materialising the slot). Mirrors the
    fixture publisher's expected input shape."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output = tmp_path / "input.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3]}), output)
    return output


# --- mkdir ordering: out_dir exists before the user function runs --------


def test_main_creates_out_dir_before_invoking_user_function(
    tmp_path,
    monkeypatch,
    http_ok_url,
):
    """A ctx-aware user function may surface ``ctx.work_dir`` (==
    ``out_dir`` for function) and try to write a sidecar there;
    the runtime must create the directory before the call, not after.
    Builds a publisher whose body asserts the directory exists
    immediately on entry."""
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    write_sample_parquet(srcdata / "rows.parquet")

    project = copy_project(
        tmp_path,
        monkeypatch,
        "work_dir_project",
    )
    from my_pkg.publisher import my_publisher  # noqa: E402

    extracted = build_test_bundle(
        tmp_path,
        project,
        my_publisher,
    )

    request_path = request_with_state_slots(tmp_path / "request.yaml")
    inputs_manifest = tmp_path / "inputs.yaml"
    write_inputs_manifest_for([str(seed_input_parquet(tmp_path))], inputs_manifest)
    out_dir = tmp_path / "fn_out"  # deliberately does NOT exist yet
    outputs_manifest = tmp_path / "outputs.yaml"
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    set_arguments(
        monkeypatch,
        request_path,
        extracted,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        tmp_path / "live",
        state_dir=state_dir,
        responder_url=http_ok_url,
    )
    status = main()
    forget("my_pkg")

    assert status == 0, status


# --- ctx parameter works for @transformer and @subscriber too -----------
#
# The runtime in ``run_function`` is kind-agnostic: it constructs the
# FunctionCtx and drains it based on ``has_transaction_ctx_param(func)`` only,
# regardless of which decorator wrapped the user function. These tests
# pin that behaviour for the two non-publisher kinds so anyone narrowing
# the ctx surface in the future hits a test failure.


def build_extracted_bundle(
    tmp_path: Path,
    monkeypatch,
    request,
    *,
    project_name: str,
    pkg_name: str,
    module_name: str,
    decorated_name: str,
) -> Path:
    """Copy a single-module user project from the test resources, import
    the decorated function, build + extract the bundle, return the
    extracted dir. Cleans up ``sys.modules`` registration via a pytest
    ``request`` finalizer."""
    project = copy_project(
        tmp_path,
        monkeypatch,
        project_name,
    )
    mod = importlib.import_module(f"{pkg_name}.{module_name}")
    request.addfinalizer(lambda: forget(pkg_name))
    return build_test_bundle(
        tmp_path,
        project,
        getattr(mod, decorated_name),
    )


def test_main_ctx_aware_transformer_hydrates_and_drains(
    tmp_path,
    monkeypatch,
    request,
    http_ok_url,
):
    """A @transformer that declares a trailing ``ctx: TransactionCtx`` gets the
    same hydrate / drain / write treatment as a
    publisher. Verifies the runtime is kind-agnostic."""
    extracted = build_extracted_bundle(
        tmp_path,
        monkeypatch,
        request,
        project_name="tx_ctx_project",
        pkg_name="tx_ctx_pkg",
        module_name="transformer",
        decorated_name="my_transformer",
    )

    # Only the previous state: transformers run in chains without
    # source-plugin, so the stored-state path is the one exercised.
    request_path = request_with_state_slots(
        tmp_path / "request.yaml",
        outputs=("t_out",),
        previous={"smoke.last-seen": "from-state-in"},
    )
    run_with_state(
        tmp_path,
        extracted,
        monkeypatch,
        http_ok_url,
        request_path,
    )
    assert stored_state(tmp_path) == {"smoke.last-seen": "tx-after-from-state-in"}


def test_main_ctx_aware_subscriber_hydrates_and_drains(
    tmp_path,
    monkeypatch,
    request,
    http_ok_url,
):
    """A @subscriber that declares a trailing ``ctx: TransactionCtx`` also
    round-trips state cleanly. Pins kind-agnostic runtime behaviour
    for the third decorator kind."""
    dest_out = tmp_path / "sub_dest"
    dest_out.mkdir()
    extracted = build_extracted_bundle(
        tmp_path,
        monkeypatch,
        request,
        project_name="sub_ctx_project",
        pkg_name="sub_ctx_pkg",
        module_name="subscriber",
        decorated_name="my_subscriber",
    )

    request_path = request_with_state_slots(
        tmp_path / "request.yaml",
        previous={"smoke.last-seen": "from-state-in"},
    )
    run_with_state(
        tmp_path,
        extracted,
        monkeypatch,
        http_ok_url,
        request_path,
    )
    assert stored_state(tmp_path) == {"smoke.last-seen": "sub-after-from-state-in"}
    assert (landing_path(tmp_path) / "0.t").is_file()
