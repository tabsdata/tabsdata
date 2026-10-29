#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for the run-function command.

Covers:
  * Helper-function unit tests (`_load_function_def`,
    `_infer_table_modes`, `_materialize_input_slot`,
    `_invoke_user_function`, `_output_targets`, `_sink_outputs`).
  * `main()` error paths and end-to-end happy path: bundle a real
    publisher, write an inputs manifest pointing at a real parquet,
    invoke `main()`, verify the output parquet contents + the
    outputs manifest.
"""

from __future__ import annotations

import importlib
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
import yaml

from tabsdatak._info import PublisherInfo
from tabsdatak.api import FunctionCtx, TableFrameSpec, TableFramesSpec
from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak.server._tdrunner.function._commons.document.request import (
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.solver.secrets import ProjectSecret
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.state import (
    STATE_FILE,
    StateOutput,
    read_state,
    write_state,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
)
from tabsdatak.server._tdrunner.function.commands.run_function import (
    _FunctionContext,
    _infer_table_modes,
    _invoke_user_function,
    _load_function_def,
    _materialize_input_slot,
    _output_table_names,
    _output_targets,
    _scan_input,
    _sink_outputs,
    main,
)
from tabsdatak.server._tdrunner.function.commands.run_function.error import (
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
from tabsdatak.spi import DestContext, SrcPluginCtx, TableMode
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode
from test_tabsdatak.server.conftest import TESTING_RESOURCES_FOLDER
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    info_payload as _info,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak.server._tdrunner.function.commands.run_function.runner"


# --- shared fixtures ----------------------------------------------------


def _write_sample_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).write_parquet(path)


def _slot_entry(name: str, table_pos: int, files: list[str]) -> InputEntry:
    return InputEntry(
        name=name,
        table_pos=table_pos,
        tables=tuple(
            InputRecord(file=file, input_idx=index) for index, file in enumerate(files)
        ),
    )


_PROJECTS = Path(TESTING_RESOURCES_FOLDER) / "run_function"


def _copy_project(tmp_path: Path, monkeypatch, name: str) -> Path:
    """Copy a user project from the test resources into ``tmp_path`` and
    make it importable."""
    project = tmp_path / name
    shutil.copytree(
        _PROJECTS / name, project, ignore=shutil.ignore_patterns("__pycache__")
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    return project


def _bundle(tmp_path: Path, project: Path, function) -> Path:
    bundle_path = tmp_path / f"{project.name}.tar.zst"
    build_bundle(function, project, bundle_path)
    extracted = tmp_path / f"{project.name}_extracted"
    extract_bundle(bundle_path, extracted)
    return extracted


def _forget(package: str) -> None:
    for name in list(sys.modules):
        if name == package or name.startswith(f"{package}."):
            del sys.modules[name]


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """Materialise a user project on disk with a real @publisher
    function, make it importable, return the project root."""
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    yield _copy_project(tmp_path, monkeypatch, "publisher_project")
    _forget("my_pkg")


@pytest.fixture
def fixture_publisher(user_project) -> PublisherInfo:
    from my_pkg.publisher import my_publisher  # noqa: E402

    assert isinstance(my_publisher, PublisherInfo)
    return my_publisher


@pytest.fixture
def built_bundle(tmp_path, user_project, fixture_publisher) -> Path:
    return _bundle(tmp_path, user_project, fixture_publisher)


# A trivial @transformer source used to build a transformer bundle.
# Just enough to round-trip through `_load_function_def` and verify
# the kind-dispatch picks TransformerInfo. The body never runs.
@pytest.fixture
def transformer_bundle(tmp_path, monkeypatch) -> Path:
    """Materialise a tiny @transformer-decorated function in a fresh
    project, build + extract its bundle, return the extracted dir."""
    project = _copy_project(tmp_path, monkeypatch, "tx_project")
    from tx_pkg.transformer import my_transformer  # noqa: E402

    yield _bundle(tmp_path, project, my_transformer)

    _forget("tx_pkg")


# A trivial @subscriber-decorated function used to verify kind dispatch
# routes correctly to SubscriberInfo and that output table names are
# synthesised (LocalFileDest with one path → cardinality 1 → one
# synthetic `out_0`).
@pytest.fixture
def subscriber_bundle(tmp_path, monkeypatch) -> Path:
    """Materialise a tiny @subscriber-decorated function in a fresh
    project, build + extract its bundle, return the extracted dir."""
    project = _copy_project(tmp_path, monkeypatch, "sub_project")
    from sub_pkg.subscriber import my_subscriber  # noqa: E402

    yield _bundle(tmp_path, project, my_subscriber)

    _forget("sub_pkg")


# A trivial @stream_publisher-decorated function used to verify kind
# dispatch routes correctly to StreamPublisherInfo. By the time
# run-function executes the function-publisher chain has already
# materialised inputs on disk (via run_source_plugin's stream-publisher
# substitution + stamp-inputs) so the call shape here is identical to a
# regular publisher's.
@pytest.fixture
def stream_publisher_bundle(tmp_path, monkeypatch) -> Path:
    """Materialise a tiny @stream_publisher-decorated function in a
    fresh project, build + extract its bundle, return the extracted
    dir."""
    project = _copy_project(tmp_path, monkeypatch, "sp_project")
    from sp_pkg.stream_publisher import my_stream_publisher  # noqa: E402

    yield _bundle(tmp_path, project, my_stream_publisher)

    _forget("sp_pkg")


# A 2-path publisher (cardinality 2). The source plugin produces one
# slot per path; the function receives one TableFramesSpec parameter
# per slot. Concatenates the two inputs into one output.
@pytest.fixture
def multi_publisher_bundle(tmp_path, monkeypatch) -> Path:
    """A publisher whose source has 2 paths → cardinality 2 → user
    function has 2 input parameters."""
    srcdata = tmp_path / "mp_srcdata"
    srcdata.mkdir()

    project = _copy_project(tmp_path, monkeypatch, "mp_project")
    from mp_pkg.publisher import multi_publisher  # noqa: E402

    yield _bundle(tmp_path, project, multi_publisher)

    _forget("mp_pkg")


# A 3-input transformer. Mixes SINGLE and ARRAY modes to also exercise
# the per-slot mode inference. Concatenates everything.
@pytest.fixture
def multi_transformer_bundle(tmp_path, monkeypatch) -> Path:
    """A transformer with 3 input_tables → 3 input parameters,
    mixing SINGLE and ARRAY slot modes."""
    project = _copy_project(tmp_path, monkeypatch, "mtx_project")
    from mtx_pkg.transformer import multi_transformer  # noqa: E402

    yield _bundle(tmp_path, project, multi_transformer)

    _forget("mtx_pkg")


@pytest.fixture(
    params=["udf_transformer", "broken_udf_transformer"], ids=["valid", "broken"]
)
def udf_transformer_bundle(request, tmp_path, monkeypatch) -> tuple[Path, bool]:
    """A transformer applying a UDF; the broken one returns a row short."""
    project = _copy_project(tmp_path, monkeypatch, "udf_project")
    function = getattr(importlib.import_module("udf_pkg.transformer"), request.param)

    yield (
        _bundle(tmp_path, project, function),
        request.param == "broken_udf_transformer",
    )

    _forget("udf_pkg")


@pytest.fixture(
    params=["quality_udf_transformer", "failing_quality_udf_transformer"],
    ids=["passing", "failing"],
)
def quality_udf_transformer_bundle(request, tmp_path, monkeypatch):
    """A transformer applying a UDF with data quality on its output; the
    failing one does not tolerate the row its Fail operator flags."""
    project = _copy_project(tmp_path, monkeypatch, "udf_project")
    module = importlib.import_module("udf_pkg.transformer")

    yield (
        _bundle(tmp_path, project, getattr(module, request.param)),
        module,
        request.param == "failing_quality_udf_transformer",
    )

    _forget("udf_pkg")


def _input_table(entry: dict, index: int) -> dict:
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


def _output_table(entry: dict, index: int) -> dict:
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


def _stored(root: Path, table: str) -> Path:
    return root / "storage" / f"{table}.t"


def _landing(root: Path) -> Path:
    return root / "landing"


def _stored_output(root: Path, table: str, index: int) -> dict:
    location = {"uri": _stored(root, table).as_uri(), "env_prefix": "ROOT"}
    return _output_table({"table": table, "location": location}, index)


def _write_request(path: Path, *outputs: str) -> Path:
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": _info(
                    function_landing={
                        "uri": _landing(path.parent).as_uri(),
                        "env_prefix": "ROOT",
                    }
                ),
                "output": [
                    _stored_output(path.parent, table, index)
                    for index, table in enumerate(outputs)
                ],
            }
        })
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


# --- unit: _load_function_def -------------------------------------------


def test_load_function_def_happy(built_bundle):
    info = _load_function_def(built_bundle)
    assert isinstance(info, PublisherInfo)
    assert list(info.output_tables) == ["my_table", "untouched"]


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


def test_load_function_def_unresolvable_func_raises_bad_user_code(built_bundle):
    """Edit the manifest's func reference to point at a missing module.
    The import failure surfaces via pydantic ValidationError → mapped
    to BadUserCodeError."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadUserCodeError):
        _load_function_def(built_bundle)


def test_load_function_def_unknown_kind_raises_bad_bundle(built_bundle):
    """An envelope with a `kind` the runner doesn't know about is an
    unsupported bundle, not a user-code error."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["kind"] = "autoPublisherDef"  # not yet handled here
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadBundleError):
        _load_function_def(built_bundle)


def test_load_function_def_missing_kind_raises_bad_bundle(built_bundle):
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    del data["kind"]
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadBundleError):
        _load_function_def(built_bundle)


def test_load_function_def_transformer_kind_loads_transformer_info(
    transformer_bundle,
):
    """A transformerDef bundle deserialises as TransformerInfo."""
    from tabsdatak._info import TransformerInfo

    info = _load_function_def(transformer_bundle)
    assert isinstance(info, TransformerInfo)
    assert list(info.output_tables) == ["t_out"]


def test_load_function_def_subscriber_kind_loads_subscriber_info(
    subscriber_bundle,
):
    """A subscriberDef bundle deserialises as SubscriberInfo."""
    from tabsdatak._info import SubscriberInfo

    info = _load_function_def(subscriber_bundle)
    assert isinstance(info, SubscriberInfo)
    # SubscriberInfo has no `output_tables` attribute — names get
    # synthesised by `_output_table_names`.
    assert not hasattr(info, "output_tables")


def test_load_function_def_stream_publisher_kind_loads_stream_publisher_info(
    stream_publisher_bundle,
):
    """A streamPublisherDef bundle deserialises as StreamPublisherInfo.

    Without this, `tdk fn trigger` on a `@stream_publisher` fails at
    `0080-run-function` with WrongKindError (RF_4) — even though
    upstream steps (`run-source-plugin`'s substitution) have done their
    job materialising the staged inputs."""
    from tabsdatak._info import StreamPublisherInfo

    info = _load_function_def(stream_publisher_bundle)
    assert isinstance(info, StreamPublisherInfo)
    assert list(info.output_tables) == ["people_streamed"]


# --- unit: _output_table_names -----------------------------------------


def test_output_table_names_publisher_returns_declared_names(fixture_publisher):
    # Publisher fixture declares ["my_table", "untouched"].
    assert _output_table_names(fixture_publisher) == ["my_table", "untouched"]


def test_output_table_names_subscriber_synthesises_indexed_names(
    subscriber_bundle,
):
    """Subscribers don't declare output table names — they're
    synthesised as `out_0, out_1, ...` from the destination's
    cardinality. LocalFileDest with one path → cardinality 1."""
    info = _load_function_def(subscriber_bundle)
    assert _output_table_names(info) == ["out_0"]


def test_output_table_names_subscriber_unregistered_dest_raises_no_plugin(
    subscriber_bundle, monkeypatch
):
    """If the destination class has no registered DestDef, surface
    EXIT_NO_PLUGIN rather than letting the bare KeyError bubble."""
    info = _load_function_def(subscriber_bundle)

    def _raise(_cls):
        raise KeyError("no DestDef registered")

    monkeypatch.setattr(f"{_MOD}.get_dest", _raise)
    with pytest.raises(NoPluginError):
        _output_table_names(info)


# --- unit: _infer_table_modes ------------------------------------------


def _fn_single(in_1: TableFrameSpec):
    pass


def _fn_array(in_1: TableFramesSpec):
    pass


def _fn_mixed(a: TableFrameSpec, b: TableFramesSpec, c: TableFrameSpec):
    pass


def _fn_no_params():
    pass


def _fn_unannotated(in_1):
    pass


def _fn_wrong_annotation(in_1: int):
    pass


def test_infer_table_modes_single_slot():
    assert _infer_table_modes(_fn_single) == [TableMode.SINGLE]


def test_infer_table_modes_array_slot():
    assert _infer_table_modes(_fn_array) == [TableMode.ARRAY]


def test_infer_table_modes_mixed_slots():
    """Transformer-style: SINGLE and ARRAY parameters interleaved."""
    assert _infer_table_modes(_fn_mixed) == [
        TableMode.SINGLE,
        TableMode.ARRAY,
        TableMode.SINGLE,
    ]


def test_infer_table_modes_no_params_returns_empty():
    assert _infer_table_modes(_fn_no_params) == []


def test_infer_table_modes_unannotated_param_raises_bad_user_code():
    with pytest.raises(BadUserCodeError):
        _infer_table_modes(_fn_unannotated)


def test_infer_table_modes_wrong_annotation_raises_bad_user_code():
    with pytest.raises(BadUserCodeError):
        _infer_table_modes(_fn_wrong_annotation)


# --- unit: _scan_input --------------------------------------------------


def test_scan_input_happy(tmp_path):
    p = tmp_path / "x.parquet"
    _write_sample_parquet(p)
    lf = _scan_input(TableFile.of(InputRecord(file=str(p))))
    assert isinstance(lf, pl.LazyFrame)


def test_scan_input_garbage_raises_bad_input_parquet(tmp_path):
    p = tmp_path / "x.parquet"
    p.write_bytes(b"not a parquet")
    with pytest.raises(BadInputParquetError):
        _scan_input(TableFile.of(InputRecord(file=str(p))))


def test_scan_input_from_a_stored_location(tmp_path):
    p = tmp_path / "x.parquet"
    _write_sample_parquet(p)
    record = InputRecord(location=Location(uri=p.as_uri(), env_prefix="ROOT"))
    lf = _scan_input(TableFile.of(record))
    assert lf.collect()["name"].to_list() == ["a", "b", "c"]


def test_scan_input_with_an_unusable_mount_raises_bad_input_parquet(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__REGION", "eu")
    record = InputRecord(location=Location(uri="s3://b/x.parquet", env_prefix="ROOT"))
    with pytest.raises(BadInputParquetError, match="s3://b/x.parquet"):
        _scan_input(TableFile.of(record))


# --- unit: _materialize_input_slot --------------------------------------


def test_materialise_single_zero_files_is_none(tmp_path):
    assert (
        _materialize_input_slot(
            _slot_entry("in_1", 0, []), table_mode=TableMode.SINGLE, info=None
        )
        is None
    )


def test_materialise_single_one_file_is_table_frame(tmp_path):
    p = tmp_path / "x.parquet"
    _write_sample_parquet(p)
    out = _materialize_input_slot(
        _slot_entry("in_1", 0, [str(p)]), table_mode=TableMode.SINGLE, info=None
    )
    assert isinstance(out, TableFrame)


def test_materialise_single_two_files_raises_bad_manifest(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write_sample_parquet(a)
    _write_sample_parquet(b)
    with pytest.raises(BadManifestError):
        _materialize_input_slot(
            _slot_entry("in_1", 0, [str(a), str(b)]),
            table_mode=TableMode.SINGLE,
            info=None,
        )


def test_materialise_single_chunked_publisher_input_is_one_table_frame(
    tmp_path, fixture_publisher
):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).write_parquet(a)
    pl.DataFrame({"id": [None, None], "name": ["c", "d"]}).write_parquet(b)

    out = _materialize_input_slot(
        _slot_entry("in_1", 0, [str(a), str(b)]),
        table_mode=TableMode.SINGLE,
        info=fixture_publisher,
    )

    assert isinstance(out, TableFrame)
    df = out._to_polars_lf().collect()
    assert df["name"].to_list() == ["a", "b", "c", "d"]
    assert df["id"].to_list() == [1, 2, None, None]
    assert df["id"].dtype == pl.Int64


def test_materialise_array_returns_list_of_table_frames(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write_sample_parquet(a)
    _write_sample_parquet(b)
    out = _materialize_input_slot(
        _slot_entry("in_1", 0, [str(a), str(b)]),
        table_mode=TableMode.ARRAY,
        info=None,
    )
    assert isinstance(out, list)
    assert len(out) == 2
    assert all(isinstance(x, TableFrame) for x in out)


def test_materialise_array_empty_is_empty_list():
    out = _materialize_input_slot(
        _slot_entry("in_1", 0, []), table_mode=TableMode.ARRAY, info=None
    )
    assert out == []


# --- unit: _invoke_user_function ----------------------------------------


def _fake_table_frame() -> TableFrame:
    return TableFrame._build(pl.DataFrame({"x": [1]}).lazy(), CreationMode.MUT)


def test_invoke_user_function_happy():
    def fn(in_1):
        return (in_1, None)

    result = _invoke_user_function(fn, [_fake_table_frame()], ["t1", "t2"])
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], TableFrame)
    assert result[1] is None


def test_invoke_user_function_runs_without_the_storage_secrets(monkeypatch):
    variable = "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_SECRET_ACCESS_KEY"
    monkeypatch.setenv(variable, "secret")
    seen = {}

    def fn(in_1):
        seen["value"] = os.environ.get(variable)
        return in_1

    _invoke_user_function(fn, [_fake_table_frame()], ["t1"])

    assert seen == {"value": None}
    assert os.environ[variable] == "secret"


def test_invoke_user_function_raises_maps_to_user_function_failed():
    def fn(in_1):
        raise RuntimeError("boom")

    with pytest.raises(UserFunctionFailedError):
        _invoke_user_function(fn, [_fake_table_frame()], ["t1"])


def test_invoke_user_function_non_tuple_return_raises_shape_mismatch():
    # With more than one output slot a bare (non-tuple) return is still
    # rejected -- the single-output convenience does not apply.
    def fn(in_1):
        return _fake_table_frame()  # missing tuple wrap, but 2 outputs expected

    with pytest.raises(OutputShapeMismatchError):
        _invoke_user_function(fn, [_fake_table_frame()], ["t1", "t2"])


def test_invoke_user_function_bare_single_output_is_wrapped():
    # Single-output convenience: a lone value returned bare is wrapped
    # into a 1-tuple, mirroring the relaxed signature check.
    def fn(in_1):
        return in_1  # bare TableFrame, one output slot

    result = _invoke_user_function(fn, [_fake_table_frame()], ["t1"])
    assert isinstance(result, tuple)
    assert len(result) == 1
    assert isinstance(result[0], TableFrame)


def test_invoke_user_function_bare_single_output_none_is_wrapped():
    # A bare ``None`` for a single output slot is also wrapped.
    def fn(in_1):
        return None

    result = _invoke_user_function(fn, [_fake_table_frame()], ["t1"])
    assert result == (None,)


def test_invoke_user_function_wrong_length_raises_shape_mismatch():
    def fn(in_1):
        return (_fake_table_frame(), _fake_table_frame())  # returns 2, expects 1

    with pytest.raises(OutputShapeMismatchError):
        _invoke_user_function(fn, [_fake_table_frame()], ["t1"])


def test_invoke_user_function_wrong_element_type_raises_shape_mismatch():
    def fn(in_1):
        return ("not a table frame",)

    with pytest.raises(OutputShapeMismatchError):
        _invoke_user_function(fn, [_fake_table_frame()], ["t1"])


# --- unit: _output_targets ----------------------------------------------


def _request(tmp_path: Path, *outputs: str) -> Request:
    return Request.load(
        _write_request(tmp_path / "request.yaml", *outputs), RunFunctionErrorCode.RF_1
    )


def test_output_targets_of_a_publisher_are_their_stored_locations(
    tmp_path, fixture_publisher
):
    targets = _output_targets(
        _request(tmp_path, "untouched", "my_table"),
        fixture_publisher,
        ["my_table", "untouched"],
    )
    assert [target.uri for target in targets] == [
        _stored(tmp_path, "my_table").as_uri(),
        _stored(tmp_path, "untouched").as_uri(),
    ]


def test_output_targets_reject_a_table_without_location(tmp_path, fixture_publisher):
    with pytest.raises(BadRequestError, match="untouched"):
        _output_targets(
            _request(tmp_path, "my_table"),
            fixture_publisher,
            ["my_table", "untouched"],
        )


def test_output_targets_of_a_subscriber_are_its_landing_files(
    tmp_path, subscriber_bundle
):
    info = _load_function_def(subscriber_bundle)
    targets = _output_targets(_request(tmp_path), info, ["out_0", "out_1"])
    assert [target.uri for target in targets] == [
        f"{_landing(tmp_path).as_uri()}/0.t",
        f"{_landing(tmp_path).as_uri()}/1.t",
    ]


# --- unit: _sink_outputs ------------------------------------------------


HOST = ColumnPolicy(Zone.HOST)


def _targets(tmp_path: Path, *tables: str) -> list[TableFile]:
    return [TableFile.local(_stored(tmp_path, table)) for table in tables]


def test_sink_outputs_writes_each_table_frame(tmp_path):
    result = (_fake_table_frame(), _fake_table_frame())
    targets = _targets(tmp_path, "t1", "t2")
    entries = _sink_outputs(result, ["t1", "t2"], targets, HOST).outputs
    assert [entry.name for entry in entries] == ["t1", "t2"]
    for entry, target in zip(entries, targets):
        assert entry.uri == target.uri
        assert entry.stats == target.stats()
        assert entry.stats.row_count == 1


def test_sink_outputs_none_value_records_no_uri_nor_stats(tmp_path):
    result = (None, _fake_table_frame())
    entries = _sink_outputs(
        result, ["t1", "t2"], _targets(tmp_path, "t1", "t2"), HOST
    ).outputs
    assert entries[0] == OutputEntry(name="t1")
    assert entries[1].stats is not None
    assert not _stored(tmp_path, "t1").exists()


def test_sink_outputs_write_failure_maps_to_output_write_failed(tmp_path, monkeypatch):
    def broken_sink(self, *arguments):
        raise RuntimeError("disk full")

    monkeypatch.setattr(TableFrame, "_observed", broken_sink)
    with pytest.raises(OutputWriteFailedError, match="t1.t"):
        _sink_outputs((_fake_table_frame(),), ["t1"], _targets(tmp_path, "t1"), HOST)


def test_sink_outputs_failure_while_writing_maps_to_output_write_failed(
    tmp_path, monkeypatch
):
    def fail(batch):
        raise RuntimeError("disk full")

    def failing_sink(self, *arguments):
        return pl.LazyFrame({"x": [1]}).map_batches(fail)

    monkeypatch.setattr(TableFrame, "_observed", failing_sink)
    with pytest.raises(OutputWriteFailedError, match="t1, t2"):
        _sink_outputs(
            (_fake_table_frame(), _fake_table_frame()),
            ["t1", "t2"],
            _targets(tmp_path, "t1", "t2"),
            HOST,
        )


def test_sink_outputs_unreadable_stats_map_to_output_write_failed(
    tmp_path, monkeypatch
):
    def unreadable(self):
        raise OSError("gone")

    monkeypatch.setattr(TableFile, "stats", unreadable)
    with pytest.raises(OutputWriteFailedError, match="stats of output 't1'"):
        _sink_outputs((_fake_table_frame(),), ["t1"], _targets(tmp_path, "t1"), HOST)


def test_sink_outputs_writes_every_output_in_one_pass(tmp_path, monkeypatch):
    collected = []
    collect_all = pl.collect_all

    def counting(plans, **options):
        collected.append(len(plans))
        return collect_all(plans, **options)

    monkeypatch.setattr(pl, "collect_all", counting)
    _sink_outputs(
        (_fake_table_frame(), _fake_table_frame(), None),
        ["t1", "t2", "t3"],
        _targets(tmp_path, "t1", "t2", "t3"),
        HOST,
    )
    assert collected == [2]


def _state_output(tmp_path: Path, values: dict) -> StateOutput:
    source = tmp_path / "state" / STATE_FILE
    source.parent.mkdir(parents=True, exist_ok=True)
    write_state(values, source)
    return StateOutput(
        name=STATE_TABLE,
        source=TableFile.local(source),
        target=TableFile.local(_stored(tmp_path, STATE_TABLE)),
    )


def test_sink_outputs_writes_the_state_in_the_same_pass(tmp_path, monkeypatch):
    collected = []
    collect_all = pl.collect_all

    def counting(plans, **options):
        collected.append(len(plans))
        return collect_all(plans, **options)

    monkeypatch.setattr(pl, "collect_all", counting)
    state = _state_output(tmp_path, {"cursor": 250})
    manifest = _sink_outputs(
        (_fake_table_frame(),),
        ["t1"],
        _targets(tmp_path, "t1"),
        HOST,
        state=state,
    )
    assert collected == [2]
    assert [entry.name for entry in manifest.outputs] == ["t1"]
    assert manifest.state == OutputEntry(
        name=STATE_TABLE, uri=state.target.uri, stats=state.target.stats()
    )
    assert _stored_state(tmp_path) == {"cursor": 250}


def test_sink_outputs_writes_the_state_without_outputs(tmp_path):
    state = _state_output(tmp_path, {})
    manifest = _sink_outputs(
        (None,), ["t1"], _targets(tmp_path, "t1"), HOST, state=state
    )
    assert manifest.outputs == (OutputEntry(name="t1"),)
    assert manifest.state.stats.column_count == 0
    assert _stored_state(tmp_path) == {}


def test_sink_outputs_without_state_records_none(tmp_path):
    manifest = _sink_outputs(
        (_fake_table_frame(),), ["t1"], _targets(tmp_path, "t1"), HOST
    )
    assert manifest.state is None


def test_sink_outputs_state_write_failure_maps_to_output_write_failed(
    tmp_path, monkeypatch
):
    def broken_copy(self, target):
        raise RuntimeError("disk full")

    monkeypatch.setattr(TableFile, "copy", broken_copy)
    with pytest.raises(OutputWriteFailedError, match=STATE_TABLE):
        _sink_outputs(
            (None,),
            ["t1"],
            _targets(tmp_path, "t1"),
            HOST,
            state=_state_output(tmp_path, {}),
        )


@pytest.mark.parametrize("zone", [Zone.HOST, Zone.LAND])
def test_sink_outputs_keeps_system_columns(tmp_path, zone):
    from tabsdatak.tableframe._engine.constant.column.prefixes import (
        TD_SYSTEM_COLUMN_PREFIX,
    )

    _sink_outputs(
        (_fake_table_frame(),),
        ["t1"],
        _targets(tmp_path, "t1"),
        ColumnPolicy(zone),
    )
    columns = pl.read_parquet_schema(_stored(tmp_path, "t1"))
    assert any(c.startswith(TD_SYSTEM_COLUMN_PREFIX) for c in columns)


# --- main: error paths --------------------------------------------------


def test_main_missing_request_returns_bad_request(tmp_path, monkeypatch, http_ok_url):
    _set_argv(
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


def test_main_missing_bundle_returns_bad_bundle(tmp_path, monkeypatch, http_ok_url):
    req = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        req,
        tmp_path / "no-bundle",
        tmp_path / "inputs.yaml",
        tmp_path / "out",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BadBundleError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run-function", "--request", "x"])
    assert main() == 1


# --- main: end-to-end ---------------------------------------------------


def test_main_end_to_end_happy_path(tmp_path, built_bundle, monkeypatch, http_ok_url):
    """Real publisher, real bundle, real inputs manifest pointing at a
    real parquet. Invoke main(); verify outputs."""
    # Write the request.
    request_path = _write_request(tmp_path / "request.yaml", "my_table", "untouched")

    # Write a sample parquet and an inputs manifest pointing at it.
    raw_parquet = tmp_path / "raw" / "rows.parquet"
    _write_sample_parquet(raw_parquet)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(inputs=(_slot_entry("in_1", 0, [str(raw_parquet)]),)).write(
        inputs_manifest
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
        responder_url=http_ok_url,
    )
    rc = main()
    assert rc == 0, f"main() returned {rc}"

    # One output was written to its stored location for `my_table` (the
    # function returned a LazyFrame), nothing for `untouched` (function
    # returned None), and nothing locally.
    my_table_file = _stored(tmp_path, "my_table")
    assert my_table_file.is_file()
    assert not _stored(tmp_path, "untouched").exists()
    assert not list(out_dir.rglob("*.parquet"))

    # The user function added a `marker` column; the underlying data
    # comes through unchanged.
    df = pl.read_parquet(my_table_file)
    assert "marker" in df.columns
    assert df["marker"].to_list() == ["from_user_fn"] * 3
    assert df["id"].to_list() == [1, 2, 3]
    assert df["name"].to_list() == ["a", "b", "c"]

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
    tmp_path, built_bundle, monkeypatch, http_ok_url
):
    """If the outputs-manifest write fails, surface OutputsManifestWriteFailedError."""
    request_path = _write_request(tmp_path / "request.yaml", "my_table", "untouched")
    raw_parquet = tmp_path / "raw" / "rows.parquet"
    _write_sample_parquet(raw_parquet)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(inputs=(_slot_entry("in_1", 0, [str(raw_parquet)]),)).write(
        inputs_manifest
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
        responder_url=http_ok_url,
    )
    assert main() == OutputsManifestWriteFailedError.exit_code


# --- main: multi-cardinality (regression — see commit history) -----------


def test_main_multi_cardinality_publisher_concatenates_two_slots(
    tmp_path, multi_publisher_bundle, monkeypatch, http_ok_url
):
    """A publisher whose source has 2 paths produces 2 input slots, and
    the user function declares 2 parameters. Exercises the full _run()
    flow with input_count > 1 on the publisher path — guards against
    the regression where _input_count was hardcoded to 1 for
    publishers."""
    request_path = _write_request(tmp_path / "request.yaml", "concat")

    # One parquet per slot.
    a_parquet = tmp_path / "raw" / "a.parquet"
    b_parquet = tmp_path / "raw" / "b.parquet"
    a_parquet.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2], "name": ["a1", "a2"]}).write_parquet(a_parquet)
    pl.DataFrame({"id": [10, 11], "name": ["b1", "b2"]}).write_parquet(b_parquet)

    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            _slot_entry("in_a", 0, [str(a_parquet)]),
            _slot_entry("in_b", 1, [str(b_parquet)]),
        )
    ).write(inputs_manifest)

    out_dir = tmp_path / "fn_out"
    outputs_manifest = tmp_path / "outputs.yaml"
    _set_argv(
        monkeypatch,
        request_path,
        multi_publisher_bundle,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    rc = main()
    assert rc == 0, f"main() returned {rc}"

    concat_file = _stored(tmp_path, "concat")
    assert concat_file.is_file()
    df = pl.read_parquet(concat_file)
    # Both inputs got concatenated (2 + 2 rows).
    assert df.height == 4
    assert sorted(df["id"].to_list()) == [1, 2, 10, 11]


def test_main_multi_cardinality_transformer_with_mixed_modes(
    tmp_path, multi_transformer_bundle, monkeypatch, http_ok_url
):
    """A transformer with 3 input_tables (mixed SINGLE/ARRAY/SINGLE) and
    one output. Exercises the full _run() flow with input_count > 1 on
    the transformer path AND with mixed slot modes — guards against
    regressions in both per-slot mode inference and the slot count."""
    request_path = _write_request(tmp_path / "request.yaml", "merged")

    # Three input parquets: a (SINGLE), b (ARRAY, one file), c (SINGLE).
    a = tmp_path / "raw" / "a.parquet"
    b = tmp_path / "raw" / "b.parquet"
    c = tmp_path / "raw" / "c.parquet"
    a.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1], "name": ["a"]}).write_parquet(a)
    pl.DataFrame({"id": [2], "name": ["b"]}).write_parquet(b)
    pl.DataFrame({"id": [3], "name": ["c"]}).write_parquet(c)

    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            _slot_entry("a", 0, [str(a)]),
            _slot_entry("b", 1, [str(b)]),
            _slot_entry("c", 2, [str(c)]),
        )
    ).write(inputs_manifest)

    out_dir = tmp_path / "fn_out"
    outputs_manifest = tmp_path / "outputs.yaml"
    _set_argv(
        monkeypatch,
        request_path,
        multi_transformer_bundle,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    rc = main()
    assert rc == 0, f"main() returned {rc}"

    merged_file = _stored(tmp_path, "merged")
    assert merged_file.is_file()
    df = pl.read_parquet(merged_file)
    assert df.height == 3
    assert sorted(df["id"].to_list()) == [1, 2, 3]


def test_main_transformer_with_a_udf(
    tmp_path, udf_transformer_bundle, monkeypatch, http_ok_url
):
    bundle, broken = udf_transformer_bundle
    request_path = _write_request(tmp_path / "request.yaml", "t_out")
    source = tmp_path / "raw" / "in.parquet"
    _write_sample_parquet(source)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(inputs=(_slot_entry("t_in", 0, [str(source)]),)).write(
        inputs_manifest
    )
    out_dir = tmp_path / "fn_out"
    _set_argv(
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
    df = pl.read_parquet(_stored(tmp_path, "t_out"))
    assert df.select("id", "name", "doubled", "label").to_dicts() == [
        {"id": 1, "name": "a", "doubled": 2, "label": "A"},
        {"id": 2, "name": "b", "doubled": 4, "label": "B"},
        {"id": 3, "name": "c", "doubled": 6, "label": "C"},
    ]
    assert "$td.id" in df.columns


def test_main_evaluates_a_udf_once_per_row_with_data_quality(
    tmp_path, quality_udf_transformer_bundle, monkeypatch, http_ok_url
):
    bundle, module, failing = quality_udf_transformer_bundle
    request_path = _write_request(
        tmp_path / "request.yaml", "t_out", "t_out_quality", "t_out_quality_summary"
    )
    source = tmp_path / "raw" / "in.parquet"
    _write_sample_parquet(source)
    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(inputs=(_slot_entry("t_in", 0, [str(source)]),)).write(
        inputs_manifest
    )
    out_dir = tmp_path / "fn_out"
    _set_argv(
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
    assert summary.select("dq.total", "dq.true", "dq.false").row(0) == (3, 2, 1)


def test_main_manifest_length_mismatch_returns_bad_manifest(
    tmp_path, multi_publisher_bundle, monkeypatch, http_ok_url
):
    """The multi-cardinality publisher declares 2 input slots; feeding
    it a 1-entry manifest must surface BadManifestError rather than
    silently dropping the second slot."""
    request_path = _write_request(tmp_path / "request.yaml")
    a_parquet = tmp_path / "raw" / "a.parquet"
    a_parquet.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1]}).write_parquet(a_parquet)

    inputs_manifest = tmp_path / "inputs.yaml"
    InputManifest(inputs=(_slot_entry("in_a", 0, [str(a_parquet)]),)).write(
        inputs_manifest
    )

    _set_argv(
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
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_run_function"


# --- T5: FunctionCtx + initial_values lifecycle -------------------------


def test_function_context_is_a_function_ctx_subclass():
    """``_FunctionContext`` must be a concrete FunctionCtx so that
    ``isinstance(ctx, FunctionCtx)`` holds at the call boundary, and
    must NOT also satisfy the SrcPluginCtx / DestContext role tags."""
    c = _FunctionContext(
        work_dir=Path("/work"),
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
    assert isinstance(c, FunctionCtx)
    assert not isinstance(c, SrcPluginCtx)
    assert not isinstance(c, DestContext)


def test_function_context_serves_the_project_secret():
    c = _FunctionContext(
        work_dir=Path("/work"),
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
    assert c.secret_names == ["aws"]
    assert c.secret_keys("aws") == ["key1"]
    assert c.secret("aws", "key1") == "mykey1"
    assert c.secret("aws.key1") == "mykey1"


def test_infer_table_modes_skips_trailing_ctx_param():
    """When the user function declares a trailing TransactionCtx-typed param,
    ``_infer_table_modes`` must skip it — only the input slots are
    real table-mode-bearing parameters."""

    def f(in_1: TableFrameSpec, ctx: FunctionCtx) -> TableFrameSpec:
        return None

    modes = _infer_table_modes(f)
    assert modes == [TableMode.SINGLE]


def test_infer_table_modes_does_not_skip_without_ctx():
    """No trailing ctx → every parameter is a slot. Sanity check that
    the trim only kicks in when has_transaction_ctx_param is True."""

    def f(in_1: TableFrameSpec, in_2: TableFrameSpec) -> TableFrameSpec:
        return None

    modes = _infer_table_modes(f)
    assert modes == [TableMode.SINGLE, TableMode.SINGLE]


# Helper to build a self-contained user project with a ctx-aware
# publisher whose user function reads / writes initial_values through
# the trailing ctx parameter.
@pytest.fixture
def ctx_aware_built_bundle(tmp_path, monkeypatch) -> Path:
    """Build + extract a bundle whose @publisher function declares a
    trailing ``ctx: TransactionCtx`` parameter."""
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    _write_sample_parquet(srcdata / "rows.parquet")

    project = _copy_project(tmp_path, monkeypatch, "ctx_project")
    from my_pkg.publisher import my_publisher  # noqa: E402

    yield _bundle(tmp_path, project, my_publisher)

    _forget("my_pkg")


def _write_inputs_manifest_for(input_files: list[str], path: Path) -> None:
    """Helper: write a minimal inputs manifest mapping ``in_1`` to
    ``input_files`` (ARRAY mode → list)."""
    InputManifest(inputs=(_slot_entry("in_1", 0, input_files),)).write(path)


STATE_TABLE = "td.fn_state__F-1"


def _previous_state(root: Path) -> Path:
    return root / "storage" / "previous-state.t"


def _request_with_state_slots(
    path: Path,
    *,
    outputs: tuple[str, ...] = ("my_table",),
    previous: dict | None = None,
) -> Path:
    info = _info()
    info["execution_id"] = 2
    info["transaction_id"] = 3
    info["physical_function_run_id"] = 4
    info["function_landing"] = {
        "uri": _landing(path.parent).as_uri(),
        "env_prefix": "ROOT",
    }
    state_in = None
    if previous is not None:
        _previous_state(path.parent).parent.mkdir(parents=True, exist_ok=True)
        write_state(previous, _previous_state(path.parent))
        state_in = {"uri": _previous_state(path.parent).as_uri(), "env_prefix": "ROOT"}
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info,
                "output": [
                    _stored_output(path.parent, table, index)
                    for index, table in enumerate(outputs)
                ],
                "system_input": [
                    _input_table({"table": STATE_TABLE, "location": state_in}, 0)
                ],
                "system_output": [_stored_output(path.parent, STATE_TABLE, 0)],
            }
        })
    )
    return path


def _stored_state(root: Path) -> dict:
    return read_state(TableFile.local(_stored(root, STATE_TABLE)))


def _run_with_state(
    tmp_path, bundle, monkeypatch, http_ok_url, request_path, *, local=None
) -> OutputManifest:
    inputs_manifest = tmp_path / "inputs.yaml"
    _write_inputs_manifest_for([str(_seed_input_parquet(tmp_path))], inputs_manifest)
    outputs_manifest = tmp_path / "outputs.yaml"
    state_folder = tmp_path / "state"
    state_folder.mkdir()
    if local is not None:
        write_state(local, state_folder / STATE_FILE)
    _set_argv(
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
    rc = main()
    assert rc == 0, rc
    return OutputManifest.load(outputs_manifest)


def test_main_ctx_aware_function_first_run_writes_its_state(
    tmp_path, ctx_aware_built_bundle, monkeypatch, http_ok_url
):
    """First run for the flow: no previous state. The user function
    sees ``None`` for ``get_attr``, sets a value, and run-function writes
    the drained state to system_output[0] with its stats."""
    req = _request_with_state_slots(tmp_path / "request.yaml")
    manifest = _run_with_state(
        tmp_path, ctx_aware_built_bundle, monkeypatch, http_ok_url, req
    )
    assert _stored_state(tmp_path) == {"my-key": "from-user-fn"}
    assert read_state(TableFile.local(tmp_path / "state" / STATE_FILE)) == {
        "my-key": "from-user-fn"
    }
    assert manifest.state == OutputEntry(
        name=STATE_TABLE,
        uri=_stored(tmp_path, STATE_TABLE).as_uri(),
        stats=TableFile.local(_stored(tmp_path, STATE_TABLE)).stats(),
    )
    assert manifest.state.stats.row_count == 1


def test_main_ctx_aware_function_prefers_this_run_state(
    tmp_path, ctx_aware_built_bundle, monkeypatch, http_ok_url
):
    """When run-source-plugin already drained this run's state, the user
    function reads it, not the previous run's."""
    req = _request_with_state_slots(
        tmp_path / "request.yaml", previous={"my-key": "from-state-in"}
    )
    _run_with_state(
        tmp_path,
        ctx_aware_built_bundle,
        monkeypatch,
        http_ok_url,
        req,
        local={"my-key": "preset"},
    )
    assert _stored_state(tmp_path) == {"my-key": "again-after-preset"}


def test_main_ctx_aware_function_reads_the_previous_state_where_stored(
    tmp_path, ctx_aware_built_bundle, monkeypatch, http_ok_url
):
    """Without this run's state (transformer / subscriber chains), the
    user function hydrates from the previous run's stored state."""
    req = _request_with_state_slots(
        tmp_path / "request.yaml", previous={"my-key": "from-state-in"}
    )
    _run_with_state(tmp_path, ctx_aware_built_bundle, monkeypatch, http_ok_url, req)
    assert _stored_state(tmp_path) == {"my-key": "again-after-from-state-in"}


def test_main_function_without_ctx_writes_no_state(
    tmp_path, built_bundle, monkeypatch, http_ok_url
):
    """A function with no ctx drains nothing; without a state drained by
    run-source-plugin there is no state to write."""
    req = _request_with_state_slots(
        tmp_path / "request.yaml", outputs=("my_table", "untouched")
    )
    manifest = _run_with_state(tmp_path, built_bundle, monkeypatch, http_ok_url, req)
    assert not (tmp_path / "state" / STATE_FILE).exists()
    assert not _stored(tmp_path, STATE_TABLE).exists()
    assert manifest.state is None


def test_main_function_without_ctx_writes_the_source_plugin_state(
    tmp_path, built_bundle, monkeypatch, http_ok_url
):
    """The state run-source-plugin drained is written even when the user
    function has no ctx."""
    req = _request_with_state_slots(
        tmp_path / "request.yaml", outputs=("my_table", "untouched")
    )
    manifest = _run_with_state(
        tmp_path,
        built_bundle,
        monkeypatch,
        http_ok_url,
        req,
        local={"cursor": 250},
    )
    assert _stored_state(tmp_path) == {"cursor": 250}
    assert manifest.state.name == STATE_TABLE


def _seed_input_parquet(tmp_path: Path) -> Path:
    """Write a simple parquet so the inputs manifest references a real
    file (run-function won't fail materialising the slot). Mirrors the
    fixture publisher's expected input shape."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = tmp_path / "input.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3]}), out)
    return out


# --- mkdir ordering: out_dir exists before the user function runs --------


def test_main_creates_out_dir_before_invoking_user_function(
    tmp_path, monkeypatch, http_ok_url
):
    """A ctx-aware user function may surface ``ctx.work_dir`` (==
    ``out_dir`` for run-function) and try to write a sidecar there;
    the runtime must create the directory before the call, not after.
    Builds a publisher whose body asserts the directory exists
    immediately on entry."""
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    _write_sample_parquet(srcdata / "rows.parquet")

    project = _copy_project(tmp_path, monkeypatch, "work_dir_project")
    from my_pkg.publisher import my_publisher  # noqa: E402

    extracted = _bundle(tmp_path, project, my_publisher)

    req = _request_with_state_slots(tmp_path / "request.yaml")
    inputs_manifest = tmp_path / "inputs.yaml"
    _write_inputs_manifest_for([str(_seed_input_parquet(tmp_path))], inputs_manifest)
    out_dir = tmp_path / "fn_out"  # deliberately does NOT exist yet
    outputs_manifest = tmp_path / "outputs.yaml"
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    _set_argv(
        monkeypatch,
        req,
        extracted,
        inputs_manifest,
        out_dir,
        outputs_manifest,
        tmp_path / "live",
        state_dir=state_dir,
        responder_url=http_ok_url,
    )
    rc = main()
    _forget("my_pkg")

    assert rc == 0, rc


# --- ctx parameter works for @transformer and @subscriber too -----------
#
# The runtime in ``run_function`` is kind-agnostic: it constructs the
# FunctionCtx and drains it based on ``has_transaction_ctx_param(func)`` only,
# regardless of which decorator wrapped the user function. These tests
# pin that behaviour for the two non-publisher kinds so anyone narrowing
# the ctx surface in the future hits a test failure.


def _build_extracted_bundle(
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
    project = _copy_project(tmp_path, monkeypatch, project_name)
    mod = importlib.import_module(f"{pkg_name}.{module_name}")
    request.addfinalizer(lambda: _forget(pkg_name))
    return _bundle(tmp_path, project, getattr(mod, decorated_name))


def test_main_ctx_aware_transformer_hydrates_and_drains(
    tmp_path, monkeypatch, request, http_ok_url
):
    """A @transformer that declares a trailing ``ctx: TransactionCtx`` gets the
    same hydrate / drain / write treatment as a
    publisher. Verifies the runtime is kind-agnostic."""
    extracted = _build_extracted_bundle(
        tmp_path,
        monkeypatch,
        request,
        project_name="tx_ctx_project",
        pkg_name="tx_ctx_pkg",
        module_name="transformer",
        decorated_name="my_transformer",
    )

    # Only the previous state: transformers run in chains without
    # run-source-plugin, so the stored-state path is the one exercised.
    req = _request_with_state_slots(
        tmp_path / "request.yaml",
        outputs=("t_out",),
        previous={"smoke.last-seen": "from-state-in"},
    )
    _run_with_state(tmp_path, extracted, monkeypatch, http_ok_url, req)
    assert _stored_state(tmp_path) == {"smoke.last-seen": "tx-after-from-state-in"}


def test_main_ctx_aware_subscriber_hydrates_and_drains(
    tmp_path, monkeypatch, request, http_ok_url
):
    """A @subscriber that declares a trailing ``ctx: TransactionCtx`` also
    round-trips state cleanly. Pins kind-agnostic runtime behaviour
    for the third decorator kind."""
    dest_out = tmp_path / "sub_dest"
    dest_out.mkdir()
    extracted = _build_extracted_bundle(
        tmp_path,
        monkeypatch,
        request,
        project_name="sub_ctx_project",
        pkg_name="sub_ctx_pkg",
        module_name="subscriber",
        decorated_name="my_subscriber",
    )

    req = _request_with_state_slots(
        tmp_path / "request.yaml", previous={"smoke.last-seen": "from-state-in"}
    )
    _run_with_state(tmp_path, extracted, monkeypatch, http_ok_url, req)
    assert _stored_state(tmp_path) == {"smoke.last-seen": "sub-after-from-state-in"}
    assert (_landing(tmp_path) / "0.t").is_file()
