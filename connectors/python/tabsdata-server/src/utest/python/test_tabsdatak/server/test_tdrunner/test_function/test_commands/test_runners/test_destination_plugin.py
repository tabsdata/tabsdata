#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
import yaml

from tabsdatak._info import SubscriberInfo
from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME, build_bundle
from tabsdatak.server._tdrunner.function._commons.context.destination import (
    DestinationContext,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Responder
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    FunctionBundle,
)
from tabsdatak.server._tdrunner.function._handlers.bundle import BundleArchive
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.handoff import DestinationHandoff
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin import (
    main,
)
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.error import (  # noqa: E501
    BadBundleError,
    BadConnectionError,
    BadManifestError,
    BadRequestError,
    BadUserCodeError,
    NoPluginError,
    OutputReadFailedError,
    PluginFailedError,
    RunDestinationPluginErrorCode,
    WrongKindError,
)
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.options import (  # noqa: E501
    RunDestinationPluginArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.runner import (  # noqa: E501
    DEFINITION_ERRORS,
    KINDS,
    DestinationPlugin,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


RUNNER_MODULE = (
    "tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.runner"
)


# --- shared fixtures ----------------------------------------------------


USER_SUBSCRIBER_SOURCE = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDest

_DEST = LocalFileDest(
    paths=["sink.parquet"],
)


@subscriber(
    destination=_DEST,
    input_tables=["c1/t_in"],
)
def my_subscriber(in_1: TableFrameSpec) -> TableFrameSpec:
    return in_1
"""


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """User project carrying a @subscriber. The destination writes to
    a sibling `destination/` folder under tmp_path."""
    destination_folder = tmp_path / "destination"
    destination_folder.mkdir()
    project = tmp_path / "project"
    package = project / "sub_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "subscriber.py").write_text(USER_SUBSCRIBER_SOURCE)
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
def built_bundle(
    tmp_path,
    user_project,
    fixture_subscriber,
) -> Path:
    bundle_path = tmp_path / "bundle.tar.zst"
    build_bundle(
        fixture_subscriber,
        user_project,
        bundle_path,
    )
    extracted = tmp_path / "extracted"
    BundleArchive(bundle_path).extract(extracted)
    return extracted


def write_request(path: Path, bundle_uri: str = "file:///bundle.tar.zst") -> Path:
    path.write_text(
        yaml.safe_dump({
            "Function": {
                "info": info_payload(
                    bundle_uri,
                    execution_id=42,
                    transaction_id=99,
                    logical_function_run_id=7,
                    physical_function_run_id=7,
                    function_landing={
                        "uri": (path.parent / "landing").as_uri(),
                        "env_prefix": "ROOT",
                    },
                ),
            }
        })
    )
    return path


def land(request_path: Path, frame: pl.DataFrame) -> OutputEntry:
    request = Request.load(request_path, RunDestinationPluginErrorCode.RDP_1)
    target = TableFile.stored(request.info.landing_file(0))
    value = TableFrame._build(frame.lazy(), CreationMode.MUT)
    ColumnPolicy(Zone.LAND).sink(value, target).collect()
    return OutputEntry(name="out_0", uri=target.uri)


def write_connection_file(path: Path, base_path: Path) -> Path:
    """Write the connection.yaml the command loads via --connection-file.

    The connection is no longer carried on the function descriptor; the
    runtime reads it from this file (the collection's connection)."""
    from tabsdatak.conn.localfile import LocalFileDestConn

    path.write_text(
        yaml.safe_dump(LocalFileDestConn(base_path=str(base_path)).to_dict())
    )
    return path


def set_arguments(
    monkeypatch,
    request_path: Path,
    bundle_folder: Path,
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
            "destination-plugin",
            "--request",
            str(request_path),
            "--bundle-folder",
            str(bundle_folder),
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


def destination_plugin(tmp_path: Path, **state) -> DestinationPlugin:
    command = DestinationPlugin(
        RunDestinationPluginArguments(
            request=tmp_path / "request.yaml",
            bundle_folder=tmp_path / "bundle",
            outputs_manifest=tmp_path / "outputs.yaml",
            connection_file=tmp_path / "connection.yaml",
            liveness=tmp_path / "liveness",
            responder_url="http://127.0.0.1:1",
            responder_method="POST",
            responder_authorization="Bearer tok",
            journal_file=tmp_path / "journal.yaml",
            work_folder=tmp_path / "work",
        )
    )
    for name, value in state.items():
        setattr(command, name, value)
    return command


def load_function_definition(bundle_folder: Path) -> SubscriberInfo:
    return FunctionBundle(bundle_folder).definition(KINDS, DEFINITION_ERRORS)


# --- unit: DestinationContext --------------------------------------------------


def destination_context(**overrides) -> DestinationContext:
    """Default-fill the new DestContext fields so each test only has to
    override what it's exercising."""
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
    return DestinationContext(**defaults)


def test_dest_context_exposes_fields():
    context = destination_context()
    assert context.work_dir == Path("/work")
    assert context.execution_id == "exec1"
    assert context.transaction_id == "tx1"
    assert context.triggered_on == datetime.fromisoformat(
        "2026-06-04T07:14:28.376000+00:00"
    )
    assert context.collection_id == "c_id"
    assert context.collection == "c_name"
    assert context.function_id == "fn_run_42"
    assert context.function == "publish_people"


def test_dest_context_get_attr_returns_none_for_unset_key():
    """``get_attr`` honours the ``returns None if unset`` contract on
    a fresh context."""
    assert destination_context().get_attr("k") is None


def test_dest_context_set_attr_round_trips_via_get_attr():
    """``set_attr`` writes through; a subsequent ``get_attr`` sees
    the value."""
    context = destination_context()
    context.set_attr("k", "v")
    assert context.get_attr("k") == "v"


def test_dest_context_add_metadata_is_noop():
    """No observability sink wired yet -- ``add_metadata`` is a no-op."""
    assert destination_context().add_metadata("k", "v") is None


# --- unit: load_function_definition -------------------------------------------


def test_load_function_def_happy(built_bundle):
    subscriber = load_function_definition(built_bundle)
    assert isinstance(subscriber, SubscriberInfo)


def test_load_function_def_missing_manifest_raises_bad_bundle(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(BadBundleError):
        load_function_definition(empty)


def test_load_function_def_malformed_json_raises_bad_bundle(tmp_path):
    bad = tmp_path / "bad-bundle"
    bad.mkdir()
    (bad / BUNDLE_MANIFEST_FILENAME).write_text("{ not json")
    with pytest.raises(BadBundleError):
        load_function_definition(bad)


def test_load_function_def_wrong_kind_raises_wrong_kind(built_bundle):
    """A bundle whose kind is not subscriberDef must surface as
    WrongKindError so the orchestrator routes it elsewhere."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["kind"] = "publisherDef"
    manifest.write_text(json.dumps(data))
    with pytest.raises(WrongKindError):
        load_function_definition(built_bundle)


def test_load_function_def_unresolvable_func_raises_bad_user_code(built_bundle):
    """A `func` pointing at a missing module surfaces as
    BadUserCodeError via pydantic's ValidationError chain."""
    manifest = built_bundle / BUNDLE_MANIFEST_FILENAME
    data = json.loads(manifest.read_text())
    data["spec"]["func"] = "nonexistent.module:nope"
    manifest.write_text(json.dumps(data))
    with pytest.raises(BadUserCodeError):
        load_function_definition(built_bundle)


# --- unit: resolve_plugin ----------------------------------------------


def test_resolve_plugin_happy(tmp_path, fixture_subscriber):
    plugin = destination_plugin(
        tmp_path, subscriber=fixture_subscriber
    ).resolve_plugin()
    # LocalFileDestPlugin is what's registered for LocalFileDest.
    assert plugin.__class__.__name__ == "LocalFileDestPlugin"


def test_resolve_plugin_unknown_dest_raises_no_plugin(
    tmp_path,
    fixture_subscriber,
    monkeypatch,
):
    def _raise(_destination_class):
        raise KeyError("no DestDef registered")

    monkeypatch.setattr(f"{RUNNER_MODULE}.get_dest", _raise)
    with pytest.raises(NoPluginError):
        destination_plugin(tmp_path, subscriber=fixture_subscriber).resolve_plugin()


# --- unit: read_tables -------------------------------------------------


def file_handoff(tmp_path: Path) -> DestinationHandoff:
    return DestinationHandoff(
        base_folder=tmp_path / "work",
        legacy=True,
    )


def test_read_tables_maps_an_output_without_data_to_bad_manifest(tmp_path):
    request = Request.load(
        write_request(tmp_path / "request.yaml"),
        RunDestinationPluginErrorCode.RDP_1,
    )
    manifest = OutputManifest(outputs=(OutputEntry(name="out_0"),))
    command = destination_plugin(
        tmp_path,
        handoff=file_handoff(tmp_path),
        request=request,
        manifest=manifest,
    )
    with pytest.raises(BadManifestError):
        command.read_tables()


def test_read_tables_maps_an_unreadable_landing_file_to_read_failed(tmp_path):
    request = Request.load(
        write_request(tmp_path / "request.yaml"),
        RunDestinationPluginErrorCode.RDP_1,
    )
    entry = OutputEntry(name="out_0", uri=request.info.landing_file(0).uri)
    command = destination_plugin(
        tmp_path,
        handoff=file_handoff(tmp_path),
        request=request,
        manifest=OutputManifest(outputs=(entry,)),
    )
    with pytest.raises(OutputReadFailedError):
        command.read_tables()


# --- unit: write_tables ------------------------------------------------


def writing_command(tmp_path: Path, plugin) -> DestinationPlugin:
    return destination_plugin(
        tmp_path,
        handoff=file_handoff(tmp_path),
        request=Request.load(
            write_request(tmp_path / "request.yaml"),
            RunDestinationPluginErrorCode.RDP_1,
        ),
        plugin=plugin,
        connection=object(),
        subscriber=SimpleNamespace(destination=object()),
    )


def test_write_tables_returns_normally(tmp_path):
    calls = []

    class Plugin:
        def write_out(
            self,
            context,
            connection,
            destination,
            tables,
        ):
            calls.append((context, connection, destination, tables))

    writing_command(tmp_path, Plugin()).write_tables()
    assert len(calls) == 1


def test_write_tables_wraps_plugin_exception(tmp_path):
    class Plugin:
        def write_out(
            self,
            context,
            connection,
            destination,
            tables,
        ):
            raise RuntimeError("boom")

    with pytest.raises(PluginFailedError):
        writing_command(tmp_path, Plugin()).write_tables()


# --- main: error paths --------------------------------------------------


def test_main_missing_request_returns_bad_request(
    tmp_path,
    monkeypatch,
    http_ok_url,
):
    set_arguments(
        monkeypatch,
        tmp_path / "no.yaml",
        tmp_path / "bundle",
        tmp_path / "outputs.yaml",
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
        tmp_path / "no-bundle",
        tmp_path / "outputs.yaml",
        tmp_path / "live",
        connection_file=tmp_path / "connection.yaml",
        responder_url=http_ok_url,
    )
    assert main() == BadBundleError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["destination-plugin"],
    )
    assert main() == 1


def test_main_missing_outputs_manifest_returns_bad_manifest(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    request_path = write_request(tmp_path / "request.yaml")
    connection_file = write_connection_file(
        tmp_path / "connection.yaml",
        tmp_path / "destination",
    )
    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        tmp_path / "no-outputs.yaml",
        tmp_path / "live",
        connection_file=connection_file,
        responder_url=http_ok_url,
    )
    assert main() == BadManifestError.exit_code


def test_main_all_null_manifest_skips_write_out(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    # A subscriber that returns None for every output slot produced no new data
    # this run, so the destination plugin is skipped and main() succeeds.
    request_path = write_request(tmp_path / "request.yaml")
    connection_file = write_connection_file(
        tmp_path / "connection.yaml",
        tmp_path / "destination",
    )
    outputs_manifest = tmp_path / "outputs.yaml"
    OutputManifest(outputs=(OutputEntry(name="out_0"),)).write(outputs_manifest)
    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        outputs_manifest,
        tmp_path / "live",
        connection_file=connection_file,
        responder_url=http_ok_url,
    )
    assert main() == 0


# --- main: end-to-end --------------------------------------------------


def test_main_end_to_end_happy_path(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    """Real subscriber, real LocalFileDestPlugin. Land an output with every
    framework column, point the outputs manifest at it, invoke main();
    verify the destination plugin shipped only the user's columns.
    """
    request_path = write_request(tmp_path / "request.yaml")
    landed = land(request_path, pl.DataFrame({"id": [1, 2, 3], "name": list("abc")}))

    outputs_manifest = tmp_path / "outputs.yaml"
    OutputManifest(outputs=(landed,)).write(outputs_manifest)

    # The connection (with the destination base_path) now comes from the
    # connection file the runtime loads, not from the function descriptor.
    connection_file = write_connection_file(
        tmp_path / "connection.yaml",
        tmp_path / "destination",
    )

    liveness = tmp_path / "live"
    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        outputs_manifest,
        liveness,
        connection_file=connection_file,
        responder_url=http_ok_url,
    )

    status = main()
    assert status == 0, f"main() returned {status}"

    # LocalFileDest wrote sink.parquet under tmp_path/destination_folder (the
    # base_path baked into the user project's destination).
    shipped = tmp_path / "destination" / "sink.parquet"
    assert shipped.is_file()
    shipped_frame = pl.read_parquet(shipped)
    assert shipped_frame.columns == ["id", "name"]
    assert shipped_frame["id"].to_list() == [1, 2, 3]
    assert shipped_frame["name"].to_list() == ["a", "b", "c"]

    assert liveness.exists()


def shipping_setup(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    request_path = write_request(tmp_path / "request.yaml")
    landed = land(request_path, pl.DataFrame({"id": [1], "name": ["a"]}))
    outputs_manifest = tmp_path / "outputs.yaml"
    OutputManifest(outputs=(landed,)).write(outputs_manifest)
    connection_file = write_connection_file(
        tmp_path / "connection.yaml",
        tmp_path / "destination",
    )
    set_arguments(
        monkeypatch,
        request_path,
        built_bundle,
        outputs_manifest,
        tmp_path / "live",
        connection_file=connection_file,
        responder_url=http_ok_url,
    )
    return tmp_path / "destination" / "sink.parquet"


def test_main_logs_that_a_legacy_plugin_is_deprecated(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
    capfd,
):
    shipping_setup(
        tmp_path,
        built_bundle,
        monkeypatch,
        http_ok_url,
    )
    assert main() == 0
    output = capfd.readouterr()
    assert (
        "LocalFileDestPlugin.write_out takes the deprecated list[TableFileSpec]"
        in output.out + output.err
    )


def test_main_tells_the_server_before_it_writes(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    shipped = shipping_setup(
        tmp_path,
        built_bundle,
        monkeypatch,
        http_ok_url,
    )
    seen: list[bool] = []
    monkeypatch.setattr(
        Responder,
        "destination_write",
        lambda _responder: seen.append(shipped.exists()),
    )
    assert main() == 0
    assert seen == [False]
    assert shipped.is_file()


def test_main_writes_nothing_when_the_server_cannot_be_told(
    tmp_path,
    built_bundle,
    monkeypatch,
    http_ok_url,
):
    shipped = shipping_setup(
        tmp_path,
        built_bundle,
        monkeypatch,
        http_ok_url,
    )

    def unreachable(_responder):
        raise OSError("apiserver unreachable")

    monkeypatch.setattr(
        Responder,
        "destination_write",
        unreachable,
    )
    assert main() != 0
    assert not shipped.exists()


# --- decorator metadata ------------------------------------------------


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
        == "td-p-dst-regular"
    )


def test_a_connection_that_cannot_be_loaded_is_reported(tmp_path):
    command = destination_plugin(tmp_path)
    with pytest.raises(BadConnectionError) as raised:
        command.load_connection()
    assert raised.value.api_error is RunDestinationPluginErrorCode.RDP_11
