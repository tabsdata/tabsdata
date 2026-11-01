#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import urllib.error
from pathlib import Path
from types import SimpleNamespace

import pytest

# Imported at module level so string-form annotations on locally-built
# @stream_publisher fixtures resolve via `get_type_hints`.
# Module-level so the bundle serialiser can resolve ``module:qualname``
# on the decorated function.
from tabsdatak.api import (  # noqa: F401, E402
    TableFrameSpec,
    TableFramesSpec,
    stream_publisher as _stream_publisher_decorator,  # noqa: E402
)
from tabsdatak.conn.kafka import (  # noqa: E402
    KafkaStreamSrc as _KafkaStreamSrc,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.run import shutdown
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    FunctionBundle,
)
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin import (
    runner,
)
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin.error import (  # noqa: E501
    BadBundleError,
    BadConnectionError,
    BadRequestError,
    NoPluginError,
    RunStreamSourcePluginErrorCode,
    UnsupportedStageSchemeError,
    WrongKindError,
)
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin.options import (  # noqa: E501
    RunStreamSourcePluginArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin.runner import (  # noqa: E501
    DEFINITION_ERRORS,
    KINDS,
    StreamSourcePlugin,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


@_stream_publisher_decorator(
    source=_KafkaStreamSrc(data_format="json", schema="{}", topic="t"),
    batch_max_secs=60,
    output_tables=["t"],
)
def _module_level_stream_pub(t: TableFramesSpec) -> TableFrameSpec:
    return None


def stream_source_plugin(tmp_path: Path, **state) -> StreamSourcePlugin:
    command = StreamSourcePlugin(
        RunStreamSourcePluginArguments(
            request=tmp_path / "request.yaml",
            bundle_folder=tmp_path / "bundle",
            output_folder=tmp_path / "output",
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


def _load_function_def(bundle_dir: Path):
    return FunctionBundle(bundle_dir).definition(KINDS, DEFINITION_ERRORS)


def _write_function_def(bundle_dir: Path, info) -> None:
    from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME

    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / BUNDLE_MANIFEST_FILENAME).write_text(json.dumps(info.to_dict()))


# --- function definition -----------------------------------------------


def test_load_function_def_accepts_stream_publisher(tmp_path):
    """The streamer pod's loader accepts kind=streamPublisherDef."""
    from tabsdatak._info import KIND_STREAM_PUBLISHER_DEF, StreamPublisherInfo

    bundle_dir = tmp_path / "bundle"
    _write_function_def(bundle_dir, _module_level_stream_pub)
    info = _load_function_def(bundle_dir)
    assert isinstance(info, StreamPublisherInfo)
    assert info.__kind__ == KIND_STREAM_PUBLISHER_DEF


def test_load_function_def_rejects_publisher_kind(tmp_path):
    """The streamer pod is meaningful only for stream publishers; a
    regular @publisher's function_def.json surfaces as WrongKindError
    (RSSP_4)."""
    from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME

    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    # Hand-craft a kind=publisherDef payload -- enough to trigger the
    # kind check; the deeper schema isn't reached.
    (bundle_dir / BUNDLE_MANIFEST_FILENAME).write_text(
        json.dumps({"kind": "publisherDef"})
    )
    with pytest.raises(WrongKindError):
        _load_function_def(bundle_dir)


def test_load_function_def_missing_bundle_raises_bad_bundle(tmp_path):
    with pytest.raises(BadBundleError):
        _load_function_def(tmp_path / "does-not-exist")


# --- resolve_plugin --------------------------------------------------


def test_resolve_definition_returns_the_registered_stream_source(tmp_path):
    from tabsdatak.conn.kafka._stream_plugin import KafkaStreamSrcPlugin

    command = stream_source_plugin(tmp_path, publisher=_module_level_stream_pub)
    definition = command.resolve_definition()
    assert definition.plugin is KafkaStreamSrcPlugin
    assert definition.cardinality(_module_level_stream_pub.source) == 1


def test_resolve_definition_unknown_stream_src_raises_no_plugin(tmp_path, monkeypatch):
    """If ``get_stream_src`` raises KeyError, we surface NoPluginError
    (RSSP_5) with a clear src_type message."""

    def _raise(_cls):
        raise KeyError("no stream source registered")

    monkeypatch.setattr(runner, "get_stream_src", _raise)
    command = stream_source_plugin(tmp_path, publisher=_module_level_stream_pub)
    with pytest.raises(NoPluginError):
        command.resolve_definition()


def test_a_stage_no_store_supports_is_refused(tmp_path):
    request = SimpleNamespace(
        info=SimpleNamespace(function_stage=Location(uri="ftp://bucket/stage"))
    )
    with pytest.raises(UnsupportedStageSchemeError):
        stream_source_plugin(tmp_path, request=request).open_stage()


# --- tabsdata_version sourcing ----------------------------------------


def test_tabsdata_version_is_imported_in_command_module():
    """NU6 sources `tabsdata_version` for the state envelope (per Q4)
    from ``tabsdatak.extensions.variant.version.version``. Guard the
    import name stays stable so reviewers don't silently lose the
    plumbing on a refactor."""
    assert callable(runner.tabsdata_version)
    # Returns a non-empty string in test envs where the package is
    # installed; in containers without the version metadata the
    # function returns NO_VERSION (also a string).
    v = runner.tabsdata_version()
    assert isinstance(v, str)
    assert v != ""


# --- error code wiring (typed exit codes) -----------------------------


def test_rssp_1_resolves_to_bad_request():
    exc = RunStreamSourcePluginErrorCode.RSSP_1.exception(path="/r.yaml", detail="boom")

    assert isinstance(exc, BadRequestError)
    assert exc.exit_code == BadRequestError.exit_code


def test_rssp_4_resolves_to_wrong_kind():
    exc = RunStreamSourcePluginErrorCode.RSSP_4.exception(
        kind="publisherDef", manifest="/m.json", allowed=["streamPublisherDef"]
    )
    assert isinstance(exc, WrongKindError)
    assert exc.exit_code == WrongKindError.exit_code


def test_rssp_5_resolves_to_no_plugin():
    exc = RunStreamSourcePluginErrorCode.RSSP_5.exception(
        src_type="MySrc", detail="oops"
    )
    assert isinstance(exc, NoPluginError)


def test_rssp_7_resolves_to_unsupported_stage_scheme():
    exc = RunStreamSourcePluginErrorCode.RSSP_7.exception(uri="s3://b", scheme="s3")
    assert isinstance(exc, UnsupportedStageSchemeError)
    assert exc.exit_code == UnsupportedStageSchemeError.exit_code
    assert "s3" in str(exc)


# --- report_completion -----------------------------------------------


def failing_done(error: BaseException):
    def done():
        raise error

    return done


@pytest.fixture
def shutdown_flag(monkeypatch):
    monkeypatch.setattr(shutdown, "requested", False)


def test_completion_lost_to_a_stopped_apiserver_is_tolerated_on_shutdown(
    tmp_path, monkeypatch, shutdown_flag
):
    shutdown.request_shutdown()
    command = stream_source_plugin(tmp_path)
    monkeypatch.setattr(
        command.responder,
        "done",
        failing_done(urllib.error.URLError("connection refused")),
    )

    assert command.report_completion() is None


def test_completion_failure_outside_shutdown_propagates(
    tmp_path, monkeypatch, shutdown_flag
):
    error = urllib.error.URLError("connection refused")
    command = stream_source_plugin(tmp_path)
    monkeypatch.setattr(command.responder, "done", failing_done(error))

    with pytest.raises(urllib.error.URLError) as raised:
        command.report_completion()
    assert raised.value is error


def test_completion_rejected_on_shutdown_propagates(
    tmp_path, monkeypatch, shutdown_flag
):
    shutdown.request_shutdown()
    error = urllib.error.HTTPError("http://r", 403, "Forbidden", {}, None)
    command = stream_source_plugin(tmp_path)
    monkeypatch.setattr(command.responder, "done", failing_done(error))

    with pytest.raises(urllib.error.HTTPError):
        command.report_completion()


def test_a_shutdown_turns_the_context_inactive(tmp_path, shutdown_flag):
    context = SimpleNamespace(running=True)
    context.deactivate = lambda: setattr(context, "running", False)
    stream_source_plugin(tmp_path, context=context).shutdown(15, None)
    assert context.running is False
    assert shutdown.requested is True


def test_a_connection_that_cannot_be_loaded_is_reported(tmp_path):
    command = stream_source_plugin(tmp_path)
    with pytest.raises(BadConnectionError) as raised:
        command.load_connection()
    assert raised.value.api_error is RunStreamSourcePluginErrorCode.RSSP_9


class SendingPlugin:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def run(self, ctx, conn, src) -> None:
        ctx.pending = [["0.0.t", "0.1.t"]]
        if self.error is not None:
            raise self.error


def streaming(tmp_path: Path, plugin: SendingPlugin) -> StreamSourcePlugin:
    context = SimpleNamespace(pending=[[]], staging=SimpleNamespace(uri="file:///s"))
    context.unstage = lambda: setattr(context, "pending", [[]])
    publisher = SimpleNamespace(
        func=_module_level_stream_pub,
        source=None,
        thresholds=None,
        flush_interval_mins=None,
    )
    return stream_source_plugin(
        tmp_path,
        context=context,
        plugin=plugin,
        publisher=publisher,
        connection=None,
    )


def test_what_the_plugin_left_unpublished_is_unstaged(tmp_path):
    command = streaming(tmp_path, SendingPlugin())
    command.stream()
    assert command.context.pending == [[]]


def test_a_failed_plugin_still_unstages_what_it_sent(tmp_path):
    command = streaming(tmp_path, SendingPlugin(RuntimeError("broker gone")))
    with pytest.raises(Exception) as raised:
        command.stream()
    assert raised.value.api_error is RunStreamSourcePluginErrorCode.RSSP_6
    assert command.context.pending == [[]]


def test_a_failed_unstage_is_only_a_warning(tmp_path, caplog):
    command = streaming(tmp_path, SendingPlugin())

    def unreachable() -> None:
        raise PermissionError("stage not mounted")

    command.context.unstage = unreachable
    command.stream()
    assert "Cannot unstage" in caplog.text
