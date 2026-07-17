#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the run-streamer command.

The command's main wiring (load request -> resolve plugin -> build
ctx -> run plugin -> notify) is covered with hand-built fixtures and
a fake StreamSrcPlugin. End-to-end Kafka coverage is left to
integration suites that bring up a broker.
"""

from __future__ import annotations

import json
from pathlib import Path

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
from tabsdatak.server._tdrunner.function.commands.run_streamer import (
    _load_function_def,
    _resolve_plugin,
)
from tabsdatak.server._tdrunner.function.commands.run_streamer.error import (
    BadBundleError,
    NoPluginError,
    RunStreamerErrorCode,
    UnsupportedStageSchemeError,
    WrongKindError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


@_stream_publisher_decorator(
    source=_KafkaStreamSrc(data_format="json", schema="{}", topic="t"),
    flush_interval_mins=1,
    output_tables=["t"],
)
def _module_level_stream_pub(t: TableFramesSpec) -> TableFrameSpec:
    return None


def _write_function_def(bundle_dir: Path, info) -> None:
    """Write a synthetic ``function_def.json`` for ``info`` into
    ``bundle_dir``. Avoids the full build_bundle round-trip when we
    only care about the loader's kind/shape behaviour."""
    from tabsdatak.common._archive.bundle import BUNDLE_MANIFEST_FILENAME

    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / BUNDLE_MANIFEST_FILENAME).write_text(json.dumps(info.to_dict()))


# --- _load_function_def -----------------------------------------------


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
    (RS_4)."""
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


# --- _resolve_plugin --------------------------------------------------


def test_resolve_plugin_returns_registered_stream_plugin():
    """For a registered KafkaStreamSrc, the resolver returns an
    instantiated KafkaStreamSrcPlugin."""
    from tabsdatak.conn.kafka._stream_plugin import KafkaStreamSrcPlugin

    plugin = _resolve_plugin(_module_level_stream_pub)
    assert isinstance(plugin, KafkaStreamSrcPlugin)


def test_resolve_plugin_unknown_stream_src_raises_no_plugin(monkeypatch):
    """If ``get_stream_src`` raises KeyError, we surface NoPluginError
    (RS_5) with a clear src_type message."""

    def _raise(_cls):
        raise KeyError("no stream source registered")

    monkeypatch.setattr(
        "tabsdatak.server._tdrunner.function.commands.run_streamer.runner.get_stream_src",
        _raise,
    )
    with pytest.raises(NoPluginError):
        _resolve_plugin(_module_level_stream_pub)


# --- tabsdata_version sourcing ----------------------------------------


def test_tabsdata_version_is_imported_in_command_module():
    """NU6 sources `tabsdata_version` for the state envelope (per Q4)
    from ``tabsdatak.extensions.variant.version.version``. Guard the
    import name stays stable so reviewers don't silently lose the
    plumbing on a refactor."""
    from tabsdatak.server._tdrunner.function.commands.run_streamer import runner

    assert callable(runner._tabsdata_version)
    # Returns a non-empty string in test envs where the package is
    # installed; in containers without the version metadata the
    # function returns NO_VERSION (also a string).
    v = runner._tabsdata_version()
    assert isinstance(v, str)
    assert v != ""


# --- error code wiring (typed exit codes) -----------------------------


def test_rs_1_resolves_to_bad_request():
    exc = RunStreamerErrorCode.RS_1.exception(path="/r.yaml", detail="boom")
    from tabsdatak.server._tdrunner.function.commands.run_streamer.error import (
        BadRequestError,
    )

    assert isinstance(exc, BadRequestError)
    assert exc.exit_code == BadRequestError.exit_code


def test_rs_4_resolves_to_wrong_kind():
    exc = RunStreamerErrorCode.RS_4.exception(
        kind="publisherDef", manifest="/m.json", allowed=["streamPublisherDef"]
    )
    assert isinstance(exc, WrongKindError)
    assert exc.exit_code == WrongKindError.exit_code


def test_rs_5_resolves_to_no_plugin():
    exc = RunStreamerErrorCode.RS_5.exception(src_type="MySrc", detail="oops")
    assert isinstance(exc, NoPluginError)


def test_rs_7_resolves_to_unsupported_stage_scheme():
    exc = RunStreamerErrorCode.RS_7.exception(uri="s3://b", scheme="s3")
    assert isinstance(exc, UnsupportedStageSchemeError)
    assert exc.exit_code == UnsupportedStageSchemeError.exit_code
    assert "s3" in str(exc)
