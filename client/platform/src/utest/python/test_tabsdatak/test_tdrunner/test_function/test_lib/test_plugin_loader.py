#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import cloudpickle
import pytest

from tabsdatak._tdrunner.function._lib.plugin_loader import (
    DESTINATION_PLUGIN_FILENAME,
    PLUGINS_DIRNAME,
    SOURCE_PLUGIN_FILENAME,
    PluginNotFoundError,
    PluginProtocolError,
    load_destination_plugin,
    load_source_plugin,
)
from tabsdatak._tdrunner.function._lib.plugin_protocol import (
    DestinationPlugin,
    SourcePlugin,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- fakes ---


class _RunnableFake:
    """A class with a callable `_run` — satisfies both protocols."""

    def __init__(self, tag: str = "ok"):
        self.tag = tag

    def _run(self, *args, **kwargs):
        return self.tag


class _NoRunFake:
    """A class without `_run` — should fail the protocol check."""

    def hello(self):
        return "world"


def _drop_plugin(bundle_dir: Path, filename: str, obj: object) -> Path:
    plugins = bundle_dir / PLUGINS_DIRNAME
    plugins.mkdir(parents=True, exist_ok=True)
    target = plugins / filename
    with open(target, "wb") as fh:
        cloudpickle.dump(obj, fh)
    return target


# --- happy path ---


def test_load_source_plugin(tmp_path):
    _drop_plugin(tmp_path, SOURCE_PLUGIN_FILENAME, _RunnableFake("source"))
    plugin = load_source_plugin(tmp_path)
    assert isinstance(plugin, SourcePlugin)
    assert plugin._run() == "source"


def test_load_destination_plugin(tmp_path):
    _drop_plugin(tmp_path, DESTINATION_PLUGIN_FILENAME, _RunnableFake("destination"))
    plugin = load_destination_plugin(tmp_path)
    assert isinstance(plugin, DestinationPlugin)
    assert plugin._run() == "destination"


# --- missing-file errors ---


def test_load_source_missing_raises(tmp_path):
    with pytest.raises(PluginNotFoundError, match="source.pickle"):
        load_source_plugin(tmp_path)


def test_load_destination_missing_raises(tmp_path):
    with pytest.raises(PluginNotFoundError, match="destination.pickle"):
        load_destination_plugin(tmp_path)


def test_plugin_not_found_inherits_filenotfound(tmp_path):
    """Catchable as the stdlib FileNotFoundError."""
    try:
        load_source_plugin(tmp_path)
    except FileNotFoundError as exc:
        assert isinstance(exc, PluginNotFoundError)
    else:
        pytest.fail("expected PluginNotFoundError")


# --- protocol mismatch ---


def test_load_source_protocol_mismatch(tmp_path):
    _drop_plugin(tmp_path, SOURCE_PLUGIN_FILENAME, _NoRunFake())
    with pytest.raises(PluginProtocolError, match="_run"):
        load_source_plugin(tmp_path)


def test_load_destination_protocol_mismatch(tmp_path):
    _drop_plugin(tmp_path, DESTINATION_PLUGIN_FILENAME, _NoRunFake())
    with pytest.raises(PluginProtocolError, match="_run"):
        load_destination_plugin(tmp_path)


def test_load_plain_dict_fails_protocol_check(tmp_path):
    _drop_plugin(tmp_path, SOURCE_PLUGIN_FILENAME, {"some": "data"})
    with pytest.raises(PluginProtocolError):
        load_source_plugin(tmp_path)


def test_plugin_protocol_error_inherits_typeerror(tmp_path):
    _drop_plugin(tmp_path, SOURCE_PLUGIN_FILENAME, _NoRunFake())
    try:
        load_source_plugin(tmp_path)
    except TypeError as exc:
        assert isinstance(exc, PluginProtocolError)
    else:
        pytest.fail("expected PluginProtocolError")


# --- protocol shape ---


def test_runnable_satisfies_both_protocols():
    obj = _RunnableFake()
    assert isinstance(obj, SourcePlugin)
    assert isinstance(obj, DestinationPlugin)


def test_norun_satisfies_neither_protocol():
    obj = _NoRunFake()
    assert not isinstance(obj, SourcePlugin)
    assert not isinstance(obj, DestinationPlugin)
