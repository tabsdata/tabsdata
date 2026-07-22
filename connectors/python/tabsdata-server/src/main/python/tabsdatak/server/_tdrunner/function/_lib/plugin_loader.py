#
# Copyright 2026 Tabsdata Inc.
#

"""Loader for source / destination plugins from a function bundle.

Plugins are cloudpickled blobs the bundle producer drops at:

    <bundle_dir>/plugins/source.pickle
    <bundle_dir>/plugins/destination.pickle

This is a contract owned by the new T2 architecture (the legacy
`_tabsserver` runner read the filename from a `function_config.input/
output` mapping; here we use fixed names since our `Request` doesn't
carry plugin metadata, and we don't import tabsdata to read its config).
"""

from __future__ import annotations

from pathlib import Path

import cloudpickle

from tabsdatak.server._tdrunner.function._lib.plugin_protocol import (
    DestinationPlugin,
    SourcePlugin,
)

PLUGINS_DIRNAME = "plugins"
SOURCE_PLUGIN_FILENAME = "source.pickle"
DESTINATION_PLUGIN_FILENAME = "destination.pickle"


class PluginNotFoundError(FileNotFoundError):
    """The plugin pickle file is missing from the bundle."""


class PluginProtocolError(TypeError):
    """Loaded plugin doesn't expose the expected `_run` attribute."""


def load_source_plugin(bundle_dir: Path) -> SourcePlugin:
    """Load the source plugin from
    ``<bundle_dir>/plugins/source.pickle``.

    Raises:
        PluginNotFoundError: if the pickle file does not exist.
        PluginProtocolError: if the loaded object does not satisfy
            :class:`SourcePlugin` (i.e. has no callable `_run`).
    """
    return _load(
        bundle_dir / PLUGINS_DIRNAME / SOURCE_PLUGIN_FILENAME,
        SourcePlugin,
    )


def load_destination_plugin(bundle_dir: Path) -> DestinationPlugin:
    """Load the destination plugin from
    ``<bundle_dir>/plugins/destination.pickle``.

    Raises:
        PluginNotFoundError: if the pickle file does not exist.
        PluginProtocolError: if the loaded object does not satisfy
            :class:`DestinationPlugin`.
    """
    return _load(
        bundle_dir / PLUGINS_DIRNAME / DESTINATION_PLUGIN_FILENAME,
        DestinationPlugin,
    )


def _load(plugin_path: Path, protocol: type) -> object:
    if not plugin_path.exists():
        raise PluginNotFoundError(f"plugin file not found: {plugin_path}")
    with open(plugin_path, "rb") as fh:
        plugin = cloudpickle.load(fh)
    if not isinstance(plugin, protocol):
        raise PluginProtocolError(
            f"loaded plugin {type(plugin).__name__} does not implement "
            f"{protocol.__name__} (no callable `_run`)"
        )
    return plugin
