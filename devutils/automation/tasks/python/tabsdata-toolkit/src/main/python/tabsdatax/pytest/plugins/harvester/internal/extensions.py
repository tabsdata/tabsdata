#
#  Copyright 2026 Tabs Data Inc.
#

"""PluginTracker — plugin registration and activity tracking.

Snapshots all registered plugins at configure time and classifies
them as builtin, conftest, or third-party.  Tracks whether a plugin
is currently "active" (i.e. a hook belonging to it is executing).
"""

from __future__ import annotations

from tabsdatax.pytest.plugins.harvester.internal.schema import PluginState
from tabsdatax.pytest.plugins.harvester.internal.writer import MetaMessage, MetaWriter


def _get_plugin_module_name(plugin: object) -> str:
    """Get the best module/package name for a plugin object.

    Plugins can be module objects (``__name__`` is the module path) or
    class instances (``__module__`` is the defining module).
    """
    # For module objects: __name__ is "xdist.plugin", "pytest_cov.plugin", etc.
    mod_name = getattr(plugin, "__name__", "") or ""
    if mod_name:
        return mod_name
    # For class instances / functions: __module__ is "xdist.dsession", etc.
    return getattr(plugin, "__module__", "") or ""


def _classify_plugin(name: str, plugin: object) -> str | None:
    """Classify a plugin.  Returns None to skip it entirely.

    - ``"builtin"``     — ships with pytest
      (module starts with ``_pytest`` or ``pytest``)
    - ``"first-party"`` — from tabsdata-toolkit
      (module starts with ``tabsdatax``)
    - ``"third-party"`` — installed package extending
      pytest (everything else that's real)
    - ``None``          — skip (conftest files, path-based entries, pluggy internals)
    """
    if not name or isinstance(name, int):
        return None
    name_str = str(name)
    # Skip numeric IDs (pluggy internal artifacts)
    if name_str.isdigit():
        return None
    # Skip internal registrations (e.g. _cov, _pytest internals)
    if name_str.startswith("_"):
        return None
    # Skip conftest files and path-based entries
    if name_str.startswith("conftest") or "/" in name_str or "\\" in name_str:
        return None
    mod = _get_plugin_module_name(plugin)
    # Skip pluggy internals and objects with no identifiable module
    if not mod or mod.startswith("pluggy"):
        return None
    # Builtin: _pytest.* or pytest (but NOT pytest_cov, pytest_html, etc.)
    mod_top = mod.split(".")[0]
    if mod_top in ("_pytest", "pytest"):
        return "builtin"
    if mod.startswith("tabsdatax"):
        return "first-party"
    return "third-party"


class PluginTracker:
    """Tracks registered pytest plugins classified as builtin, first-party,
    or third-party."""

    def __init__(self, writer: MetaWriter) -> None:
        self._writer = writer
        self._known: set[str] = set()

    def snapshot(self, pluginmanager) -> None:
        """Snapshot all real plugins registered with pytest."""
        for name, plugin in pluginmanager.list_name_plugin():
            ptype = _classify_plugin(name, plugin)
            if ptype is None:
                continue
            display_name = str(name)
            if len(display_name) > 40:
                display_name = display_name[:37] + "..."
            if display_name in self._known:
                continue
            self._known.add(display_name)
            self._writer.enqueue(
                MetaMessage(
                    kind="plugin",
                    key=display_name,
                    payload=PluginState(
                        plugin_type=ptype,
                        active=False,
                    ).to_dict(),
                )
            )

    def set_active(self, name: str, active: bool) -> None:
        """Mark a plugin as active or inactive."""
        if name not in self._known:
            return
        self._writer.enqueue(
            MetaMessage(
                kind="plugin",
                key=name,
                payload={"active": active},
            )
        )
