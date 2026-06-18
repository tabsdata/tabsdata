#
# Copyright 2026 Tabs Data Inc.
#

"""Registration contract for this connector.

Every ``tabsdatak.connectors`` entry point declared in this
connector's own ``pyproject.toml`` must resolve to a well-formed
registry def (:class:`~tabsdatak.spi.SrcDef` /
:class:`~tabsdatak.spi.DestDef` / :class:`~tabsdatak.spi.StreamSrcDef`)
carrying the metadata the registry relies on.

The check reads the connector's own ``pyproject.toml`` and imports
each declared target directly, so it validates the registration
wiring regardless of whether the distribution is pip-installed in the
current environment -- unlike a registry-discovery test, which only
sees connectors that happen to be installed.
"""

from __future__ import annotations

import importlib
import tomllib
from pathlib import Path

import pytest

from tabsdatak.api import Dest, Src, StreamSrc
from tabsdatak.spi import (
    DestDef,
    DestPlugin,
    Explorer,
    SrcDef,
    SrcPlugin,
    StreamSrcDef,
    StreamSrcPlugin,
    TableMode,
)

_GROUP = "tabsdatak.connectors"


def _declared_entry_points() -> dict[str, str]:
    """``name -> "module:attr"`` for this connector's
    ``tabsdatak.connectors`` entry points, read from the nearest
    ``pyproject.toml`` above this test file."""
    for parent in Path(__file__).resolve().parents:
        pyproject = parent / "pyproject.toml"
        if pyproject.exists():
            data = tomllib.loads(pyproject.read_text())
            return (
                data.get("project", {})
                .get("entry-points", {})
                .get(_GROUP, {})
            )
    raise AssertionError("could not locate the connector's pyproject.toml")


def _load(value: str) -> object:
    module, _, attr = value.partition(":")
    return getattr(importlib.import_module(module), attr)


_ENTRY_POINTS = _declared_entry_points()


def test_declares_connector_entry_points() -> None:
    assert _ENTRY_POINTS, (
        "connector declares no 'tabsdatak.connectors' entry points in "
        "pyproject.toml"
    )


@pytest.mark.parametrize("name", sorted(_ENTRY_POINTS))
def test_registration(name: str) -> None:
    defx = _load(_ENTRY_POINTS[name])

    if isinstance(defx, SrcDef):
        assert defx.src_type, name
        assert issubclass(defx.src, Src), name
        assert isinstance(defx.table_mode, TableMode), name
        assert callable(defx.cardinality), name
        assert issubclass(defx.plugin, SrcPlugin), name
        assert defx.explorer is None or issubclass(defx.explorer, Explorer), name
    elif isinstance(defx, DestDef):
        assert defx.dest_type, name
        assert issubclass(defx.dest, Dest), name
        assert callable(defx.cardinality), name
        assert issubclass(defx.plugin, DestPlugin), name
        assert defx.explorer is None or issubclass(defx.explorer, Explorer), name
    elif isinstance(defx, StreamSrcDef):
        assert defx.src_type, name
        assert issubclass(defx.src, StreamSrc), name
        assert callable(defx.cardinality), name
        assert issubclass(defx.plugin, StreamSrcPlugin), name
        assert defx.explorer is None or issubclass(defx.explorer, Explorer), name
    else:
        pytest.fail(
            f"entry point {name!r} resolved to {type(defx).__name__}; "
            f"expected SrcDef / DestDef / StreamSrcDef"
        )
