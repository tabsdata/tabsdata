#
# Copyright 2026 Tabsdata Inc.
#

"""Registration contract for this connector.

The defs have to arrive through the ``tabsdatak.connectors`` entry-point scan:
:func:`tabsdatak._connectors._scan` demotes a failed ``ep.load()``, an object of
the wrong type and a duplicate ``type_`` to a warning and drops the connector, so
only indexing the registry by ``type_`` proves the entry point really loads.
Importing the def directly would pass with the connector absent from the
registry.

This connector registers two sources -- a SOQL query source and a report source --
and no destination.
"""

from __future__ import annotations

import importlib
import tomllib
import warnings
from pathlib import Path
from typing import cast

from tabsdatak import _connectors
from tabsdatak.conn.salesforce import SalesforceReportSrc, SalesforceSrc
from tabsdatak.spi import SrcDef

_GROUP = "tabsdatak.connectors"

_SOQL_TYPE = "salesforce-sosql-in"
_REPORT_TYPE = "salesforce-report-in"


def _declared_entry_points() -> dict[str, str]:
    """``name -> "module:attr"`` for this connector's ``tabsdatak.connectors``
    entry points, read from the nearest ``pyproject.toml`` above this file."""
    for parent in Path(__file__).resolve().parents:
        pyproject = parent / "pyproject.toml"
        if pyproject.exists():
            data = tomllib.loads(pyproject.read_text())
            return data.get("project", {}).get("entry-points", {}).get(_GROUP, {})
    raise AssertionError("could not locate the connector's pyproject.toml")


_ENTRY_POINTS = _declared_entry_points()


def test_soql_src_def():
    d = _connectors._registry()[_SOQL_TYPE]
    assert isinstance(d, SrcDef)
    sd = cast(SrcDef, d)
    assert sd.src == SalesforceSrc


def test_report_src_def():
    d = _connectors._registry()[_REPORT_TYPE]
    assert isinstance(d, SrcDef)
    sd = cast(SrcDef, d)
    assert sd.src == SalesforceReportSrc


def test_the_two_sources_are_distinct_defs():
    registry = _connectors._registry()
    assert registry[_SOQL_TYPE] is not registry[_REPORT_TYPE]


def test_registers_no_destination():
    assert not [
        type_
        for type_ in _connectors._registry()
        if type_.startswith("salesforce-") and type_.endswith("-out")
    ]


def test_declared_entry_points_are_the_registered_defs():
    assert len(_ENTRY_POINTS) == 2
    registry = _connectors._registry()
    for name, value in sorted(_ENTRY_POINTS.items()):
        module, _, attr = value.partition(":")
        defx = getattr(importlib.import_module(module), attr)
        assert registry.get(defx.type_) is defx, name


def test_scan_does_not_warn_about_this_connector():
    tokens = {*_ENTRY_POINTS, _SOQL_TYPE, _REPORT_TYPE}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        _connectors.reload_connectors()
    assert [
        str(w.message)
        for w in caught
        if any(token in str(w.message) for token in tokens)
    ] == []
