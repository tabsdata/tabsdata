#
#  Copyright 2026 Tabsdata Inc.
#

"""Tests for the connector-resources extractor worker.

Covers filename derivation and ``perform()``: writing each connector's icon
plus a sibling ``.json`` metadata file (holding ``system``) directly into the
output directory (no ``icon/`` segment), skipping icon-less defs, and failing
loudly when a declared icon cannot be read.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from tabsdatak.server._tdserver.workers import conn_resources_extractor as extractor
from tabsdatak.server._tdserver.workers.conn_resources_extractor import (
    ConnResourcesExtractor,
    icon_filename,
    metadata_filename,
    resource_stem,
)
from tabsdatak.server._tdserver.workers.conn_resources_extractor.error import (
    ResourceExtractionError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- Helpers --------------------------------------------------------------


def _plugin(module: str, qualname: str) -> type:
    """A stand-in plugin class with a controlled ``<module>.<class>``."""
    cls = type(qualname, (), {})
    cls.__module__ = module
    cls.__qualname__ = qualname
    return cls


def _src_def(plugin: type, icon: str | None, system: str) -> SimpleNamespace:
    # `plugin_class` reads `.src` first, then `.dest`; a src def exposes `.src`.
    return SimpleNamespace(src=plugin, icon=icon, system=system)


def _dest_def(plugin: type, icon: str | None, system: str) -> SimpleNamespace:
    # A dest def has no `.src`, so `plugin_class` falls through to `.dest`.
    return SimpleNamespace(dest=plugin, icon=icon, system=system)


# --- Filename derivation --------------------------------------------------


def test_resource_stem_turns_dots_into_dashes():
    plugin = _plugin("tabsdatak.conn.mysql", "MySQLSrc")
    assert resource_stem(plugin) == "tabsdatak-conn-mysql-MySQLSrc"


def test_icon_and_metadata_filenames_share_stem_and_differ_by_suffix():
    plugin = _plugin("tabsdatak.conn.mysql", "MySQLSrc")
    assert icon_filename(plugin) == "tabsdatak-conn-mysql-MySQLSrc.svg"
    assert metadata_filename(plugin) == "tabsdatak-conn-mysql-MySQLSrc.json"


# --- perform() ------------------------------------------------------------


def test_perform_writes_icon_and_metadata_directly_in_output_dir(
    tmp_path, monkeypatch
):
    source = tmp_path / "source" / "mysql.svg"
    source.parent.mkdir(parents=True)
    source.write_text("<svg>mysql</svg>")

    plugin = _plugin("tabsdatak.conn.mysql", "MySQLSrc")
    definition = _src_def(
        plugin, icon="conn/mysql/resources/mysql.svg", system="MySQL"
    )
    monkeypatch.setattr(extractor, "available_definitions", lambda: [definition])
    monkeypatch.setattr(extractor, "td_resource", lambda icon: source)

    out = tmp_path / "connectors"
    worker = ConnResourcesExtractor(output_dir=out)
    worker.perform()

    svg = out / "tabsdatak-conn-mysql-MySQLSrc.svg"
    meta = out / "tabsdatak-conn-mysql-MySQLSrc.json"
    # Both files land directly under output_dir -- the old `icon/` segment is gone.
    assert not (out / "icon").exists()
    assert svg.read_text() == "<svg>mysql</svg>"
    assert json.loads(meta.read_text()) == {"system": "MySQL"}
    assert worker.copied == 1
    assert worker.skipped == 0
    assert worker.failures == []


def test_perform_handles_both_src_and_dest_defs(tmp_path, monkeypatch):
    source = tmp_path / "mysql.svg"
    source.write_text("<svg>mysql</svg>")

    src_plugin = _plugin("tabsdatak.conn.mysql", "MySQLSrc")
    dest_plugin = _plugin("tabsdatak.conn.mysql", "MySQLDest")
    definitions = [
        _src_def(src_plugin, "conn/mysql/resources/mysql.svg", "MySQL"),
        _dest_def(dest_plugin, "conn/mysql/resources/mysql.svg", "MySQL"),
    ]
    monkeypatch.setattr(extractor, "available_definitions", lambda: definitions)
    monkeypatch.setattr(extractor, "td_resource", lambda icon: source)

    out = tmp_path / "connectors"
    worker = ConnResourcesExtractor(output_dir=out)
    worker.perform()

    for stem in ("tabsdatak-conn-mysql-MySQLSrc", "tabsdatak-conn-mysql-MySQLDest"):
        assert (out / f"{stem}.svg").read_text() == "<svg>mysql</svg>"
        assert json.loads((out / f"{stem}.json").read_text()) == {"system": "MySQL"}
    assert worker.copied == 2
    assert worker.skipped == 0


def test_perform_skips_defs_without_icon(tmp_path, monkeypatch):
    plugin = _plugin("tabsdatak.conn.noicon", "NoIconSrc")
    definition = _src_def(plugin, icon=None, system="NoIcon")
    monkeypatch.setattr(extractor, "available_definitions", lambda: [definition])

    def _never(icon):  # td_resource must not be consulted for icon-less defs
        raise AssertionError("td_resource should not be called when icon is None")

    monkeypatch.setattr(extractor, "td_resource", _never)

    out = tmp_path / "connectors"
    worker = ConnResourcesExtractor(output_dir=out)
    worker.perform()

    assert worker.skipped == 1
    assert worker.copied == 0
    assert worker.failures == []
    # The directory is created but nothing is written for the skipped def.
    assert list(out.iterdir()) == []


def test_perform_records_failure_and_raises_when_icon_missing(tmp_path, monkeypatch):
    plugin = _plugin("tabsdatak.conn.broken", "BrokenSrc")
    definition = _src_def(
        plugin, icon="conn/broken/resources/missing.svg", system="Broken"
    )
    monkeypatch.setattr(extractor, "available_definitions", lambda: [definition])

    def _missing(icon):
        raise FileNotFoundError(icon)

    monkeypatch.setattr(extractor, "td_resource", _missing)

    out = tmp_path / "connectors"
    worker = ConnResourcesExtractor(output_dir=out)
    with pytest.raises(ResourceExtractionError):
        worker.perform()

    assert worker.copied == 0
    assert len(worker.failures) == 1
    assert "tabsdatak.conn.broken.BrokenSrc" in worker.failures[0]
    # Neither file is written for a connector whose icon could not be read.
    assert not (out / "tabsdatak-conn-broken-BrokenSrc.svg").exists()
    assert not (out / "tabsdatak-conn-broken-BrokenSrc.json").exists()
