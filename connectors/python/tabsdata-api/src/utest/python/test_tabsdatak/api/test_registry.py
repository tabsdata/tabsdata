#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the connector registry *machinery* in
:mod:`tabsdatak._connectors`.

Per-connector registration (does *this* connector publish a
well-formed ``SrcDef`` / ``DestDef`` / ``StreamSrcDef``?) lives next
to each connector as ``test_registration.py``. This module covers
only the registry engine itself -- discovery typing, snapshot
isolation, class->def resolution, and reload behaviour -- using
whatever connectors happen to be installed in the current
environment. ``localfile`` ships with the api package's reference
install and is therefore always present.
"""

from __future__ import annotations

import pytest

from tabsdatak._connectors import (
    avail_dests,
    avail_srcs,
    avail_stream_srcs,
    def_for,
    explorer_for,
    explorer_for_conn,
    get_dest,
    get_src,
    reload_connectors,
)
from tabsdatak.api import Dest, Src
from tabsdatak.spi import (
    Conn,
    DestDef,
    DestPlugin,
    Explorer,
    SrcDef,
    SrcPlugin,
    StreamSrcDef,
    TableMode,
)


def test_src_entries_are_well_typed():
    for src_type, defx in avail_srcs().items():
        assert isinstance(defx, SrcDef), src_type
        assert defx.type_ == src_type
        assert defx.system, src_type
        assert issubclass(defx.src, Src), src_type
        assert isinstance(defx.table_mode, TableMode), src_type
        assert callable(defx.cardinality), src_type
        assert issubclass(defx.plugin, SrcPlugin), src_type
        assert defx.explorer is None or issubclass(defx.explorer, Explorer)


def test_dest_entries_are_well_typed():
    for dest_type, defx in avail_dests().items():
        assert isinstance(defx, DestDef), dest_type
        assert defx.type_ == dest_type
        assert defx.system, dest_type
        assert issubclass(defx.dest, Dest), dest_type
        assert callable(defx.cardinality), dest_type
        assert issubclass(defx.plugin, DestPlugin), dest_type
        assert defx.explorer is None or issubclass(defx.explorer, Explorer)


def test_stream_src_entries_are_well_typed():
    for src_type, defx in avail_stream_srcs().items():
        assert isinstance(defx, StreamSrcDef), src_type


def test_avail_returns_a_fresh_dict():
    # Mutating the returned dict must not poison the cached registry.
    snapshot = avail_srcs()
    snapshot["bogus"] = None  # type: ignore[assignment]
    assert "bogus" not in avail_srcs()


# ---------------------------------------------------------------------------
# get_src / get_dest by class
# ---------------------------------------------------------------------------


def test_get_src_resolves_class_to_defx():
    # ``localfile`` is part of the api package's reference install and
    # is always registered, so class->def resolution is exercised in
    # every environment.
    from tabsdatak.conn.localfile import LocalFileSrc

    defx = get_src(LocalFileSrc)
    assert defx.src is LocalFileSrc
    assert defx.type_ == "local-file-in"


def test_get_dest_resolves_class_to_defx():
    from tabsdatak.conn.localfile import LocalFileDest

    defx = get_dest(LocalFileDest)
    assert defx.dest is LocalFileDest
    assert defx.type_ == "local-file-out"


def test_get_src_unregistered_class_raises_keyerror():
    class NotRegistered(Src):
        pass

    with pytest.raises(KeyError):
        get_src(NotRegistered)


# ---------------------------------------------------------------------------
# explorer_for / def_for / explorer_for_conn resolvers
# ---------------------------------------------------------------------------


def test_explorer_for_known_type_returns_explorer():
    from tabsdatak.conn.localfile._plugin import LocalFileSrcExplorer

    assert explorer_for("local-file-in") is LocalFileSrcExplorer


def test_explorer_for_unknown_type_returns_none():
    assert explorer_for("no-such-type") is None


def test_def_for_known_type_returns_def():
    defx = def_for("local-file-in")
    assert defx is not None
    assert defx.type_ == "local-file-in"


def test_def_for_unknown_type_returns_none():
    assert def_for("no-such-type") is None


def test_explorer_for_conn_resolves_by_conn_class():
    from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileSrcConn
    from tabsdatak.conn.localfile._plugin import (
        LocalFileDestExplorer,
        LocalFileSrcExplorer,
    )

    assert explorer_for_conn(LocalFileSrcConn) is LocalFileSrcExplorer
    assert explorer_for_conn(LocalFileDestConn) is LocalFileDestExplorer


def test_explorer_for_conn_accepts_an_instance():
    # Resolution keys on the class, so even an uninitialized instance resolves.
    from tabsdatak.conn.localfile import LocalFileSrcConn
    from tabsdatak.conn.localfile._plugin import LocalFileSrcExplorer

    inst = LocalFileSrcConn.__new__(LocalFileSrcConn)
    assert explorer_for_conn(inst) is LocalFileSrcExplorer


def test_explorer_for_conn_unregistered_returns_none():
    class NotRegistered(Conn):
        pass

    assert explorer_for_conn(NotRegistered) is None


# ---------------------------------------------------------------------------
# Reload behaviour.
# ---------------------------------------------------------------------------


def test_reload_rebuilds_registry():
    # reload_connectors discards the cached registries and rebuilds.
    # Nothing previously discovered may be lost, and the always-present
    # reference connector must survive.
    before_srcs = set(avail_srcs())
    before_dests = set(avail_dests())
    reload_connectors()
    assert before_srcs <= set(avail_srcs())
    assert before_dests <= set(avail_dests())
    assert "local-file-in" in avail_srcs()
    assert "local-file-out" in avail_dests()
