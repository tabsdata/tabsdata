#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems.postgrescdc.backend import Postgrescdc


def _server(setting: str | None, *, after_reload: str | None = None):
    """A Postgrescdc whose psql calls answer as a server whose
    ``output_plugin_libraries`` is `setting` (``None``: no such setting)."""
    system = Postgrescdc()
    system.statements = []

    def psql(container, sql):
        system.statements.append(sql)
        if sql.startswith("select current_setting"):
            return "f|" if setting is None else f"t|{setting}"
        if sql.startswith("show"):
            return after_reload
        return ""

    system._psql = psql
    return system


def test_a_server_without_the_setting_is_left_alone():
    system = _server(None)
    system._allow_wal2json("C")
    assert len(system.statements) == 1


def test_a_server_that_already_allows_wal2json_is_left_alone():
    system = _server("pgoutput, test_decoding, wal2json")
    system._allow_wal2json("C")
    assert len(system.statements) == 1


def test_wal2json_is_added_to_what_the_server_allows():
    system = _server(
        "pgoutput, test_decoding", after_reload="pgoutput, test_decoding, wal2json"
    )
    system._allow_wal2json("C")
    alter, reload, _ = system.statements[1:]
    assert alter == (
        "alter system set output_plugin_libraries = "
        "'pgoutput', 'test_decoding', 'wal2json'"
    )
    assert reload == "select pg_reload_conf()"


def test_wal2json_still_not_allowed_after_the_reload_fails():
    system = _server("pgoutput, test_decoding", after_reload="pgoutput, test_decoding")
    with pytest.raises(TdxError, match="still not allowed"):
        system._allow_wal2json("C")


def test_a_domain_of_an_existing_system_is_allowed_too(monkeypatch):
    system = _server("pgoutput")
    allowed = []
    monkeypatch.setattr(system, "_allow_wal2json", allowed.append)
    monkeypatch.setattr(
        Postgrescdc.__mro__[1], "provision_domain", lambda *args, **kwargs: None
    )
    system.provision_domain({"CONTAINER": "tdx2_postgrescdc"}, "d1", {})
    assert allowed == ["tdx2_postgrescdc"]
