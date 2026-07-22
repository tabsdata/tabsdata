#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common import _config


# ---------------------------------------------------------------------------
# remove_tabsdata_cfgs
# ---------------------------------------------------------------------------


def test_remove_tabsdata_cfgs_passes_through_none():
    assert _config.remove_tabsdata_cfgs(None) is None


def test_remove_tabsdata_cfgs_empty_dict_returns_empty_dict():
    assert _config.remove_tabsdata_cfgs({}) == {}


def test_remove_tabsdata_cfgs_strips_prefixed_keys():
    cfgs = {
        "tabsdata.debug": True,
        "tabsdata.trace_id": "abc",
        "sqlalchemy.connect_args": {"timeout": 5},
        "mysql.charset": "utf8mb4",
    }
    assert _config.remove_tabsdata_cfgs(cfgs) == {
        "sqlalchemy.connect_args": {"timeout": 5},
        "mysql.charset": "utf8mb4",
    }


def test_remove_tabsdata_cfgs_keeps_dict_when_no_tabsdata_keys():
    cfgs = {"sqlalchemy.connect_args": {"timeout": 5}}
    assert _config.remove_tabsdata_cfgs(cfgs) == cfgs


def test_remove_tabsdata_cfgs_returns_empty_when_only_tabsdata_keys():
    cfgs = {"tabsdata.a": 1, "tabsdata.b": 2}
    assert _config.remove_tabsdata_cfgs(cfgs) == {}


def test_remove_tabsdata_cfgs_does_not_mutate_input():
    cfgs = {"tabsdata.debug": True, "sqlalchemy.x": 1}
    out = _config.remove_tabsdata_cfgs(cfgs)
    # Input untouched.
    assert cfgs == {"tabsdata.debug": True, "sqlalchemy.x": 1}
    # Output is a fresh dict.
    assert out is not cfgs


def test_remove_tabsdata_cfgs_only_strips_top_level_keys():
    # Nested dicts are payloads for whatever library owns the parent
    # key -- their inner keys are not inspected even if they happen
    # to start with ``tabsdata.``.
    cfgs = {
        "tabsdata.debug": True,  # stripped
        "sqlalchemy.connect_args": {
            "tabsdata.inner": "kept",  # nested -- kept verbatim
            "timeout": 5,
        },
    }
    out = _config.remove_tabsdata_cfgs(cfgs)
    assert out == {
        "sqlalchemy.connect_args": {
            "tabsdata.inner": "kept",
            "timeout": 5,
        },
    }


def test_remove_tabsdata_cfgs_keeps_key_that_just_contains_prefix():
    # ``my.tabsdata.foo`` is NOT prefixed by ``tabsdata.``; it's only
    # ``str.startswith`` that flags a key as reserved.
    cfgs = {"my.tabsdata.foo": 1, "tabsdata.foo": 2}
    assert _config.remove_tabsdata_cfgs(cfgs) == {"my.tabsdata.foo": 1}


def test_remove_tabsdata_cfgs_prefix_constant_matches_check():
    # The implementation uses ``TABSDATA_CFG_PREFIX``; if it ever
    # drifts from the dotted-prefix convention this test surfaces it.
    assert _config.TABSDATA_CFG_PREFIX == "tabsdata."
