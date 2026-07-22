#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_utils as utils


# ---------------------------------------------------------------------------
# add_driver_to_uri (caller-supplied driver map)
# ---------------------------------------------------------------------------


_DRIVERS_FULL = {
    "mysql": "mysqlconnector",
    "postgresql": "psycopg2",
    "mssql": "pyodbc",
    "oracle": "oracledb",
}


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("mysql://h/db", "mysql+mysqlconnector://h/db"),
        ("postgresql://h/db", "postgresql+psycopg2://h/db"),
        ("mssql://h/db", "mssql+pyodbc://h/db"),
        ("oracle://h/db", "oracle+oracledb://h/db"),
    ],
)
def test_add_driver_injects_default_driver_from_map(uri, expected):
    assert utils.add_driver_to_uri(uri, drivers=_DRIVERS_FULL) == expected


@pytest.mark.parametrize(
    "uri",
    [
        "mysql+pymysql://h/db",
        "postgresql+psycopg://h/db",
        "mssql+pymssql://h/db",
        "oracle+cx_oracle://h/db",
    ],
)
def test_add_driver_preserves_explicit_driver(uri):
    assert utils.add_driver_to_uri(uri, drivers=_DRIVERS_FULL) == uri


@pytest.mark.parametrize(
    "uri",
    [
        "sqlite:///foo.db",
        "duckdb:///bar.db",
        "snowflake://x.y.z/db",  # not in the map
    ],
)
def test_add_driver_passes_through_uris_outside_map(uri):
    assert utils.add_driver_to_uri(uri, drivers=_DRIVERS_FULL) == uri


def test_add_driver_empty_map_is_noop():
    assert utils.add_driver_to_uri("mysql://h/db", drivers={}) == "mysql://h/db"


def test_add_driver_only_substitutes_for_the_supplied_scheme():
    drivers = {"mysql": "mysqlconnector"}
    assert utils.add_driver_to_uri(
        "postgresql://h/db", drivers=drivers,
    ) == "postgresql://h/db"


# ---------------------------------------------------------------------------
# normalize_scheme (caller-supplied alias map)
# ---------------------------------------------------------------------------


_ALIASES = {
    "mariadb": "mysql",
    "postgres": "postgresql",
}


def test_normalize_scheme_replaces_bare_alias():
    assert utils.normalize_scheme(
        "mariadb://host/db", aliases=_ALIASES,
    ) == "mysql://host/db"


def test_normalize_scheme_replaces_driver_qualified_alias():
    assert utils.normalize_scheme(
        "mariadb+pymysql://host/db", aliases=_ALIASES,
    ) == "mysql+pymysql://host/db"


def test_normalize_scheme_postgres_alias():
    assert utils.normalize_scheme(
        "postgres://host/db", aliases=_ALIASES,
    ) == "postgresql://host/db"


def test_normalize_scheme_passes_through_canonical_scheme():
    assert utils.normalize_scheme(
        "mysql://host/db", aliases=_ALIASES,
    ) == "mysql://host/db"


def test_normalize_scheme_empty_aliases_is_noop():
    assert utils.normalize_scheme(
        "mariadb://host/db", aliases={},
    ) == "mariadb://host/db"
