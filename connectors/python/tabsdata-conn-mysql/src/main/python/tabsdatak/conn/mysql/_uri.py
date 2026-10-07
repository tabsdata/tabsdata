#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.sqlalchemy_utils import (
    add_driver_to_uri,
    normalize_scheme,
)

_SCHEME_ALIASES: dict[str, str] = {"mariadb": "mysql"}

_DRIVERS: dict[str, str] = {"mysql": "mysqlconnector"}
_CDC_DRIVERS: dict[str, str] = {"mysql": "pymysql"}


def normalize_mysql_uri(uri: str) -> str:
    """Canonical URI for every SQLAlchemy engine this connector builds:
    ``mariadb`` folds into ``mysql``, and a bare ``mysql://`` gets the
    ``mysqlconnector`` driver; an explicit ``+driver`` is honored.
    """
    uri = normalize_scheme(uri, aliases=_SCHEME_ALIASES)
    return add_driver_to_uri(uri, drivers=_DRIVERS)


def normalize_mysql_cdc_uri(uri: str) -> str:
    """The stream's canonical URI: as :func:`normalize_mysql_uri`, but a bare
    ``mysql://`` gets ``pymysql``, the driver the binlog reader and the
    initial load connect with, so one ``conn_cfg`` fits every connection the
    stream opens."""
    uri = normalize_scheme(uri, aliases=_SCHEME_ALIASES)
    return add_driver_to_uri(uri, drivers=_CDC_DRIVERS)
