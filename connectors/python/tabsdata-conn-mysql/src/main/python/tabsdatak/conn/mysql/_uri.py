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


def normalize_mysql_uri(uri: str) -> str:
    """Canonical URI for every SQLAlchemy engine this connector builds:
    ``mariadb`` folds into ``mysql``, and a bare ``mysql://`` gets the
    ``mysqlconnector`` driver; an explicit ``+driver`` is honored.
    """
    uri = normalize_scheme(uri, aliases=_SCHEME_ALIASES)
    return add_driver_to_uri(uri, drivers=_DRIVERS)
