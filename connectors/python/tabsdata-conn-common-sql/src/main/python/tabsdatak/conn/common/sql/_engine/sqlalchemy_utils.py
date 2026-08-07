#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from typing import Any, Mapping

import sqlalchemy
from sqlalchemy.engine import URL, Engine

logger = logging.getLogger(__name__)


def create_engine(url: str | URL, **kwargs: Any) -> Engine:
    """Build a SQLAlchemy engine through :func:`sqlalchemy.create_engine`,
    logging how the URL resolved to a driver and a dialect implementation.

    Every connector creates its engines here so that the resolution is
    traceable from a function's log alone. The URL is rendered with the
    password hidden, so the caller may pass one with credentials in it.
    """
    engine = sqlalchemy.create_engine(url, **kwargs)
    logger.info(
        "SQLAlchemy engine: URI: %s - URL driver name: %s - Engine driver: %s"
        " - Dialect: %s",
        engine.url.render_as_string(hide_password=True),
        engine.url.drivername,
        engine.driver,
        type(engine.dialect).__module__,
    )
    return engine


def normalize_scheme(uri: str, *, aliases: Mapping[str, str]) -> str:
    """Replace the URI's scheme with its canonical form, if it appears
    in ``aliases``. ``aliases`` maps the user-typed scheme to the
    canonical one (e.g. ``{"mariadb": "mysql"}``).

    The match is exact on the bare scheme (``mariadb://``) and on the
    driver-qualified prefix (``mariadb+...``). The rest of the URI is
    untouched.
    """
    for alias, canonical in aliases.items():
        if uri.startswith(f"{alias}://") or uri.startswith(f"{alias}+"):
            return uri.replace(alias, canonical, 1)
    return uri


def add_driver_to_uri(uri: str, *, drivers: Mapping[str, str]) -> str:
    """Inject a default DBAPI driver into a bare ``<scheme>://`` URI
    if one isn't already present. ``drivers`` maps the scheme to the
    driver to inject (e.g. ``{"mysql": "mysqlconnector"}``).

    If the URI already carries a ``+driver`` qualifier the function
    is a no-op.
    """
    for scheme, driver in drivers.items():
        bare_prefix = f"{scheme}://"
        if uri.startswith(bare_prefix):
            return uri.replace(bare_prefix, f"{scheme}+{driver}://", 1)
    return uri
