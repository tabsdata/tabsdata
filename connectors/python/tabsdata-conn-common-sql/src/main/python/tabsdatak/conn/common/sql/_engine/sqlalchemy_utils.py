#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Mapping


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
