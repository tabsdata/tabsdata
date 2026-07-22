#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdata._credentials import UserPasswordCredentials


@dataclass
class StarRocksConfig:
    """Connection parameters for a StarRocks server.

    Args:
        host: StarRocks FE hostname or IP address.
        port: MySQL protocol port. Defaults to ``9030``.
        database: Target database name. Defaults to ``"default"``.
        credentials: Optional :class:`~tabsdata.UserPasswordCredentials` for
            authentication. When ``None`` the StarRocks ``root`` user with
            no password is used.
    """

    host: str
    port: int = 9030
    database: str = "default"
    credentials: UserPasswordCredentials | None = None
