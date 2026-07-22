#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdata._credentials import UserPasswordCredentials


@dataclass
class ClickHouseConfig:
    """Connection parameters for a ClickHouse server.

    Args:
        host: ClickHouse server hostname or IP address.
        port: HTTP(S) port. Defaults to ``8123`` (plain HTTP). Use ``8443``
            for HTTPS.
        database: Target database name. Defaults to ``"default"``.
        credentials: Optional :class:`~tabsdata.UserPasswordCredentials` for
            authentication. When ``None`` the ClickHouse ``default`` user with
            no password is used.
        secure: If ``True``, connect over HTTPS/TLS. Defaults to ``False``.
    """

    host: str
    port: int = 8123
    database: str = "default"
    credentials: UserPasswordCredentials | None = None
    secure: bool = False
