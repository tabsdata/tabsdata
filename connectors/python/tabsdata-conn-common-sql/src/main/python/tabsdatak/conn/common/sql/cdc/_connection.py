#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from urllib.parse import urlparse

from sqlalchemy import Engine, make_url

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import sqlalchemy_utils

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import UserPassword, _convert
from tabsdatak.spi import Conn

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class CdcConnectionSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def __repr__(self) -> str:
        return (
            f"CdcConnectionSettings("
            f"host={self.host!r}, "
            f"port={self.port!r}, "
            f"dbname={self.dbname!r}, "
            f"user='***redacted***', "
            f"password='***redacted***')"
        )


class CdcConn(Conn, ABC):
    """
    Base connection configuration for Change Data Capture (CDC) connectors.
    """

    uri: str
    credentials: UserPassword | None = None

    @staticmethod
    def _engine(conn: "CdcConn") -> Engine:
        url = make_url(_convert(conn.uri, str))
        if conn.credentials is not None:
            url = url.set(
                username=_convert(conn.credentials.user, str),
                password=_convert(conn.credentials.password, str),
            )
        connect_args = dict(getattr(conn, "conn_cfg", None) or {})
        return sqlalchemy_utils.create_engine(url, connect_args=connect_args)

    @staticmethod
    def _default_host() -> str:
        return "localhost"

    @abstractmethod
    def _default_port(self) -> int: ...

    @abstractmethod
    def _default_dbname(self) -> str: ...

    def _settings(self) -> CdcConnectionSettings:
        parsed = urlparse(_convert(self.uri, str))
        if self.credentials:
            user = _convert(self.credentials.user, str)
            password = _convert(self.credentials.password, str)
        else:
            user = parsed.username
            password = parsed.password
        return CdcConnectionSettings(
            host=parsed.hostname or self._default_host(),
            port=parsed.port or self._default_port(),
            dbname=parsed.path.strip("/") if parsed.path else self._default_dbname(),
            user=user,
            password=password,
        )
