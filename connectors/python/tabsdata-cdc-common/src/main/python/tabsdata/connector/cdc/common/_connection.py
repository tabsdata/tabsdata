#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Union
from urllib.parse import urlparse

from sqlalchemy import Engine, create_engine

from tabsdata._credentials import UserPasswordCredentials

# noinspection PyProtectedMember
from tabsdata._io.connections.connections import Conn
from tabsdata._io.sql_io_helpers.helper_utils import obtain_sqlalchemy_url
from tabsdata._io.sql_io_helpers.sql_helper_configs import SQLConnConfig

if TYPE_CHECKING:
    from tabsdata.connector.cdc.db2._connection import Db2CdcConn
    from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
    from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn

    CdcConnType = Union[MySQLCdcConn, PostgresCdcConn, Db2CdcConn]


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
    credentials: UserPasswordCredentials | None

    @staticmethod
    def _engine(conn: CdcConnType) -> Engine:
        config = SQLConnConfig(
            uri=conn.uri,
            credentials=conn.credentials,
            cx_src_configs_sql=dict(conn._cx_configs or {}),
        )
        url = obtain_sqlalchemy_url(config)
        return create_engine(url, connect_args=config.cx_src_configs_sql or {})

    @staticmethod
    def _default_host() -> str:
        return "localhost"

    @abstractmethod
    def _default_port(self) -> int: ...

    @abstractmethod
    def _default_dbname(self) -> str: ...

    def _settings(self) -> CdcConnectionSettings:
        parsed = urlparse(self.uri)
        if self.credentials:
            user = self.credentials.user.secret_value
            password = self.credentials.password.secret_value
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
