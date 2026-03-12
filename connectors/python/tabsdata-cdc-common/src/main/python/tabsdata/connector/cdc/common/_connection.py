#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union

from sqlalchemy import Engine, create_engine

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


class CdcConn(Conn):
    @staticmethod
    def _engine(conn: CdcConnType) -> Engine:
        config = SQLConnConfig(
            uri=conn.uri,
            credentials=conn.credentials,
            cx_src_configs_sql=dict(conn._cx_configs or {}),
        )
        url = obtain_sqlalchemy_url(config)
        return create_engine(url, connect_args=config.cx_src_configs_sql or {})
