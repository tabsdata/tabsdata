#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from typing_extensions import deprecated

from tabsdata._credentials import UserPasswordCredentials

# noinspection PyProtectedMember
from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._utils.annotations import unstable, unstable_category

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.cdc.common._connection import CdcConn
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@unstable()
@deprecated(
    """Class 'MySQLCdcConn' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'MySQLCdcConn' may undergo API and feature changes as it "
            "evolves toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class MySQLCdcConn(CdcConn, MySQLConn):
    @_td_validate_call(ErrorCode.CDCMYSQL1)
    def __init__(
        self,
        uri: str,
        credentials: UserPasswordCredentials | None = None,
        cx_src_configs_mysql: dict | None = None,
    ):
        MySQLConn.__init__(
            self,
            uri=uri,
            credentials=credentials,
            cx_src_configs_mysql=cx_src_configs_mysql,
        )

    @property
    def _cx_configs(self) -> dict:
        return self.cx_src_configs_mysql

    def _connect(self):
        return self._engine(self)
