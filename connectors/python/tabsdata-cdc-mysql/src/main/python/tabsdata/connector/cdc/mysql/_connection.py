#
# Copyright 2026 Tabsdata Inc.
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
    """MySQL CDC connection config. Extends MySQLConn with CDC settings.
    MySQL CDC connection configuration.
    """

    @_td_validate_call(ErrorCode.CDCMYSQL1)
    def __init__(
        self,
        uri: str,
        credentials: UserPasswordCredentials | None = None,
        cx_src_configs_mysql: dict | None = None,
    ):
        """
        Initialize a MySQLCdcConn instance.

        Args:
            uri: The MySQL connection URI.
            credentials: Optional user/password credentials for authentication.
            cx_src_configs_mysql: Optional MySQL-specific connection configuration
                parameters.
        """
        MySQLConn.__init__(
            self,
            uri=uri,
            credentials=credentials,
            cx_src_configs_mysql=cx_src_configs_mysql,
        )

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "MySQLCdcConn":
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = UserPasswordCredentials(**credentials)
        return cls(**configs)

    @property
    def _cx_configs(self) -> dict:
        return self.cx_src_configs_mysql

    def _default_port(self) -> int:
        return 3306

    def _default_dbname(self) -> str:
        return "mysql"

    def _connect(self):
        return self._engine(self)
