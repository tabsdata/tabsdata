#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from typing_extensions import deprecated

from tabsdata._credentials import UserPasswordCredentials

# noinspection PyProtectedMember
from tabsdata._io.connections.postgres_connection import PostgresConn
from tabsdata._utils.annotations import unstable, unstable_category

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.cdc.common._connection import CdcConn
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@unstable()
@deprecated(
    """Class 'PostgresCdcConn' is considered unstable and is subject to change in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'PostgresCdcConn' may undergo API and feature changes as it "
            "evolves toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class PostgresCdcConn(CdcConn, PostgresConn):
    """PostgreSQL CDC connection config. Extends PostgresConn with CDC settings.
    PostgreSQL CDC connection configuration.
    """

    @_td_validate_call(ErrorCode.CDCPOSTGRES1)
    def __init__(
        self,
        uri: str,
        credentials: UserPasswordCredentials | None = None,
        cx_src_configs_postgres: dict | None = None,
    ):
        """
        Initialize a PostgresCdcConn instance.

        Args:
            uri: The PostgreSQL connection URI.
            credentials: Optional user/password credentials for authentication.
            cx_src_configs_postgres: Optional PostgreSQL-specific connection
                configuration parameters.
        """
        PostgresConn.__init__(
            self,
            uri=uri,
            credentials=credentials,
            cx_src_configs_postgres=cx_src_configs_postgres,
        )

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "PostgresCdcConn":
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = UserPasswordCredentials(**credentials)
        return cls(**configs)

    @property
    def _cx_configs(self) -> dict:
        return self.cx_src_configs_postgres

    def _default_port(self) -> int:
        return 5432

    def _default_dbname(self) -> str:
        return "postgres"

    def _connect(self):
        return self._engine(self)
