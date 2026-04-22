#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from tabsdata._io.connections.connections import Conn
from tabsdata._secret import Secret, build_secret
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SnowflakeConn(Conn):
    """
    Connection configuration for Snowflake.
    """

    @_td_validate_call(ErrorCode.DECE119)
    def __init__(
        self,
        account: str | Secret,
        user: str | Secret,
        password: str | Secret,
        role: str | Secret,
        database: str | Secret,
        schema: str | Secret,
        warehouse: str | Secret,
    ):
        """
        Initialize a SnowflakeConn instance.

        Args:
            account: Snowflake account.
            user: Snowflake user.
            password: Snowflake user's Personal Access Token (PAT).
            role: Snowflake role.
            database: Snowflake database.
            schema: Snowflake schema.
            warehouse: Snowflake warehouse.

        All fields accept either a plain string (wrapped as a DirectSecret)
        or a Secret object (EnvironmentSecret, HashiCorpSecret, etc.).
        """
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.database = database
        self.schema = schema
        self.warehouse = warehouse

    @property
    def account(self) -> Secret:
        """
        The Snowflake account.
        """
        return self._account

    @account.setter
    @_td_validate_call(ErrorCode.DECE119)
    def account(self, account: str | Secret):
        self._account = build_secret(account)

    @property
    def user(self) -> Secret:
        """
        The Snowflake user.
        """
        return self._user

    @user.setter
    @_td_validate_call(ErrorCode.DECE119)
    def user(self, user: str | Secret):
        self._user = build_secret(user)

    @property
    def password(self) -> Secret:
        """
        The Snowflake user's Personal Access Token (PAT).
        """
        return self._password

    @password.setter
    @_td_validate_call(ErrorCode.DECE119)
    def password(self, password: str | Secret):
        self._password = build_secret(password)

    @property
    def role(self) -> Secret:
        """
        The Snowflake role.
        """
        return self._role

    @role.setter
    @_td_validate_call(ErrorCode.DECE119)
    def role(self, role: str | Secret):
        self._role = build_secret(role)

    @property
    def database(self) -> Secret:
        """
        The Snowflake database.
        """
        return self._database

    @database.setter
    @_td_validate_call(ErrorCode.DECE119)
    def database(self, database: str | Secret):
        self._database = build_secret(database)

    @property
    def schema(self) -> Secret:
        """
        The Snowflake schema.
        """
        return self._schema

    @schema.setter
    @_td_validate_call(ErrorCode.DECE119)
    def schema(self, schema: str | Secret):
        self._schema = build_secret(schema)

    @property
    def warehouse(self) -> Secret:
        """
        The Snowflake warehouse.
        """
        return self._warehouse

    @warehouse.setter
    @_td_validate_call(ErrorCode.DECE119)
    def warehouse(self, warehouse: str | Secret):
        self._warehouse = build_secret(warehouse)

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "SnowflakeConn":
        """
        Construct a SnowflakeConn from a plain dict of configs, e.g. one
        loaded from a NamedConn JSON catalog entry.

        Each value may be a plain string (wrapped as DirectSecret) or a
        nested dict specifying the secret type (hydrated to a Secret
        instance before construction so the constructor's ``str | Secret``
        validation accepts it).
        """
        configs = {
            key: build_secret(value) if isinstance(value, dict) else value
            for key, value in configs.items()
        }
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SnowflakeConn):
            return False
        return (
            self.account == other.account
            and self.user == other.user
            and self.password == other.password
            and self.role == other.role
            and self.database == other.database
            and self.schema == other.schema
            and self.warehouse == other.warehouse
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"account={self.account}, "
            f"user={self.user}, "
            f"password={self.password}, "
            f"role={self.role}, "
            f"database={self.database}, "
            f"schema={self.schema}, "
            f"warehouse={self.warehouse})"
        )
