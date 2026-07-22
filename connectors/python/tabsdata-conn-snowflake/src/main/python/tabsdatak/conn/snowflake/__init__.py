#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake connector -- public API.

Two classes, imported from `tabsdatak.conn.snowflake`:

* `SnowflakeDestConn` -- the connection: a Snowflake `account`,
  `UserPassword` credentials, and optional default `role` /
  `database` / `schema` / `warehouse` / `stage`.
* `SnowflakeDest` -- the `@subscriber` destination: a list of
  target `tables`, one parquet slot per table (loaded with `PUT` +
  `COPY INTO` through a stage).

Snowflake is destination-only -- there is no `SnowflakeSrc`. The
connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `SnowflakeDest`.

Examples:
    .. code-block:: python

        @subscriber(destination=SnowflakeDest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(rows: TableFrameSpec) -> TableFrameSpec:
            return rows
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Dest, StrOrSecretSpec

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    CfgSpec,
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _validation
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword
"""The credential type for Snowflake connections: `UserPassword`
(user + password / PAT)."""

AccountSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.ACCOUNT_PATTERN)
]


# Snowflake identifier shape (unquoted or double-quoted), shared by
# both the dedicated identifier field (`stage`) and the Name fields
# (`role` / `database` / `schema` / `warehouse`) -- all four
# follow the same identifier rules.

IdentifierSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.IDENTIFIER_PATTERN)
]

NameSpec: TypeAlias = IdentifierSpec
SnowflakeTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.SNOWFLAKE_TABLE_FQN_VALIDATOR,
]
SnowflakeTableNamesSpec: TypeAlias = list[SnowflakeTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- SnowflakeDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal["snowflake.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(SnowflakeErrorCode.SNOWFLAKE_1, kw_only=True)
class SnowflakeDestConn(Conn):
    """Snowflake account connection.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each string field accepts a plain string or
    a `Secret`. `role` / `database` / `schema` / `warehouse`
    follow the same Snowflake identifier rules as `stage`.

    Args:
        account: A valid Snowflake account identifier / locator, e.g.
            `ABCDEFG-XY12345` (alphanumerics with dots, hyphens and
            underscores; starts and ends alphanumeric).
        credentials: `UserPassword` (user + password or PAT).
        role: Optional default role to `USE` before writing.
        database: Optional default database. Required when `schema`
            is set.
        schema: Optional default schema. Requires `database`.
        warehouse: Optional default warehouse; the plugin issues
            `USE WAREHOUSE` only when set.
        stage: Optional permanent stage for `PUT` + `COPY INTO`;
            when unset the plugin creates a session-scoped
            `TEMPORARY` stage. A Snowflake identifier -- unquoted, or
            double-quoted for case-sensitive / special-char names.
        conn_cfg: Optional connector-config dict (see `CfgSpec`),
            forwarded to snowflake-connector-python.
    """

    account: AccountSpec
    credentials: CredentialsSpec
    role: NameSpec | None = None
    database: NameSpec | None = None
    schema: NameSpec | None = None
    warehouse: NameSpec | None = None
    stage: IdentifierSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _account(self) -> str:
        return _convert(self.account, str)

    def _role(self) -> str:
        return _convert(self.role, str)

    def _database(self) -> str:
        return _convert(self.database, str)

    def _schema(self) -> str:
        return _convert(self.schema, str)

    def _warehouse(self) -> str:
        return _convert(self.warehouse, str)

    def _stage(self) -> str:
        return _convert(self.stage, str)

    def validate(self) -> None:
        """Cross-field validation: `schema` requires `database`.

        Called by the framework; raises `SnowflakeValidateException`.
        """
        if self.schema is not None and self.database is None:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_2.exception(schema=self.schema)


# --- Destination -----------------------------------------------------


@_api.dataclass(SnowflakeErrorCode.SNOWFLAKE_3, kw_only=True)
class SnowflakeDest(Dest):
    """Snowflake destination for `@subscriber` -- one parquet slot per table.

    :categories: destination

    Args:
        tables: Target table names, one write slot per entry. Each is a
            1-, 2- or 3-part name (`table`, `schema.table` or
            `database.schema.table`); double-quote a part for
            case-sensitive / special-char names.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` issues `TRUNCATE` before `COPY INTO` (so
            the existing schema is preserved).
        dest_cfg: Optional connector config; the only supported key is
            `snowflake.logging_level`.
    """

    tables: SnowflakeTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    dest_cfg: DestCfgSpec | None = None
