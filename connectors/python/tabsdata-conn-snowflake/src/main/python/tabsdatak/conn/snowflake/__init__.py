"""Snowflake connector — public API.

Defines the dataclasses a flow author wires onto a publisher,
subscriber, or transformer to target Snowflake:

* :class:`SnowflakeDestConn` -- per-account connection (account id,
  user/password, optional default role/database/schema/warehouse/
  stage).
* :class:`SnowflakeDest` -- destination wrapper bound to a
  :class:`SnowflakeDestConn` plus a list of target table names. Used
  by the destination plugin (``COPY INTO`` via a stage).

Snowflake is dest-only in this connector; there is no
``SnowflakeSrc``. Identifier and account fields accept ``Secret``
references so values can be injected from the runtime
environment rather than hard-coded.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import StringConstraints

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Conn, Dest, Secret

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import CfgSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _validation
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

AccountSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.ACCOUNT_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.ACCOUNT_PATTERN)],
]


# Snowflake identifier shape (unquoted or double-quoted), shared by
# both the dedicated identifier field (``stage``) and the Name fields
# (``role`` / ``database`` / ``schema`` / ``warehouse``) -- all four
# follow the same identifier rules.

IdentifierSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.IDENTIFIER_PATTERN)],
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.IDENTIFIER_PATTERN)
    ],
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

    :param account: Snowflake account locator (e.g. ``ABCDEFG-XY12345``).
        Accepts a plain str or ``Secret``.
    :param credentials: :class:`UserPassword` (user + password / PAT).
    :param role: Default Snowflake role. Optional.
    :param database: Default database. Required if ``schema`` is set.
    :param schema: Default schema. Requires ``database``.
    :param warehouse: Default warehouse to ``USE`` before write.
        Optional -- the dest plugin issues ``USE WAREHOUSE`` only when set.
    :param stage: Permanent stage to use for ``PUT`` + ``COPY INTO``.
        When unset the plugin creates a session-scoped ``TEMPORARY`` stage.
    :param conn_cfg: Optional driver-specific config dict
        (snowflake-connector-python kwargs).

    Cross-field rule: ``schema`` without ``database`` raises
    ``SNOWFLAKE_2``.
    """

    __kind__ = "snowflakeDestConn"

    account: AccountSpec
    credentials: CredentialsSpec
    role: NameSpec | None = None
    database: NameSpec | None = None
    schema: NameSpec | None = None
    warehouse: NameSpec | None = None
    stage: IdentifierSpec | None = None
    conn_cfg: CfgSpec | None = None

    def validate(self) -> None:
        if self.schema is not None and self.database is None:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_2.exception(schema=self.schema)


# --- Destination -----------------------------------------------------


@_api.dataclass(SnowflakeErrorCode.SNOWFLAKE_3, kw_only=True)
class SnowflakeDest(Dest):
    """Snowflake destination — write one parquet per target table.

    :param tables: Fully-qualified Snowflake table names
        (``[<db>.]<schema>.<table>`` or bare). Identifier parts
        accept double-quoted spelling for case-sensitive / special-
        char names.
    :param if_table_exists: ``"append"`` (default) or ``"replace"``
        (issues ``TRUNCATE`` before ``COPY INTO`` to preserve the
        existing schema).
    :param dest_cfg: Optional dest cfg dict; the only key the plugin
        consumes today is ``"snowflake.logging_level"``.
    """

    tables: SnowflakeTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    dest_cfg: DestCfgSpec | None = None
