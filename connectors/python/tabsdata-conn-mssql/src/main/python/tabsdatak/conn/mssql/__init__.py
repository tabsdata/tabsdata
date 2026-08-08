#
# Copyright 2026 Tabsdata Inc.
#

"""MSSQL connector -- public API.

Four classes, imported from `tabsdatak.conn.mssql`:

* `MSSQLSrcConn` / `MSSQLDestConn` -- the connection: a SQLAlchemy
  or ODBC `uri` and `UserPassword` credentials, plus an optional
  `entity_whitelist` and `conn_cfg`.
* `MSSQLSrc` -- the `@publisher` source: a list of SELECT
  `queries`, one output slot per query.
* `MSSQLDest` -- the `@subscriber` destination: a list of target
  `tables`, one output slot per table.

The connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `MSSQLSrc` / `MSSQLDest`.

Examples:
    .. code-block:: python

        @publisher(source=MSSQLSrc(queries=["SELECT * FROM wld_city"]),
                   output_tables=["cities"])
        def ingest(cities: TableFrameSpec) -> TableFrameSpec:
            return cities

        @subscriber(destination=MSSQLDest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(rows: TableFrameSpec) -> TableFrameSpec:
            return rows
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# Private modules are imported as modules (not by symbol) so each
# call-site is explicit about reaching into private surface.
# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    BasicDictSpec,
    Dest,
    Src,
    StrOrSecretSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import (
    CfgSpec,
    EntityWhitelistSpec,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _validation
from tabsdatak.conn.mssql.error import MssqlErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword
"""The credential type for MSSQL connections: `UserPassword` (user +
password)."""

UriSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.MSSQL_URI_VALIDATION,
]
QueriesSpec: TypeAlias = list[str]
MSSQLTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.MSSQL_TABLE_FQN_VALIDATION,
]
MSSQLTableNamesSpec: TypeAlias = list[MSSQLTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- MSSQLSrc.src_cfg keys ------------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "mssql.logging_level",
    "tabsdata.sql.sql.readEngine",
    "tabsdata.sql.chunkSize",
    "tabsdata.src_metadata.drop",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- MSSQLDest.dest_cfg keys ----------------------------------------
DestCfgKey: TypeAlias = Literal[
    "mssql.logging_level",
    "tabsdata.sql.sql.writeEngine",
    "tabsdata.sql.chunkSize",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection ------------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_1, kw_only=True)
class MSSQLSrcConn(Conn):
    """MSSQL connection used by `MSSQLSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. The `uri` accepts a plain string or a
    `Secret`.

    Args:
        uri: An MSSQL connection URI -- either a SQLAlchemy URI
            (`mssql[+driver]://host[:port][/db]`; the driver defaults
            to `pyodbc`) or a raw ODBC connection string
            (`DRIVER=...;SERVER=...;DATABASE=...`).
        credentials: `UserPassword` (user + password).
        entity_whitelist: Optional list of regex patterns narrowing
            which tables the source considers; `None` (the default)
            filters nothing.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


@_api.dataclass(MssqlErrorCode.MSSQL_1, kw_only=True)
class MSSQLDestConn(Conn):
    """MSSQL connection used by `MSSQLDest`.

    :categories: destination

    Field-identical to `MSSQLSrcConn` but a distinct class; source and
    destination connections are separate types and cannot be used in
    place of one another. See `MSSQLSrcConn` for the field meanings.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_2, kw_only=True)
class MSSQLSrc(Src):
    """MSSQL source for `@publisher` -- one output slot per query.

    :categories: source

    Every table this source publishes automatically carries two
    metadata columns: `@td.sql.query`, the text of the query that
    produced the row, and `@td.sql.initial_values`, the JSON
    serialization of the bind values that query ran with (an empty JSON
    object when it has no binds). Setting the `src_cfg` key
    `tabsdata.src_metadata.drop` to `True` leaves them off.

    Args:
        queries: SELECT statements, one output slot per entry.
        transactional: When `True` (the default), run all queries in
            one transactional session.
        schema_overrides: Optional per-query column dtype hints; when
            given, must have the same length as `queries`.
        initial_values: Optional `{bind_name: value}` seed dict. Each
            name binds into the query as a SQLAlchemy `:var`
            parameter; it seeds the first run only, after which the
            plugin reads `ctx.get_attr(bind_name)` and the function
            persists new values with `ctx.set_attr(bind_name, ...)`.
        src_cfg: Optional connector config; supported keys are
            `mssql.logging_level`, `tabsdata.sql.sql.readEngine`,
            `tabsdata.sql.chunkSize` and `tabsdata.src_metadata.drop`.

    Examples:
        Publish multiple SQL Server tables, one output slot per query in
        order:

        .. code-block:: python

            @publisher(
                source=MSSQLSrc(
                    queries=[
                        "SELECT * FROM wld_country",
                        "SELECT * FROM wld_city",
                    ],
                ),
                output_tables=["ms_country", "ms_city"],
            )
            def ingest(country: TableFrameSpec, city: TableFrameSpec):
                return country, city

        Incremental read -- SQL Server has no `LIMIT`, so page with
        `OFFSET ... FETCH NEXT` (which needs `ORDER BY`); an
        `initial_values` bind seeds `:wm` on the first run and `ctx`
        advances it each run:

        .. code-block:: python

            @publisher(
                source=MSSQLSrc(
                    queries=[
                        "SELECT * FROM wld_city WHERE id > :wm "
                        "ORDER BY id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"
                    ],
                    initial_values={"wm": 0},
                ),
                output_tables=["ms_city_inc"],
            )
            def ingest_inc(city: TableFrameSpec, ctx: TrxCtx):
                if city is not None:
                    row = city.max_for("id")
                    if row is not None and row["id"] is not None:
                        ctx.set_attr("wm", int(row["id"]))
                return city
    """

    # devguide-ref: source bind-value seeding & ctx scratch state

    queries: QueriesSpec
    transactional: bool = True
    schema_overrides: SchemaOverrideSpec | None = None
    initial_values: BasicDictSpec | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation: `schema_overrides` length must match
        `queries`.

        Called by the framework; raises `MSSQLValidateException`.
        """
        if self.schema_overrides is not None and len(self.schema_overrides) != len(
            self.queries
        ):
            raise MssqlErrorCode.MSSQL_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(MssqlErrorCode.MSSQL_4, kw_only=True)
class MSSQLDest(Dest):
    """MSSQL destination for `@subscriber` -- one output slot per table.

    :categories: destination

    Args:
        tables: Target table names, one write slot per entry. Each is a
            1-, 2- or 3-part name (`table`, `schema.table` or
            `database.schema.table`); bracket-quote a part
            (`[name]`) to allow special characters.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` truncates the table before inserting.
        transactional: When `True` (the default), wrap every table
            write in one transactional session.
        dest_cfg: Optional connector config; supported keys are
            `mssql.logging_level`, `tabsdata.sql.sql.writeEngine`
            and `tabsdata.sql.chunkSize`.

    Note:
        Column types are picked only when the target table is created (with
        `"replace"`, or `"append"` to a missing table); writing into an existing
        table uses that table's columns. A tz-aware `Datetime` becomes a
        `DATETIMEOFFSET`, so SQL Server is the one target that keeps the offset
        rather than storing the value naive. See `TYPE_MAPPING.md` in
        `tabsdata-conn-common-sql` for the full matrix.
    """

    tables: MSSQLTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
