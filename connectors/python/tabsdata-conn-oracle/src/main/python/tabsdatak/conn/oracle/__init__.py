#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle connector -- public API.

Imported from `tabsdatak.conn.oracle`. Four classes:

* `OracleSrcConn` / `OracleDestConn` -- the connection: a SQLAlchemy
  URI plus `UserPassword` credentials. Attribute-identical but distinct
  classes, one for source and one for destination connections.
* `OracleSrc` -- the `@publisher` source: a list of SELECT
  `queries`, one output slot per query.
* `OracleDest` -- the `@subscriber` destination: a list of target
  `tables`, one output slot per table.

The connection (with credentials) is registered on a collection and
resolved from the function's collection at run time -- it is never passed
to a decorator. A flow function only references the source / destination.

Examples:
    .. code-block:: python

        @publisher(source=OracleSrc(queries=["SELECT * FROM city"]),
                   output_tables=["cities"])
        def ingest(cities: TableFrameSpec) -> TableFrameSpec:
            return cities

        @subscriber(destination=OracleDest(tables=["cap_rows"]),
                    input_tables=["mycoll/cities"])
        def publish(cities: TableFrameSpec) -> TableFrameSpec:
            return cities
URI and credential fields accept `Secret` references. Cross-cutting
cfg keys live in `src_cfg` / `dest_cfg` (chunk size, read/write
engine selection).
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
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _validation
from tabsdatak.conn.oracle.error import OracleErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword
"""The credential type for Oracle connections: `UserPassword` (user +
password), spliced into the URI by the engine factory."""

UriSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.URI_PATTERN)
]
"""A SQLAlchemy-style Oracle URI, e.g.
`oracle+oracledb://host:port/service`. The scheme must be `oracle`
(an optional `+driver` is allowed). Accepts a plain string or a
`Secret`."""

QueriesSpec: TypeAlias = list[str]
"""A non-empty, ordered list of SELECT statements -- one output slot per
query."""

OracleTableNameSpec: TypeAlias = Annotated[
    str,
    _validation.ORACLE_TABLE_FQN_VALIDATOR,
]
"""An Oracle table name, either `table` or `schema.table` (at most
two dot-separated parts). Parts may be double-quoted; Oracle has no way
to embed a literal double-quote inside a quoted identifier."""

OracleTableNamesSpec: TypeAlias = list[OracleTableNameSpec]
"""A list of Oracle table names (see `OracleTableNameSpec`)."""

IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
"""Write disposition when the target table exists: `"append"` adds
rows; `"replace"` truncates before insert."""


# --- OracleSrc.src_cfg keys -----------------------------------------
SrcCfgKey: TypeAlias = Literal[
    "oracle.logging_level",
    "tabsdata.sql.sql.readEngine",
    "tabsdata.sql.chunkSize",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- OracleDest.dest_cfg keys ---------------------------------------
DestCfgKey: TypeAlias = Literal[
    "oracle.logging_level",
    "tabsdata.sql.sql.writeEngine",
    "tabsdata.sql.chunkSize",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_1, kw_only=True)
class OracleSrcConn(Conn):
    """Oracle connection used by `OracleSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time.

    Args:
        uri: SQLAlchemy-style Oracle URI
            (`oracle+oracledb://host:port/service`). Accepts a plain
            string or a `Secret`.
        credentials: `UserPassword` -- user + password spliced into
            the URI by the engine factory.
        entity_whitelist: Optional list of regex patterns narrowing
            which tables this connection contributes to discovery.
            `None` (the default) means no filtering.
        conn_cfg: Optional driver-side config forwarded to SQLAlchemy as
            `connect_args`.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


@_api.dataclass(OracleErrorCode.ORACLE_1, kw_only=True)
class OracleDestConn(Conn):
    """Oracle connection used by `OracleDest`.

    :categories: destination

    Attribute-identical to `OracleSrcConn` but a distinct class; source
    and destination connections are separate types and cannot be used in
    place of one another. See `OracleSrcConn` for the field meanings.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_2, kw_only=True)
class OracleSrc(Src):
    """Oracle source for `@publisher` -- one output slot per query.

    :categories: source

    Every table this source publishes automatically carries two
    metadata columns: `@td.sql.query`, the text of the query that
    produced the row, and `@td.sql.initial_values`, the JSON
    serialization of the bind values that query ran with (an empty JSON
    object when it has no binds).

    Args:
        queries: SELECT statements; one output slot per query, in order.
        transactional: When `True` (default), run all queries inside
            one transactional session.
        schema_overrides: Optional per-query column dtype hints. When
            given it must have exactly one entry per query (same length
            and order as `queries`); each entry is a
            `{column: DataType}` map or `None`.
        initial_values: Optional `{bind_name: value}` seed dict. Each
            name is bound into the query as a SQLAlchemy `:var`
            parameter at execute time, and acts as the first-run seed:
            the plugin reads `ctx.get_attr(bind_name)` per bind and
            falls back to this value when the ctx store has no entry.
            Persist the next value with `ctx.set_attr(bind_name, v)`.
        src_cfg: Optional config mapping over the allowed keys only
            (`oracle.logging_level`, `tabsdata.sql.sql.readEngine`,
            `tabsdata.sql.chunkSize`).

    Examples:
        Publish multiple Oracle tables, one output slot per query in order:

        .. code-block:: python

            @publisher(
                source=OracleSrc(
                    queries=[
                        "SELECT * FROM wld_country",
                        "SELECT * FROM wld_city",
                    ],
                ),
                output_tables=["or_country", "or_city"],
            )
            def ingest(country: TableFrameSpec, city: TableFrameSpec):
                return country, city

        Incremental read: an `initial_values` bind seeds the `:wm`
        watermark, `FETCH FIRST n ROWS ONLY` pages the scan, and `ctx`
        advances it each run. Oracle folds unquoted identifiers to
        uppercase, so the watermark column is read as `ID`:

        .. code-block:: python

            @publisher(
                source=OracleSrc(
                    queries=[
                        "SELECT * FROM wld_city WHERE id > :wm "
                        "ORDER BY id FETCH FIRST 10 ROWS ONLY"
                    ],
                    initial_values={"wm": 0},
                ),
                output_tables=["or_city_inc"],
            )
            def ingest_inc(city: TableFrameSpec, ctx: TrxCtx):
                if city is not None:
                    row = city.max_for("ID")
                    if row is not None and row["ID"] is not None:
                        ctx.set_attr("wm", int(row["ID"]))
                return city
    """

    # devguide-ref: trx context state (initial_values seed / cursor resume)
    queries: QueriesSpec
    transactional: bool = True
    schema_overrides: SchemaOverrideSpec | None = None
    initial_values: BasicDictSpec | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        """Check `schema_overrides` has one entry per query.

        Called by the framework; raises `OracleValidateException` when
        the lengths differ.
        """
        if self.schema_overrides is not None and len(self.schema_overrides) != len(
            self.queries
        ):
            raise OracleErrorCode.ORACLE_3.exception(
                queries=len(self.queries),
                overrides=len(self.schema_overrides),
            )


# --- Destination -----------------------------------------------------


@_api.dataclass(OracleErrorCode.ORACLE_4, kw_only=True)
class OracleDest(Dest):
    """Oracle destination for `@subscriber` -- one target table per slot.

    :categories: destination

    Args:
        tables: Target table names, qualified or bare (see
            `OracleTableNameSpec`); one input slot per table, in order.
        if_table_exists: `"append"` (default) adds rows; `"replace"`
            truncates the table before inserting.
        transactional: When `True` (default), wrap every table write
            in one transactional session.
        dest_cfg: Optional config mapping over the allowed keys only
            (`oracle.logging_level`, `tabsdata.sql.sql.writeEngine`,
            `tabsdata.sql.chunkSize`).

    Note:
        Column types are picked only when the target table is created (with
        `"replace"`, or `"append"` to a missing table); writing into an existing
        table uses that table's columns. Oracle needs three substitutions:
        `Float32` / `Float64` become `BINARY_FLOAT` / `BINARY_DOUBLE`, and `Time`
        is stored as a `TIMESTAMP` on 1970-01-01, since Oracle has no
        time-of-day type -- it reads back carrying that date. A tz-aware
        `Datetime` loses its offset (stored naive). See `TYPE_MAPPING.md` in
        `tabsdata-conn-common-sql` for the full matrix.
    """

    tables: OracleTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    transactional: bool = True
    dest_cfg: DestCfgSpec | None = None
