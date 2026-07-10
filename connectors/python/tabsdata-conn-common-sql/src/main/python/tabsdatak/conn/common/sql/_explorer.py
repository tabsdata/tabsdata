#
# Copyright (c) 2026 Tabsdata Inc.
#

from abc import abstractmethod
from dataclasses import field
from typing import ClassVar

from sqlalchemy import (
    column as sa_column,
    create_engine,
    engine,
    inspect,
    select as sa_select,
    table as sa_table,
    text as sa_text,
)

import tabsdatak._api as _api
from tabsdatak.conn.common.explorer import BaseConnMetaModel, Column
from tabsdatak.error import ApiErrorCode
from tabsdatak.conn.common.sql.error import SqlCommonErrorCode
from tabsdatak.spi import (
    Conn,
    Entity,
    Explorer,
)


@_api.dataclass(SqlCommonErrorCode.SQL_COMMON_2)
class SQLEntity(Entity):
    """A table or view in a SQL database."""

    database: str = ""
    schema: str = ""
    table: str = ""
    columns: list[Column] = field(default_factory=list)

    def fqn(self) -> str:
        return self.connection_type.entity_fqn(
            {
                "database": self.database,
                "schema": self.schema,
                "owner": self.schema,
                "catalog": self.database,
                "table": self.table,
            }
        )

    def field_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_dict(self) -> dict:
        d = {}
        if self.database:
            d["database"] = self.database
        if self.schema:
            d["schema"] = self.schema
        d["table"] = self.table
        d["fqn"] = self.fqn()
        d["columns"] = [c.to_dict() for c in self.columns]
        return d

    @classmethod
    def from_dict(cls, data: dict, connection_type: "BaseConnMetaModel") -> "SQLEntity":
        return cls(
            connection_type=connection_type,
            database=data.get("database", ""),
            schema=data.get("schema", ""),
            table=data.get("table", ""),
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
        )


@_api.dataclass(SqlCommonErrorCode.SQL_COMMON_3)
class SQLConnMetaModel(BaseConnMetaModel):
    """SQL databases: database → schema → table → column."""

    type: str = "common_sql"
    description: str = (
        "Relational database systems with databases, schemas, tables, and columns"
    )
    hierarchy: list[str] = field(
        default_factory=lambda: ["database", "schema", "table", "column"]
    )
    entity_level: str = "table"
    entity_fully_qualified_name: list[str] = field(
        default_factory=lambda: ["database", "schema", "table"]
    )

    def entity_fqn(self, entity: dict) -> str:
        parts = [entity.get(k, "") for k in self.entity_fully_qualified_name]
        return ".".join(p for p in parts if p)


class SqlAlchemyConnectionExplorer(Explorer):
    """Abstract base for SQLAlchemy-based metadata and data extraction.

    Subclasses implement:
        make_url()          — build a SQLAlchemy connection URL
        system_schemas()    — schemas to skip
        schema_filter()     — which info key restricts the scan
        map_entity_fields() — how to map schema to entity database/schema

    All SQLAlchemy-backed explorers share a :class:`SQLConnMetaModel`.
    Concrete subclasses inherit that default rather than parameterising
    it; the binding is fixed at the class level.
    """

    meta_model: ClassVar[BaseConnMetaModel] = SQLConnMetaModel()
    entity_type: ClassVar[type[Entity]] = SQLEntity

    @abstractmethod
    def make_url(self, conn: Conn) -> str | engine.URL:
        """Build a SQLAlchemy connection URL from connection object."""

    def system_schemas(self) -> frozenset[str]:
        """Return the set of system/internal schemas to skip."""
        return frozenset()

    def schema_filter(self, conn: Conn) -> str | None:
        """Extract the schema/owner/database filter from connection info."""
        return None

    def map_entity_fields(self, conn: Conn, schema: str | None) -> dict:
        """Map the iterated schema name to entity database/schema fields."""
        return {
            "database": schema or "",
            "schema": schema or "",
        }

    def engine_kwargs(self, conn: Conn) -> dict:
        """Return extra kwargs to pass to :func:`sqlalchemy.create_engine`.

        Default implementation returns ``{}``. Override when a dialect
        needs auth material that can't be expressed in the URL (e.g.
        BigQuery's ``credentials_info`` / ``credentials_path``).
        """
        return {}

    def connectivity_probe(self) -> str:
        """SQL used by :meth:`check_connect` to verify a live connection.

        ``SELECT 1`` is the most widely supported probe, but some dialects
        reject it without a ``FROM`` clause (e.g. Db2 needs
        ``FROM SYSIBM.SYSDUMMY1``). Such connectors override this.
        """
        return "SELECT 1"

    def check_connect(self, conn: Conn) -> None:
        url = self.make_url(conn)
        eng = create_engine(url, **self.engine_kwargs(conn))
        try:
            with eng.connect() as conn:
                conn.execute(sa_text(self.connectivity_probe()))
        finally:
            eng.dispose()

    def metadata(self, conn: Conn) -> list[SQLEntity]:
        url = self.make_url(conn)
        restrict_to = self.schema_filter(conn)

        eng = create_engine(url, **self.engine_kwargs(conn))
        insp = inspect(eng)

        if restrict_to:
            schemas = [restrict_to]
        else:
            try:
                schemas = insp.get_schema_names()
            except NotImplementedError:
                schemas = [None]

        skip = frozenset(s.upper() for s in self.system_schemas())
        entities: list[SQLEntity] = []

        for schema in schemas:
            if schema is not None and schema.upper() in skip:
                continue

            table_names = insp.get_table_names(schema=schema)
            view_names = insp.get_view_names(schema=schema)
            fields = self.map_entity_fields(conn, schema)

            for table_name in table_names + view_names:
                pk_columns = _get_primary_keys(insp, table_name, schema)
                columns = _get_columns(insp, table_name, schema, pk_columns)

                entities.append(
                    SQLEntity(
                        connection_type=self.meta_model,
                        database=fields["database"],
                        schema=fields["schema"],
                        table=table_name,
                        columns=columns,
                    )
                )

        eng.dispose()
        return entities

    def data(self, conn: Conn, entity: Entity, limit: int = 100) -> list[dict]:
        url = self.make_url(conn)
        eng = create_engine(url, **self.engine_kwargs(conn))
        sql_entity = entity  # type: SQLEntity
        schema = sql_entity.schema or None
        t = sa_table(
            sql_entity.table,
            *[sa_column(c.name) for c in sql_entity.columns],
            schema=schema,
        )
        stmt = sa_select(t).limit(limit)
        try:
            with eng.connect() as eng_conn:
                result = eng_conn.execute(stmt)
                columns = list(result.keys())
                return [dict(zip(columns, row)) for row in result.fetchall()]
        finally:
            eng.dispose()


def _simplify_type(raw: str) -> str:
    """Simplify a verbose column type to a short canonical name."""
    s = raw.strip()
    if " COLLATE " in s:
        s = s[: s.index(" COLLATE ")]
    if "(" in s:
        s = s[: s.index("(")]
    return s.strip().upper()


def _get_columns(
    insp, table_name: str, schema: str | None, pk_columns: set[str] = frozenset()
) -> list[Column]:
    """Get column metadata for a table.

    ``Column`` is a frozen dataclass, so the primary-key flag is set at
    construction time from ``pk_columns`` rather than mutated afterwards.
    """
    columns = []
    for col_info in insp.get_columns(table_name, schema=schema):
        try:
            col_type = _simplify_type(str(col_info["type"]))
        except Exception:
            col_type = "UNKNOWN"

        columns.append(
            Column(
                name=col_info["name"],
                type=col_type,
                nullable=col_info.get("nullable", True),
                primary_key=col_info["name"] in pk_columns,
                description=col_info.get("comment", "") or "",
            )
        )
    return columns


def _get_primary_keys(insp, table_name: str, schema: str | None) -> set[str]:
    """Get primary key column names for a table."""
    try:
        pk = insp.get_pk_constraint(table_name, schema=schema)
        return set(pk.get("constrained_columns", []))
    except Exception:
        return set()
