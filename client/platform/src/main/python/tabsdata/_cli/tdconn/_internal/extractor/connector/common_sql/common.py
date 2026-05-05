#
# Copyright (c) 2026 Tabsdata Inc.
#

"""SQLAlchemy-based metadata extraction — common base class and helpers."""

from abc import abstractmethod
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

from tabsdata._cli.tdconn._internal.extractor.api import (
    Column,
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
    SQLConnMetaModel,
    SQLEntity,
)  # noqa: F401 — Entity is re-exported by subclasses via entity_type


class SqlAlchemyConnectionExtractor(ConnectionExtractor):
    """Abstract base for SQLAlchemy-based metadata and data extraction.

    Subclasses implement:
        make_url()          — build a SQLAlchemy connection URL
        system_schemas()    — schemas to skip
        schema_filter()     — which info key restricts the scan
        map_entity_fields() — how to map schema to entity database/schema

    All SQLAlchemy-backed extractors share a :class:`SQLConnMetaModel`.
    Concrete subclasses inherit that default rather than parameterising
    it; the binding is fixed at the class level.
    """

    meta_model: ClassVar[ConnMetaModel] = SQLConnMetaModel()
    entity_type: ClassVar[type[Entity]] = SQLEntity

    @abstractmethod
    def make_url(self, info: dict) -> str | engine.URL:
        """Build a SQLAlchemy connection URL from connection info."""
        ...

    def system_schemas(self) -> frozenset[str]:
        """Return the set of system/internal schemas to skip."""
        return frozenset()

    def schema_filter(self, info: dict) -> str | None:
        """Extract the schema/owner/database filter from connection info."""
        return None

    def map_entity_fields(self, info: dict, schema: str | None) -> dict:
        """Map the iterated schema name to entity database/schema fields."""
        return {
            "database": info.get("database", "") or schema or "",
            "schema": schema or "",
        }

    def engine_kwargs(self, info: dict) -> dict:
        """Return extra kwargs to pass to :func:`sqlalchemy.create_engine`.

        Default implementation returns ``{}``. Override when a dialect
        needs auth material that can't be expressed in the URL (e.g.
        BigQuery's ``credentials_info`` / ``credentials_path``).
        """
        return {}

    def check_connect(self, info: dict) -> None:
        url = self.make_url(info)
        eng = create_engine(url, **self.engine_kwargs(info))
        try:
            with eng.connect() as conn:
                # SELECT 1 is the most widely supported connectivity probe.
                conn.execute(sa_text("SELECT 1"))
        finally:
            eng.dispose()

    def metadata(self, info: dict) -> list[SQLEntity]:
        url = self.make_url(info)
        restrict_to = self.schema_filter(info)

        eng = create_engine(url, **self.engine_kwargs(info))
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
            fields = self.map_entity_fields(info, schema)

            for table_name in table_names + view_names:
                columns = _get_columns(insp, table_name, schema)
                pk_columns = _get_primary_keys(insp, table_name, schema)

                for col in columns:
                    if col.name in pk_columns:
                        col.primary_key = True

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

    def data(self, info: dict, entity: Entity, limit: int = 100) -> list[dict]:
        url = self.make_url(info)
        eng = create_engine(url, **self.engine_kwargs(info))
        sql_entity = entity  # type: SQLEntity
        schema = sql_entity.schema or None
        t = sa_table(
            sql_entity.table,
            *[sa_column(c.name) for c in sql_entity.columns],
            schema=schema,
        )
        stmt = sa_select(t).limit(limit)
        try:
            with eng.connect() as conn:
                result = conn.execute(stmt)
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


def _get_columns(insp, table_name: str, schema: str | None) -> list[Column]:
    """Get column metadata for a table."""
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
                primary_key=False,
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
