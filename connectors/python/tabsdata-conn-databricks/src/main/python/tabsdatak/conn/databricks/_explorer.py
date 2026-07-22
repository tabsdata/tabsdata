#
#  Copyright 2026 Tabsdata Inc.
#

"""Databricks Unity Catalog metadata explorer.

Databricks has no SQLAlchemy layer: connectivity + catalog metadata come
from the ``databricks-sdk`` WorkspaceClient (Unity Catalog APIs) and
row-level sampling from a ``databricks-sql`` warehouse connection -- the
same libraries the runtime uses (see ``_impl.py``, whose client builders
this reuses). The connector reuses the shared SQL entity model
(``SQLConnMetaModel`` / ``SQLEntity``): Unity Catalog's
catalog -> schema -> table maps onto the model's database -> schema ->
table.
"""

from __future__ import annotations

import logging
from typing import ClassVar, TypeAlias

from tabsdatak.conn.common._explorer import Column
from tabsdatak.conn.common.sql._explorer import SQLConnMetaModel, SQLEntity
from tabsdatak.conn.databricks import DatabricksDestConn, DatabricksSrcConn
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

logger = logging.getLogger(__name__)

# Either Databricks connection flavour -- source or destination.
DatabricksConn: TypeAlias = DatabricksSrcConn | DatabricksDestConn

# Unity Catalog schemas skipped during a full (no-schema-filter) scan.
_SYSTEM_SCHEMAS = frozenset({"information_schema"})


class DatabricksConnectionExplorer(Explorer):
    """Explore a Databricks workspace's Unity Catalog.

    The scan is pinned to the connection's ``catalog``; ``schema``, when
    set, restricts it to that single schema (otherwise every non-system
    schema in the catalog is scanned).
    """

    meta_model: ClassVar[ConnMetaModel] = SQLConnMetaModel()
    entity_type: ClassVar[type[Entity]] = SQLEntity

    def check_connect(self, conn: DatabricksConn) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks import _impl

        client = _impl._build_workspace_client(conn)
        # current_user.me() is the cheapest authenticated request.
        client.current_user.me()

    def metadata(self, conn: DatabricksConn) -> list[SQLEntity]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks import _impl

        catalog = conn._catalog() if conn.catalog is not None else ""
        schema_filter = conn._schema() if conn.schema is not None else None

        client = _impl._build_workspace_client(conn)

        if schema_filter:
            schemas = [schema_filter]
        else:
            try:
                schemas = [
                    s.name
                    for s in client.schemas.list(catalog_name=catalog)
                    if s.name and s.name.lower() not in _SYSTEM_SCHEMAS
                ]
            except Exception:
                logger.warning(
                    "Cannot list schemas in catalog %r", catalog, exc_info=True
                )
                return []

        entities: list[SQLEntity] = []
        for schema_name in schemas:
            try:
                tables = list(
                    client.tables.list(catalog_name=catalog, schema_name=schema_name)
                )
            except Exception:
                logger.warning(
                    "Cannot list tables in %s.%s",
                    catalog,
                    schema_name,
                    exc_info=True,
                )
                continue

            for table in tables:
                entities.append(
                    SQLEntity(
                        connection_type=self.meta_model,
                        # Unity Catalog's catalog.schema.table maps onto the
                        # SQL model's database.schema.table.
                        database=catalog,
                        schema=schema_name,
                        table=table.name or "",
                        columns=_columns(table),
                    )
                )
        return entities

    def data(
        self, conn: DatabricksConn, entity: SQLEntity, limit: int = 100
    ) -> list[dict]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks import _impl

        # Sampling runs SQL, so it needs a warehouse (``_build_sql_connection``
        # resolves ``conn.warehouse`` / ``warehouse_id`` and raises if
        # neither is set); Unity Catalog metadata above needs none.
        client = _impl._build_workspace_client(conn)
        fqn = ".".join(
            f"`{part}`"
            for part in (entity.database, entity.schema, entity.table)
            if part
        )
        connection = _impl._build_sql_connection(client, conn)
        try:
            with connection.cursor() as cursor:
                # ``limit`` is coerced to int so it can't smuggle SQL; the
                # identifiers come from our own Unity Catalog scan.
                cursor.execute(f"SELECT * FROM {fqn} LIMIT {int(limit)}")
                columns = [c[0] for c in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            connection.close()


def _columns(table) -> list[Column]:
    """Map a Unity Catalog ``TableInfo``'s columns to :class:`Column`.

    Unity Catalog exposes no primary-key flag on ``ColumnInfo``, so
    ``primary_key`` is always ``False``.
    """
    columns: list[Column] = []
    for col in table.columns or []:
        col_type = _simplify_type(col.type_name.value if col.type_name else "STRING")
        columns.append(
            Column(
                name=col.name or "",
                type=col_type,
                nullable=col.nullable if col.nullable is not None else True,
                primary_key=False,
                description=col.comment or "",
            )
        )
    return columns


def _simplify_type(raw: str) -> str:
    """Simplify a Unity Catalog type name to a short canonical upper-case
    form, dropping any precision/scale args (``DECIMAL(18, 2)`` -> ``DECIMAL``)."""
    s = raw.strip()
    if "(" in s:
        s = s[: s.index("(")]
    return s.strip().upper()
