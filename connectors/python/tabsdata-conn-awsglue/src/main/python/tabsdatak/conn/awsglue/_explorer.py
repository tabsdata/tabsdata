#
# Copyright 2026 Tabsdata Inc.
#

"""AWS Glue Data Catalog metadata explorer.

The catalog is read through the same Glue client the write path uses. The
connector reuses the shared SQL entity model (`SQLConnMetaModel` /
`SQLEntity`): a Glue database maps onto the model's database, and the schema
level is left empty.
"""

from __future__ import annotations

import logging
from typing import ClassVar

from tabsdatak.conn.awsglue import AwsGlueDestConn

# noinspection PyProtectedMember
from tabsdatak.conn.common._explorer import Column
from tabsdatak.conn.common.sql._explorer import SQLConnMetaModel, SQLEntity
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

logger = logging.getLogger(__name__)

_ICEBERG = "ICEBERG"


class AwsGlueConnectionExplorer(Explorer):
    """Explore the Iceberg tables of an AWS Glue Data Catalog.

    The scan covers the connection's `database` when it names one, and every
    database of the catalog otherwise. Only Iceberg tables are reported: they
    are the ones the destination writes.
    """

    meta_model: ClassVar[ConnMetaModel] = SQLConnMetaModel()
    entity_type: ClassVar[type[Entity]] = SQLEntity

    def check_connect(self, conn: AwsGlueDestConn) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.awsglue import _impl

        glue = _impl.glue_catalog(conn).glue
        database = conn._database()
        if database is None:
            glue.get_databases(MaxResults=1)
        else:
            glue.get_database(Name=database)

    def metadata(self, conn: AwsGlueDestConn) -> list[SQLEntity]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.awsglue import _impl

        glue = _impl.glue_catalog(conn).glue
        database = conn._database()
        if database is None:
            try:
                databases = sorted(
                    found["Name"]
                    for page in glue.get_paginator("get_databases").paginate()
                    for found in page["DatabaseList"]
                )
            except Exception:
                logger.warning("Cannot list the databases", exc_info=True)
                return []
        else:
            databases = [database]

        entities: list[SQLEntity] = []
        for database in databases:
            try:
                tables = [
                    table
                    for page in glue.get_paginator("get_tables").paginate(
                        DatabaseName=database
                    )
                    for table in page["TableList"]
                ]
            except Exception:
                logger.warning("Cannot list the tables in %s", database, exc_info=True)
                continue
            for table in sorted(tables, key=lambda found: found["Name"]):
                if not _is_iceberg(table):
                    continue
                entities.append(
                    SQLEntity(
                        connection_type=self.meta_model,
                        database=database,
                        table=table["Name"],
                        columns=_columns(table),
                    )
                )
        return entities

    def data(
        self, conn: AwsGlueDestConn, entity: SQLEntity, limit: int = 100
    ) -> list[dict]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.awsglue import _impl

        table = _impl.glue_catalog(conn).load_table((entity.database, entity.table))
        return table.scan(limit=int(limit)).to_arrow().to_pylist()


def _is_iceberg(table: dict) -> bool:
    return str(table.get("Parameters", {}).get("table_type", "")).upper() == _ICEBERG


def _columns(table: dict) -> list[Column]:
    return [
        Column(
            name=column["Name"],
            type=_simplify_type(column.get("Type", "")),
            nullable=_nullable(column),
            description=column.get("Comment", ""),
        )
        for column in table.get("StorageDescriptor", {}).get("Columns", [])
    ]


def _nullable(column: dict) -> bool:
    optional = column.get("Parameters", {}).get("iceberg.field.optional", "true")
    return str(optional).lower() != "false"


def _simplify_type(raw: str) -> str:
    """A Glue type name in a short upper-case form, without its arguments
    (`decimal(18,2)` -> `DECIMAL`, `array<int>` -> `ARRAY`)."""
    for opening in ("(", "<"):
        raw = raw.partition(opening)[0]
    return raw.strip().upper()
