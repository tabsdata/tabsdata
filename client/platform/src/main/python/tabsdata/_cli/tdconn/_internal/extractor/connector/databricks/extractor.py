#
# Copyright (c) 2026 Tabsdata Inc.
#

"""Databricks Unity Catalog metadata extractor."""

import sys
from typing import ClassVar

from databricks.sdk import WorkspaceClient

from tabsdata._cli.tdconn._internal.extractor.api import (
    Column,
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
    SQLConnMetaModel,
    SQLEntity,
)
from tabsdata.connector.databricks._connection import DatabricksConn

# Databricks system schemas to skip
SYSTEM_SCHEMAS = frozenset({
    "information_schema",
    "default",  # may want to include this — remove if needed
})


class DatabricksConnectionExtractor(ConnectionExtractor):
    """Extract table metadata from Databricks Unity Catalog.

    The ``info`` dict matches ``DatabricksConn``'s JSON shape:
        host_url: Databricks workspace URL (https://...)
        token: Databricks personal access token

    Extractor-level options:
        catalog: Unity Catalog name to scan (required).
        schema: optional — restrict the scan to a specific schema.
    """

    meta_model: ClassVar[ConnMetaModel] = SQLConnMetaModel()
    entity_type: ClassVar[type[Entity]] = SQLEntity

    def check_connect(self, info: dict) -> None:
        w = self._client(info)
        # current_user.me() is the cheapest authenticated request.
        w.current_user.me()

    def metadata(self, info: dict) -> list[SQLEntity]:
        catalog = info.get("catalog", "")
        schema_filter = info.get("schema")

        w = self._client(info)

        # Get schemas to scan
        if schema_filter:
            schemas = [schema_filter]
        else:
            try:
                schemas = [
                    s.name
                    for s in w.schemas.list(catalog_name=catalog)
                    if s.name and s.name.lower() not in SYSTEM_SCHEMAS
                ]
            except Exception as e:
                print(
                    f"  Warning: cannot list schemas in catalog '{catalog}': {e}",
                    file=sys.stderr,
                )
                return []

        entities = []
        for schema_name in schemas:
            try:
                tables = list(
                    w.tables.list(
                        catalog_name=catalog,
                        schema_name=schema_name,
                    )
                )
            except Exception as e:
                print(
                    f"  Warning: cannot list tables in {catalog}.{schema_name}: {e}",
                    file=sys.stderr,
                )
                continue

            for table in tables:
                columns = []
                for col in table.columns or []:
                    col_type = _simplify_type(
                        col.type_name.value if col.type_name else "STRING"
                    )
                    columns.append(
                        Column(
                            name=col.name or "",
                            type=col_type,
                            nullable=col.nullable if col.nullable is not None else True,
                            primary_key=False,
                            description=col.comment or "",
                        )
                    )

                entities.append(
                    SQLEntity(
                        connection_type=self.meta_model,
                        database=catalog,
                        schema=schema_name,
                        table=table.name or "",
                        columns=columns,
                    )
                )

        return entities

    @staticmethod
    def _client(info: dict) -> WorkspaceClient:
        conn = DatabricksConn._load_from_dict({
            k: v for k, v in info.items() if k not in ("catalog", "schema")
        })
        host = conn.host_url or ""
        # DatabricksConn keeps the token as a Secret when given a Secret, but
        # plain strings stay as plain strings on the instance.
        token = (
            conn.token.secret_value
            if hasattr(conn.token, "secret_value")
            else conn.token
        )
        if host and not host.startswith("https://"):
            host = f"https://{host}"
        return WorkspaceClient(host=host, token=token)


def _simplify_type(raw: str) -> str:
    """Simplify Databricks type names."""
    s = raw.strip()
    if "(" in s:
        s = s[: s.index("(")]
    return s.strip().upper()
