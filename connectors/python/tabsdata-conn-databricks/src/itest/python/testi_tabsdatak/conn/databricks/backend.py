#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import polars as pl
from tabsdatax.itest.fixture.backends.databricks import DatabricksDomain
from tabsdatax.itest.fixture.functions.sql import SqlStore

from tabsdatak.api import Secret
from tabsdatak.conn.databricks import DatabricksDestConn, DatabricksSrcConn

# Unity Catalog quotes identifiers with backticks.
DELIMITERS = ("`", "`")

# The connector creates a table with ``CREATE ... AS SELECT * FROM read_files(...)``,
# and read_files contributes its own schema-rescue column -- so every table the
# write path creates carries one column the loaded parquet did not.
RESCUED_DATA = "_rescued_data"


def _conn_fields(domain: DatabricksDomain) -> dict[str, Secret]:
    """Databricks authenticates with a workspace URL plus a personal access token,
    so there is no user/password pair for ``sql_credentials`` to build -- every
    field is its own ``Secret`` read straight off the descriptor."""
    return {
        "host_url": Secret(domain.host),
        "token": Secret(domain.token),
        "volume": Secret(domain.volume),
        "warehouse_id": Secret(domain.warehouse_id),
        "catalog": Secret(domain.catalog),
        "schema": Secret(domain.schema),
    }


def src_conn(domain: DatabricksDomain) -> DatabricksSrcConn:
    return DatabricksSrcConn(**_conn_fields(domain))


def dest_conn(domain: DatabricksDomain) -> DatabricksDestConn:
    return DatabricksDestConn(**_conn_fields(domain))


def store(domain: DatabricksDomain) -> DatabricksStore:
    """A store to read back / drop what a test wrote, over the domain's own
    workspace credentials."""
    return DatabricksStore(domain)


class DatabricksStore(SqlStore):
    """A ``databricks.sql``-backed :class:`SqlStore`. The connector carries no
    SQLAlchemy layer by design, so verification runs over the same warehouse
    driver the write path uses, against the domain's own catalog and schema."""

    def __init__(self, domain: DatabricksDomain) -> None:
        import databricks.sql as dbsql

        self._catalog = domain.catalog
        self._schema = domain.schema
        self._connection = dbsql.connect(
            server_hostname=domain.host,
            http_path=f"/sql/1.0/warehouses/{domain.warehouse_id}",
            access_token=domain.token,
        )

    def quote(self, identifier: str) -> str:
        open_, close = DELIMITERS
        return f"{open_}{identifier}{close}"

    def read_table(self, table: str) -> pl.DataFrame:
        with self._connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self._fqn(table)}")
            return pl.from_arrow(cursor.fetchall_arrow())

    def drop_table(self, table: str) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self._fqn(table)}")

    def column_types(self, table: str) -> dict[str, str]:
        """``name -> type`` as Databricks reports the table -- ``int``,
        ``bigint``, ``double``, ``decimal(20,4)``.

        Read through ``DESCRIBE TABLE`` rather than off a read's arrow schema: a
        widening is a property of the table, and a value that happens to survive
        a round trip says nothing about the column that carried it. The
        partition/metadata block ``DESCRIBE`` appends after the columns opens
        with a blank name, which is where the walk stops.
        """
        with self._connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE TABLE {self._fqn(table)}")
            types = {}
            for name, type_, *_ in cursor.fetchall():
                if not name or name.startswith("#"):
                    break
                types[name] = type_.lower()
            return types

    def _fqn(self, table: str) -> str:
        return ".".join(
            self.quote(part) for part in (self._catalog, self._schema, table)
        )
