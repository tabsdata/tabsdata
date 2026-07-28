#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import polars as pl
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain
from tabsdatax.itest.fixture.functions.sql import SqlStore

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn

# Snowflake quotes identifiers with double quotes.
DELIMITERS = ('"', '"')


def dest_conn(domain: SnowflakeDomain) -> SnowflakeDestConn:
    # The domain's Personal Access Token rides in the password slot -- the driver
    # accepts either there. Database and schema go in bare: tdx provisions them
    # unquoted, so Snowflake upper-folds the reference onto the stored identifier.
    return SnowflakeDestConn(
        account=domain.account,
        credentials=UserPassword(
            user=Secret(domain.username), password=Secret(domain.pat)
        ),
        database=domain.database,
        schema=domain.schema,
        warehouse=domain.warehouse,
    )


def store(domain: SnowflakeDomain) -> SnowflakeStore:
    """A store to seed / read back / drop what a test wrote, over the domain's own
    credentials."""
    return SnowflakeStore(domain)


class SnowflakeStore(SqlStore):
    """A snowflake.connector-backed :class:`SqlStore`. The connector only ever
    writes, so every verification goes through this rather than through it."""

    def __init__(self, domain: SnowflakeDomain) -> None:
        import snowflake.connector

        self._database = domain.database
        self._schema = domain.schema
        self._conn = snowflake.connector.connect(
            account=domain.account,
            user=domain.username,
            password=domain.pat,
            warehouse=domain.warehouse,
            database=domain.database,
            schema=domain.schema,
        )

    def quote(self, identifier: str) -> str:
        open_, close = DELIMITERS
        return f"{open_}{identifier}{close}"

    def read_table(self, table: str) -> pl.DataFrame:
        """The table's rows, column names lower-cased -- Snowflake stores unquoted
        identifiers upper-folded, so a read back would otherwise never line up with
        the parquet the connector loaded."""
        rows, columns = self._query(f"SELECT * FROM {self._fqn(table)}")
        return pl.DataFrame(rows, schema=[c.lower() for c in columns], orient="row")

    def drop_table(self, table: str) -> None:
        self._execute(f"DROP TABLE IF EXISTS {self._fqn(table)}")

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        fqn = self._fqn(table)
        self._execute(f"DROP TABLE IF EXISTS {fqn}")
        self._execute(f"CREATE TABLE {fqn} (id INTEGER, name VARCHAR(64))")
        self._execute(
            f"INSERT INTO {fqn} (id, name) VALUES (%s, %s)",
            [(row["id"], row["name"]) for row in rows],
        )

    def count(self, table: str) -> int:
        rows, _ = self._query(f"SELECT count(*) FROM {self._fqn(table)}")
        return rows[0][0]

    def _fqn(self, table: str) -> str:
        # Every part is referenced bare so Snowflake upper-folds it onto the
        # unquoted objects tdx provisions -- the same way the connector does.
        return f"{self._database}.{self._schema}.{table}"

    def _query(self, sql: str) -> tuple[list[tuple], list[str]]:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall(), [c[0] for c in cursor.description]
        finally:
            cursor.close()

    def _execute(self, sql: str, params: list[tuple] | None = None) -> None:
        cursor = self._conn.cursor()
        try:
            if params is None:
                cursor.execute(sql)
            else:
                cursor.executemany(sql, params)
        finally:
            cursor.close()
