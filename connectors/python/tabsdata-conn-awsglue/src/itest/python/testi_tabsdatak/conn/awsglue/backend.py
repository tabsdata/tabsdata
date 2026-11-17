#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to the connector's connection and to direct
catalog access"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from tabsdatax.itest.fixture.backends.awsglue import AwsGlueDomain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.sql import SqlStore

from tabsdatak.api import Secret
from tabsdatak.conn.awsglue import AwsGlueDestConn
from tabsdatak.conn.common.types import AwsAccessSecretKey

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.table import Table

TRX_PROPERTY = "tabsdata.trx-id"


def dest_conn(domain: AwsGlueDomain, **overrides) -> AwsGlueDestConn:
    # Tables the connector creates land under the domain's own prefix, so
    # destroying the domain clears whatever a run left behind.
    kwargs: dict = dict(
        region=domain.region,
        credentials=AwsAccessSecretKey(
            access_key_id=Secret(domain.access_key),
            secret_access_key=Secret(domain.secret_key),
        ),
        bucket=domain.bucket,
        base_path="/" + str(domain.base_path).strip("/"),
        database=domain.glue_database,
        catalog_id=domain.catalog_id or None,
    )
    kwargs.update(overrides)
    return AwsGlueDestConn(**kwargs)


class TrxCtx(PluginDestCtx):
    """A destination context of a trx of its own.

    The shared stub gives every context the same trx id, and the connector
    skips a write whose trx the table already holds -- so a test that writes a
    table twice needs each write to say which trx it belongs to.
    """

    def __init__(self, work: Path, transaction_id: str | None = None) -> None:
        super().__init__(work)
        self._trx_id = transaction_id or uuid.uuid4().hex

    @property
    def transaction_id(self) -> str:
        return self._trx_id


def store(domain: AwsGlueDomain) -> GlueStore:
    """A store to seed / read back / drop what a test wrote, over the domain's own
    credentials."""
    return GlueStore(domain)


class GlueStore(SqlStore):
    """A pyiceberg-backed :class:`SqlStore`, built from the catalog definition tdd
    records for the domain rather than from the connector's connection. The
    connector only ever writes, so every verification goes through this."""

    def __init__(self, domain: AwsGlueDomain) -> None:
        from pyiceberg.catalog import load_catalog

        definition = dict(domain.catalog_definition)
        self._catalog = load_catalog(definition.pop("name", "default"), **definition)
        self._database = domain.glue_database

    def quote(self, identifier: str) -> str:
        return identifier

    def table(self, table: str) -> Table:
        return self._catalog.load_table((self._database, table))

    def exists(self, table: str) -> bool:
        return self._catalog.table_exists((self._database, table))

    def read_table(self, table: str) -> pl.DataFrame:
        return pl.DataFrame(pl.from_arrow(self.table(table).scan().to_arrow()))

    def count(self, table: str) -> int:
        return self.read_table(table).height

    def column_types(self, table: str) -> dict[str, str]:
        """``name -> type`` as Iceberg holds the table."""
        return {
            field.name: str(field.field_type)
            for field in self.table(table).schema().fields
        }

    def snapshots(self, table: str) -> list[tuple[str, str | None]]:
        """``(operation, trx id)`` per snapshot, oldest first."""
        return [
            (
                snapshot.summary.operation.value,
                snapshot.summary.additional_properties.get(TRX_PROPERTY),
            )
            for snapshot in self.table(table).metadata.snapshots
        ]

    def data_files(self, table: str) -> list[str]:
        return self.table(table).inspect.data_files().column("file_path").to_pylist()

    def glue_parameters(self, table: str) -> dict[str, str]:
        """The parameters of the table as the Glue Data Catalog holds them, which
        is what Athena reads to tell an Iceberg table from any other."""
        found = self._catalog.glue.get_table(DatabaseName=self._database, Name=table)
        return found["Table"].get("Parameters", {})

    def drop_table(self, table: str) -> None:
        from pyiceberg.exceptions import NoSuchTableError

        try:
            self._catalog.purge_table((self._database, table))
        except NoSuchTableError:
            pass

    def create_table(self, table: str, rows: pa.Table, **kwargs) -> None:
        """Create `table` and append `rows` the way another Iceberg writer would,
        leaving its location to the catalog."""
        self.drop_table(table)
        created = self._catalog.create_table(
            (self._database, table), schema=rows.schema, **kwargs
        )
        created.append(rows)

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        frame = pl.DataFrame(rows, schema={"id": pl.Int64, "name": pl.String})
        self.create_table(table, frame.to_arrow())
