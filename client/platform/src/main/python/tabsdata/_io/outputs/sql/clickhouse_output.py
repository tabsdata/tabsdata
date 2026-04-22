#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
from typing_extensions import deprecated

from tabsdata._io.cloud_staging.clickhouse.cloud_storage import (
    AzureClickHouseStorage,
    GCSClickHouseStorage,
    S3ClickHouseStorage,
)
from tabsdata._io.cloud_staging.clickhouse.config import ClickHouseConfig
from tabsdata._io.cloud_staging.clickhouse.dialect import ClickHouseDialect
from tabsdata._io.cloud_staging.common.bulk_loader import CloudStagedBulkLoader
from tabsdata._io.cloud_staging.common.modes import SchemaEvolutionMode, WriteMode
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.connections.clickhouse_connection import ClickHouseConn
from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.outputs.shared_enums import (
    IfTableExistStrategySpec,
    SchemaEvolutionSpec,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._utils.annotations import unstable, unstable_category
from tabsdata._utils.id import encode_id
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode

logger = logging.getLogger(__name__)


@unstable()
@deprecated(
    """Class 'ClickHouseDest' is considered unstable and is subject to change
     in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'ClickHouseDest' may undergo API and feature changes as it evolves"
            " toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class ClickHouseDest(DestinationPlugin):
    """
    :categories: destination

    Destination plugin for writing data to ClickHouse using cloud-staged bulk loading.

    Data is first written as Parquet files to the configured cloud storage staging area
    (S3, Azure Blob, or GCS) and then ingested into ClickHouse via its native table
    functions (``s3()``, ``azureBlobStorage()``).
    """

    @_td_validate_call(ErrorCode.DECE110)
    def __init__(
        self,
        conn: ClickHouseConn | NamedConn,
        destination_tables: str | list[str],
        if_table_exists: IfTableExistStrategySpec = "append",
        schema_evolution: SchemaEvolutionSpec = "none",
    ):
        """
        Initialize a ClickHouseDest instance for writing data to ClickHouse.

        Args:
            conn: The ClickHouse connection configuration, including both the server
                parameters and the cloud staging configuration.
            destination_tables: The table(s) to load data into.  A single table name
                may be provided as a string; multiple tables must be a list of strings.
            if_table_exists: The strategy to follow when the table already exists.
                Defaults to ``'append'``.

                * ``'append'`` — insert new rows into the existing table.
                * ``'replace'`` — truncate the table before loading new rows.
            schema_evolution: The schema evolution strategy to use when the incoming
                data schema differs from the existing table schema.
                Defaults to ``'none'``.

                * ``'none'`` — raise an error if the schema does not match.
                * ``'iceberg'`` — apply Iceberg-style schema evolution (add columns,
                  drop columns, widen types, make columns nullable).
        """
        self.conn = conn
        self._destination_tables = self._normalize_destination_tables(
            destination_tables
        )
        self._if_table_exists = if_table_exists
        self._schema_evolution = schema_evolution

    # ── Properties ────────────────────────────────────────────────────────────────────

    @property
    def conn(self) -> ClickHouseConn | NamedConn:
        """
        The ClickHouse connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.DECE110)
    def conn(self, conn: ClickHouseConn | NamedConn):
        self._conn = conn

    @property
    def destination_tables(self) -> list[str]:
        """
        The table(s) to load data into.
        """
        return self._destination_tables

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @property
    def schema_evolution(self) -> SchemaEvolutionSpec:
        """
        The schema evolution strategy.
        """
        return self._schema_evolution

    # ── Internal helpers ──────────────────────────────────────────────────────────────

    def _normalize_destination_tables(
        self, destination_tables: str | list[str]
    ) -> list[str]:
        if isinstance(destination_tables, str):
            if not destination_tables.strip():
                raise DestinationConfigurationError(
                    ErrorCode.DECE54,
                    self.__class__.__name__,
                    destination_tables,
                )
            return [destination_tables]
        return destination_tables

    def _build_clickhouse_config(self) -> ClickHouseConfig:
        conn = self._conn
        return ClickHouseConfig(
            host=conn.host,
            port=conn.port,
            database=conn.database,
            credentials=conn.credentials,
            secure=conn.secure,
        )

    def _build_cloud_storage(self):
        staging = self._conn.staging
        if isinstance(staging, S3Config):
            return S3ClickHouseStorage(staging)
        if isinstance(staging, GCSConfig):
            return GCSClickHouseStorage(staging)
        if isinstance(staging, AzureConfig):
            return AzureClickHouseStorage(staging)
        raise TypeError(
            f"Unsupported staging config type: {type(staging).__name__}. "
            "Expected S3Config, GCSConfig, or AzureConfig."
        )

    # ── Stream ────────────────────────────────────────────────────────────────────────

    def stream(self, working_dir: str, *results: list[pl.LazyFrame | None]):
        """
        Write each result as a Parquet file and bulk load it into ClickHouse via
        the cloud staging area.

        Args:
            working_dir: Temporary directory for intermediate Parquet files.
            results: One polars LazyFrame (or ``None``) per destination table,
                in the same order as ``destination_tables``.
        """
        logger.debug(f"Triggering {self} stream")
        logger.debug(f"Number of results to write to {self}: {len(results)}")
        logger.debug(
            f"Destination tables configured for {self}: {self.destination_tables}"
        )

        if len(results) != len(self.destination_tables):
            raise ValueError(
                f"Number of results ({len(results)}) does not match "
                f"the number of destination tables ({len(self.destination_tables)}) "
                f"for {self}. No data stored in {self.__class__.__name__}."
            )

        working_path = Path(working_dir)
        tables: dict[str, list[Path]] = {}
        for table_name, result in zip(self.destination_tables, results):
            if result is None:
                logger.debug(f"Skipping table '{table_name}': result is None")
                continue
            parquet_path = working_path / f"{table_name}.parquet"
            result.collect().write_parquet(str(parquet_path))
            tables[table_name] = [parquet_path]

        if not tables:
            logger.debug("No non-None results to load; skipping bulk load")
            return

        _, upload_id = encode_id()
        write_mode = (
            WriteMode.REPLACE
            if self._if_table_exists == "replace"
            else WriteMode.APPEND
        )
        schema_evo = (
            SchemaEvolutionMode.ICEBERG
            if self._schema_evolution == "iceberg"
            else SchemaEvolutionMode.NONE
        )

        cloud_storage = self._build_cloud_storage()
        ch_config = self._build_clickhouse_config()
        dialect = ClickHouseDialect(ch_config, cloud_storage)
        loader = CloudStagedBulkLoader(
            cloud_storage=cloud_storage,
            dialect=dialect,
            write_mode=write_mode,
            schema_evolution=schema_evo,
            upload_id=upload_id,
        )
        loader.load(tables)
        logger.debug(f"Results written to {self} destination successfully")

    # ── Dunder methods ────────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"destination_tables={self.destination_tables}, "
            f"if_table_exists='{self.if_table_exists}', "
            f"schema_evolution='{self.schema_evolution}')"
        )
