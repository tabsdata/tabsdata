#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import polars as pl

from tabsdata._io.cloud_staging.common.bulk_loader import CloudStagedBulkLoader
from tabsdata._io.cloud_staging.common.modes import SchemaEvolutionMode, WriteMode
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.cloud_staging.druid.cloud_storage import (
    AzureDruidStorage,
    GCSDruidStorage,
    S3DruidStorage,
)
from tabsdata._io.cloud_staging.druid.config import DruidConfig
from tabsdata._io.cloud_staging.druid.dialect import DUMMY_TIME_COLUMN, DruidDialect
from tabsdata._io.connections.druid_connection import DruidConn
from tabsdata._io.outputs.shared_enums import IfTableExistStrategySpec
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._utils.id import encode_id
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode

logger = logging.getLogger(__name__)


class DruidDest(DestinationPlugin):
    """
    :categories: destination

    Destination plugin for writing data to Apache Druid using cloud-staged
    bulk loading.

    Data is first written as Parquet files to the configured cloud storage
    staging area (S3, Azure Blob, or GCS) and then ingested into Druid via
    a native batch ``index_parallel`` task submitted to the Overlord REST API.
    """

    @_td_validate_call(ErrorCode.DECE112)
    def __init__(
        self,
        conn: DruidConn,
        destination_tables: str | list[str],
        if_table_exists: IfTableExistStrategySpec = "append",
    ):
        """
        Initialize a DruidDest instance for writing data to Apache Druid.

        Args:
            conn: The Druid connection configuration, including both the
                Overlord parameters and the cloud staging configuration.
            destination_tables: The datasource(s) to load data into.  A single
                datasource name may be provided as a string; multiple names
                must be a list of strings.
            if_table_exists: The strategy to follow when the datasource already
                exists.  Defaults to ``'append'``.

                * ``'append'`` — add new segments alongside existing ones.
                * ``'replace'`` — drop existing segments and replace with
                  new data.
        """
        self._conn = conn
        self._destination_tables = self._normalize_destination_tables(
            destination_tables
        )
        self._if_table_exists = if_table_exists

    # ── Properties ────────────────────────────────────────────────────────────────────

    @property
    def conn(self) -> DruidConn:
        """
        The Druid connection configuration.
        """
        return self._conn

    @property
    def destination_tables(self) -> list[str]:
        """
        The datasource(s) to load data into.
        """
        return self._destination_tables

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        The strategy to follow when the datasource already exists.
        """
        return self._if_table_exists

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

    def _build_druid_config(self) -> DruidConfig:
        conn = self._conn
        auth_user = None
        auth_password = None
        if conn.credentials is not None:
            auth_user = conn.credentials.user.secret_value
            auth_password = conn.credentials.password.secret_value
        return DruidConfig(
            overlord_url=conn.overlord_url,
            time_column=conn.time_column,
            segment_granularity=conn.segment_granularity,
            max_concurrent_subtasks=conn.max_concurrent_subtasks,
            max_rows_per_segment=conn.max_rows_per_segment,
            max_rows_in_memory=conn.max_rows_in_memory,
            http_timeout=conn.http_timeout,
            auth_user=auth_user,
            auth_password=auth_password,
        )

    def _build_cloud_storage(self):
        staging = self._conn.staging
        if isinstance(staging, S3Config):
            return S3DruidStorage(staging)
        if isinstance(staging, GCSConfig):
            return GCSDruidStorage(staging)
        if isinstance(staging, AzureConfig):
            return AzureDruidStorage(staging)
        raise TypeError(
            f"Unsupported staging config type: {type(staging).__name__}. "
            "Expected S3Config, GCSConfig, or AzureConfig."
        )

    # ── Stream ────────────────────────────────────────────────────────────────────────

    def stream(self, working_dir: str, *results: list[Optional[pl.LazyFrame]]):
        """
        Write each result as a Parquet file and bulk load it into Druid via
        the cloud staging area.

        Args:
            working_dir: Temporary directory for intermediate Parquet files.
            results: One polars LazyFrame (or ``None``) per destination
                datasource, in the same order as ``destination_tables``.
        """
        logger.debug("Triggering %s stream", self)
        logger.debug("Number of results to write to %s: %d", self, len(results))
        logger.debug(
            "Destination tables configured for %s: %s", self, self.destination_tables
        )

        if len(results) != len(self.destination_tables):
            raise ValueError(
                f"Number of results ({len(results)}) does not match "
                f"the number of destination tables ({len(self.destination_tables)}) "
                f"for {self}. No data stored in {self.__class__.__name__}."
            )

        working_path = Path(working_dir)
        uses_dummy_time = self._conn.time_column is None
        tables: dict[str, list[Path]] = {}
        for table_name, result in zip(self.destination_tables, results):
            if result is None:
                logger.debug("Skipping table '%s': result is None", table_name)
                continue
            if uses_dummy_time and DUMMY_TIME_COLUMN in result.columns:
                raise ValueError(
                    f"Data for table '{table_name}' contains a column named "
                    f"'{DUMMY_TIME_COLUMN}' which is reserved by the Druid "
                    f"connector as a synthetic timestamp when no time_column "
                    f"is configured.  Either rename the column in your data or "
                    f"set time_column on your DruidConn to use one of your own "
                    f"columns as the Druid __time primary timestamp."
                )
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

        cloud_storage = self._build_cloud_storage()
        druid_config = self._build_druid_config()
        dialect = DruidDialect(druid_config, cloud_storage)
        try:
            loader = CloudStagedBulkLoader(
                cloud_storage=cloud_storage,
                dialect=dialect,
                write_mode=write_mode,
                schema_evolution=SchemaEvolutionMode.NONE,
                upload_id=upload_id,
            )
            loader.load(tables)
        finally:
            dialect.close()
        logger.debug("Results written to %s destination successfully", self)

    # ── Dunder methods ────────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"destination_tables={self.destination_tables}, "
            f"if_table_exists='{self.if_table_exists}')"
        )
