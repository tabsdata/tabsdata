#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Optional

import polars as pl
from pydantic import PositiveInt

from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.constants import _LoaderSpec
from tabsdata._io.outputs.shared_enums import (
    IfTableExistStrategySpec,
    MySQLDestWriterSpec,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._io.sql_io_helpers.sql_helper_factories import get_sql_dest_helper
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode

logger = logging.getLogger(__name__)


class MySQLDest(DestinationPlugin):
    """
    :categories: destination

    Destination plugin for writing data to MySQL.
    """

    @_td_validate_call(ErrorCode.DECE106)
    def __init__(
        self,
        conn: MySQLConn | NamedConn,
        destination_tables: str | list[str],
        if_table_exists: IfTableExistStrategySpec = "append",
        transactional: bool = True,
        chunk_size: PositiveInt = 100000,
        loader: MySQLDestWriterSpec = "polars_sqlalchemy",
    ):
        """
        Initialize a MySQLDest instance for writing data to MySQL.

        Args:
            conn: The MySQL connection configuration.
            destination_tables: The tables to create. If multiple tables are provided,
                they must be provided as a list.
            if_table_exists: The strategy to
                follow when the table already exists. Defaults to ‘append’.
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.
            transactional: Whether to use transactions for writing data to ensure
                consistency. Defaults to True
            chunk_size: The chunk size for writing large results in batches.
                Defaults to 100000.
            loader: The data processing loader to use for executing DB write operation.
        """
        self.conn = conn
        self._destination_tables = self._normalize_destination_tables(
            destination_tables
        )
        self._if_table_exists = if_table_exists
        self._transactional = transactional
        self._chunk_size = chunk_size
        self._loader = _LoaderSpec(loader)

    @property
    def conn(self) -> MySQLConn | NamedConn:
        """
        The MySQL connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.DECE106)
    def conn(self, conn: MySQLConn | NamedConn):
        self._conn = conn

    @property
    def destination_tables(self) -> list[str]:
        """
        The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_tables

    def _normalize_destination_tables(
        self, destination_tables: str | list[str]
    ) -> list[str]:
        """
        Normalize the "destination_tables" parameter.
        """
        if isinstance(destination_tables, str):
            if not destination_tables.strip():
                raise DestinationConfigurationError(
                    ErrorCode.DECE54,
                    self.__class__.__name__,
                    destination_tables,
                )
            return [destination_tables]
        return destination_tables

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @property
    def chunk_size(self) -> int:
        """
        The chunk size for writing large results in batches.
        """
        return self._chunk_size

    @property
    def transactional(self) -> bool:
        """
        Whether to use transactions for writing data to ensure consistency.
        """
        return self._transactional

    @property
    def loader(self) -> MySQLDestWriterSpec:
        """
        The data processing loader to use for executing DB write operation.
        """
        return self._loader.value

    def stream(self, working_dir: str, *results: list[Optional[pl.LazyFrame]]):
        """
        Store the results into the MySQL database.

        Args:
            working_dir: The working directory where the results are stored.
            results: The results to store in the SQL destination.
        """
        logger.debug(f"Triggering {self} stream")
        logger.debug(f"Number of results to write to {self}: {len(results)}")
        logger.debug(
            f"Destination tables configured for {self}: {self.destination_tables}"
        )

        if len(results) != len(self.destination_tables):
            raise ValueError(
                f"Number of results ({len(results)}) does not match number of"
                f" destination tables ({len(self.destination_tables)}) in {self}. No"
                f" data stored in {self.__class__.__name__}."
            )

        dest_helper = get_sql_dest_helper(self)
        dest_helper._stream(working_dir, results)
        logger.debug(f"Results written to {self} destination successfully")

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(conn={self.conn},"
            f" destination_tables={self.destination_tables},"
            f" if_table_exists='{self.if_table_exists}',"
            f" transactional={self.transactional}, chunk_size={self.chunk_size},"
            f" loader='{self.loader}')"
        )
