#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from typing import List

from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.outputs.shared_enums import IfTableExistStrategySpec
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.snowflake._connection import SnowflakeConn
from tabsdata.connector.snowflake._connector import SnowflakeDestination
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SnowflakeDest(DestinationPlugin):
    """Write to Snowflake.

    Required: conn (SnowflakeConn),
    destination_tables (str or list[str]).
    Destination plugin for writing data to Snowflake.
    """

    @_td_validate_call(ErrorCode.DECE120)
    def __init__(
        self,
        conn: SnowflakeConn | NamedConn,
        destination_table: str | list[str],
        if_table_exists: IfTableExistStrategySpec = "append",
        stage: str | None = None,
        **kwargs,
    ):
        """
        Initialize a SnowflakeDest instance.

        Args:
            conn: The Snowflake connection configuration (account, user,
                password, role, database, schema, warehouse).
            destination_table: Name or list of names of the destination
                tables to be created, replaced, or appended in Snowflake.
            if_table_exists: ``"append"`` or ``"replace"``. Defaults to
                ``"append"``.
            stage: Optional Snowflake stage name. If None, a temporary one
                is created per run.
        """
        self._destination_table = destination_table
        self._if_table_exists = if_table_exists
        self._stage = stage
        self._kwargs = kwargs
        self._snowflake_destination: SnowflakeDestination | None = None
        # Assigning conn triggers the setter, which (re)builds the inner
        # SnowflakeDestination when conn is a resolved SnowflakeConn.
        self.conn = conn

    def _build_destination(self) -> SnowflakeDestination:
        connection_parameters = {
            "account": self._conn.account,
            "user": self._conn.user,
            "password": self._conn.password,
            "role": self._conn.role,
            "database": self._conn.database,
            "schema": self._conn.schema,
            "warehouse": self._conn.warehouse,
        }
        return SnowflakeDestination(
            connection_parameters=connection_parameters,
            destination_table=self._destination_table,
            if_table_exists=self._if_table_exists,
            stage=self._stage,
            **self._kwargs,
        )

    @property
    def conn(self) -> SnowflakeConn | NamedConn:
        """
        The Snowflake connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.DECE120)
    def conn(self, conn: SnowflakeConn | NamedConn):
        self._conn = conn
        if isinstance(conn, SnowflakeConn):
            self._snowflake_destination = self._build_destination()
        else:
            self._snowflake_destination = None

    @property
    def destination_table(self) -> List[str]:
        """
        The destination table(s) where data will be stored.
        """
        if self._snowflake_destination is not None:
            return self._snowflake_destination.destination_table
        return (
            [self._destination_table]
            if isinstance(self._destination_table, str)
            else self._destination_table
        )

    @destination_table.setter
    def destination_table(self, destination_table: str | list[str]):
        if self._snowflake_destination is not None:
            self._snowflake_destination.destination_table = destination_table
        self._destination_table = destination_table

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        The strategy to follow when the target table already exists.
        """
        if self._snowflake_destination is not None:
            return self._snowflake_destination.if_table_exists
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: IfTableExistStrategySpec):
        if self._snowflake_destination is not None:
            self._snowflake_destination.if_table_exists = if_table_exists
        self._if_table_exists = if_table_exists

    @property
    def stage(self) -> str | None:
        """
        The Snowflake stage name. May be mutated at write time when a
        temporary stage is created.
        """
        if self._snowflake_destination is not None:
            return self._snowflake_destination.stage
        return self._stage

    @stage.setter
    def stage(self, stage: str | None):
        if self._snowflake_destination is not None:
            self._snowflake_destination.stage = stage
        self._stage = stage

    def stream(self, working_dir: str, *results):
        """
        Write the results to Snowflake.
        """
        if self._snowflake_destination is None:
            raise RuntimeError(
                "Cannot call stream() before the 'conn' param has been resolved to a "
                "SnowflakeConn."
            )
        return self._snowflake_destination.stream(working_dir, *results)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"destination_table={self.destination_table}, "
            f"if_table_exists={self.if_table_exists!r}, "
            f"stage={self.stage!r})"
        )
