#
# Copyright 2026 Tabs Data Inc.
#

import logging
from typing import Optional

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, sessionmaker

from tabsdata._io.outputs.sql_outputs import (
    _chunk_to_intermediate_files,
    _store_result_in_sql_table,
)
from tabsdata._io.sql_io_helpers.helper_utils import obtain_sqlalchemy_url
from tabsdata._io.sql_io_helpers.sql_helper_configs import SQLDestConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SQLAlchemyDestHelper:
    """
    Helper class for writing results to a SQL Destination using SQLAlchemy.
    """

    def __init__(self, dest: SQLDestConfig):
        """
        Initialize the SQLAlchemyDestHelper with a given SQL destination configuration.
        Args:
            dest: The SQL destination configuration.
        """
        self._dest = dest

    @property
    def dest(self) -> SQLDestConfig:
        """
        The SQL destination configuration.
        """
        return self._dest

    @dest.setter
    def dest(self, dest: SQLDestConfig):
        """
        Set the SQL destination configuration
        Args:
            dest: The SQL destination configuration.
        """
        if not isinstance(dest, SQLDestConfig):
            raise TypeError(
                "The 'dest' parameter must be an instance of"
                f" {SQLDestConfig.__name__}, got {type(dest).__name__} instead."
            )
        self._dest = dest

    def _stream(self, working_dir: str, results: tuple[Optional[pl.LazyFrame]]):
        """
        Write results into the SQL Destination

        Args:
            working_dir: The working directory to use for intermediate files.
            results: The results to store in the SQL destination.
        """
        conn_url = obtain_sqlalchemy_url(self.dest.conn)
        transactional = self.dest.transactional

        engine = create_engine(conn_url, connect_args=self.dest.conn.cx_dst_configs_sql)

        intermediate_files = _chunk_to_intermediate_files(
            destination_table_configuration=self.dest.destination_tables,
            results=results,
            working_dir=working_dir,
        )

        try:
            if transactional:
                Session = sessionmaker(bind=engine)
                session = Session()
                with session.begin():
                    logger.debug("Transaction started for SQL destination")
                    self._store_results(session, intermediate_files)
            else:
                with engine.connect() as connection:
                    logger.debug("No transaction started for SQL destination")
                    self._store_results(connection, intermediate_files)
        except Exception as e:
            logger.warning(f"Error writing data to SQL destination: {e}")
            raise e
        finally:
            engine.dispose()

    def _store_results(
        self,
        connection: Session | Connection,
        intermediate_files: list[str],
    ):
        for intermediate_file, destination_table in zip(
            intermediate_files, self.dest.destination_tables
        ):
            _store_result_in_sql_table(
                intermediate_file=intermediate_file,
                session=connection,
                destination_table=destination_table,
                if_table_exists=self.dest.if_table_exists,
                chunk_size=self.dest.chunk_size,
            )
