#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import uuid
from typing import Any

import polars as pl
from sqlalchemy import TextClause, create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from tabsdata._io.constants import (
    _LoaderSpec,
)
from tabsdata._io.sql_io_helpers.helper_utils import (
    get_numpy_dtype_from_polars,
    obtain_sqlalchemy_url,
)
from tabsdata._io.sql_io_helpers.sql_helper_configs import SQLSourceConfig
from tabsdata.exceptions import ErrorCode, TabsDataException
from tabsdata.tableframe.typing import DataType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SQLAlchemySourceHelper:
    """
    Helper class for executing SQL inputs using SQLAlchemy.
    """

    def __init__(self, source: SQLSourceConfig):
        """
        Initialize the SQLAlchemySourceHelper with a given SQL source configuration.
        Args:
            source: The SQL source configuration.
        """
        self.source = source

    @property
    def source(self) -> SQLSourceConfig:
        """
        The SQL source configuration.
        """
        return self._source

    @source.setter
    def source(self, source: SQLSourceConfig):
        """
        Set the SQL source configuration
        Args:
            source: The SQL source configuration.
        """
        if not isinstance(source, SQLSourceConfig):
            raise TypeError(
                "The 'source' parameter must be an instance of"
                f" {SQLSourceConfig.__name__}, got {type(source).__name__} instead."
            )
        self._source = source

    def _chunk(self, working_dir: str) -> list[list[str]]:
        """
        Execute the SQL queries and return the list of resulting Parquet file names.
        Args:
            working_dir: The working directory where the files will be stored.
        Returns:
            A nested list of Parquet file names where each element of
                the outer list corresponds to each given query
        """
        conn_url = obtain_sqlalchemy_url(self.source.conn)

        queries = self.source.queries
        schema_overrides_list = self.source.schema_overrides
        transactional = self.source.transactional

        engine = create_engine(
            conn_url, connect_args=self.source.conn.cx_src_configs_sql
        )

        results = []
        try:
            if transactional:
                with Session(engine) as session, session.begin():
                    results = self._run_queries(
                        queries,
                        session.connection(),
                        working_dir,
                        schema_overrides_list,
                    )
            else:
                with engine.connect() as connection:
                    results = self._run_queries(
                        queries,
                        connection,
                        working_dir,
                        schema_overrides_list,
                    )
        except Exception as e:
            logger.warning(f"Error reading data from SQL source: {e}")
            raise e
        finally:
            engine.dispose()
        return results

    def _run_queries(
        self,
        queries: list[str],
        session: Connection,
        destination: str,
        schema_overrides_list: list[dict[str, DataType]] = None,
    ) -> list[list[str]]:
        """
        Run the list of SQL queries and return the list of resulting Parquet file names.
        Args:
            queries: The list of SQL queries to execute.
            session: The SQLAlchemy connection.
            destination: The destination directory for the Parquet files.
            schema_overrides_list: List of Schema Overrides for each query.
        Returns:
            A nested list of Parquet file names for each query result.
        """
        results = []
        for idx, query in enumerate(queries):
            result = self._execute_sql_query(
                query=text(query),
                session=session,
                destination=destination,
                schema_overrides=(
                    schema_overrides_list[idx]
                    if schema_overrides_list is not None
                    else None
                ),
            )
            results.append(result)
            logger.info(f"Imported SQL query result chunks to {destination}")
        return results

    def _execute_sql_query(
        self,
        query: TextClause,
        session: Connection,
        destination: str,
        schema_overrides: dict[str, DataType] = None,
    ) -> list[str]:
        """
        Execute a single SQL query and return the resulting Parquet file name.
        Args:
            query: The SQL query to execute.
            session: The SQLAlchemy connection.
            destination: The destination directory for the Parquet file.
            schema_overrides: Schema Overrides for query
        Returns:
            List of parquet files representing each chunk.
        """
        logger.info(
            f"Importing SQL query: {query} with\ninitial values:"
            f" {self.source.initial_values}"
        )
        if self.source.loader == _LoaderSpec.POLARS_SQLALCHEMY:
            return self._execute_polars_sqlalchemy(
                query,
                session,
                destination,
                schema_overrides,
                self.source.initial_values,
            )
        elif self.source.loader == _LoaderSpec.PANDAS:
            return self._execute_pandas(
                query,
                session,
                destination,
                schema_overrides,
                self.source.initial_values,
            )
        else:
            raise TabsDataException(
                ErrorCode.SOCE52,
                self.source.__class__.__name__,
                [_LoaderSpec.POLARS_SQLALCHEMY, _LoaderSpec.PANDAS],
                self.source.loader,
            )

    def _execute_polars_sqlalchemy(
        self,
        query: TextClause,
        session: Connection,
        destination: str,
        schema_overrides: dict[str, DataType] = None,
        initial_values: dict[str, Any] = None,
    ) -> list[str]:
        intermediate_files = []
        result_height = 0
        for batch in pl.read_database(
            query,
            connection=session.execution_options(stream_results=True),
            iter_batches=True,
            batch_size=self.source.chunk_size,
            execute_options={
                "parameters": initial_values,
            },
            schema_overrides=schema_overrides,
        ):
            if batch.height == 0:
                break
            result_height += batch.height
            logger.debug(f"Fetched batch with {batch.height} records")
            uuid_string = uuid.uuid4().hex[:16]
            batch_file = os.path.join(
                destination, f"intermediate_{uuid_string}.parquet"
            )
            batch.write_parquet(batch_file, use_pyarrow=True)
            logger.debug(f"Wrote batch to {batch_file}")
            intermediate_files.append(batch_file)

        logger.info(f"Fetched total of {result_height} records for query '{query}'")
        return intermediate_files

    def _execute_pandas(
        self,
        query: TextClause,
        session: Connection,
        destination: str,
        schema_overrides: dict[str, DataType] = None,
        initial_values: dict[str, Any] = None,
    ) -> list[str]:
        import pandas as pd

        intermediate_files = []
        result_height = 0
        for batch in pd.read_sql_query(
            sql=query,
            con=session.execution_options(stream_results=True),
            chunksize=self.source.chunk_size,
            dtype=(
                {
                    # Get the corresponding NumPy dtype for each Polars dtype for
                    # pandas schema overrides
                    key: get_numpy_dtype_from_polars(value)
                    for key, value in schema_overrides.items()
                }
                if schema_overrides is not None
                else None
            ),
            params=initial_values,
        ):
            if batch.empty:
                break
            result_height += len(batch)
            logger.debug(f"Fetched batch with {len(batch)} records")
            uuid_string = uuid.uuid4().hex[:16]
            batch_file = os.path.join(
                destination, f"intermediate_{uuid_string}.parquet"
            )
            batch.to_parquet(
                batch_file,
                engine="pyarrow",
                index=False,
            )
            logger.debug(f"Wrote batch to {batch_file}")
            intermediate_files.append(batch_file)

        logger.info(f"Fetched total of {result_height} records for query '{query}'")
        return intermediate_files
