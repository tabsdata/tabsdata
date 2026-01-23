#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union

from tabsdata._io.constants import _LoaderSpec
from tabsdata._io.sql_io_helpers.sql_helper_configs import (
    SQLConnConfig,
    SQLDestConfig,
    SQLSourceConfig,
)
from tabsdata._io.sql_io_helpers.sqlalchemy_dest_helper import SQLAlchemyDestHelper
from tabsdata._io.sql_io_helpers.sqlalchemy_source_helper import SQLAlchemySourceHelper

# Conditional import for type hinting only:
# This import is guarded by TYPE_CHECKING so it is only used by static type checkers
# and does not actually execute at runtime. This avoids circular import errors while
# still providing type hints and autocompletion.
if TYPE_CHECKING:
    from tabsdata._io.inputs.sql.mariadb_input import MariaDBSrc
    from tabsdata._io.inputs.sql.mysql_input import MySQLSrc
    from tabsdata._io.inputs.sql.oracle_input import OracleSrc
    from tabsdata._io.inputs.sql.postgres_input import PostgresSrc
    from tabsdata._io.outputs.sql.mariadb_output import MariaDBDest
    from tabsdata._io.outputs.sql.mysql_output import MySQLDest
    from tabsdata._io.outputs.sql.postgres_output import PostgresDest
    from tabsdata_mssql.input.mssql_input import MSSQLSrc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_sql_source_helper(
    source: Union[MySQLSrc, PostgresSrc, MariaDBSrc, OracleSrc, MSSQLSrc],
) -> Union[SQLAlchemySourceHelper]:
    """
    Factory function to get the appropriate SQL source helper based on the source and
    loader type.
    Args:
        source: The SQL source configuration.
    Returns:
        The appropriate SQL source helper instance.
    """
    sql_source_config = _get_sql_source_config(source)
    logger.debug(f"Created SQL source config for {source}")
    if (
        sql_source_config.loader == _LoaderSpec.POLARS_SQLALCHEMY
        or sql_source_config.loader == _LoaderSpec.PANDAS
    ):
        helper = SQLAlchemySourceHelper(sql_source_config)
    # Future TODO: Add more SQL source helpers based on different engines
    else:
        raise ValueError(
            f"Unsupported loader value: {sql_source_config.loader}. "
            f"Supported loader values are: {_LoaderSpec.values()}."
        )
    logger.debug(f"Created SQL source helper: {helper}")
    return helper


def get_sql_dest_helper(
    dest: Union[MySQLDest, PostgresDest, MariaDBDest],
) -> Union[SQLAlchemyDestHelper]:
    """
    Factory function to get the appropriate SQL destination helper based on the
    destination type.
    Args:
        dest: The SQL destination configuration.
    Returns:
        The appropriate SQL destination helper instance.
    """
    sql_dest_config = _get_sql_dest_config(dest)
    logger.debug(f"Created SQL dest config for {dest}")
    if sql_dest_config.loader == _LoaderSpec.POLARS_SQLALCHEMY:
        helper = SQLAlchemyDestHelper(sql_dest_config)
    # Future TODO: Add more SQL dest helpers based on different loaders/engines
    else:
        raise ValueError(
            f"Unsupported loader value: {sql_dest_config.loader}. "
            f"Supported loader values are: {_LoaderSpec.values()}"
        )
    logger.debug(f"Created SQL dest helper: {helper}")
    return helper


def _get_sql_source_config(
    source: Union[MySQLSrc, PostgresSrc, MariaDBSrc, OracleSrc, MSSQLSrc],
) -> SQLSourceConfig:
    # Runtime import for isinstance checks:
    # This second import inside the function is necessary for runtime type checking
    # (e.g., isinstance(source, MySQLSrc)), since the TYPE_CHECKING import at top of
    # file is not available at runtime.
    from tabsdata._io.inputs.sql.mariadb_input import MariaDBSrc as _MariaDBSrc
    from tabsdata._io.inputs.sql.mysql_input import MySQLSrc as _MySQLSrc
    from tabsdata._io.inputs.sql.oracle_input import OracleSrc as _OracleSrc
    from tabsdata._io.inputs.sql.postgres_input import PostgresSrc as _PostgresSrc
    from tabsdata_mssql.input.mssql_input import MSSQLSrc as _MSSQLSrc

    if isinstance(source, _MySQLSrc):
        return SQLSourceConfig(
            conn=SQLConnConfig(
                uri=source.conn.uri,
                credentials=source.conn.credentials,
                enforce_connection_params=source.conn.enforce_connection_params,
                cx_src_configs_sql=source.conn.cx_src_configs_mysql,
                cx_dst_configs_sql=source.conn.cx_dst_configs_mysql,
            ),
            queries=source.queries,
            initial_values=source.initial_values,
            transactional=source.transactional,
            chunk_size=source.chunk_size,
            loader=source.loader,
            schema_overrides=source.schema_overrides,
        )
    elif isinstance(source, _PostgresSrc):
        return SQLSourceConfig(
            conn=SQLConnConfig(
                uri=source.conn.uri,
                credentials=source.conn.credentials,
                enforce_connection_params=source.conn.enforce_connection_params,
                cx_src_configs_sql=source.conn.cx_src_configs_postgres,
                cx_dst_configs_sql=source.conn.cx_dst_configs_postgres,
            ),
            queries=source.queries,
            initial_values=source.initial_values,
            transactional=source.transactional,
            chunk_size=source.chunk_size,
            loader=source.loader,
            schema_overrides=source.schema_overrides,
        )
    elif isinstance(source, _MariaDBSrc):
        return SQLSourceConfig(
            conn=SQLConnConfig(
                uri=source.conn._uri_with_collation,
                credentials=source.conn.credentials,
                enforce_connection_params=source.conn.enforce_connection_params,
                cx_src_configs_sql=source.conn.cx_src_configs_mariadb,
                cx_dst_configs_sql=source.conn.cx_dst_configs_mariadb,
            ),
            queries=source.queries,
            initial_values=source.initial_values,
            transactional=source.transactional,
            chunk_size=source.chunk_size,
            loader=source.loader,
            schema_overrides=source.schema_overrides,
        )
    elif isinstance(source, _OracleSrc):
        return SQLSourceConfig(
            conn=SQLConnConfig(
                uri=source.conn.uri,
                credentials=source.conn.credentials,
                enforce_connection_params=source.conn.enforce_connection_params,
                cx_src_configs_sql=source.conn.cx_src_configs_oracle,
                cx_dst_configs_sql=source.conn.cx_dst_configs_oracle,
            ),
            queries=source.queries,
            initial_values=source.initial_values,
            transactional=source.transactional,
            chunk_size=source.chunk_size,
            loader=source.loader,
            schema_overrides=source.schema_overrides,
        )
    elif isinstance(source, _MSSQLSrc):
        return SQLSourceConfig(
            conn=SQLConnConfig(
                uri=source.conn.uri,
                credentials=source.conn.credentials,
                enforce_connection_params=source.conn.enforce_connection_params,
                cx_src_configs_sql=source.conn.cx_src_configs_mssql,
                cx_dst_configs_sql=source.conn.cx_dst_configs_mssql,
            ),
            queries=source.queries,
            initial_values=source.initial_values,
            transactional=source.transactional,
            chunk_size=source.chunk_size,
            loader=source.loader,
            schema_overrides=source.schema_overrides,
        )
    else:
        raise TypeError(f"Unsupported source type: {type(source)}")


def _get_sql_dest_config(
    dest: Union[MySQLDest, PostgresDest, MariaDBDest],
) -> SQLDestConfig:
    # Runtime import for isinstance checks:
    from tabsdata._io.outputs.sql.mariadb_output import MariaDBDest as _MariaDBDest
    from tabsdata._io.outputs.sql.mysql_output import MySQLDest as _MySQLDest
    from tabsdata._io.outputs.sql.postgres_output import PostgresDest as _PostgresDest

    if isinstance(dest, _MySQLDest):
        return SQLDestConfig(
            conn=SQLConnConfig(
                uri=dest.conn.uri,
                credentials=dest.conn.credentials,
                enforce_connection_params=dest.conn.enforce_connection_params,
                cx_src_configs_sql=dest.conn.cx_src_configs_mysql,
                cx_dst_configs_sql=dest.conn.cx_dst_configs_mysql,
            ),
            destination_tables=dest.destination_tables,
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            chunk_size=dest.chunk_size,
            loader=dest.loader,
        )
    elif isinstance(dest, _PostgresDest):
        return SQLDestConfig(
            conn=SQLConnConfig(
                uri=dest.conn.uri,
                credentials=dest.conn.credentials,
                enforce_connection_params=dest.conn.enforce_connection_params,
                cx_src_configs_sql=dest.conn.cx_src_configs_postgres,
                cx_dst_configs_sql=dest.conn.cx_dst_configs_postgres,
            ),
            destination_tables=dest.destination_tables,
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            chunk_size=dest.chunk_size,
            loader=dest.loader,
        )
    elif isinstance(dest, _MariaDBDest):
        return SQLDestConfig(
            conn=SQLConnConfig(
                uri=dest.conn._uri_with_collation,
                credentials=dest.conn.credentials,
                enforce_connection_params=dest.conn.enforce_connection_params,
                cx_src_configs_sql=dest.conn.cx_src_configs_mariadb,
                cx_dst_configs_sql=dest.conn.cx_dst_configs_mariadb,
            ),
            destination_tables=dest.destination_tables,
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            chunk_size=dest.chunk_size,
            loader=dest.loader,
        )
    else:
        raise TypeError(f"Unsupported destination type: {type(dest)}")
