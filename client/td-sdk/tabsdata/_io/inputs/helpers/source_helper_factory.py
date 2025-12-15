#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union

from tabsdata._io.inputs.helpers.helper_constants import _LoaderSpec
from tabsdata._io.inputs.helpers.helper_sql_configs import (
    SQLConnConfig,
    SQLSourceConfig,
)
from tabsdata._io.inputs.helpers.sqlalchemy_source_helper import SQLAlchemySourceHelper

# Conditional import for type hinting only:
# This import is guarded by TYPE_CHECKING so it is only used by static type checkers
# and does not actually execute at runtime. This avoids circular import errors while
# still providing type hints and autocompletion.
if TYPE_CHECKING:
    from tabsdata._io.inputs.sql.mysql_input import MySQLSrc
    from tabsdata._io.inputs.sql.postgres_input import PostgresSrc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_sql_source_helper(
    source: Union[MySQLSrc, PostgresSrc],
) -> Union[SQLAlchemySourceHelper]:
    """
    Factory function to get the appropriate SQL source helper based on the source and
    loader type.
    Args:
        source (Union[MySQLSrc, PostgresSrc]): The SQL source configuration.

    Returns:
        Union[SQLAlchemySourceHelper]: The appropriate SQL source helper instance.
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
        return ValueError(
            f"Unsupported loader value: {sql_source_config.loader}. "
            f"Supported loader values are: {_LoaderSpec.values()}."
        )
    logger.debug(f"Created SQL source helper: {helper}")
    return helper


def _get_sql_source_config(source: Union[MySQLSrc, PostgresSrc]) -> SQLSourceConfig:
    # Runtime import for isinstance checks:
    # This second import inside the function is necessary for runtime type checking
    # (e.g., isinstance(source, MySQLSrc)), since the TYPE_CHECKING import at top of
    # file is not available at runtime.
    from tabsdata._io.inputs.sql.mysql_input import MySQLSrc as _MySQLSrc
    from tabsdata._io.inputs.sql.postgres_input import PostgresSrc as _PostgresSrc

    if isinstance(source, _MySQLSrc):
        return SQLSourceConfig(
            conn=SQLConnConfig(
                uri=source.conn.uri,
                credentials=source.conn.credentials,
                enforce_connection_params=source.conn.enforce_connection_params,
                cx_src_configs_sql=source.conn.cx_src_configs_sql,
                cx_dst_configs_sql=source.conn.cx_dst_configs_sql,
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
    else:
        raise TypeError(f"Unsupported source type: {type(source)}")
