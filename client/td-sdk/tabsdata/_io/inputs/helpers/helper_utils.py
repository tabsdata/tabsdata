#
# Copyright 2025 Tabs Data Inc.
#

import logging
from typing import Any
from urllib.parse import urlparse

import numpy as np

from tabsdata._io.constants import MYSQL_SCHEME, POSTGRES_SCHEMES
from tabsdata._io.inputs.helpers.helper_constants import POLARS_TO_NUMPY_DTYPE_MAP
from tabsdata.tableframe.typing import DataType

logger = logging.getLogger(__name__)


def replace_initial_values_placeholder(
    conn_uri: str, query: str, initial_values: dict[str, Any]
) -> str:
    """
    Replace initial values placeholders in the query based on the database type.
    Args:
        conn_uri (str): The connection URI of the database.
        query (str): The SQL query with placeholders.
        initial_values (dict[str, Any]): A dictionary of initial values to replace
            in the query.
    Returns:
        str: The SQL query with initial values placeholders replaced.
    """
    parsed_conn_uri = urlparse(conn_uri)
    conn_uri_scheme = parsed_conn_uri.scheme.lower()
    logger.debug(
        f"Replacing initial values {initial_values.keys()} syntax in query: {query}"
    )
    if conn_uri_scheme == MYSQL_SCHEME or conn_uri_scheme in POSTGRES_SCHEMES:
        for key in initial_values.keys():
            query = query.replace(f":{key}", f"%({key})s")
    # Future TODO: Add more database parameter styles here as needed
    else:
        raise ValueError(
            f"Unsupported database scheme '{conn_uri_scheme}' in URI '{conn_uri}'."
        )
    logger.debug(f"Query after replacing initial values syntax: {query}")
    return query


def get_numpy_dtype_from_polars(
    td_dtype: DataType,
) -> np.dtype:
    """
    Get the corresponding NumPy dtype for a given Polars dtype.
    Args:
        td_dtype (DataType): The Polars data type.
    Returns:
        np.dtype: The corresponding NumPy data type.
    Raises:
        ValueError: If the Polars dtype is not in the defined mapping.
    """
    try:
        numpy_dtype = POLARS_TO_NUMPY_DTYPE_MAP[td_dtype.base_type()]
        logger.debug(
            f"Mapped Polars DataType '{td_dtype}' to NumPy dtype '{numpy_dtype}'."
        )
        return numpy_dtype
    except KeyError:
        raise ValueError(
            f"No numpy dtype mapping defined for Polars DataType '{td_dtype}'."
        )
