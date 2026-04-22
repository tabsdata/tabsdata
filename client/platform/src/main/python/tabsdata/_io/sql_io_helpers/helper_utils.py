#
# Copyright 2026 Tabs Data Inc.
#

import logging

import numpy as np
from sqlalchemy.engine import URL, make_url

from tabsdata._io.constants import ODBC_CONNECT_PARAM
from tabsdata._io.sql_io_helpers.helper_constants import POLARS_TO_NUMPY_DTYPE_MAP
from tabsdata._io.sql_io_helpers.sql_helper_configs import SQLConnConfig
from tabsdata._utils.sql_utils import (
    add_driver_to_uri,
    obtain_uri,
    validate_user_password,
)
from tabsdata.tableframe.typing import DataType

logger = logging.getLogger(__name__)


def get_numpy_dtype_from_polars(
    td_dtype: DataType,
) -> np.dtype:
    """
    Get the corresponding NumPy dtype for a given Polars dtype.
    Args:
        td_dtype: The Polars data type.
    Returns:
        The corresponding NumPy data type.
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


def obtain_sqlalchemy_url(conn: SQLConnConfig) -> URL:
    uri = obtain_uri(conn, log=True)

    # If the driver is missing, add it.
    # If the driver is already present, this is a no-op.
    uri = add_driver_to_uri(uri, log=True)

    conn_url = make_url(uri)

    user = conn.credentials.user.secret_value
    password = conn.credentials.password.secret_value
    validate_user_password(user=user, password=password)
    if ODBC_CONNECT_PARAM in conn_url.query:
        conn.cx_src_configs_sql["UID"] = user
        conn.cx_dst_configs_sql["UID"] = user
        conn.cx_src_configs_sql["PWD"] = password
        conn.cx_dst_configs_sql["PWD"] = password
    else:
        conn_url = conn_url.set(
            username=user,
            password=password,
        )
    return conn_url
