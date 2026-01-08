#
# Copyright 2025 Tabs Data Inc.
#

import logging

import numpy as np

from tabsdata._io.inputs.helpers.helper_constants import POLARS_TO_NUMPY_DTYPE_MAP
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
