#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import glob
import hashlib
import logging
import os
import re
from functools import partial
from typing import TYPE_CHECKING

import polars as pl

import tabsdata._utils.tableframe._helpers as td_helpers
from tabsdata._tabsserver.function.logging_utils import pad_string

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._common import drop_system_columns

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import _unwrap_table_frame
from tabsdata.tableframe.lazyframe.frame import TableFrame

if TYPE_CHECKING:
    import pyarrow as pa

    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata._tabsserver.function.results_collection import (
        ResultsCollection,
    )

logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


def store_results(
    execution_context: ExecutionContext,
    results: ResultsCollection,
):
    logger.info(pad_string("[Storing results]"))
    logger.info(
        f"Storing results in destination '{execution_context.function_config.output}'"
    )
    destination_plugin = execution_context.destination
    logger.debug("Running the destination plugin")
    destination_plugin._run(execution_context, results)
    logger.debug("Destination plugin run completed successfully")


def get_table_meta_info_from_lf(lf: pl.LazyFrame) -> dict:
    """
    Extracts table information from a Polars LazyFrame.
    This function retrieves the schema and other metadata from the LazyFrame.

    :param lf: Polars LazyFrame
    :return: Dictionary containing table information
    """
    lf = drop_system_columns(lf)
    columns = lf.width
    rows = lf.select(pl.len()).collect().to_series().item()
    schema_hash = arrow_schema_hash(get_arrow_schema(lf), sort_schema=True)
    return {"column_count": columns, "row_count": rows, "schema_hash": schema_hash}


# Get the user's schema of a Tabsdata Parquet file as an Arrow schema
def get_arrow_schema(lazy_frame: pl.LazyFrame) -> pa.Schema:
    polars_schema = lazy_frame.collect_schema()
    polars_dataframe = polars_schema.to_frame(eager=True)
    return polars_dataframe.to_arrow().schema


# Computes the hash of an Arrow schema, optionally sorting the schema fields first.
# (going sorted allows to find equivalent schemas with different field order)
def arrow_schema_hash(schema: pa.Schema, sort_schema=True) -> str:
    if sort_schema:

        import pyarrow as pa

        sorted_fields = sorted(schema, key=lambda field: field.name)
        schema = pa.schema(sorted_fields)
    serialized_schema = schema.serialize()
    return hashlib.sha256(serialized_schema).hexdigest()


def remove_system_columns_and_convert(result: TableFrame) -> pl.LazyFrame:
    try:
        # Note: this converts result from a TableFrame to a LazyFrame
        return _unwrap_table_frame(result)
    except pl.exceptions.ColumnNotFoundError as e:
        logger.error(
            "Missing one of the following system columns"
            f" '{td_helpers.SYSTEM_COLUMNS}'. This indicates tampering in the data."
            " Ensure you are not modifying system columns in your data."
        )
        logger.error(f"Error: {e}")
        raise ValueError(
            "Missing one of the following system columns"
            f" '{td_helpers.SYSTEM_COLUMNS}'. This indicates tampering in the data."
            " Ensure you are not modifying system columns in your data."
        ) from e


def _get_matching_files(pattern, file_extension=None):
    # Construct the full pattern
    # Use glob to get the list of matching files
    file_extension = file_extension or pattern.split(".")[-1]
    matching_files = glob.glob(pattern)
    ordered_files = sorted(matching_files, key=partial(_extract_index, file_extension))
    logger.debug(f"Matching files: {ordered_files}")
    return ordered_files


# Sort the files to ensure that they are processed in the correct order
# This only works if the files are of the form <something>_index.<extension>,
# otherwise it might produce false positives or negatives
def _extract_index(file_extension, filename):
    base_filename = os.path.basename(filename)
    pattern = r"_(\d+)\." + re.escape(file_extension) + r"$"
    match = re.search(pattern, base_filename)
    if match:
        return int(match.group(1))
    else:
        raise ValueError(f"Filename '{filename}' does not contain an index.")
