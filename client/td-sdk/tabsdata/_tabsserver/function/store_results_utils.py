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
from typing import TYPE_CHECKING, List

import polars as pl

import tabsdata._utils.tableframe._helpers as td_helpers
from tabsdata._io.output import (
    TableOutput,
)
from tabsdata._tabsserver.function.logging_utils import pad_string
from tabsdata._tabsserver.function.native_tables_utils import sink_lf_to_location
from tabsdata._tabsserver.function.yaml_parsing import Table

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
) -> List[dict]:
    logger.info(pad_string("[Storing results]"))
    logger.info(
        f"Storing results in destination '{execution_context.function_config.output}'"
    )
    modified_tables = []
    if destination_plugin := execution_context.destination_plugin:
        logger.debug("Running the destination plugin")
        destination_plugin._run(execution_context, results)
    else:
        destination = execution_context.non_plugin_destination
        if isinstance(destination, TableOutput):
            modified_tables = store_results_in_table(
                results, destination, execution_context
            )
        else:
            logger.error(
                f"Storing results in destination of type '{type(destination)}' "
                f"not supported. Destination: {destination}"
            )
            raise ValueError(
                f"Storing results in destination of type '{type(destination)}' not"
                " supported"
            )
    return modified_tables


# noinspection PyProtectedMember
def store_results_in_table(
    results: ResultsCollection,
    destination: TableOutput,
    execution_context: ExecutionContext,
) -> List[dict]:
    results.normalize_frame()
    # Right now, source provides very little information, but we use it to do a small
    # sanity check and to ensure that everything is running properly
    # TODO: Decide if we want to add more checks here
    request_output_entry_list = execution_context.request.output
    logger.info(
        f"Storing results in tables '{request_output_entry_list}' and "
        f"matching them with destination '{destination}'"
    )
    table_list = []
    # Note: destination.table is a list of strings, it can't be a single string because
    # when we serialised it we stored it as such even if it was a single one.
    if len(request_output_entry_list) != len(destination.table):
        logger.error(
            "Number of tables in the execution context output"
            f" ({len(request_output_entry_list)}: "
            f"{request_output_entry_list}) does not match the "
            "number"
            f" of tables in the destination ({len(destination.table)}: "
            f"{destination.table}). No data stored."
        )
        raise ValueError(
            "Number of tables in the execution context output"
            f" ({len(request_output_entry_list)}: "
            f"{request_output_entry_list}) does not match the "
            "number"
            f" of tables in the destination ({len(destination.table)}: "
            f"{destination.table}). No data stored."
        )
    for request_output_entry, table_name_in_decorator in zip(
        request_output_entry_list, destination.table
    ):
        if isinstance(request_output_entry, Table):
            match_tables_and_verify(request_output_entry, table_name_in_decorator)
            table_list.append(request_output_entry)
        else:
            logger.error(
                f"Invalid table type: {type(request_output_entry)}. No data stored."
            )
            raise TypeError(
                f"Invalid table type: {type(request_output_entry)}. No data stored."
            )
    logger.debug(f"Table list obtained: {table_list}")
    logger.debug(f"Obtained a total of {len(results)} results")
    if len(results) != len(table_list):
        logger.error(
            f"Number of results obtained ({len(results)}) does not match the number of "
            f"tables to store ({len(table_list)}). No data stored."
        )
        raise ValueError(
            f"Number of results obtained ({len(results)}) does not match the number of "
            f"tables to store ({len(table_list)}). No data stored."
        )
    modified_tables = []
    for result, table in zip(results, table_list):
        logger.info(f"Storing result in table '{table}'")
        if isinstance(result.value, TableFrame):
            # First we create a new TableFrame where system columns to be kept are kept,
            # and those requiring regeneration are regenerated with new to persist
            # values.
            result_value: TableFrame = result.value
            lf = result_value._to_lazy()
            tf = TableFrame.__build__(df=lf, mode="sys", idx=result_value._idx)
            sink_lf_to_location(tf._to_lazy(), execution_context, table.location)
            table_meta_info = get_table_meta_info_from_lf(lf)
            table_info = {"name": table.name, "meta_info": table_meta_info}
            modified_tables.append(table_info)
            logger.debug(
                f"Result stored in table '{table}', added to modified_tables "
                f"list with information '{table_info}'"
            )
        elif result is None:
            logger.warning(f"Result is None. No data stored: '{table}'.")
        elif result.value is None:
            logger.warning(f"Result value is None. No data stored: '{table}'.")
        else:
            logger.error(
                f"Invalid result type: '{type(result.value)}'. No data stored."
            )
            raise TypeError(
                f"Invalid result type: '{type(result.value)}'. No data stored."
            )
    logger.info("Results stored in tables")
    logger.debug(f"Modified tables: {modified_tables}")
    return modified_tables


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


def match_tables_and_verify(
    execution_context_table: Table, destination_table_name: str
):
    # For now, we do only this small check for the table name, but we could
    # add more checks in the future.
    logger.debug(
        f"Matching table '{execution_context_table}' with destination table"
        f" '{destination_table_name}'"
    )
    if execution_context_table.name != destination_table_name:
        logger.warning(
            f"Execution context table name '{execution_context_table.name}' does not "
            f"match the destination table name '{destination_table_name}'"
        )


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
