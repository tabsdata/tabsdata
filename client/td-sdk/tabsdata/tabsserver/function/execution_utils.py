#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Tuple

import polars as pl

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._generators as td_generators
from tabsdata.format import (
    CSVFormat,
    FileFormat,
    LogFormat,
    NDJSONFormat,
    ParquetFormat,
)
from tabsdata.io.input import TableInput
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tableuri import build_table_uri_object
from tabsdata.tabsserver.function import environment_import_utils
from tabsdata.tabsserver.function.logging_utils import pad_string
from tabsdata.tabsserver.function.native_tables_utils import (
    scan_lf_from_location,
    scan_tf_from_table,
    sink_lf_to_location,
)
from tabsdata.tabsserver.function.offset_utils import OffsetReturn
from tabsdata.tabsserver.function.results_collection import ResultsCollection
from tabsdata.tabsserver.function.yaml_parsing import (
    Location,
    Table,
    TableVersions,
)

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._context import TableFrameContext

if TYPE_CHECKING:
    from tabsdata.io.input import Input
    from tabsdata.tabsserver.function.execution_context import ExecutionContext
    from tabsdata.tabsserver.function.offset_utils import Offset

logger = logging.getLogger(__name__)

SOURCES_FOLDER = "sources"


def execute_function_from_config(
    execution_context: ExecutionContext,
) -> ResultsCollection:
    environment_import_utils.update_syspath(execution_context.paths.code_folder)
    # Decided to keep this logic with the code_folder to make it OS-agnostic. If we
    # stored the function_file with the code_folder prefix, we would have to handle
    # the path differently for different OSs.
    logger.info(pad_string("[Obtaining function parameters]"))
    parameters = obtain_user_provided_function_parameters(execution_context)
    logger.info("Function parameters obtained")
    logger.info(pad_string("[Executing function]"))
    logger.info("Starting execution of function provided by the user")
    result = execution_context.user_provided_function(*parameters)
    logger.info("Finished executing function provided by the user")
    if execution_context.status.offset.returns_values:
        result = update_initial_values(execution_context, result)
    result = ResultsCollection(result)
    result.check_collection_integrity()
    return result


def update_initial_values(execution_context: ExecutionContext, result):
    logger.info("New offset generated")
    if source_plugin := execution_context.source_plugin:
        # TODO: For clarity, remove this if once all builtin inputs have been updated
        #  as plugins
        if source_plugin._offset_return == OffsetReturn.ATTRIBUTE.value:
            logger.debug("Obtaining new offset from the plugin object")
            # The new initial values are stored in the plugin
            new_initial_values = source_plugin.initial_values
        elif source_plugin._offset_return == OffsetReturn.FUNCTION.value:
            # TODO: Duplicated code, will be removed once the last source is migrated.
            # The new initial values are part of the result
            logger.debug("Obtaining new offset from the plugin result")
            if isinstance(result, tuple):
                *result, new_initial_values = result
                result = tuple(result)
            else:
                new_initial_values = result
                result = (None,)
        else:
            raise ValueError(
                f"Invalid offset return type: {source_plugin._offset_return}"
            )
    else:
        # If working with a source, the new initial values are part of the result
        if isinstance(result, tuple):
            *result, new_initial_values = result
            result = tuple(result)
        else:
            new_initial_values = result
            result = (None,)
    execution_context.status.offset.update_new_values(new_initial_values)
    return result


def obtain_user_provided_function_parameters(
    execution_context: ExecutionContext,
) -> list[TableFrame | None | list[TableFrame | None]]:
    if source_plugin := execution_context.source_plugin:
        logger.debug("Running the source plugin")
        # noinspection PyProtectedMember
        parameters = source_plugin._run(execution_context)
    else:
        # TODO: Remake this to also use execution_context, trying to finish only the
        #  plugin section for now
        non_plugin_source = execution_context.non_plugin_source
        working_dir = execution_context.paths.output_folder
        parameters = trigger_non_plugin_source(
            non_plugin_source,
            working_dir,
            execution_context,
            execution_context.status.offset,
        )
    return parameters


def convert_tuple_to_list(
    result: TableFrame | Tuple[TableFrame],
) -> list[TableFrame] | TableFrame:
    if isinstance(result, tuple):
        return list(result)
    else:
        return result


def trigger_non_plugin_source(
    source: Input,
    working_dir: str,
    execution_context: ExecutionContext,
    initial_values: Offset = None,
    idx: td_generators.IdxGenerator | None = None,
) -> list[TableFrame | None | list[TableFrame | None]]:
    destination_folder = os.path.join(working_dir, SOURCES_FOLDER)
    os.makedirs(destination_folder, exist_ok=True)
    if isinstance(source, TableInput):
        logger.debug("Triggering TableInput")
        # When loading tabsdata tables, we return tuples of (uri, table), so that
        # coming operations can use information on the request for further processing.
        result = execute_table_importer(source, execution_context)
        logger.info("Loaded tables successfully")
        return result
    else:
        logger.error(f"Invalid source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid source type: {type(source)}. No data imported.")


def execute_table_importer(
    source: TableInput,
    execution_context: ExecutionContext,
) -> list[TableFrame | None | list[TableFrame | None]]:
    # Right now, source provides very little information, but we use it to do a small
    # sanity check and to ensure that everything is running properly
    context_request_input = execution_context.request.input
    logger.info(
        f"Importing tables '{context_request_input}' and matching them"
        f" with source '{source}'"
    )
    tableframe_list: list[TableFrame | None | list[TableFrame | None]] = []
    # Note: source.uri is a list of URIs, it can't be a single URI because when we
    # serialised it we stored it as such even if it was a single one.
    if len(context_request_input) != len(source.table):
        logger.error(
            "Number of tables in the execution context input"
            f" ({len(context_request_input)}) does not match the "
            "number of"
            f" URIs in the source ({len(source.table)}). No data imported."
        )
        raise ValueError(
            "Number of tables in the execution context input"
            f" ({len(context_request_input)}) does not match the "
            "number of"
            f" URIs in the source ({len(source.table)}). No data imported."
        )
    for execution_context_input_entry, source_table_str in zip(
        context_request_input, source.table
    ):
        if isinstance(execution_context_input_entry, Table):
            verify_source_tables_match(execution_context_input_entry, source_table_str)
            tf = scan_tf_from_table(
                execution_context,
                execution_context_input_entry,
                fail_on_none_uri=False,
            )
            tableframe_list.append(tf)
        elif isinstance(execution_context_input_entry, TableVersions):
            logger.debug(
                f"Matching TableVersions '{execution_context_input_entry}' with source"
                f" '{source_table_str}'"
            )
            list_of_table_objects = execution_context_input_entry.list_of_table_objects
            versioned_tableframes_list: list[TableFrame | None] = []
            for table in list_of_table_objects:
                verify_source_tables_match(table, source_table_str)
                tf = scan_tf_from_table(
                    execution_context,
                    table,
                    fail_on_none_uri=False,
                )
                versioned_tableframes_list.append(tf)
            tableframe_list.append(versioned_tableframes_list)
        else:
            logger.error(
                f"Invalid table type: {type(execution_context_input_entry)}. No data"
                " imported."
            )
            raise TypeError(
                f"Invalid table type: {type(execution_context_input_entry)}. No data"
                " imported."
            )
    logger.debug(f"TableFrame list obtained: {tableframe_list}")
    return tableframe_list


def verify_source_tables_match(execution_context_table: Table, source_table_str: str):
    # For now, we do only this small check for the table name, but we could
    # add more checks in the future.
    logger.debug(
        f"Matching table '{execution_context_table}' with source '{source_table_str}'"
    )
    source_table_uri = build_table_uri_object(source_table_str)
    if execution_context_table.name != source_table_uri.table:
        logger.debug(
            f"Source table '{source_table_str}' converted to TableURI:"
            f" '{source_table_uri}'"
        )
        logger.warning(
            f"Execution context table name '{execution_context_table.name}' does not "
            f"match the source table name '{source_table_uri.table}'"
        )
    return


INPUT_FORMAT_CLASS_TO_IMPORTER_FORMAT = {
    CSVFormat: "csv",
    LogFormat: "log",
    NDJSONFormat: "nd-json",
    ParquetFormat: "parquet",
}


def format_object_to_string(file_format: FileFormat) -> str:
    logger.debug(f"Converting format object to string: {file_format}")
    if isinstance(file_format, FileFormat):
        # noinspection PyTypeChecker
        return INPUT_FORMAT_CLASS_TO_IMPORTER_FORMAT.get(type(file_format))
    else:
        logger.error(f"Invalid format type: {type(file_format)}")
        raise TypeError(f"Invalid format type: {type(file_format)}")


def convert_characters_to_ascii(dictionary: dict) -> dict:
    for key, value in dictionary.items():
        if isinstance(value, str) and len(value) == 1:
            dictionary[key] = ord(value)
        elif isinstance(value, dict):
            dictionary[key] = convert_characters_to_ascii(value)
    return dictionary


def load_sources(
    execution_context: ExecutionContext,
    local_sources: list | str | None,
    idx: td_generators.IdxGenerator | None = None,
    working_dir: str | None = None,
) -> list[TableFrame | None | list[TableFrame | None]]:
    """
    Given a list of sources, load them into tabsdata TableFrames.
    :param execution_context: The context of the function.
    :param local_sources: A list of lists of paths to parquet files. Each element is
    :param idx: Table index generator to use for global indexing.
        either a string to a single file or a list of strings to multiple files.
    :param working_dir: The working directory where the sources are located. If
        populated, it will be prepended to the source paths.
    :return: A list were each element is either a DataFrame or a list of DataFrames.
    """
    if isinstance(local_sources, str) or local_sources is None:
        logger.debug(f"Obtained single source '{local_sources}', converting to list")
        local_sources = [local_sources]
    logger.debug(f"Loading list of sources: {local_sources}")
    if working_dir:
        logger.debug(f"Using working directory '{working_dir}'")
    if idx is None:
        idx = td_generators.IdxGenerator()
    sources: list[TableFrame | list[TableFrame]] = []
    for source in local_sources:
        logger.debug(f"Loading single source: {source}")
        if isinstance(source, list):
            sources.append(
                load_sources_from_list(
                    execution_context, idx, source, working_dir=working_dir
                )
            )
        else:
            sources.append(
                load_source(
                    execution_context,
                    idx,
                    make_tableframe_context(source, working_dir=working_dir),
                )
            )
    return sources


def load_sources_from_list(
    execution_context: ExecutionContext,
    idx: td_generators.IdxGenerator,
    source_list: list,
    working_dir: str | None = None,
) -> list[TableFrame]:
    sources: list[TableFrame] = []
    for source in source_list:
        sources.append(
            load_source(
                execution_context,
                idx,
                make_tableframe_context(source, working_dir=working_dir),
            )
        )
    return sources


def make_tableframe_context(
    source: TableFrameContext | str | None,
    working_dir: str | None = None,
) -> TableFrameContext | None:
    if isinstance(source, str):
        logger.debug(f"Using source '{source}' and working directory '{working_dir}'")
        source = os.path.join(working_dir, source) if working_dir else source
        tableframe_context = TableFrameContext(source)
    elif isinstance(source, TableFrameContext):
        tableframe_context = source
    elif not source:
        tableframe_context = None
    else:
        raise ValueError(
            "Invalid source type. Expected 'str', 'TableFrameContext' or None; got"
            f" '{type(source).__name__}' instead"
        )
    return tableframe_context


# When table_frame_context.table is not None, it means the table was loaded from the
# repository, implying it already has an index (idx).
# When table_frame_context.table is None, it means the table was loaded from a
# publisher, implying it requires a new index (idx).
def load_source(
    execution_context: ExecutionContext,
    idx: td_generators.IdxGenerator,
    table_frame_context: TableFrameContext | None,
) -> TableFrame | None:
    if table_frame_context is None:
        logger.warning("TableFrame context to source is None. No data loaded.")
        return None

    if table_frame_context.path is None:
        logger.warning("Path to source is None. No data loaded.")
        return None

    logger.debug(f"Loading parquet file from path: {table_frame_context.path}")
    lf = pl.scan_parquet(table_frame_context.path)
    logger.debug("Loaded parquet file successfully")

    if table_frame_context.table is None:
        return store_source_raw_data(execution_context, lf, idx)
    else:
        raise ValueError(
            "The code should not be reaching this point after the rework in TD-461"
        )


# ToDo: Pending storing metadata. This will require deciding how to determine which
#       information defines each raw data file depending on the source type.
def store_source_raw_data(
    execution_context: ExecutionContext,
    lf: pl.LazyFrame,
    idx: td_generators.IdxGenerator,
) -> TableFrame:
    logger.info("Storing raw data...")

    request = execution_context.request
    function_data = request.function_data
    if not function_data:
        raise ValueError("The function data location is required for publishers")
    elif function_data.uri is None:
        raise ValueError("The uri for the function data is required for publishers")

    tf = TableFrame.__build__(df=lf, mode="raw", idx=idx)
    uri = function_data.uri
    # noinspection PyProtectedMember
    uri = uri.rstrip("/").rstrip("\\") + f"/e/{request.work}/r/{tf._idx}.t"

    file_location = Location({"uri": uri, "env_prefix": function_data.env_prefix})

    logger.debug(f"Performing sink of raw data to '{file_location}'")
    # noinspection PyProtectedMember
    lf = tf._to_lazy()
    sink_lf_to_location(lf, execution_context, file_location)
    logger.debug("File for raw data stored successfully")
    # noinspection PyProtectedMember
    return TableFrame.__build__(
        df=scan_lf_from_location(
            execution_context,
            file_location,
            fail_on_none_uri=True,
        ),
        mode="tab",
        idx=tf._idx,
    )
