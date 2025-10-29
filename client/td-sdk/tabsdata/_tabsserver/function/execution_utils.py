#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._generators as td_generators
from tabsdata._tabsserver.function import environment_import_utils
from tabsdata._tabsserver.function.logging_utils import pad_string
from tabsdata._tabsserver.function.native_tables_utils import (
    scan_lf_from_location,
    sink_lf_to_location,
)
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata._tabsserver.function.results_collection import ResultsCollection
from tabsdata._tabsserver.function.yaml_parsing import (
    Location,
)
from tabsdata._utils.tableframe._constants import EMPTY_VERSION

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._context import TableFrameContext
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tableframe.lazyframe.properties import TableFrameProperties

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext

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
    source_plugin = execution_context.source
    # noinspection PyProtectedMember
    if source_plugin._offset_return == OffsetReturn.ATTRIBUTE.value:
        logger.debug("Obtaining new offset from the plugin object")
        # The new initial values are stored in the plugin
        new_initial_values = source_plugin.initial_values
    elif source_plugin._offset_return == OffsetReturn.FUNCTION.value:
        # The new initial values are part of the result
        logger.debug("Obtaining new offset from the plugin result")
        if isinstance(result, tuple):
            *result, new_initial_values = result
            result = tuple(result)
        else:
            new_initial_values = result
            result = (None,)
    else:
        # noinspection PyProtectedMember
        raise ValueError(f"Invalid offset return type: {source_plugin._offset_return}")
    execution_context.status.offset.update_new_values(new_initial_values)
    return result


def obtain_user_provided_function_parameters(
    execution_context: ExecutionContext,
) -> list[TableFrame | None | list[TableFrame | None]]:
    logger.debug("Running the source plugin")
    # noinspection PyProtectedMember
    parameters = execution_context.source._run(execution_context)
    return parameters


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


# noinspection PyUnreachableCode
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

    properties: TableFrameProperties = (
        TableFrameProperties.builder()
        .with_execution(request.execution_id)
        .with_transaction(request.transaction_id)
        .with_version(EMPTY_VERSION)
        .with_timestamp(request.triggered_on)
        .build()
    )
    tf = TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=idx,
        properties=properties,
    )

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
        properties=properties,
    )
