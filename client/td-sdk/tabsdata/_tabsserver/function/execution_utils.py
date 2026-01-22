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
from tabsdata._io.stage_trigger import LocalFileStageTriggerContext, StageTrigger
from tabsdata._table_actions.actions import OnTablesAction
from tabsdata._tabsserver.function import environment_import_utils
from tabsdata._tabsserver.function.global_utils import convert_uri_to_path
from tabsdata._tabsserver.function.logging_utils import pad_string
from tabsdata._tabsserver.function.native_tables_utils import (
    scan_lf_from_location,
    sink_lf_to_location,
)
from tabsdata._tabsserver.function.offset_utils import OffsetReturn
from tabsdata._tabsserver.function.results_collection import ResultsCollection
from tabsdata._tabsserver.function.typing import (
    DerivedTableFrameSpec,
    InputTableSpec,
    OutputTableSpec,
)
from tabsdata._tabsserver.function.yaml_parsing import Location, StageTriggerYaml
from tabsdata._utils.tableframe._constants import EMPTY_VERSION

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._context import TableFrameContext
from tabsdata.dataquality import DataQuality
from tabsdata.tableframe._actions.dataquality.processor import DataQualityProcessor
from tabsdata.tableframe.lazyframe.frame import DataTable, TableFrame
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

    result = execute_function(execution_context, parameters)

    return result


def execute_listener_from_config(
    execution_context: ExecutionContext,
):
    environment_import_utils.update_syspath(execution_context.paths.code_folder)
    # Decided to keep this logic with the code_folder to make it OS-agnostic. If we
    # stored the function_file with the code_folder prefix, we would have to handle
    # the path differently for different OSs.
    result = execute_listener(execution_context)
    return result


def execute_listener(
    execution_context: ExecutionContext,
):
    logger.info(pad_string("[Executing stage trigger]"))
    stage_trigger = execution_context.stage_trigger
    logger.debug(f"Using stage trigger: {stage_trigger}")
    request: StageTriggerYaml = execution_context.request
    if not isinstance(stage_trigger, StageTrigger):
        raise ValueError(
            f"Requested a stage trigger run for {stage_trigger}, but that "
            "class does not inherit from StageTrigger."
        )
    fn_trigger = request.fn_trigger
    staging_folder = convert_uri_to_path(execution_context.request.function_stage_uri)
    os.makedirs(staging_folder, exist_ok=True)
    stage_context = LocalFileStageTriggerContext(
        staging_folder,
        fn_trigger,
        execution_context.logger,
        working_dir=execution_context.paths.output_folder,
    )
    logger.debug(f"Stage trigger context: {stage_context}")
    logger.info(
        f"Calling the 'run' method of the provided stage trigger {stage_trigger}"
    )
    result = stage_trigger.run(stage_context)
    logger.info(f"Stage trigger executed with result '{result}'")
    return result


def execute_function(
    execution_context: ExecutionContext,
    input_table_frames: InputTableSpec,
) -> ResultsCollection:
    logger.info(pad_string("[Executing function]"))

    check_actions(execution_context)

    enrich_table_frames(execution_context, input_table_frames)
    output_table_frames: OutputTableSpec = []
    derived_table_frames: DerivedTableFrameSpec = []
    dq_processor = DataQualityProcessor.from_execution_context(execution_context)

    pre_function_run(
        execution_context,
        dq_processor,
        input_table_frames,
        output_table_frames,
        derived_table_frames,
    )
    function_run(
        execution_context,
        dq_processor,
        input_table_frames,
        output_table_frames,
        derived_table_frames,
    )
    post_function_run(
        execution_context,
        dq_processor,
        input_table_frames,
        output_table_frames,
        derived_table_frames,
    )

    logger.info("Executing function finished")

    otf = tuple(output_table_frames)
    dtf = tuple(
        derived_table_frame.derived_tf for derived_table_frame in derived_table_frames
    )
    results = otf + dtf

    return ResultsCollection(results)


def pre_function_run(
    _execution_context: ExecutionContext,
    _dq_processor: DataQualityProcessor,
    _input_table_frames: InputTableSpec,
    _output_table_frames: OutputTableSpec,
    _derived_table_frames: DerivedTableFrameSpec,
):
    logger.info("Starting execution of function pre-processing")

    logger.info("Finished execution of function pre-processing")


def function_run(
    execution_context: ExecutionContext,
    _dq_processor: DataQualityProcessor,
    input_table_frames: InputTableSpec,
    output_table_frames: OutputTableSpec,
    _derived_table_frames: DerivedTableFrameSpec,
):
    logger.info("Starting execution of function provided by the user")

    result = execution_context.user_provided_function(*input_table_frames)

    logger.info("Finished executing function provided by the user")

    logger.info("Building the user-provided function result")

    if execution_context.status.offset.returns_values:
        result = update_initial_values(execution_context, result)
    result = ResultsCollection(result)
    result.check_collection_integrity()

    logger.info("Finished building the user-provided function result")

    output_table_frames.extend(result.to_list())


def post_function_run(
    _execution_context: ExecutionContext,
    dq_processor: DataQualityProcessor,
    input_table_frames: InputTableSpec,
    output_table_frames: OutputTableSpec,
    derived_table_frames: DerivedTableFrameSpec,
):
    logger.info("Starting execution of function post-processing")

    dq_processor.tables_data(
        input_table_frames,
        output_table_frames,
        derived_table_frames,
    )

    logger.info("Finished execution of function post-processing")


def check_actions(execution_context: ExecutionContext):
    try:
        # noinspection PyUnresolvedReferences
        input_actions = execution_context.user_provided_function.input_actions
    except AttributeError:
        input_actions = []
    filter_actions(input_actions)
    try:
        output_actions = execution_context.user_provided_function.output_actions
    except AttributeError:
        output_actions = []
    filter_actions(output_actions)


def filter_actions(actions: list[OnTablesAction]):
    if actions is not None:
        for i, action in enumerate(actions):
            if action is None:
                raise ValueError(
                    f"Configured actions contain 'None' at index {i}. "
                    "No entry can be null."
                )
            if not isinstance(action, DataQuality):
                raise ValueError(
                    f"Configured actions contain an invalid type at index {i}: "
                    f"expected DataQuality, but got {type(action).__name__}."
                )


def enrich_table_frames(
    execution_context: ExecutionContext,
    input_table_frames: InputTableSpec,
):
    from tabsdata import TableInput

    source = execution_context.source
    if isinstance(source, TableInput):
        input_table_uris = source._table_uris_list

        if len(input_table_frames) != len(input_table_uris):
            raise ValueError(
                "Mismatched lengths between input table frames "
                f"({len(input_table_frames)}) "
                f"and input tables ({len(input_table_uris)})"
            )

        for i, (table_frame, table_uri) in enumerate(
            zip(input_table_frames, input_table_uris)
        ):
            data_table = DataTable(name=table_uri.table, position=i)
            if table_frame is None:
                logger.debug(f"Skipping enrichment for None TableFrame @ index: {i}")
                continue
            elif isinstance(table_frame, TableFrame):
                logger.debug(f"Enriching TableFrame @ index: {i} with {data_table}")
                table_frame._data_table = data_table
            elif isinstance(table_frame, list):
                for table_frame_item in table_frame:
                    if table_frame_item is None:
                        logger.debug(
                            f"Skipping enrichment for None TableFrame item @ index: {i}"
                        )
                    else:
                        logger.debug(
                            f"Enriching TableFrame item @ index: {i} with {data_table}"
                        )
                        table_frame_item._data_table = data_table
            else:
                raise ValueError(
                    f"Unexpected type found in input table frames: {type(table_frame)}"
                )
    else:
        logger.debug("No input tables provided")


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
) -> InputTableSpec:
    logger.debug("Running the source plugin")

    input_actions = execution_context.input_actions
    output_actions = execution_context.output_actions
    execution_context.user_provided_function.input_actions = input_actions
    execution_context.user_provided_function.output_actions = output_actions

    # noinspection PyProtectedMember
    parameters = execution_context.source._run(execution_context)
    return parameters


def load_sources(
    execution_context: ExecutionContext,
    local_sources: list | str | None,
    idx: td_generators.IdxGenerator | None = None,
    working_dir: str | None = None,
) -> InputTableSpec:
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
