#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os

# noinspection PyProtectedMember
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import polars as pl

import tabsdata._utils.tableframe._generators as td_generators
from tabsdata._io.stage_trigger import LocalFileStageTriggerContext, StageTrigger
from tabsdata._table_actions.actions import OnTablesAction
from tabsdata._tabsserver.function import environment_import_utils
from tabsdata._tabsserver.function.checkpoint_store import CheckpointStore
from tabsdata._tabsserver.function.global_utils import convert_uri_to_path
from tabsdata._tabsserver.function.logging_utils import pad_string
from tabsdata._tabsserver.function.named_connection_utils import (
    resolve_named_conn_if_needed,
)
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
from tabsdata.tableframe.lazyframe.properties import (
    TableFrameProperties,
    TableFramePropertiesBuilder,
)

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


def execute_stage_trigger_from_config(
    execution_context: ExecutionContext,
):
    environment_import_utils.update_syspath(execution_context.paths.code_folder)
    # Decided to keep this logic with the code_folder to make it OS-agnostic. If we
    # stored the function_file with the code_folder prefix, we would have to handle
    # the path differently for different OSs.
    result = execute_stage_trigger(execution_context)
    return result


def execute_stage_trigger(
    execution_context: ExecutionContext,
):
    logger.info(pad_string("[Executing stage trigger]"))
    stage_trigger = execution_context.trigger_by
    logger.debug(f"Using stage trigger: {stage_trigger}")
    resolve_named_conn_if_needed(stage_trigger, execution_context.project_id)
    request: StageTriggerYaml = execution_context.request
    if not isinstance(stage_trigger, StageTrigger):
        raise ValueError(
            f"Requested a stage trigger run for {stage_trigger}, but that "
            "class does not inherit from StageTrigger."
        )
    fn_trigger = request.fn_trigger
    staging_folder = convert_uri_to_path(execution_context.request.function_stage_uri)
    os.makedirs(staging_folder, exist_ok=True)
    start_time = stage_trigger.start
    time_now = datetime.now(tz=timezone.utc)
    if start_time is not None:
        logger.info(
            f"The starting time for the stage trigger is '{start_time}', "
            "verifying whether to start or wait for that time."
        )
        if not start_time.tzinfo:
            raise ValueError(
                f"Start time '{start_time}' does not have any timezone information."
            )
        if start_time > time_now:
            time_to_sleep: timedelta = start_time - time_now
            # time_to_sleep.total_seconds() returns the duration in seconds (float)
            sleep_seconds = time_to_sleep.total_seconds()

            logger.info(
                f"Current time '{time_now}' is before target time '{start_time}'."
                f" Sleeping for {sleep_seconds:.2f} seconds until target time is"
                " reached."
            )
            time.sleep(sleep_seconds)
            logger.info(f"Woke up. Target time '{start_time}' should now be reached.")
        else:
            logger.info(
                f"Target datetime '{start_time}' is in the past or present "
                f"(current time '{time_now}'). No sleep needed."
            )
    checkpoint = CheckpointStore(request.function_checkpoint, execution_context)
    stage_context = LocalFileStageTriggerContext(
        staging_folder,
        fn_trigger,
        execution_context.logger,
        working_dir=execution_context.paths.output_folder,
        end=stage_trigger.end,
        checkpoint=checkpoint,
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
    input_actions = execution_context.input_actions
    filter_actions(input_actions)
    output_actions = execution_context.output_actions
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

    # TODO: this logic will most likely be removed as the final way to verify this
    #  flow is discussed:
    from tabsdata._io.inputs.stage_input import ManifestStage, Stage
    from tabsdata._io.inputs.table_inputs import TableInput
    from tabsdata.connector.cdc.common._connector import CdcTrigger

    trigger_by = execution_context.trigger_by
    source = execution_context.source
    if isinstance(trigger_by, CdcTrigger) and isinstance(source, Stage):
        # The isinstance check is redundant, but it helps ensure everything is going
        # as expected
        logger.debug(
            f"Using a CdcTrigger for this function ({trigger_by}), "
            f"so we will use a ManifestStage instead of the Stage {source}"
        )
        source = ManifestStage(use_existing_data=source.use_existing_data)
        logger.debug(f"Using new ManifestStage {source}")
        execution_context._source_plugin = source

    resolve_named_conn_if_needed(source, execution_context.project_id)

    # noinspection PyProtectedMember
    parameters = source._run(execution_context)
    # Raw-data persistence applies only to publisher sources. ``TableInput``
    # bypasses ``load_sources`` entirely (its ``stream`` uses
    # ``scan_tf_from_table`` to return repository-indexed TableFrames that
    # already carry their own idx and metadata), so re-stamping them as raw
    # data would both overwrite valid metadata and fail because non-publisher
    # functions are not provisioned with a raw-data location. The framework's
    # ``TableInput`` class is the sole gatekeeper here: a custom plugin
    # subclassing ``SourcePlugin`` cannot look like a ``TableInput`` and so
    # cannot bypass persistence; conversely, anyone subclassing ``TableInput``
    # inherits its repository-loading ``stream`` and is correct to skip it.
    if not isinstance(source, TableInput):
        parameters = store_sources_raw_data(execution_context, parameters)
    return parameters


def load_sources(
    execution_context: ExecutionContext,
    local_sources: list | str | None,
    working_dir: str | None = None,
) -> InputTableSpec:
    """
    Given a list of sources, normalize them into tabsdata TableFrames so that the
    return value is an InputTableSpec (a list whose elements are TableFrames or
    nested lists of TableFrames).

    Idx generation and request-derived metadata are intentionally NOT applied
    here: those are part of the raw-data persistence concern and are handled by
    ``store_sources_raw_data`` / ``store_source_raw_data`` so that the framework
    owns idx assignment regardless of how the source plugin produced its output.

    :param execution_context: The context of the function.
    :param local_sources: A list of lists of paths to parquet files. Each element is
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
    sources: list[TableFrame | list[TableFrame]] = []
    for source in local_sources:
        logger.debug(f"Loading single source: {source}")
        if isinstance(source, list):
            sources.append(
                load_sources_from_list(
                    execution_context, source, working_dir=working_dir
                )
            )
        else:
            sources.append(
                load_source(
                    execution_context,
                    make_tableframe_context(source, working_dir=working_dir),
                )
            )
    return sources


def load_sources_from_list(
    execution_context: ExecutionContext,
    source_list: list,
    working_dir: str | None = None,
) -> list[TableFrame]:
    sources: list[TableFrame] = []
    for source in source_list:
        sources.append(
            load_source(
                execution_context,
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
# repository (already indexed). When table_frame_context.table is None, the table was
# loaded from a publisher and is treated as raw data; idx and metadata are assigned
# later, by ``store_source_raw_data``.
def load_source(
    execution_context: ExecutionContext,
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
        # Wrap the LazyFrame in a TableFrame so the InputTableSpec contract
        # (every element is a TableFrame or a nested list of TableFrames) is
        # satisfied. The idx and properties applied here are placeholders:
        # ``store_source_raw_data`` re-stamps the TableFrame in 'raw' mode with
        # the framework-managed idx and request-derived properties before
        # sinking it, so the framework owns idx generation regardless of how
        # the source plugin produced its output.
        return TableFrame.__build__(
            df=lf,
            mode="raw",
            idx=td_generators.IdxGenerator(),
            properties=TableFramePropertiesBuilder.empty(),
        )
    else:
        raise ValueError(
            "Unreachable branch in load_source: a TableFrameContext with a "
            "non-None 'table' represents a repository-loaded TableFrame, "
            "which since the TD-461 rework is built directly by "
            "execute_table_importer via scan_tf_from_table and never flows "
            "through load_source."
        )


def store_sources_raw_data(
    execution_context: ExecutionContext,
    sources: InputTableSpec,
) -> InputTableSpec:
    """
    Walk an InputTableSpec and persist every TableFrame it contains as raw data
    in the function data location. Returns a new InputTableSpec where each
    TableFrame has been re-read from its persisted location so that downstream
    steps in the flow see the materialized version.

    A single ``IdxGenerator`` is created here and shared across every persisted
    TableFrame in the spec, ensuring globally unique indices regardless of how
    many groups, nested lists or custom plugins contributed to ``sources``.
    """
    logger.debug("Persisting plugin output as raw data")
    idx = td_generators.IdxGenerator()
    persisted: list = []
    for source in sources:
        if isinstance(source, list):
            persisted.append([
                store_source_raw_data(execution_context, s, idx) for s in source
            ])
        else:
            persisted.append(store_source_raw_data(execution_context, source, idx))
    return persisted


# ToDo: Pending storing metadata. This will require deciding how to determine which
#       information defines each raw data file depending on the source type.
def store_source_raw_data(
    execution_context: ExecutionContext,
    source: TableFrame | None,
    idx: td_generators.IdxGenerator,
) -> TableFrame | None:
    if source is None:
        return None

    logger.info("Storing raw data...")

    request = execution_context.request
    function_data = request.function_data
    if not function_data:
        raise ValueError("The function data location is required for publishers")
    elif function_data.uri is None:
        raise ValueError("The uri for the function data is required for publishers")

    properties: TableFrameProperties = (
        TableFrameProperties
        .builder()
        .with_execution(request.execution_id)
        .with_transaction(request.transaction_id)
        .with_version(EMPTY_VERSION)
        .with_timestamp(request.triggered_on)
        .build()
    )

    # Re-stamp the TableFrame in 'raw' mode with the framework-managed idx and
    # request-derived properties, dropping any system columns that may have
    # been populated upstream (placeholders set by ``load_source``, or
    # arbitrary values set by a custom plugin's ``stream`` implementation).
    # noinspection PyProtectedMember
    tf = TableFrame.__build__(
        df=source._to_lazy(),
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
    sink_lf_to_location(tf._to_lazy(), execution_context, file_location)
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
