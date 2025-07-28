#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import subprocess
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Tuple
from urllib.parse import unquote

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
from tabsdata.io.input import (
    AzureSource,
    LocalFileSource,
    MariaDBSource,
    MySQLSource,
    OracleSource,
    PostgresSource,
    S3Source,
    TableInput,
)
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tableuri import build_table_uri_object
from tabsdata.tabsserver.function import environment_import_utils
from tabsdata.tabsserver.function.cloud_connectivity_utils import (
    SERVER_SIDE_AWS_ACCESS_KEY_ID,
    SERVER_SIDE_AWS_REGION,
    SERVER_SIDE_AWS_SECRET_ACCESS_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_NAME,
    obtain_and_set_azure_credentials,
    obtain_and_set_s3_credentials,
    set_s3_region,
)
from tabsdata.tabsserver.function.global_utils import (
    CURRENT_PLATFORM,
    convert_path_to_uri,
)
from tabsdata.tabsserver.function.logging_utils import pad_string
from tabsdata.tabsserver.function.native_tables_utils import (
    scan_lf_from_location,
    scan_tf_from_table,
    sink_lf_to_location,
)
from tabsdata.tabsserver.function.offset_utils import (
    OFFSET_LAST_MODIFIED_VARIABLE_NAME,
)
from tabsdata.tabsserver.function.results_collection import ResultsCollection
from tabsdata.tabsserver.function.yaml_parsing import (
    Location,
    Table,
    TableVersions,
    TransporterAzure,
    TransporterCSVFormat,
    TransporterEnv,
    TransporterJsonFormat,
    TransporterLocalFile,
    TransporterLogFormat,
    TransporterParquetFormat,
    TransporterS3,
    V1ImportFormat,
    parse_import_report_yaml,
    store_import_as_yaml,
)
from tabsdata.utils.sql_utils import obtain_uri

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
    logger.info("New initial values generated")
    if source_plugin := execution_context.source_plugin:
        # If working with a plugin, the new initial values are stored in the plugin
        new_initial_values = source_plugin.initial_values
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
    if isinstance(source, LocalFileSource):
        logger.debug("Triggering LocalFileSource")
        local_sources = execute_file_importer(
            source, destination_folder, initial_values
        )
    elif isinstance(source, S3Source):
        logger.debug("Triggering S3Source")
        obtain_and_set_s3_credentials(source.credentials)
        set_s3_region(source.region)
        local_sources = execute_file_importer(
            source, destination_folder, initial_values
        )
    elif isinstance(source, AzureSource):
        logger.debug("Triggering AzureSource")
        obtain_and_set_azure_credentials(source.credentials)
        local_sources = execute_file_importer(
            source, destination_folder, initial_values
        )
    elif isinstance(source, MySQLSource):
        logger.debug("Triggering MySQLSource")
        local_sources = execute_sql_importer(source, destination_folder, initial_values)
    elif isinstance(source, PostgresSource):
        logger.debug("Triggering PostgresSource")
        local_sources = execute_sql_importer(source, destination_folder, initial_values)
    elif isinstance(source, MariaDBSource):
        logger.debug("Triggering MariaDBSource")
        local_sources = execute_sql_importer(source, destination_folder, initial_values)
    elif isinstance(source, OracleSource):
        logger.debug("Triggering OracleSource")
        local_sources = execute_sql_importer(source, destination_folder, initial_values)
    elif isinstance(source, TableInput):
        logger.debug("Triggering TableInput")
        # When loading tabsdata tables, we return tuples of (uri, table), so that
        # coming operations can use information on the request for further processing.
        result = execute_table_importer(source, execution_context)
        logger.info("Loaded tables successfully")
        return result
    else:
        logger.error(f"Invalid source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid source type: {type(source)}. No data imported.")
    # Upload all files in a specific folder, with parquet format always
    logger.debug(f"Local sources: '{local_sources}'")
    result = load_sources(execution_context, local_sources, idx)
    logger.info("Loaded sources successfully")
    return result


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


def execute_sql_importer(
    source: MariaDBSource | MySQLSource | OracleSource | PostgresSource,
    destination: str,
    initial_values: Offset,
) -> list | dict:
    if isinstance(source.query, str):
        source_list = [
            execute_sql_query(source, destination, source.query, initial_values)
        ]
    elif isinstance(source.query, list):
        source_list = []
        for query in source.query:
            source_list.append(
                execute_sql_query(source, destination, query, initial_values)
            )
    else:
        logger.error(
            f"Invalid source data, expected 'str' or 'list' but got: {source.query}"
        )
        raise TypeError(
            f"Invalid source data, expected 'str' or 'list' but got: {source.query}"
        )
    return source_list


def replace_initial_values(query: str, initial_values: dict) -> str:
    """
    Replace the placeholders in the query with the initial values
    """
    logger.debug(f"Replacing initial values {initial_values} in query: {query}")
    for key, value in initial_values.items():
        query = query.replace(f":{key}", str(value))
    return query


def execute_sql_query(
    source: MariaDBSource | MySQLSource | OracleSource | PostgresSource,
    destination: str,
    query: str,
    initial_values: Offset,
) -> str | None:
    logger.info(f"Importing SQL query: {query}")
    if source.initial_values:
        initial_values.returns_values = True
        initial_values = (
            source.initial_values
            if initial_values.use_decorator_values
            else initial_values.current_offset
        )
        query = replace_initial_values(query, initial_values)
    if isinstance(source, MySQLSource):
        logger.info("Importing SQL query from MySQL")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, PostgresSource):
        logger.info("Importing SQL query from Postgres")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, MariaDBSource):
        logger.info("Importing SQL query from MariaDB")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, OracleSource):
        logger.info("Importing SQL query from Oracle")
        uri = obtain_uri(source, log=True, add_credentials=True)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    else:
        logger.error(f"Invalid SQL source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid SQL source type: {type(source)}. No data imported.")
    if loaded_frame.is_empty():
        logger.warning(f"No data obtained from query: '{query}'")
        return None
    else:
        destination_file = os.path.join(
            destination, f"{datetime.now(tz=timezone.utc).timestamp()}.parquet"
        )
        loaded_frame.write_parquet(destination_file)
        logger.info(f"Imported SQL query to: {destination_file}")
        return destination_file


def execute_file_importer(
    source: AzureSource | LocalFileSource | S3Source,
    destination_folder: str,
    initial_values: Offset = None,
) -> list:
    """
    Import files from a source to a destination. The source can be either a local file
        or an S3 bucket. The destination is always a local folder. The result is a list
        of files that were imported. Each element of the list is a list of paths to
        parquet files.
    :return: A list of files that were imported. Each element of the list is a list
        of paths to parquet files.
    """
    if isinstance(source, LocalFileSource):
        # noinspection PyProtectedMember
        # Unquote the uri, since the pathlib.Path.as_uri() method encodes the path
        # with percent-encoding, and some wildcard characters are encoded.
        location_list = [
            unquote(convert_path_to_uri(path)) for path in source._path_list
        ]
    elif isinstance(source, (S3Source, AzureSource)):
        # noinspection PyProtectedMember
        location_list = source._uri_list
    else:
        logger.error(f"Invalid source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid source type: {type(source)}. No data imported.")
    destination_folder = (
        destination_folder
        if destination_folder.endswith(os.sep)
        else destination_folder + os.sep
    )
    last_modified = None
    lastmod_info = None
    if source.initial_last_modified:
        last_modified = source.initial_last_modified
        processed_initial_last_modified = datetime.fromisoformat(last_modified)
        logger.debug(
            f"Last modified time '{last_modified}' converted to "
            f"datetime object '{processed_initial_last_modified}'."
        )
        if processed_initial_last_modified.tzinfo is None:
            logger.error(
                f"Last modified time '{last_modified}', converted to "
                f"datetime object '{processed_initial_last_modified}' "
                "is not timezone-aware, but having a timezone is a "
                "requirement "
                "for initial_last_modified."
            )
            raise ValueError(
                f"Last modified time '{last_modified}', converted to "
                f"datetime object '{processed_initial_last_modified}' "
                "is not timezone-aware, but having a timezone is a "
                "requirement "
                "for initial_last_modified."
            )
        utc_initial_last_modified = processed_initial_last_modified.astimezone(
            timezone.utc
        )
        logger.debug(
            f"Last modified time '{last_modified}' converted to "
            f" UTC datetime object '{utc_initial_last_modified}'."
        )
        utc_last_modified_string = utc_initial_last_modified.isoformat(
            timespec="microseconds"
        )
        logger.debug(
            f"Last modified time '{last_modified}' converted to "
            f"UTC string '{utc_last_modified_string}'."
        )
        last_modified = utc_last_modified_string
        if initial_values.use_decorator_values:
            logger.debug("Using decorator last modified value")
        else:
            logger.debug("Using stored last modified value")
            lastmod_info = initial_values.current_offset.get(
                OFFSET_LAST_MODIFIED_VARIABLE_NAME
            )
    logger.debug(f"Last modified: '{last_modified}'; lastmod_info: '{lastmod_info}'")
    source_list = []
    for location in location_list:
        sources, lastmod_info = execute_single_file_import(
            origin_location_uri=location,
            destination_folder=destination_folder,
            file_format=source.format,
            initial_last_modified=last_modified,
            user_source=source,
            lastmod_info=lastmod_info,
        )
        source_list.append(sources)
    if source.initial_last_modified:
        logger.debug("Capturing new last modified information")
        initial_values.update_new_values(
            {OFFSET_LAST_MODIFIED_VARIABLE_NAME: lastmod_info}
        )
    return source_list


def is_wildcard_pattern(pattern: str) -> bool:
    return any(char in pattern for char in "*?")


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


def obtain_transporter_import(
    origin_location_uri: str,
    destination_folder: str,
    file_format: FileFormat,
    initial_last_modified: str | None,
    user_source: LocalFileSource | S3Source | AzureSource,
    lastmod_info: str = None,
):
    # Create the transporter source object
    if isinstance(user_source, S3Source):
        transporter_source = TransporterS3(
            origin_location_uri,
            access_key=TransporterEnv(SERVER_SIDE_AWS_ACCESS_KEY_ID),
            secret_key=TransporterEnv(SERVER_SIDE_AWS_SECRET_ACCESS_KEY),
            region=(
                TransporterEnv(SERVER_SIDE_AWS_REGION) if user_source.region else None
            ),
        )
    elif isinstance(user_source, AzureSource):
        transporter_source = TransporterAzure(
            origin_location_uri,
            account_name=TransporterEnv(SERVER_SIDE_AZURE_ACCOUNT_NAME),
            account_key=TransporterEnv(SERVER_SIDE_AZURE_ACCOUNT_KEY),
        )
    elif isinstance(user_source, LocalFileSource):
        transporter_source = TransporterLocalFile(origin_location_uri)
    else:
        logger.error(f"Importing from '{user_source}' not supported.")
        raise TypeError(f"Importing from '{user_source}' not supported.")
    logger.debug(f"Source config: {transporter_source}")

    # Create the transporter format object
    if isinstance(file_format, CSVFormat):
        transporter_format = TransporterCSVFormat(file_format)
    elif isinstance(file_format, LogFormat):
        transporter_format = TransporterLogFormat()
    elif isinstance(file_format, NDJSONFormat):
        transporter_format = TransporterJsonFormat()
    elif isinstance(file_format, ParquetFormat):
        transporter_format = TransporterParquetFormat()
    else:
        logger.error(f"Invalid file format: {type(file_format)}. No data imported.")
        raise TypeError(f"Invalid file format: {type(file_format)}. No data imported.")
    logger.debug(f"Format config: {transporter_format}")

    # Create transporter target object
    transporter_target = TransporterLocalFile(convert_path_to_uri(destination_folder))
    logger.debug(f"Target config: {transporter_target}")

    logger.debug(
        f"Using initial_lastmod: '{initial_last_modified}' "
        f"and lastmod_info: '{lastmod_info}'"
    )

    transporter_import = V1ImportFormat(
        source=transporter_source,
        target=transporter_target,
        format=transporter_format,
        initial_lastmod=initial_last_modified,
        lastmod_info=lastmod_info,
    )

    logger.debug(f"Transporter import config: {transporter_import}")
    return transporter_import


# noinspection DuplicatedCode
def execute_single_file_import(
    origin_location_uri: str,
    destination_folder: str,
    file_format: FileFormat,
    initial_last_modified: str | None,
    user_source: LocalFileSource | S3Source | AzureSource,
    lastmod_info: str = None,
) -> (list[str] | str, str | None):
    """
    Import a file from a location to a destination with a specific format. The file is
        imported using a binary, and the result returned is always a list of parquet
        files. If the location contained a wildcard for the files, the list might
        contain one or more elements.
    :return: list of imported files if using a wildcard pattern, single file if not.
    """
    transporter_import = obtain_transporter_import(
        origin_location_uri,
        destination_folder,
        file_format,
        initial_last_modified,
        user_source,
        lastmod_info,
    )

    yaml_request_file = os.path.join(destination_folder, f"request_{uuid.uuid4()}.yaml")
    store_import_as_yaml(
        transporter_import,
        yaml_request_file,
    )

    binary = "transporter.exe" if CURRENT_PLATFORM.is_windows() else "transporter"
    report_file = os.path.join(destination_folder, f"report_{uuid.uuid4()}.yaml")
    arguments = f"--request {yaml_request_file} --report {report_file}"
    logger.debug(f"Importing files with command: {binary} {arguments}")
    subprocess_result = subprocess.run(
        [binary] + arguments.split(), capture_output=True, text=True
    )
    if subprocess_result.returncode != 0:
        logger.error(
            "Error importing file (return code "
            f"'{subprocess_result.returncode}'):"
            f" {subprocess_result.stderr}"
        )
        raise Exception(
            "Error importing file (return code "
            f"'{subprocess_result.returncode}'):"
            f" {subprocess_result.stderr}"
        )

    result = parse_import_report_yaml(report_file)
    files = result.files
    logger.debug(f"Parsed import report: {result}")
    if is_wildcard_pattern(origin_location_uri):
        source_list = []
        if files:
            for dictionary in files:
                source_list.append(dictionary.get("to"))
            logger.info(f"Imported files to: '{source_list}'")
        else:
            logger.info("No files imported")
    else:
        source_list = files[0].get("to") if files else None
        if not source_list:
            logger.info("No file imported")
        # If the data is not a wildcard pattern, the result is a single file
        else:
            logger.info(f"Imported file to: '{source_list}'")
    logger.debug(f"New lastmod_info: '{result.lastmod_info}'")
    return source_list, result.lastmod_info


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
