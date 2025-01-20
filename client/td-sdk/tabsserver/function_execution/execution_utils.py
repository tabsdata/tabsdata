#
# Copyright 2024 Tabs Data Inc.
#

import ast
import json
import logging
import os
import pathlib
import subprocess
import tempfile
from collections.abc import Callable
from datetime import datetime
from typing import List, Tuple
from urllib.parse import unquote

import base32hex
import cloudpickle
import polars as pl
from uuid_v7.base import uuid7

import tabsdata as td
import tabsdata.tableframe.lazyframe.frame as td_frame
from tabsdata import SourcePlugin
from tabsdata.format import (
    CSVFormat,
    FileFormat,
    LogFormat,
    NDJSONFormat,
    ParquetFormat,
)
from tabsdata.tableuri import build_table_uri_object
from tabsdata.tabsdatafunction import (
    AzureSource,
    Input,
    LocalFileSource,
    MariaDBSource,
    MySQLSource,
    OracleSource,
    PostgresSource,
    S3Source,
    TableInput,
    build_input,
)
from tabsdata.utils.bundle_utils import (
    CODE_FOLDER,
    CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY,
    CONFIG_ENTRY_POINT_KEY,
    CONFIG_INPUTS_KEY,
    PLUGINS_FOLDER,
)

from . import environment_import_utils, sql_utils
from .cloud_connectivity_utils import (
    obtain_and_set_azure_credentials,
    obtain_and_set_s3_credentials,
    set_s3_region,
)
from .global_utils import (
    CURRENT_PLATFORM,
    TABSDATA_IDENTIFIER_COLUMN,
    convert_path_to_uri,
)
from .initial_values_utils import (
    INITIAL_VALUES,
    INITIAL_VALUES_LAST_MODIFIED_VARIABLE_NAME,
)
from .yaml_parsing import InputYaml, Table, TableVersions

logger = logging.getLogger(__name__)

INPUT_PLUGIN_FOLDER = "plugin_files"
SOURCES_FOLDER = "sources"


def execute_function_from_config(
    config: dict, working_dir: str, execution_context: InputYaml
):
    function_file = config[CONFIG_ENTRY_POINT_KEY][CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY]
    code_folder = os.path.join(working_dir, CODE_FOLDER)
    environment_import_utils.update_syspath(code_folder)
    # Decided to keep this logic with the code_folder to make it OS-agnostic. If we
    # stored the function_file with the code_folder prefix, we would have to handle
    # the path differently for different OSs.
    with open(os.path.join(code_folder, function_file), "rb") as f:
        met = cloudpickle.load(f)
    return execute_function_with_config(config, met, working_dir, execution_context)


def execute_function_with_config(
    config: dict, met: Callable, working_dir: str, execution_context: InputYaml
):
    input_config = config.get(CONFIG_INPUTS_KEY)
    if SourcePlugin.IDENTIFIER in input_config:
        importer_plugin_file = input_config.get(SourcePlugin.IDENTIFIER)
        plugins_folder = os.path.join(working_dir, PLUGINS_FOLDER)
        with open(os.path.join(plugins_folder, importer_plugin_file), "rb") as f:
            importer_plugin = cloudpickle.load(f)
        destination_dir = os.path.join(working_dir, INPUT_PLUGIN_FOLDER)
        os.makedirs(destination_dir, exist_ok=True)
        logger.info(
            f"Importing files with plugin '{importer_plugin}' to '{destination_dir}'"
        )
        # Add new value of initial values to plugin if provided
        if INITIAL_VALUES.current_initial_values:
            importer_plugin.initial_values = INITIAL_VALUES.current_initial_values
            logger.debug(
                f"Updated plugin initial values: {importer_plugin.initial_values}"
            )
        logger.info("Starting plugin import")
        resulting_files = importer_plugin.trigger_input(destination_dir)
        logger.info(
            f"Imported files with plugin '{importer_plugin}' to "
            f"'{destination_dir}'. Resulting files: '{resulting_files}'"
        )
        if importer_plugin.initial_values:
            INITIAL_VALUES.returns_values = True
        if isinstance(resulting_files, str):
            resulting_files_paths = os.path.join(destination_dir, resulting_files)
        else:
            resulting_files_paths = [
                os.path.join(destination_dir, file) for file in resulting_files
            ]
        source_config = LocalFileSource(path=resulting_files_paths)
        logger.info(f"Triggering source with config: {source_config}")
        parameters = trigger_source(source_config, working_dir)
    else:
        source_config = build_input(config.get(CONFIG_INPUTS_KEY))
        parameters = trigger_source(source_config, working_dir, execution_context)
    logger.info("Executing function provided by the user.")
    result = met(*parameters)
    logger.info("Finished executing function provided by the user")
    if INITIAL_VALUES.returns_values:
        logger.info("New initial values generated")
        if SourcePlugin.IDENTIFIER in input_config:
            # If working with a plugin, the new initial values are stored in the plugin
            new_initial_values = importer_plugin.initial_values
        else:
            # If working with a source, the new initial values are part of the result
            if isinstance(result, tuple):
                *result, new_initial_values = result
            else:
                new_initial_values = result
                result = None
        if not isinstance(new_initial_values, dict):
            logger.error(
                f"Invalid type for new initial values: {type(new_initial_values)}."
                " No initial values stored."
            )
            raise TypeError(
                f"Invalid type for new initial values: {type(new_initial_values)}."
                " No initial values stored."
            )
        INITIAL_VALUES.update_new_values(new_initial_values)
    result = convert_tuple_to_list(result)
    # TODO: Remove this once we are confident that the system columns are always
    #   added by the TableFrame
    result = assemble_result_columns(result)
    return result


def convert_tuple_to_list(
    result: td.TableFrame | Tuple[td.TableFrame],
) -> List[td.TableFrame] | td.TableFrame:
    if isinstance(result, tuple):
        return list(result)
    else:
        return result


def assemble_result_columns(result: td.TableFrame | None | List[td.TableFrame | None]):
    if isinstance(result, td.TableFrame):
        result = td_frame._assemble_columns(result)
    elif isinstance(result, list):
        result = [assemble_result_columns(table) for table in result]
    return result


def trigger_source(
    source: Input, working_dir: str, execution_context: InputYaml = None
):
    # Call binary to import files
    destination_folder = os.path.join(working_dir, SOURCES_FOLDER)
    os.makedirs(destination_folder, exist_ok=True)
    if isinstance(source, LocalFileSource):
        logger.debug("Triggering LocalFileSource")
        local_sources = execute_file_importer(source, destination_folder)
    elif isinstance(source, S3Source):
        logger.debug("Triggering S3Source")
        obtain_and_set_s3_credentials(source.credentials)
        set_s3_region(source.region)
        local_sources = execute_file_importer(source, destination_folder)
    elif isinstance(source, AzureSource):
        logger.debug("Triggering AzureSource")
        obtain_and_set_azure_credentials(source.credentials)
        local_sources = execute_file_importer(source, destination_folder)
    elif isinstance(source, MySQLSource):
        logger.debug("Triggering MySQLSource")
        local_sources = execute_sql_importer(source, destination_folder)
    elif isinstance(source, PostgresSource):
        logger.debug("Triggering PostgresSource")
        local_sources = execute_sql_importer(source, destination_folder)
    elif isinstance(source, MariaDBSource):
        logger.debug("Triggering MariaDBSource")
        local_sources = execute_sql_importer(source, destination_folder)
    elif isinstance(source, OracleSource):
        logger.debug("Triggering OracleSource")
        local_sources = execute_sql_importer(source, destination_folder)
    elif isinstance(source, TableInput):
        logger.debug("Triggering TableInput")
        local_sources = execute_table_importer(source, execution_context)
    else:
        logger.error(f"Invalid source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid source type: {type(source)}. No data imported.")
    # Upload all files in a specific folder, with parquet format always
    logger.debug(f"Local sources: '{local_sources}'")
    result = load_sources(local_sources)
    logger.info("Loaded sources successfully")
    return result


def execute_table_importer(
    source: TableInput, execution_context: InputYaml
) -> List[str]:
    # Right now, source provides very little information, but we use it to do a small
    # sanity check and to ensure that everything is running properly
    execution_context_input_entry_list = execution_context.input
    logger.info(
        f"Importing tables '{execution_context_input_entry_list}' and matching them"
        f" with source '{source}'"
    )
    table_list = []
    # Note: source.uri is a list of URIs, it can't be a single URI because when we
    # serialised it we stored it as such even if it was a single one.
    if len(execution_context_input_entry_list) != len(source.table):
        logger.error(
            "Number of tables in the execution context input"
            f" ({len(execution_context_input_entry_list)}) does not match the "
            "number of"
            f" URIs in the source ({len(source.table)}). No data imported."
        )
        raise ValueError(
            "Number of tables in the execution context input"
            f" ({len(execution_context_input_entry_list)}) does not match the "
            "number of"
            f" URIs in the source ({len(source.table)}). No data imported."
        )
    for execution_context_input_entry, source_table_str in zip(
        execution_context_input_entry_list, source.table
    ):
        logger.info(f"Unpacking '{execution_context_input_entry}'")
        if isinstance(execution_context_input_entry, Table):
            real_table_uri = obtain_table_uri_and_verify(
                execution_context_input_entry, source_table_str
            )
            table_list.append(real_table_uri)
        elif isinstance(execution_context_input_entry, TableVersions):
            logger.debug(
                f"Matching TableVersions '{execution_context_input_entry}' with source"
                f" URI '{source_table_str}'"
            )
            list_of_table_objects = execution_context_input_entry.list_of_table_objects
            list_of_table_uris = []
            for table in list_of_table_objects:
                real_table_uri = obtain_table_uri_and_verify(table, source_table_str)
                list_of_table_uris.append(real_table_uri)
            table_list.append(list_of_table_uris)
        else:
            logger.error(
                f"Invalid table type: {type(execution_context_input_entry)}. No data"
                " imported."
            )
            raise TypeError(
                f"Invalid table type: {type(execution_context_input_entry)}. No data"
                " imported."
            )
    logger.debug(f"Table list obtained: {table_list}")
    return table_list


def obtain_table_uri_and_verify(
    execution_context_table: Table, source_table_str: str
) -> str:
    # For now, we do only this small check for the table name, but we could
    # add more checks in the future.
    logger.debug(
        f"Matching table '{execution_context_table}' with source URI"
        f" '{source_table_str}'"
    )
    source_table_uri = build_table_uri_object(source_table_str)
    logger.debug(
        f"Source table '{source_table_str}' converted to TableURI: '{source_table_uri}'"
    )
    if execution_context_table.name != source_table_uri.table:
        logger.warning(
            f"Execution context table name '{execution_context_table.name}' does not "
            f"match the source table name '{source_table_uri.table}'"
        )
    table_uri = execution_context_table.uri
    logger.debug(f"Table URI: {table_uri}")
    return table_uri


def execute_sql_importer(
    source: MariaDBSource | MySQLSource | OracleSource | PostgresSource,
    destination: str,
) -> list | dict:
    if isinstance(source.query, str):
        source_list = [execute_sql_query(source, destination, source.query)]
    elif isinstance(source.query, list):
        source_list = []
        for query in source.query:
            source_list.append(execute_sql_query(source, destination, query))
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
) -> str:
    logger.info(f"Importing SQL query: {query}")
    if source.initial_values:
        INITIAL_VALUES.returns_values = True
        initial_values = INITIAL_VALUES.current_initial_values or source.initial_values
        query = replace_initial_values(query, initial_values)
    if isinstance(source, MySQLSource):
        logger.info("Importing SQL query from MySQL")
        uri = sql_utils.obtain_uri(source)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, PostgresSource):
        logger.info("Importing SQL query from Postgres")
        uri = sql_utils.obtain_uri(source)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, MariaDBSource):
        logger.info("Importing SQL query from MariaDB")
        uri = sql_utils.obtain_uri(source)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    elif isinstance(source, OracleSource):
        logger.info("Importing SQL query from Oracle")
        uri = sql_utils.obtain_uri(source)
        loaded_frame = pl.read_database_uri(query=query, uri=uri)
    else:
        logger.error(f"Invalid SQL source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid SQL source type: {type(source)}. No data imported.")
    # TODO: Convert into a plugin
    #   https://tabsdata.atlassian.net/browse/TAB-14
    loaded_frame = loaded_frame.with_columns(
        pl.first()
        .map_batches(lambda x: td_id_column(x.len()), is_elementwise=True)
        .cast(pl.String)
        .alias(TABSDATA_IDENTIFIER_COLUMN)
    )
    destination_file = os.path.join(
        destination, f"{datetime.now().timestamp()}.parquet"
    )
    loaded_frame.write_parquet(destination_file)
    logger.info(f"Imported SQL query to: {destination_file}")
    return destination_file


# TODO: Convert into a plugin
# https://tabsdata.atlassian.net/browse/TAB-14
def td_id():
    i = uuid7().bytes
    return base32hex.b32encode(i)[:26]


# TODO: Convert into a plugin
# https://tabsdata.atlassian.net/browse/TAB-14
def td_id_column(size: int):
    b = []
    for i in range(size):
        b.append(td_id())
    return pl.Series("uuid", b, dtype=pl.String)


def execute_file_importer(
    source: AzureSource | LocalFileSource | S3Source,
    destination: str,
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
        location_list = [convert_path_to_uri(path) for path in source._path_list]
    elif isinstance(source, (S3Source, AzureSource)):
        location_list = source._uri_list
    else:
        logger.error(f"Invalid source type: {type(source)}. No data imported.")
        raise TypeError(f"Invalid source type: {type(source)}. No data imported.")
    destination = destination if destination.endswith(os.sep) else destination + os.sep
    destination = pathlib.Path(destination).as_uri()
    last_modified = None
    if source.initial_last_modified:
        last_modified = INITIAL_VALUES.current_initial_values.get(
            INITIAL_VALUES_LAST_MODIFIED_VARIABLE_NAME, source.initial_last_modified
        )
    logger.debug(f"Last modified: {last_modified}")
    source_list = []
    for location in location_list:
        source_list.append(
            execute_single_file_import(
                location=location,
                destination=destination,
                format=source.format,
                initial_last_modified=last_modified,
            )
        )
    if source.initial_last_modified:
        new_last_modified = datetime.now().isoformat()
        INITIAL_VALUES.add_new_value(
            INITIAL_VALUES_LAST_MODIFIED_VARIABLE_NAME, new_last_modified
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


def format_object_to_string(format: FileFormat) -> str:
    logger.debug(f"Converting format object to string: {format}")
    if isinstance(format, FileFormat):
        return INPUT_FORMAT_CLASS_TO_IMPORTER_FORMAT.get(type(format))
    else:
        logger.error(f"Invalid format type: {type(format)}")
        raise TypeError(f"Invalid format type: {type(format)}")


def format_object_to_config_dict(format: FileFormat) -> dict:
    logger.debug(f"Converting format object to config dict: {format}")
    if isinstance(format, CSVFormat):
        config_dict = {
            "parse_options": {
                "separator": format.separator,  # Default for the polars importer, it
                # expects its unicode value of 44 as an integer
                "quote_char": format.quote_char,  # Default for the polars importer, it
                # expects its unicode value of 34 as an integer
                "eol_char": format.eol_char,  # Default for the polars importer, it
                # expects its unicode value of 10 as an integer
                # Default encoding for the polars importer
                "encoding": format.input_encoding,
                "null_values": format.input_null_values,
                "missing_is_null": format.input_missing_is_null,
                "truncate_ragged_lines": format.input_truncate_ragged_lines,
                "comment_prefix": format.input_comment_prefix,
                "try_parse_dates": format.input_try_parse_dates,
                "decimal_comma": format.input_decimal_comma,
            },
            "has_header": format.input_has_header,
            "skip_rows": format.input_skip_rows,
            "skip_rows_after_header": format.input_skip_rows_after_header,
            "raise_if_empty": format.input_raise_if_empty,
            "ignore_errors": format.input_ignore_errors,
        }
        logger.debug(f"CSV format config: {config_dict}")
        return config_dict
    elif isinstance(format, ParquetFormat):
        # Currently we only allow loading parquet files with the default configuration.
        # In the future, this piece might be extended to support more options.
        config_dict = {}
        logger.debug(f"Parquet format config: {config_dict}")
        return config_dict
    elif isinstance(format, LogFormat):
        # Currently we only allow loading log files with the default configuration.
        # In the future, this piece might be extended to support more options.
        config_dict = {}
        logger.debug(f"Log format config: {config_dict}")
        return config_dict
    elif isinstance(format, NDJSONFormat):
        # Currently we only allow loading json files with the default configuration.
        # In the future, this piece might be extended to support more options.
        config_dict = {}
        logger.debug(f"NDJSON format config: {config_dict}")
        return config_dict
    else:
        logger.error(f"Invalid format type: {type(format)}")
        raise TypeError(f"Invalid format type: {type(format)}")


def execute_single_file_import(
    location: str, destination: str, format: FileFormat, initial_last_modified: str
) -> list | str:
    """
    Import a file from a location to a destination with a specific format. The file is
        imported using a binary, and the result returned is always a list of parquet
        files. If the location contained a wildcard for the files, the list might
        contain one or more elements.
    :return: list of imported files if using a wildcard pattern, single file if not.
    """
    with (
        tempfile.NamedTemporaryFile() as temporary_out_file,
        tempfile.TemporaryDirectory() as temporary_destination,
    ):
        basedir, data = os.path.split(location)
        # Unquote the data, since the pathlib.Path.as_uri() method encodes the path
        # with percent-encoding, and some wildcard characters are encoded.
        data = unquote(data)
        arguments = (
            f"--location {basedir} --file-pattern {data} --to"
            f" {destination} --format"
            f" {format_object_to_string(format)} --out"
            f" {temporary_out_file.name}"
        )
        if initial_last_modified:
            arguments += f" --modified-since {initial_last_modified}"
        format_config = format_object_to_config_dict(format)
        format_config = convert_characters_to_ascii(format_config)
        if format_config:  # Check if there are other keys in the format dict
            logger.debug(f"Format config: {format_config}")
            with open(
                os.path.join(temporary_destination, "format_config.json"), "w"
            ) as temporary_format_config_file:
                json.dump(format_config, temporary_format_config_file)
                arguments += f" --format-config {temporary_format_config_file.name}"
        binary = "importer.exe" if CURRENT_PLATFORM.is_windows() else "importer"
        # Both make tools and supervisor dataset launchers take care of ensuring
        # importer can be located in the PATH environment variable.
        path_to_binary = binary
        # TODO: Reformat once a decision about the binary packaging is made
        #  https://tabsdata.atlassian.net/browse/TAB-27
        logger.debug(f"Importing files with command: {path_to_binary} {arguments}")
        result = subprocess.run(
            [path_to_binary] + arguments.split(), capture_output=True, text=True
        )

        if result.returncode != 0:
            logger.error(f"Error importing file: {result.stderr}")
            raise Exception(f"Error importing file: {result.stderr}")
        result = ast.literal_eval(temporary_out_file.read().decode("utf-8"))
        if is_wildcard_pattern(data):
            source_list = []
            if result:
                for dictionary in result:
                    source_list.append(dictionary.get("to"))
            else:
                logger.info("No files imported")
            logger.info(f"Imported files to: '{source_list}'")
        else:
            source_list = result[0].get("to") if result else None
            if not source_list:
                logger.info("No files imported")
            # If the data is not a wildcard pattern, the result is a single file
            logger.info(f"Imported file to: '{source_list}'")
    return source_list


def convert_characters_to_ascii(dictionary: dict) -> dict:
    for key, value in dictionary.items():
        if isinstance(value, str) and len(value) == 1:
            dictionary[key] = ord(value)
        elif isinstance(value, dict):
            dictionary[key] = convert_characters_to_ascii(value)
    return dictionary


def load_sources(
    local_sources: list,
) -> list:
    """
    Given a list of sources, load them into polars DataFrames.
    :param local_sources: A list of lists of paths to parquet files. Each element is
        either a string to a single file or a list of strings to multiple files.
    :return: A list were each element is either a DataFrame or a list of DataFrames.
    """
    logger.debug(f"Loading list of sources: {local_sources}")
    sources = []
    for source in local_sources:
        logger.debug(f"Loading single source: {source}")
        if isinstance(source, list):
            sources.append(load_sources_from_list(source))
        else:
            sources.append(load_source(source))
    return sources


def load_sources_from_list(source_list: list) -> List[td.TableFrame]:
    return [load_source(path) for path in source_list]


def load_source(path_to_source: str | os.PathLike) -> td.TableFrame | None:
    if path_to_source is None:
        logger.warning("Path to source is None. No data loaded.")
        return None
    logger.debug(f"Loading parquet file from path: {path_to_source}")
    result = pl.scan_parquet(path_to_source) if path_to_source else None
    result = td.TableFrame.__build__(result)
    logger.debug("Loaded parquet file successfully.")
    return result
