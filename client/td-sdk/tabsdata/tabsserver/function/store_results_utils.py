#
# Copyright 2024 Tabs Data Inc.
#


import datetime
import glob
import logging
import os
import re
import subprocess
import time
from typing import List

import cloudpickle
import polars as pl

# from pyiceberg.catalog import load_catalog
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import tabsdata as td
import tabsdata.utils.tableframe._helpers as td_helpers
from tabsdata.format import CSVFormat, FileFormat, NDJSONFormat, ParquetFormat
from tabsdata.io.output import (  # Catalog,
    FRAGMENT_INDEX_PLACEHOLDER,
    AzureDestination,
    LocalFileDestination,
    MariaDBDestination,
    MySQLDestination,
    OracleDestination,
    PostgresDestination,
    S3Destination,
    TableOutput,
    build_output,
)
from tabsdata.io.plugin import DestinationPlugin
from tabsdata.tabsserver.function.logging_utils import pad_string
from tabsdata.tabsserver.function.results_collection import Result, ResultsCollection

# from tabsdata.secret import _recursively_evaluate_secret
from tabsdata.utils.bundle_utils import PLUGINS_FOLDER
from tabsdata.utils.sql_utils import add_driver_to_uri, obtain_uri

from . import sql_utils
from .cloud_connectivity_utils import (
    SERVER_SIDE_AWS_ACCESS_KEY_ID,
    SERVER_SIDE_AWS_REGION,
    SERVER_SIDE_AWS_SECRET_ACCESS_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_NAME,
    obtain_and_set_azure_credentials,
    obtain_and_set_s3_credentials,
    set_s3_region,
)
from .global_utils import (
    CSV_EXTENSION,
    CURRENT_PLATFORM,
    NDJSON_EXTENSION,
    PARQUET_EXTENSION,
    TABSDATA_EXTENSION,
    convert_path_to_uri,
    convert_uri_to_path,
)
from .yaml_parsing import (
    InputYaml,
    Table,
    TransporterAzure,
    TransporterEnv,
    TransporterLocalFile,
    TransporterS3,
    V1CopyFormat,
    store_copy_as_yaml,
)

logger = logging.getLogger(__name__)
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)

FORMAT_TO_POLARS_WRITE_FUNCTION = {
    CSV_EXTENSION: pl.LazyFrame.sink_csv,
    NDJSON_EXTENSION: pl.LazyFrame.sink_ndjson,
    PARQUET_EXTENSION: pl.LazyFrame.sink_parquet,
    TABSDATA_EXTENSION: pl.LazyFrame.sink_parquet,
}

FORMAT_TO_POLARS_EAGER_WRITE_FUNCTION = {
    CSV_EXTENSION: pl.DataFrame.write_csv,
    NDJSON_EXTENSION: pl.DataFrame.write_ndjson,
    PARQUET_EXTENSION: pl.DataFrame.write_parquet,
    TABSDATA_EXTENSION: pl.DataFrame.write_parquet,
}

DATA_VERSION_PLACEHOLDER = "$DATA_VERSION"
EXPORT_TIMESTAMP_PLACEHOLDER = "$EXPORT_TIMESTAMP"
SCHEDULER_TIMESTAMP_PLACEHOLDER = "$SCHEDULER_TIMESTAMP"
TRIGGER_TIMESTAMP_PLACEHOLDER = "$TRIGGER_TIMESTAMP"


def replace_placeholders_in_path(path: str, execution_context: InputYaml) -> str:
    new_path = path
    if DATA_VERSION_PLACEHOLDER in new_path:
        new_path = new_path.replace(
            DATA_VERSION_PLACEHOLDER, str(execution_context.dataset_data_version)
        )
    if EXPORT_TIMESTAMP_PLACEHOLDER in new_path:
        new_path = new_path.replace(
            EXPORT_TIMESTAMP_PLACEHOLDER, str(round(time.time() * 1000))
        )
    if TRIGGER_TIMESTAMP_PLACEHOLDER in new_path:
        new_path = new_path.replace(
            TRIGGER_TIMESTAMP_PLACEHOLDER, str(execution_context.triggered_on)
        )
    if SCHEDULER_TIMESTAMP_PLACEHOLDER in new_path:
        new_path = new_path.replace(
            SCHEDULER_TIMESTAMP_PLACEHOLDER,
            str(execution_context.execution_plan_triggered_on),
        )
    logger.info(f"Replaced placeholders in path '{path}' with '{new_path}'")
    return new_path


def store_results(
    results: ResultsCollection,
    output_configuration: dict,
    working_dir: str | os.PathLike,
    execution_context: InputYaml,
    output_folder: str,
) -> List[str]:
    logger.info(pad_string("[Storing results]"))
    logger.info(
        f"Storing results in destination '{output_configuration}', "
        f"with working_dir '{working_dir}'"
    )
    modified_tables = []
    if DestinationPlugin.IDENTIFIER in output_configuration:
        export_using_plugin(output_configuration, output_folder, results, working_dir)
    else:
        destination = build_output(output_configuration)

        if isinstance(
            destination,
            (
                MariaDBDestination,
                MySQLDestination,
                OracleDestination,
                PostgresDestination,
            ),
        ):
            store_results_in_sql(results, destination, output_folder)
        elif isinstance(destination, TableOutput):
            modified_tables = store_results_in_table(
                results, destination, execution_context
            )
        elif isinstance(destination, LocalFileDestination):
            store_results_in_files(
                results, destination, output_folder, execution_context
            )
        elif isinstance(destination, AzureDestination):
            obtain_and_set_azure_credentials(destination.credentials)
            store_results_in_files(
                results, destination, output_folder, execution_context
            )
        elif isinstance(destination, S3Destination):
            obtain_and_set_s3_credentials(destination.credentials),
            set_s3_region(destination.region),
            store_results_in_files(
                results, destination, output_folder, execution_context
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


def export_using_plugin(
    output_configuration: dict,
    output_folder: str,
    results: ResultsCollection,
    working_dir: str | os.PathLike,
):
    exporter_plugin_file = output_configuration.get(DestinationPlugin.IDENTIFIER)
    plugins_folder = os.path.join(working_dir, PLUGINS_FOLDER)
    with open(os.path.join(plugins_folder, exporter_plugin_file), "rb") as f:
        exporter_plugin = cloudpickle.load(f)
    logger.info(f"Exporting files with plugin '{exporter_plugin}'")
    logger.info("Starting plugin export")
    results_to_provide = []
    for result in results:
        result_value = result.value
        if isinstance(result_value, td.TableFrame):
            intermediate_result = remove_system_columns_and_convert(result_value)
        elif result_value is None:
            intermediate_result = None
        elif isinstance(result_value, list):
            intermediate_result = [
                (
                    remove_system_columns_and_convert(single_result)
                    if isinstance(single_result, td.TableFrame)
                    else result
                )
                for single_result in result_value
            ]
        else:
            logger.error(
                "The result of a registered function must be a TableFrame,"
                f" None or a list of TableFrames, got '{type(result_value)}'"
                " instead"
            )
            raise TypeError(
                "The result of a registered function must be a TableFrame,"
                f" None or a list of TableFrames, got '{type(result_value)}'"
                " instead"
            )
        results_to_provide.append(intermediate_result)
    exporter_plugin.trigger_output(output_folder, *results_to_provide)
    logger.info(f"Exported files with plugin '{exporter_plugin}'")


def store_results_in_table(
    results: ResultsCollection,
    destination: TableOutput,
    execution_context: InputYaml,
) -> List[str]:
    results.convert_none_to_empty_frame()
    # Right now, source provides very little information, but we use it to do a small
    # sanity check and to ensure that everything is running properly
    # TODO: Decide if we want to add more checks here
    execution_context_output_entry_list = execution_context.output
    logger.info(
        f"Storing results in tables '{execution_context_output_entry_list}' and "
        f"matching them with destination '{destination}'"
    )
    table_list = []
    # Note: destination.table is a list of strings, it can't be a single string because
    # when we serialised it we stored it as such even if it was a single one.
    if len(execution_context_output_entry_list) != len(destination.table):
        logger.error(
            "Number of tables in the execution context output"
            f" ({len(execution_context_output_entry_list)}) does not match the "
            "number"
            f" of tables in the destination ({len(destination.table)}). No data stored."
        )
        raise ValueError(
            "Number of tables in the execution context output"
            f" ({len(execution_context_output_entry_list)}) does not match the "
            "number"
            f" of tables in the destination ({len(destination.table)}). No data stored."
        )
    for execution_context_output_entry, source_table_uri in zip(
        execution_context_output_entry_list, destination.table
    ):
        logger.info(f"Unpacking '{execution_context_output_entry}'")
        if isinstance(execution_context_output_entry, Table):
            real_table_uri = obtain_table_uri_and_verify(
                execution_context_output_entry, source_table_uri
            )
            table_list.append(
                {
                    "uri": real_table_uri,
                    "name": execution_context_output_entry.name,
                }
            )
        else:
            logger.error(
                f"Invalid table type: {type(execution_context_output_entry)}. No data"
                " stored."
            )
            raise TypeError(
                f"Invalid table type: {type(execution_context_output_entry)}. No data"
                " stored."
            )
    logger.debug(f"Table list obtained: {table_list}")
    logger.debug(f"Obtained a total of {len(results)} results")
    if len(results) != len(table_list):
        logger.error(
            "Number of results obtained does not match the number of tables to store."
            " No data stored."
        )
        raise ValueError(
            "Number of results obtained does not match the number of tables to store."
            " No data stored."
        )
    modified_tables = []
    for result, table in zip(results, table_list):
        logger.info(f"Storing result in table '{table}'")
        table_path = convert_uri_to_path(table.get("uri"))
        logger.debug(f"URI converted to path {table_path}")
        if isinstance(result.value, td.TableFrame):
            result_value: td.TableFrame = result.value
            store_polars_lf_in_file(result_value._lf, table_path)
        else:
            logger.error(
                f"Invalid result type: '{type(result.value)}'. No data stored."
            )
            raise TypeError(
                f"Invalid result type: '{type(result.value)}'. No data stored."
            )
        modified_tables.append(table.get("name"))
        logger.debug(f"Result stored in table '{table}', added to modified_tables list")
    logger.info("Results stored in tables")
    logger.debug(f"Modified tables: {modified_tables}")
    return modified_tables


def obtain_table_uri_and_verify(
    execution_context_table: Table, destination_table_name: str
) -> str:
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
    table_uri = execution_context_table.uri
    logger.debug(f"Table URI: {table_uri}")
    return table_uri


def store_results_in_sql(
    results: ResultsCollection,
    destination: (
        MariaDBDestination | MySQLDestination | OracleDestination | PostgresDestination
    ),
    output_folder: str,
):
    logger.info(f"Storing results in SQL destination '{destination}'")
    if isinstance(
        destination,
        (MariaDBDestination, MySQLDestination, OracleDestination, PostgresDestination),
    ):
        uri = obtain_uri(destination, log=True, add_credentials=True)
        uri = add_driver_to_uri(uri, log=True)
        if isinstance(destination, MariaDBDestination):
            uri = sql_utils.add_mariadb_collation(uri)
        destination_table_configuration = destination.destination_table
        destination_if_table_exists = destination.if_table_exists
        engine = create_engine(uri)
        try:
            create_session_and_store(
                engine,
                results,
                destination_table_configuration,
                destination_if_table_exists,
                output_folder,
            )
            logger.info("Results stored in SQL destination")
        except Exception:
            logger.error("Error storing results in SQL destination")
            raise
        finally:
            engine.dispose()
    else:
        logger.error(f"Storing results in destination '{destination}' not supported.")
        raise TypeError(
            f"Storing results in destination '{destination}' not supported."
        )


def create_session_and_store(
    engine: sqlalchemy.engine.base.Engine,
    results: ResultsCollection,
    destination_table_configuration: str | List[str],
    destination_if_table_exists: str,
    output_folder: str,
):
    Session = sessionmaker(bind=engine)
    session = Session()
    with session.begin():
        if isinstance(destination_table_configuration, str):
            destination_table_configuration = [destination_table_configuration]
        elif isinstance(destination_table_configuration, list):
            pass
        else:
            logger.error(
                "destination_table must be a string or a list of strings, "
                f"got {type(destination_table_configuration)} instead"
            )
            raise TypeError(
                "destination_table must be a string or a list of strings, "
                f"got {type(destination_table_configuration)} instead"
            )

        if len(results) != len(destination_table_configuration):
            logger.error(
                "The number of destination tables does not match the number of results."
            )
            logger.error(f"Destination tables: '{destination_table_configuration}'")
            logger.error(f"Number or results: {len(results)}")
            raise TypeError(
                "The number of destination tables does not match the number of results."
            )
        for result, destination_table in zip(results, destination_table_configuration):
            store_result_in_sql_table(
                result,
                session,
                destination_table,
                destination_if_table_exists,
                output_folder,
            )


def store_results_in_files(
    results: ResultsCollection,
    destination: LocalFileDestination | AzureDestination | S3Destination,
    output_folder: str,
    execution_context: InputYaml,
):
    logger.info(f"Storing results in File destination '{destination}'")
    results.convert_none_to_empty_frame()

    destination_path = obtain_destination_path(destination)
    if isinstance(destination_path, str):
        destination_path = [destination_path]
    elif isinstance(destination_path, list):
        pass
    else:
        logger.error(
            "Parameter 'path' must be a string or a list of strings, got"
            f" '{type(destination_path)}' instead"
        )
        raise TypeError(
            "Parameter 'path' must be a string or a list of strings, got"
            f" '{type(destination_path)}' instead"
        )

    if len(results) != len(destination_path):
        logger.error(
            "The number of destination files does not match the number of results."
        )
        logger.error(f"Destination files: '{destination_path}'")
        logger.error(f"Number or results: {len(results)}")
        raise TypeError(
            "The number of destination tables does not match the number of results."
        )

    logger.debug(
        f"Pairing destination path '{destination_path}' with results '{results}'"
    )
    for number, (result, destination_file) in enumerate(zip(results, destination_path)):
        logger.debug(
            f"Storing result {number} in destination file '{destination_file}'"
        )
        result = result.value
        if result is None:
            logger.error(
                "Result is None. However, any None result should have been "
                "converted to an empty TableFrame."
            )
            raise ValueError(
                "Result is None. However, any None result should have been "
                "converted to an empty TableFrame."
            )
        elif isinstance(result, td.TableFrame):
            intermediate_files = [os.path.join(output_folder, str(number))]
            result = [result]
            destination_files = [destination_file]
        elif isinstance(result, list):
            verify_fragment_destination(destination, destination_file)
            intermediate_files = []
            destination_files = []
            for fragment_number in range(len(result)):
                intermediate_file = os.path.join(
                    output_folder, f"{number}_with_fragment_{fragment_number}"
                )
                intermediate_files.append(intermediate_file)
                individual_destination_file = destination_file.replace(
                    FRAGMENT_INDEX_PLACEHOLDER, str(fragment_number)
                )
                destination_files.append(individual_destination_file)
        else:
            logger.error(
                "The result of a registered function must be a TableFrame,"
                f" None or a list of TableFrames, got '{type(result)}' instead"
            )
            raise TypeError(
                "The result of a registered function must be a TableFrame,"
                f" None or a list of TableFrames, got '{type(result)}' instead"
            )

        # At this point, we should always have a list called result with all the
        # results (either a single one for a single TableFrame or a list of
        # TableFrames for a fragmented destination). The same should happen for
        # intermediate_files and destination_files

        for (
            individual_result,
            individual_intermediate_file,
            individual_destination_file,
        ) in zip(result, intermediate_files, destination_files):
            store_result_using_transporter(
                individual_result,
                individual_destination_file,
                individual_intermediate_file,
                destination,
                output_folder,
                execution_context,
            )
        # if destination.catalog:
        #     logger.info("Storing file in catalog")
        #     catalog = destination.catalog
        #     store_file_in_catalog(
        #         catalog, destination_file, catalog.tables[number], result
        #     )
    logger.info("Results stored in LocalFile destination")


def verify_fragment_destination(
    destination: LocalFileDestination | AzureDestination | S3Destination,
    destination_file: str,
):
    if not destination.allow_fragments:
        logger.error(
            "Destination does not allow fragments, but the result is a list "
            "of TableFrames."
        )
        raise TypeError(
            "Destination does not allow fragments, but the result is a list "
            "of TableFrames."
        )
    if FRAGMENT_INDEX_PLACEHOLDER not in destination_file:
        logger.error(
            f"Destination file '{destination_file}' does not contain the fragment index"
            f" placeholder '{FRAGMENT_INDEX_PLACEHOLDER}', but is trying to store a"
            " list of TableFrames."
        )
        raise ValueError(
            f"Destination file '{destination_file}' does not contain the fragment index"
            f" placeholder '{FRAGMENT_INDEX_PLACEHOLDER}', but is trying to store a"
            " list of TableFrames."
        )
    return


def obtain_destination_path(destination):
    if isinstance(destination, LocalFileDestination):
        destination_path = destination.path
    elif isinstance(destination, (AzureDestination, S3Destination)):
        destination_path = destination.uri
    else:
        logger.error(f"Storing results in destination '{destination}' not supported.")
        raise TypeError(
            f"Storing results in destination '{destination}' not supported."
        )
    return destination_path


INPUT_FORMAT_CLASS_TO_EXTENSION = {
    CSVFormat: CSV_EXTENSION,
    NDJSONFormat: NDJSON_EXTENSION,
    ParquetFormat: PARQUET_EXTENSION,
}


# def store_file_in_catalog(
#     catalog: Catalog, path_to_table_file: str, destination_table: str, df:
#       td.TableFrame
# ):
#     logger.debug(f"Storing file in catalog '{catalog}'")
#     definition = catalog.definition
#     logger.debug(f"Catalog definition: {definition}")
#     definition = _recursively_evaluate_secret(definition)
#     iceberg_catalog = load_catalog(**definition)
#     logger.debug(f"Catalog loaded: {iceberg_catalog}")
#     logger.debug(f"Obtaining or creating table '{destination_table}' in catalog")
#     schema = df.schema
#     data = {name: [] for name in schema.keys()}
#     pyarrow_empty_df = pl.DataFrame(data, schema=schema).to_arrow()
#     pyarrow_schema = pyarrow_empty_df.schema
#     logger.debug(f"Converted schema '{schema} to pyarrow schema '{pyarrow_schema}'")
#     table = iceberg_catalog.create_table_if_not_exists(
#         identifier=destination_table,
#         schema=pyarrow_schema,
#     )
#     logger.debug(f"Table obtained or created: {table}")
#
#     with table.update_schema(
#         allow_incompatible_changes=catalog.allow_incompatible_changes
#     ) as update_schema:
#         logger.debug(
#             f"Unioning schema by name with schema {pyarrow_schema} and "
#             "allow_incompatible_changes "
#             f"set to '{catalog.allow_incompatible_changes}'"
#         )
#         update_schema.union_by_name(pyarrow_schema)
#
#     if catalog.if_table_exists == "replace":
#         logger.debug(f"Replacing table '{destination_table}'")
#         table.overwrite(pyarrow_empty_df)
#     logger.debug(f"Adding file '{path_to_table_file}' to table '{destination_table}'")
#     table.add_files([path_to_table_file], check_duplicate_files=True)
#     logger.debug(f"File '{path_to_table_file}' added to table '{destination_table}'")


def store_result_using_transporter(
    result: td.TableFrame,
    destination_path: str,
    intermediate_file: str,
    destination: LocalFileDestination | AzureDestination | S3Destination,
    output_folder: str,
    execution_context: InputYaml,
):
    intermediate_file = (
        intermediate_file
        + "."
        + INPUT_FORMAT_CLASS_TO_EXTENSION[type(destination.format)]
    )
    logger.info(
        f"Storing result in destination file '{destination_path}' with intermediate"
        f" file '{intermediate_file}'"
    )
    destination_path = replace_placeholders_in_path(destination_path, execution_context)
    result: pl.LazyFrame = remove_system_columns_and_convert(result)
    store_polars_lf_in_file(result, intermediate_file, destination.format)

    transporter_origin_file = convert_path_to_uri(intermediate_file)
    origin = TransporterLocalFile(transporter_origin_file)
    logger.debug(f"Origin file: {origin}")
    if isinstance(destination, S3Destination):
        destination = TransporterS3(
            destination_path,
            access_key=TransporterEnv(SERVER_SIDE_AWS_ACCESS_KEY_ID),
            secret_key=TransporterEnv(SERVER_SIDE_AWS_SECRET_ACCESS_KEY),
            region=(
                TransporterEnv(SERVER_SIDE_AWS_REGION) if destination.region else None
            ),
        )
    elif isinstance(destination, AzureDestination):
        destination = TransporterAzure(
            destination_path,
            account_name=TransporterEnv(SERVER_SIDE_AZURE_ACCOUNT_NAME),
            account_key=TransporterEnv(SERVER_SIDE_AZURE_ACCOUNT_KEY),
        )
    elif isinstance(destination, LocalFileDestination):
        destination = TransporterLocalFile(convert_path_to_uri(destination_path))
    else:
        logger.error(f"Storing results in destination '{destination}' not supported.")
        raise TypeError(
            f"Storing results in destination '{destination}' not supported."
        )
    copy_pair = [[origin, destination]]

    current_timestamp = int(
        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000000
    )
    yaml_request_file = os.path.join(output_folder, f"request_{current_timestamp}.yaml")
    store_copy_as_yaml(
        V1CopyFormat(copy_pair),
        yaml_request_file,
    )
    binary = "transporter.exe" if CURRENT_PLATFORM.is_windows() else "transporter"
    report_file = os.path.join(output_folder, f"report_{current_timestamp}.yaml")
    arguments = f"--request {yaml_request_file} --report {report_file}"
    logger.debug(f"Exporting files with command: {binary} {arguments}")
    subprocess_result = subprocess.run(
        [binary] + arguments.split(), capture_output=True, text=True
    )
    if subprocess_result.returncode != 0:
        logger.error(f"Error exporting file: {subprocess_result.stderr}")
        raise Exception(f"Error exporting file: {subprocess_result.stderr}")
    # TODO: add a call to the transporter here


def store_result_in_sql_table(
    result: Result,
    session: sqlalchemy.orm.Session,
    destination_table: str,
    if_table_exists: str,
    output_folder: str,
):
    logger.info(f"Storing result in SQL table: {destination_table}")
    result = result.value
    if result is None:
        logger.info("Result is None. No data stored.")
        return
    elif isinstance(result, td.TableFrame):
        pass
    else:
        logger.error(f"Incorrect result type: '{type(result)}'. No data stored.")
        raise TypeError(f"Incorrect result type: '{type(result)}'. No data stored.")
    # Note: this warning is due to the fact that if_table_exists must be one of
    # the following: "fail", "replace", "append". This is enforced by the
    # Output class, so we can safely ignore this warning.
    logger.debug(f"Using strategy in case table exists: {if_table_exists}")
    result: pl.LazyFrame = remove_system_columns_and_convert(result)
    current_timestamp = str(round(time.time() * 1000))
    intermediate_file = f"intermediate_{destination_table}_{current_timestamp}.csv"
    intermediate_file_path = os.path.join(output_folder, intermediate_file)
    result.sink_csv(
        intermediate_file_path,
        maintain_order=True,
    )
    # Note: Currently polars ignores batch_size provided in read_csv_batched
    #    so we leave it with the default value.
    #    See: https://github.com/pola-rs/polars/issues/19978
    reader = pl.read_csv_batched(intermediate_file_path)
    cores = os.cpu_count()
    logger.debug(f"Using {cores} cores to read the file")
    # Note: next_batches recommends using a number of batches equal or
    # greater to the amount of threads in the system. We use
    # os.cpu_count() to get the number cores as an approximation.
    while batches := reader.next_batches(2 * cores):
        for batch in batches:
            logger.debug(
                f"Writing batch of shape {batch.shape} to table {destination_table}"
            )
            batch.write_database(
                table_name=destination_table,
                connection=session,
                if_table_exists=if_table_exists,
            )
    logger.info(f"Result stored in SQL table: {destination_table}")


def store_polars_lf_in_file(
    result: pl.LazyFrame,
    result_file: str | os.PathLike,
    format: FileFormat | CSVFormat | ParquetFormat | NDJSONFormat = None,
):
    file_ending = result_file.split(".")[-1]
    if file_ending in FORMAT_TO_POLARS_WRITE_FUNCTION:
        # polars does not create parent folders when writing a file.
        folder = os.path.dirname(result_file)
        os.makedirs(folder, exist_ok=True)
        if isinstance(format, CSVFormat):
            # TODO: Add maintain_order as an option once we are using sink instead of
            #  write with our own dataframe
            write_format = {
                "separator": format.separator,
                "line_terminator": format.eol_char,
                "quote_char": format.quote_char,
                "include_header": format.output_include_header,
                "datetime_format": format.output_datetime_format,
                "date_format": format.output_date_format,
                "time_format": format.output_time_format,
                "float_scientific": format.output_float_scientific,
                "float_precision": format.output_float_precision,
                "null_value": format.output_null_value,
                "quote_style": format.output_quote_style,
            }
        else:
            write_format = {}
        logger.debug(
            f"Writing result to file '{result_file}' using format '{write_format}'"
        )

        return FORMAT_TO_POLARS_WRITE_FUNCTION[file_ending](
            result, result_file, **write_format
        )
    else:
        logger.error(
            f"Writing output file with ending {file_ending} not supported "
            "with api Polars, as this is not a recognized file extension"
        )
        raise ValueError(
            f"Writing output file with ending {file_ending} not supported with "
            "api Polars, as this is not a recognized file extension"
        )


def remove_system_columns_and_convert(result: td.TableFrame) -> pl.LazyFrame:
    try:
        # Note: this converts result from a TableFrame to a LazyFrame
        return result._lf.drop(td_helpers.SYSTEM_COLUMNS)
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


def _get_matching_files(pattern):
    # Construct the full pattern
    # Use glob to get the list of matching files
    matching_files = glob.glob(pattern)
    ordered_files = sorted(matching_files, key=_extract_index)
    logger.debug(f"Matching files: {ordered_files}")
    return ordered_files


# Sort the files to ensure that they are processed in the correct order
def _extract_index(filename):
    match = re.search(r"_(\d+)\.*", filename)
    return int(match.group(1)) if match else float("inf")
