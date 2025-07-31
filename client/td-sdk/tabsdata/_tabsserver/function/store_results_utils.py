#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import datetime
import glob
import hashlib
import logging
import os
import re
import subprocess
import time
import uuid
from functools import partial
from typing import TYPE_CHECKING, List, Tuple

import polars as pl

import tabsdata._utils.tableframe._helpers as td_helpers
from tabsdata._format import CSVFormat, FileFormat, NDJSONFormat, ParquetFormat
from tabsdata._io.output import (
    FRAGMENT_INDEX_PLACEHOLDER,
    AWSGlue,
    AzureDestination,
    LocalFileDestination,
    MariaDBDestination,
    MySQLDestination,
    OracleDestination,
    PostgresDestination,
    S3Destination,
    SchemaStrategy,
    TableOutput,
)
from tabsdata._secret import _recursively_evaluate_secret
from tabsdata._tabsserver.function import sql_utils
from tabsdata._tabsserver.function.cloud_connectivity_utils import (
    SERVER_SIDE_AWS_ACCESS_KEY_ID,
    SERVER_SIDE_AWS_REGION,
    SERVER_SIDE_AWS_SECRET_ACCESS_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_NAME,
    obtain_and_set_azure_credentials,
    obtain_and_set_s3_credentials,
    set_s3_region,
)
from tabsdata._tabsserver.function.global_utils import (
    CSV_EXTENSION,
    CURRENT_PLATFORM,
    NDJSON_EXTENSION,
    PARQUET_EXTENSION,
    TABSDATA_EXTENSION,
    convert_path_to_uri,
)
from tabsdata._tabsserver.function.logging_utils import pad_string
from tabsdata._tabsserver.function.native_tables_utils import sink_lf_to_location
from tabsdata._tabsserver.function.yaml_parsing import (
    InputYaml,
    Table,
    TransporterAzure,
    TransporterEnv,
    TransporterLocalFile,
    TransporterS3,
    V1CopyFormat,
    store_copy_as_yaml,
)
from tabsdata._utils.sql_utils import add_driver_to_uri, obtain_uri

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._common import drop_system_columns

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import _unwrap_table_frame
from tabsdata.tableframe.lazyframe.frame import TableFrame

if TYPE_CHECKING:
    import pyarrow as pa
    import sqlalchemy

    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata._tabsserver.function.results_collection import (
        Result,
        ResultsCollection,
    )

logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

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


EXECUTION_ID_PLACEHOLDER = "$EXECUTION_ID"
EXPORT_TIMESTAMP_PLACEHOLDER = "$EXPORT_TIMESTAMP"
FUNCTION_RUN_ID_PLACEHOLDER = "$FUNCTION_RUN_ID"
SCHEDULER_TIMESTAMP_PLACEHOLDER = "$SCHEDULER_TIMESTAMP"
TRIGGER_TIMESTAMP_PLACEHOLDER = "$TRIGGER_TIMESTAMP"
TRANSACTION_ID_PLACEHOLDER = "$TRANSACTION_ID"


def replace_placeholders_in_path(path: str, request: InputYaml) -> str:
    new_path = path
    new_path = new_path.replace(EXECUTION_ID_PLACEHOLDER, str(request.execution_id))
    new_path = new_path.replace(
        EXPORT_TIMESTAMP_PLACEHOLDER, str(round(time.time() * 1000))
    )
    new_path = new_path.replace(
        FUNCTION_RUN_ID_PLACEHOLDER, str(request.function_run_id)
    )
    new_path = new_path.replace(
        SCHEDULER_TIMESTAMP_PLACEHOLDER,
        str(request.scheduled_on),
    )
    new_path = new_path.replace(
        TRIGGER_TIMESTAMP_PLACEHOLDER, str(request.triggered_on)
    )
    new_path = new_path.replace(TRANSACTION_ID_PLACEHOLDER, str(request.transaction_id))
    logger.info(f"Replaced placeholders in path '{path}' with '{new_path}'")
    return new_path


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
        output_folder = execution_context.paths.output_folder
        request = execution_context.request

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
            store_results_in_files(results, destination, output_folder, request)
        elif isinstance(destination, AzureDestination):
            obtain_and_set_azure_credentials(destination.credentials)
            store_results_in_files(results, destination, output_folder, request)
        elif isinstance(destination, S3Destination):
            obtain_and_set_s3_credentials(destination.credentials),
            set_s3_region(destination.region),
            store_results_in_files(results, destination, output_folder, request)
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


def store_results_in_sql(
    results: ResultsCollection,
    destination: (
        MariaDBDestination | MySQLDestination | OracleDestination | PostgresDestination
    ),
    output_folder: str,
):

    from sqlalchemy import create_engine

    logger.info(f"Storing results in SQL destination '{destination}'")
    results.normalize_frame()
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

    from sqlalchemy.orm import sessionmaker

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
    results.normalize_frame()

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
        if result is None:
            logger.warning(
                f"Result is None. No data stored: '{number}' - '{destination_file}."
            )
        else:
            logger.debug(
                f"Storing result {number} in destination file '{destination_file}'"
            )
            destination_files, intermediate_files, result = (
                pair_result_with_destination(
                    destination, destination_file, number, output_folder, result
                )
            )

            # At this point, we should always have a list called result with all the
            # results (either a single one for a single TableFrame or a list of
            # TableFrames for a fragmented destination). The same should happen for
            # intermediate_files and destination_files

            # Destination files might be modified, for example if there are placeholders
            # to be replaced. This list will store the final name of each destination
            # file.
            resolved_destination_files = []
            resolved_results = []
            for (
                individual_result,
                individual_intermediate_file,
                individual_destination_file,
            ) in zip(result, intermediate_files, destination_files):
                if individual_result is None:
                    logger.warning(
                        "Individual result is None. No data stored:"
                        f" '{individual_intermediate_file}' -"
                        f" '{individual_destination_file}."
                    )
                    resolved_destination_file, resolved_result = None, None
                else:
                    resolved_destination_file, resolved_result = (
                        store_result_using_transporter(
                            individual_result,
                            individual_destination_file,
                            individual_intermediate_file,
                            destination,
                            output_folder,
                            execution_context,
                        )
                    )
                resolved_destination_files.append(resolved_destination_file)
                resolved_results.append(resolved_result)
            if hasattr(destination, "catalog") and destination.catalog is not None:
                logger.info("Storing file(s) in catalog")
                catalog = destination.catalog
                store_file_in_catalog(
                    catalog,
                    resolved_destination_files,
                    catalog.tables[number],
                    resolved_results,
                    number,
                )
    logger.info("Results stored in destination")


def pair_result_with_destination(
    destination: LocalFileDestination | AzureDestination | S3Destination,
    destination_file: str,
    number: int,
    output_folder: str,
    result: Result,
):
    result = result.value
    if result is None:
        logger.warning("Result is No data will be stored for this TableFrame.")
        intermediate_files = [os.path.join(output_folder, str(number))]
        result = [result]
        destination_files = [destination_file]
    elif isinstance(result, TableFrame):
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
    return destination_files, intermediate_files, result


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


def store_file_in_catalog(
    catalog: AWSGlue,
    path_to_table_files: List[str],
    destination_table: str,
    lf_list: List[pl.LazyFrame],
    index: int,
):

    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError

    logger.debug(f"Storing file in catalog '{catalog}'")
    definition = catalog.definition
    logger.debug(f"Catalog definition: {definition}")
    definition = _recursively_evaluate_secret(definition)
    iceberg_catalog = load_catalog(**definition)
    logger.debug(f"Catalog loaded: {iceberg_catalog}")
    schemas = []
    for lf in lf_list:
        if lf is None:
            logger.warning("LazyFrame is None. No data stored in catalog.")
        else:
            empty_df = lf.limit(0).collect()
            schema = empty_df.schema
            pyarrow_individual_empty_df = empty_df.to_arrow()
            pyarrow_individual_schema = pyarrow_individual_empty_df.schema
            schemas.append(pyarrow_individual_schema)
            logger.debug(
                f"Converted schema '{schema} to pyarrow schema '"
                f"{pyarrow_individual_schema}'"
            )

    if not schemas:
        logger.warning("No data stored. Storing no data in catalog.")
        return

    pyarrow_schema = pa.unify_schemas(schemas)
    logger.debug(f"Obtained pyarrow schema '{pyarrow_schema}'")
    logger.debug(f"Obtaining table '{destination_table}'")
    try:
        table = iceberg_catalog.load_table(destination_table)
        logger.debug("Table obtained successfully")
    except NoSuchTableError:
        if (location := catalog.auto_create_at[index]) is not None:
            logger.debug(
                f"Table '{destination_table}' not found, but auto_create_at is set to "
                f"'{location}'"
            )
            table = iceberg_catalog.create_table(
                identifier=destination_table, schema=pyarrow_schema, location=location
            )
            logger.debug("Table created successfully")
        else:
            logger.error(
                f"Table '{destination_table}' not found and auto_create_at is None"
            )
            raise

    # At this point, we know for sure that the table exists, and all DDL operations
    # are done (which are not guaranteed to be atomic). Now we can add the files to
    # the table inside a transaction.
    with table.transaction() as trx:
        if catalog.schema_strategy == SchemaStrategy.UPDATE.value:
            logger.debug("Updating schema")
            with trx.update_schema(
                allow_incompatible_changes=catalog.allow_incompatible_changes
            ) as update_schema:
                logger.debug(
                    f"Unioning schema by name with schema {pyarrow_schema} and "
                    "allow_incompatible_changes "
                    f"set to '{catalog.allow_incompatible_changes}'"
                )
                update_schema.union_by_name(pyarrow_schema)
        else:
            logger.debug(
                f"Schema strategy is set to '{catalog.schema_strategy}', not updating"
                " schema"
            )

        if catalog.if_table_exists == "replace":
            logger.debug(
                f"Replacing table '{destination_table}' since "
                "if_table_exists is set to 'replace'"
            )
            trx.delete("True")
        logger.debug(
            f"Adding file(s) '{path_to_table_files}' to table '{destination_table}'"
        )
        trx.add_files(path_to_table_files)
        logger.debug(
            f"File '{path_to_table_files}' added to table '{destination_table}'"
        )


def store_result_using_transporter(
    result: TableFrame,
    destination_path: str,
    intermediate_file: str,
    destination: LocalFileDestination | AzureDestination | S3Destination,
    output_folder: str,
    execution_context: InputYaml,
) -> Tuple[str, pl.LazyFrame]:
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

    return destination_path, result


def store_result_in_sql_table(
    result: Result,
    session: sqlalchemy.orm.Session,
    destination_table: str,
    if_table_exists: str,
    output_folder: str,
):

    import pyarrow as pa
    import pyarrow.parquet as pq

    logger.info(f"Storing result in SQL table: {destination_table}")
    result = result.value
    if result is None:
        logger.info("Result is None. No data stored.")
        return
    elif isinstance(result, TableFrame):
        pass
    else:
        logger.error(f"Incorrect result type: '{type(result)}'. No data stored.")
        raise TypeError(f"Incorrect result type: '{type(result)}'. No data stored.")
    # Note: this warning is due to the fact that if_table_exists must be one of
    # the following: "fail", "replace", "append". This is enforced by the
    # Output class, so we can safely ignore this warning.
    logger.debug(f"Using strategy in case table exists: {if_table_exists}")
    result: pl.LazyFrame = remove_system_columns_and_convert(result)
    intermediate_file = f"intermediate_{destination_table}_{uuid.uuid4()}.parquet"
    intermediate_file_path = os.path.join(output_folder, intermediate_file)
    chunk_size = 10000
    logger.debug(f"Writing intermediate file '{intermediate_file_path}'")
    result.sink_parquet(
        intermediate_file_path,
        maintain_order=True,
    )
    parquet_file = pq.ParquetFile(intermediate_file_path)
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        chunk_table = pa.Table.from_batches(batches=[batch])
        df = pl.from_arrow(chunk_table)
        logger.debug(f"Writing batch of shape {df.shape} to table {destination_table}")
        df.write_database(
            table_name=destination_table,
            connection=session,
            if_table_exists=if_table_exists,
        )
        if_table_exists = "append"
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
        logger.debug(f"Creating folder to store the file: '{folder}'")
        os.makedirs(folder, exist_ok=True)
        if isinstance(format, CSVFormat):
            # TODO: Add maintain_order as an option once we are using sink instead of
            #  write with our own dataframe
            write_format = {
                "maintain_order": True,
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
            write_format = {
                "maintain_order": True,
            }
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
