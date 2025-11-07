#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import uuid
from typing import List

import polars as pl

from tabsdata._io.outputs.shared_enums import (
    IfTableExistsStrategy,
    IfTableExistStrategySpec,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._secret import _recursively_evaluate_secret
from tabsdata._tabsserver.function.global_utils import convert_path_to_uri
from tabsdata._utils.id import encode_id

TRACE = logging.DEBUG - 1

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

snowflake_original_logger_levels = {}


def modify_snowflake_logger_levels():
    for logger_name, logger_instance in logging.Logger.manager.loggerDict.items():
        if isinstance(logger_instance, logging.PlaceHolder):
            continue

        if logger_name == "snowflake" or logger_name.startswith("snowflake."):
            logger_object = logging.getLogger(logger_name)
            snowflake_original_logger_levels[logger_name] = logger_object.level
            logger_object.setLevel(logging.ERROR)

    if "snowflake" not in snowflake_original_logger_levels:
        logger_object = logging.getLogger("snowflake")
        snowflake_original_logger_levels["snowflake"] = logger_object.level
        logger_object.setLevel(logging.ERROR)


def restore_snowflake_logger_levels():
    for logger_name, logger_level in snowflake_original_logger_levels.items():
        logging.getLogger(logger_name).setLevel(logger_level)


class SnowflakeDestination(DestinationPlugin):

    def __init__(
        self,
        connection_parameters: dict,
        destination_table: List[str] | str,
        if_table_exists: IfTableExistStrategySpec = "append",
        stage: str | None = None,
        **kwargs,
    ):
        """
        Initializes the SnowflakeDestination with the configuration desired to store
            the data.

        Args:

        """
        try:
            modify_snowflake_logger_levels()

            import warnings

            warnings.filterwarnings("ignore", module="snowflake")
            from snowflake.connector import connect  # noqa: F401
        except ImportError:
            raise ImportError(
                "The 'tabsdata_snowflake' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['snowflake']'"
            )
        finally:
            restore_snowflake_logger_levels()
        self.connection_parameters = connection_parameters
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists
        self.warehouse = connection_parameters.get("warehouse")
        self.database = connection_parameters.get("database")
        self.schema = connection_parameters.get("schema")
        self.stage = stage
        self.kwargs = kwargs
        self._support_snowflake_logging_level = self.kwargs.get(
            "support_snowflake_logging_level", logging.ERROR
        )
        self._support_match_by_column_name = self.kwargs.get(
            "support_match_by_column_name", "CASE_INSENSITIVE"
        )
        self._support_purge = self.kwargs.get("support_purge", "TRUE")
        self._support_connect = self.kwargs.get("support_connect", {})

    @property
    def destination_table(self) -> list[str]:
        """
        Get the destination table(s) where the data will be stored.

        Returns:
            str | list[str]: The destination table(s).
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, value: str | list[str]):
        """
        Set the destination table(s) where the data will be stored.

        Args:
            value (str | list[str]): The destination table(s).
        """
        if isinstance(value, str):
            self._destination_table = [value]
        elif isinstance(value, list) and all(isinstance(q, str) for q in value):
            self._destination_table = value
        else:
            raise TypeError(
                "The 'destination_table' parameter must be a string or a list of "
                f"strings, got a parameter '{value}' of type '{type(value)}' "
                "instead."
            )

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        Returns the value of the if_table_exists property.
        This property determines what to do if the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, value: IfTableExistStrategySpec):
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if not isinstance(value, str):
            raise TypeError(
                "The 'if_table_exists' parameter must be a string, got a parameter of "
                f"type '{type(value)}' instead."
            )
        if value not in valid_values:
            raise ValueError(
                f"The 'if_table_exists' parameter must one of '{valid_values}', "
                f"got '{value}' instead."
            )
        self._if_table_exists = value

    def write(self, files):
        """
        This method is used to write the files to the database. It is called
        from the stream method, and it is not intended to be called directly.
        """

        modify_snowflake_logger_levels()

        import warnings

        warnings.filterwarnings("ignore", module="snowflake")

        from snowflake.connector import connect

        connection_parameters = _recursively_evaluate_secret(self.connection_parameters)
        # We need to ensure that the connection parameters are fully evaluated
        self.warehouse = connection_parameters.get("warehouse")
        self.database = connection_parameters.get("database")
        self.schema = connection_parameters.get("schema")
        conn = connect(**connection_parameters, **self._support_connect)
        for file_path, table in zip(files, self.destination_table):
            if file_path is None:
                logger.warning(f"Received None for table '{table}'. No data loaded.")
            else:
                logger.info(f"Starting upload of results to table '{table}'")
                table = self._fully_qualify_entity(table)
                logger.debug(f"Storing file '{file_path}' in table '{table}'")
                try:
                    self._upload_single_file_to_table(conn, file_path, table)
                    logger.info(f"Uploaded results to table '{table}' successfully.")
                except Exception:
                    logger.error(
                        f"Failed to upload results from file '{file_path}' to table"
                        f" '{table}'"
                    )
                    conn.close()
                    raise
        conn.close()

    def _upload_single_file_to_table(self, conn, file_path: str, table: str):
        file_name = os.path.basename(file_path)
        if self.warehouse:
            logger.debug(f"Using warehouse '{self.warehouse}' for the connection.")
            command = f"USE WAREHOUSE {self.warehouse}"
            logger.debug(f"Executing command: '{command}'")
            conn.cursor().execute(command)
        self._create_stage_if_not_exists(conn)
        self._upload_file_to_stage(conn, file_path)
        self._create_table_if_not_exists(conn, file_name, table)
        self._perform_upload_to_table(conn, file_name, table)

    def _perform_upload_to_table(self, conn, file_name: str, table: str):
        logger.debug(f"Copying file '{file_name}' to table '{table}'")
        cursor = conn.cursor()
        try:
            if self.if_table_exists == "replace":
                logger.debug(
                    "'if_table_exists' is set to 'replace', so the current "
                    f"data in table '{table}' will be truncated."
                )
                cursor.execute(f"TRUNCATE {table}")
                logger.debug("Data truncated successfully")
            copy_instruction = (
                f"COPY INTO {table} FROM @{self.stage} FILES = ('{file_name}') "
                "FILE_FORMAT = (TYPE = 'PARQUET') "
                f"MATCH_BY_COLUMN_NAME = {self._support_match_by_column_name} "
                f"PURGE = {self._support_purge}"
            )
            logger.debug(f"Executing the instruction '{copy_instruction}'")
            cursor.execute(copy_instruction)
            logger.debug(f"File copied successfully to table '{table}'")
        except Exception:
            logger.error(f"Failed to copy file '{file_name}' to table '{table}'")
            raise
        finally:
            cursor.close()

    def _upload_file_to_stage(self, conn, file_path: str):
        file_path = convert_path_to_uri(file_path)
        logger.debug(f"Putting file '{file_path}' in stage '{self.stage}'")
        cursor = conn.cursor()
        try:
            instruction = f"PUT {file_path} @{self.stage}"
            logger.debug(f"Executing the instruction '{instruction}'")
            cursor.execute(instruction)
            logger.debug("File put successfully")
        except Exception:
            logger.error(f"Failed to put file '{file_path}' in stage '{self.stage}'")
            raise
        finally:
            cursor.close()

    def _create_table_if_not_exists(self, conn, file_name: str, table: str):
        logger.debug(f"Creating table '{table}' if it doesn't exist")
        cursor = conn.cursor()
        try:
            cursor.execute(
                "CREATE FILE FORMAT IF NOT EXISTS tabsdata_parquet TYPE = PARQUET"
            )
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {table} USING template (
               SELECT array_agg(object_construct(*))
               FROM table (
                 infer_schema(
                   location => '@{self.stage}/{file_name}',
                   file_format => 'tabsdata_parquet',
                   ignore_case => true)
                   )
               )
               """
            logger.debug(f"Using ddl command {ddl}")
            cursor.execute(ddl)
            logger.debug(f"Table '{table}' created successfully or already existed.")
        except Exception:
            logger.error(f"Failed to create table '{table}'")
            raise
        finally:
            cursor.close()

    def _create_stage_if_not_exists(self, conn):
        if not self.stage:
            logger.debug(
                "No stage provided. A new temporary unique stage will be created."
            )
            id_uuid, id_code = encode_id(debug=False)
            stage = f"td_{id_code.lower()}"
            logger.debug(f"Using stage name '{stage}'. Creating it now.")
            cursor = conn.cursor()
            stage = self._fully_qualify_entity(stage)
            try:
                cursor.execute(f"CREATE TEMP STAGE IF NOT EXISTS {stage}")
                logger.debug(
                    f"Stage '{stage}' created successfully or already existed."
                )
                self.stage = stage
            except Exception:
                logger.error(f"Failed to create stage '{stage}'")
                raise
            finally:
                cursor.close()
        else:
            self.stage = self._fully_qualify_entity(self.stage)
            logger.debug(f"Using stage '{self.stage}'")

    def stream(
        self,
        working_dir: str,
        *results: List[pl.LazyFrame | None] | pl.LazyFrame | None,
    ):
        if len(results) != len(self.destination_table):
            raise ValueError(
                f"The number of results ({len(results)}) does not match the number of "
                f"tables provided ({len(self.destination_table)}. Please make "
                "sure that the number of results matches the number of tables."
            )
        logging.getLogger("snowflake").setLevel(self._support_snowflake_logging_level)
        # Chunk the results
        logger.info("Chunking the results.")
        files = self.chunk(working_dir, *results)
        logger.info("Writing the results to Snowflake.")
        self.write(files)
        logger.info("All results written successfully.")

    def chunk(
        self, working_dir: str, *results: pl.LazyFrame | None
    ) -> List[None | str]:
        list_of_files = []
        logger.info("Chunking the results")
        for index, result in enumerate(results):
            logger.debug(f"Chunking result in position {index}")
            if result is None:
                logger.warning(f"Result in position '{index}' is None.")
                list_of_files.append(None)
            else:
                uuid_string = uuid.uuid4().hex[:16]  # Using only 16 characters to avoid

                # issues with file name length in Windows
                file_name = f"intermediate_{index}_{uuid_string}.parquet".replace(
                    "-", "_"
                )
                intermediate_destination_file = os.path.join(working_dir, file_name)
                logger.debug(f"Sinking the data to {intermediate_destination_file}")
                result.sink_parquet(intermediate_destination_file)
                logger.debug(
                    f"File {intermediate_destination_file} generated successfully"
                )
                list_of_files.append(intermediate_destination_file)
            logger.debug(f"Chunked result in position {index} successfully")
        logger.info("All results chunked successfully")
        return list_of_files

    def _fully_qualify_entity(self, entity: str) -> str:
        """
        Fully qualifies the entity with the database and schema if they are provided.
        """

        logger.debug(f"Fully qualifying entity: {entity}")
        database = self.database
        schema = self.schema
        dots = entity.count(".")
        if dots == 2:
            # If the entity already has two dots, assume it's fully qualified
            logger.debug(f"Entity '{entity}' is already fully qualified.")
            return entity
        elif dots == 1:
            # If the entity has one dot, assume it is of the form schema.entity
            logger.debug(f"Entity '{entity}' has one dot, trying to add database.")
            if database:
                # If database is provided, prepend it to the entity
                logger.debug(f"Prepending database '{database}' to entity '{entity}'.")
                return f"{database}.{entity}"
            else:
                # If no database is provided, return the entity as is
                logger.debug(f"No database provided, returning entity '{entity}'.")
                return entity
        elif dots == 0:
            logger.debug(
                f"Entity '{entity}' has no dots, checking database and schema."
            )
            # If the entity has no dots, assume it's just the entity name
            # If both database and schema are provided, fully qualify the entity
            if database and schema:
                logger.debug(
                    f"Both database '{database}' and schema '{schema}' provided, "
                    f"fully qualifying entity '{entity}'."
                )
                return f"{database}.{schema}.{entity}"
            elif schema:
                # If only schema is provided, qualify with schema
                logger.debug(
                    f"Only schema '{schema}' provided, qualifying entity '{entity}'"
                    " with it."
                )
                return f"{schema}.{entity}"
            else:
                # If no database or schema is provided, return the entity as is
                logger.debug(
                    f"No database or schema provided, returning entity '{entity}'."
                )
                return entity
