#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import uuid
from typing import List, Literal

import polars as pl

from tabsdata.io.plugin import DestinationPlugin
from tabsdata.secret import _recursively_evaluate_secret
from tabsdata.tabsserver.function.global_utils import convert_path_to_uri

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


try:
    modify_snowflake_logger_levels()

    import warnings

    warnings.filterwarnings("ignore", module="snowflake")

    from snowflake.connector import connect

    MISSING_LIBRARIES = False
except ImportError:
    MISSING_LIBRARIES = True
finally:
    restore_snowflake_logger_levels()

TABSDATA_STAGE_NAME = "tabsdata_created_stage"


class SnowflakeDestination(DestinationPlugin):

    def __init__(
        self,
        connection_parameters: dict,
        destination_table: List[str] | str,
        if_table_exists: Literal["append", "replace"] = "append",
        stage: str | None = None,
        **kwargs,
    ):
        """
        Initializes the SnowflakeDestination with the configuration desired to store
            the data.

        Args:

        """
        if MISSING_LIBRARIES:
            raise ImportError(
                "The 'tabsdata_snowflake' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['snowflake']'"
            )
        self.connection_parameters = connection_parameters
        if isinstance(destination_table, str):
            self.destination_table = [destination_table]
        elif isinstance(destination_table, list) and all(
            isinstance(t, str) for t in destination_table
        ):
            self.destination_table = destination_table
        else:
            raise TypeError(
                "The 'destination_table' parameter must be a string or a list of "
                f"strings, got {destination_table} instead."
            )
        if if_table_exists not in ["append", "replace"]:
            raise ValueError(
                "The if_table_exists parameter must be either 'append' or 'replace', "
                f"got {if_table_exists} instead."
            )
        self.if_table_exists = if_table_exists
        self.stage = stage
        self.kwargs = kwargs
        self._support_snowflake_logging_level = self.kwargs.get(
            "support_snowflake_logging_level", logging.ERROR
        )
        self._support_match_by_column_name = self.kwargs.get(
            "support_match_by_column_name", "CASE_INSENSITIVE"
        )
        self._support_purge = self.kwargs.get("support_purge", "TRUE")

    def write(self, files):
        """
        This method is used to write the files to the database. It is called
        from the stream method, and it is not intended to be called directly.
        """
        connection_parameters = _recursively_evaluate_secret(self.connection_parameters)
        conn = connect(**connection_parameters)
        for file_path, table in zip(files, self.destination_table):
            logger.info(f"Starting upload of results to table {table}")
            logger.debug(f"Storing file {file_path} in table {table}")
            try:
                self._upload_single_file_to_table(conn, file_path, table)
                logger.info(f"Uploaded results to table {table} successfully.")
            except Exception:
                logger.error(
                    f"Failed to upload results from file {file_path} to table {table}"
                )
                conn.close()
                raise
        conn.close()

    def _upload_single_file_to_table(self, conn, file_path: str, table: str):
        file_name = os.path.basename(file_path)
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
                f"No stage provided, using '{TABSDATA_STAGE_NAME}'. "
                "Creating it now if it doesn't exist."
            )
            cursor = conn.cursor()
            try:
                cursor.execute(f"CREATE STAGE IF NOT EXISTS {TABSDATA_STAGE_NAME}")
                logger.debug(
                    f"Stage '{TABSDATA_STAGE_NAME}' created successfully or "
                    "already existed."
                )
                self.stage = TABSDATA_STAGE_NAME
            except Exception:
                logger.error(f"Failed to create stage '{TABSDATA_STAGE_NAME}'")
                raise
            finally:
                cursor.close()
        else:
            logger.debug(f"Using stage '{self.stage}'")

    def stream(self, working_dir: str, *results: pl.LazyFrame | None):
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
    ) -> List[None | List[str]]:
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
