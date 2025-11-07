#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import uuid
from typing import TypeAlias, Union

import polars as pl

from tabsdata._credentials import GCPCredentials, GCPServiceAccountKeyCredentials
from tabsdata._io.connections.connections import Conn
from tabsdata._io.inputs.table_inputs import TableInput
from tabsdata._io.outputs.file_outputs import GCSDestination
from tabsdata._io.outputs.shared_enums import (
    IfTableExistsStrategy,
    IfTableExistStrategySpec,
    SchemaStrategy,
    SchemaStrategySpec,
)
from tabsdata._io.plugin import DestinationPlugin, td_context
from tabsdata._tableuri import build_table_uri_object

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TableSpec: TypeAlias = Union[str, list[str], None]


class BigQueryConn(Conn):
    """
    Represents a connection configuration to BigQuery.
    """

    def __init__(
        self,
        gcs_folder: str,  # A gcs:// URI indicating the folder where the table(s)
        # will be staged before being uploaded to BigQuery. After the upload is
        # done, the files created in this folder will be deleted.
        credentials: GCPCredentials,
        project: str = None,
        dataset: str = None,
        enforce_connection_params: bool = True,  # If True, enforce that project and
        # dataset are used to fully qualify table names in the destination if
        # provided. If set to False, the connection will allow the project and/or
        # dataset to be overriden by the table names provided in the destination.
        cx_dst_configs_gcs: dict = None,
        cx_dst_configs_bigquery: dict = None,
    ):
        """
        Initializes the BigQueryConn with the configuration desired to connect
            to BigQuery.

        Args:
            gcs_folder (str): A gcs:// URI indicating the folder where the table(s)
                will be staged before being uploaded to BigQuery. After the upload is
                done, the files created in this folder will be deleted.
            credentials (GCPCredentials): The GCP credentials to use for
                authentication.
            project (str, optional): The default GCP project to use. If not provided,
                the project must be specified in the table names provided in the
                destination. Defaults to None.
            dataset (str, optional): The default BigQuery dataset to use. If not
                provided, the dataset must be specified in the table names provided
                in the destination. Defaults to None.
            enforce_connection_params (bool, optional): If True, enforce that project
                and dataset are used to fully qualify table names in the destination if
                provided. If set to False, the connection will allow the project and/or
                dataset to be overriden by the table names provided in the destination.
                Defaults to True.
            cx_dst_configs_gcs (dict, optional): Additional configuration parameters to
                pass to the GCS client. Defaults to None.
            cx_dst_configs_bigquery (dict, optional): Additional configuration
                parameters to pass to the BigQuery client. Defaults to None.
        """
        self.gcs_folder = gcs_folder
        self.credentials = credentials
        self.project = project
        self.dataset = dataset
        self.enforce_connection_params = enforce_connection_params
        self.cx_dst_configs_gcs = cx_dst_configs_gcs or {}
        self.cx_dst_configs_bigquery = cx_dst_configs_bigquery or {}

    @property
    def credentials(self) -> GCPCredentials:
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: GCPCredentials):
        if not isinstance(credentials, GCPCredentials):
            raise TypeError(
                "The credentials must be a GCPCredentials instance, got "
                f"{type(credentials)} instead."
            )
        self._credentials = credentials

    @property
    def gcs_folder(self) -> str:
        return self._gcs_folder

    @gcs_folder.setter
    def gcs_folder(self, gcs_folder: str):
        if not gcs_folder.lower().startswith("gs://"):
            raise ValueError(
                "The 'gcs_folder' parameter must be a valid GCS URI starting with "
                f"'gs://', got '{gcs_folder}' instead."
            )
        self._gcs_folder = gcs_folder

    @property
    def project(self) -> str | None:
        return self._project

    @project.setter
    def project(self, project: str | None):
        if project is not None and not isinstance(project, str):
            raise TypeError(
                "The 'project' parameter must be a string or None, got "
                f"{type(project)} instead."
            )
        self._project = project

    @property
    def dataset(self) -> str | None:
        return self._dataset

    @dataset.setter
    def dataset(self, dataset: str | None):
        if dataset is not None and not isinstance(dataset, str):
            raise TypeError(
                "The 'dataset' parameter must be a string or None, got "
                f"{type(dataset)} instead."
            )
        self._dataset = dataset

    @property
    def cx_dst_configs_gcs(self) -> dict:
        return self._cx_dst_configs_gcs

    @cx_dst_configs_gcs.setter
    def cx_dst_configs_gcs(self, cx_dst_configs_gcs: dict | None):
        if cx_dst_configs_gcs is None:
            cx_dst_configs_gcs = {}
        if not isinstance(cx_dst_configs_gcs, dict):
            raise TypeError(
                "The 'cx_dst_configs_gcs' parameter must be a dictionary or None, got "
                f"{type(cx_dst_configs_gcs)} instead."
            )
        self._cx_dst_configs_gcs = cx_dst_configs_gcs

    @property
    def cx_dst_configs_bigquery(self) -> dict:
        return self._cx_dst_configs_bigquery

    @cx_dst_configs_bigquery.setter
    def cx_dst_configs_bigquery(self, cx_dst_configs_bigquery: dict | None):
        if cx_dst_configs_bigquery is None:
            cx_dst_configs_bigquery = {}
        if not isinstance(cx_dst_configs_bigquery, dict):
            raise TypeError(
                "The 'cx_dst_configs_bigquery' parameter must be a dictionary or None, "
                f"got {type(cx_dst_configs_bigquery)} instead."
            )
        self._cx_dst_configs_bigquery = cx_dst_configs_bigquery

    @property
    def enforce_connection_params(self) -> bool:
        return self._enforce_connection_params

    @enforce_connection_params.setter
    def enforce_connection_params(self, enforce_connection_params: bool):
        if not isinstance(enforce_connection_params, bool):
            raise TypeError(
                "The 'enforce_configuration' parameter must be a boolean, got "
                f"{type(enforce_connection_params)} instead."
            )
        self._enforce_connection_params = enforce_connection_params

    def __repr__(self):
        return (
            f"BigQueryConn(gcs_folder='{self.gcs_folder}', "
            f"credentials={self.credentials}, project='{self.project}', "
            f"dataset='{self.dataset}', "
            f"enforce_connection_params={self.enforce_connection_params}, "
            f"cx_dst_configs_gcs={self.cx_dst_configs_gcs}, "
            f"cx_dst_configs_bigquery={self.cx_dst_configs_bigquery})"
        )


class BigQueryDest(DestinationPlugin):
    """
    Destination plugin to store data in BigQuery tables. The data is first stored in
        parquet files in a GCS bucket, and then loaded into the BigQuery tables.
    """

    def __init__(
        self,
        conn: BigQueryConn,
        tables: TableSpec = None,
        if_table_exists: IfTableExistStrategySpec = "append",
        schema_strategy: SchemaStrategySpec = "update",
    ):
        """
        Initializes the BigQueryDest with the configuration desired to store
            the data.

        Args:
            conn (BigQueryConn): The BigQuery connection configuration.
            tables (str | list[str] | None, optional): The table(s) to store the data
                in. If multiple tables are provided, they must be provided as a list. If
                None, the table names will be those of the input tables for the
                function. Defaults to None.
            if_table_exists ({'append', 'replace'}, optional): The strategy to
                follow when the table already exists.
                - ‘append’ will append to an existing table.
                - ‘replace’ will create a new table, overwriting an existing
                one. Defaults to 'append'.
            schema_strategy ({'update', 'strict'}, optional): The strategy to
                follow for the schema when the table already exists.
                - ‘update’ will update the schema with the possible new columns that
                    might exist in the TableFrame.
                - ‘strict’ will not modify the schema, and will fail if there is any
                    difference. Defaults to 'update'.
        """
        try:
            from google.cloud import bigquery  # noqa: F401
        except ImportError:
            raise ImportError(
                "The 'tabsdata_bigquery' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['bigquery']'"
            )
        self.conn = conn
        self.tables = tables
        self.if_table_exists = if_table_exists
        self.schema_strategy = schema_strategy

    def __repr__(self):
        return (
            f"BigQueryDest(conn={self.conn}, tables={self.tables}, "
            f"if_table_exists='{self.if_table_exists}', "
            f"schema_strategy='{self.schema_strategy}')"
        )

    @property
    def tables(self) -> list[str] | None:
        return self._tables

    @tables.setter
    def tables(self, tables):
        if tables is None:
            self._tables = None
            if not self.conn.project or not self.conn.dataset:
                raise ValueError(
                    "When the 'tables' parameter is None, both 'project' and "
                    "'dataset' parameters must be provided in the BigQueryConn."
                )
            return
        if isinstance(tables, list):
            if not all(isinstance(table, str) for table in tables):
                raise TypeError(
                    "All elements in the 'tables' list must be strings, "
                    f"got '{tables}' instead."
                )
        elif isinstance(tables, str):
            tables = [tables]
        else:
            raise TypeError(
                "The 'tables' parameter must be a string or a list of "
                f"strings or None, got '{type(tables)}' instead."
            )
        qualified_tables = []
        for table in tables:
            fully_qualified_table = self._fully_qualify_table(table)
            if fully_qualified_table.count(".") != 2:
                raise ValueError(
                    "Each table name provided must either be fully qualified (in the "
                    "form of 'project.dataset.table_name'), "
                    "or have the information provided through the 'project' or the "
                    "'dataset' parameters in the BigQueryConn. For example, "
                    "if a table of the form 'dataset.table_name' is provided, the "
                    "'project' parameter must also be provided in the connection. If a "
                    "table of the form 'table_name' is provided, both 'project' and "
                    "'dataset' must be provided in the connection."
                )
            qualified_tables.append(fully_qualified_table)
        self._tables = qualified_tables

    @property
    def conn(self) -> BigQueryConn:
        return self._conn

    @conn.setter
    def conn(self, conn: BigQueryConn):
        if not isinstance(conn, BigQueryConn):
            raise TypeError(
                "The 'conn' parameter must be a BigQueryConn instance, got "
                f"{type(conn)} instead."
            )
        self._conn = conn

    @property
    def if_table_exists(self) -> IfTableExistStrategySpec:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: IfTableExistStrategySpec):
        """
        Sets the strategy to follow when the table already exists.

        Args:
            if_table_exists ({'append', 'replace'}): The strategy to
                follow when the table already exists.
                - ‘append’ will append to an existing table.
                - ‘replace’ will create a new table, overwriting an existing
                one.
        """
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if if_table_exists not in valid_values:
            raise ValueError(
                "The 'if_table_exists' parameter in a DatabricksDestination must be "
                f"one of the following values '{valid_values}', got "
                f"'{if_table_exists}' instead."
            )
        self._if_table_exists = if_table_exists

    @property
    def schema_strategy(self) -> SchemaStrategySpec:
        """
        str: The strategy to follow when appending to a table with an existing schema.
        """
        return self._schema_strategy

    @schema_strategy.setter
    def schema_strategy(self, schema_strategy: SchemaStrategySpec):
        """
        Sets the strategy to follow when appending to a table with an existing schema.

        Args:
            schema_strategy ({'update', 'strict'}): The strategy to
                follow for the schema when the table already exists.
                - ‘update’ will update the schema with the possible new columns that
                    might exist in the TableFrame.
                - ‘strict’ will not modify the schema, and will fail if there is any
                    difference.
        """
        valid_values = [
            SchemaStrategy.UPDATE.value,
            SchemaStrategy.STRICT.value,
        ]
        if schema_strategy not in valid_values:
            raise ValueError(
                "The 'schema_strategy' parameter in a DatabricksDestination must be "
                f"one of the following values '{valid_values}', got "
                f"'{schema_strategy}' instead."
            )
        self._schema_strategy = schema_strategy

    @property
    def _stream_require_ec(self) -> bool:
        """
        Indicates whether the stream method requires an execution context.

        Returns:
            bool: True if the stream method requires an execution context,
            False otherwise.
        """
        return True

    def stream(
        self,
        working_dir: str,
        *results: list[pl.LazyFrame | None] | pl.LazyFrame | None,
    ):
        if self.tables is None:
            logger.debug(
                "The 'tables' parameter is None, getting table names from input plugin"
            )
            input_plugin: TableInput = self._ec.source
            table_uris = input_plugin._table_list
            logger.debug(f"Table URIs obtained from input plugin: {table_uris}")
            table_names = [
                build_table_uri_object(single_uri).table for single_uri in table_uris
            ]
            logger.debug(f"Table names extracted: {table_names}")
            self.tables = table_names

        if len(results) != len(self.tables):
            raise ValueError(
                f"The number of results ({len(results)}) does not match the number of "
                f"tables provided ({len(self.tables)}. Please make "
                "sure that the number of results matches the number of tables."
            )

        auxiliary_file_names = []
        for index in range(len(results)):
            uuid_string = uuid.uuid4().hex[:16]  # Using only 16 characters to avoid
            # issues with file name length in Windows
            file_name = f"auxiliary_{index}_{uuid_string}.parquet".replace("-", "_")
            auxiliary_file_names.append(file_name)
        auxiliary_uris = [
            os.path.join(self.conn.gcs_folder, file_name)
            for file_name in auxiliary_file_names
        ]
        auxiliary_gcs_destination = GCSDestination(
            uri=auxiliary_uris,
            credentials=self.conn.credentials,
            format="parquet",
        )
        execution_context = self._ec
        logger.info(
            f"Using auxiliary GCS destination with URI(s) '{auxiliary_uris}' "
            "to stage the parquet files"
        )
        logger.debug(f"Auxiliary GCS destination: {auxiliary_gcs_destination}")
        client, gcs_client = self._get_connections()
        qualified_table = None
        try:
            with td_context(auxiliary_gcs_destination, execution_context):
                auxiliary_gcs_destination.stream(
                    execution_context.paths.output_folder, *results
                )
            logger.info(
                f"Auxiliary GCS destination with URI(s) '{auxiliary_uris}' "
                "used successfully"
            )
            for qualified_table, uri, result in zip(
                self.tables, auxiliary_uris, results
            ):
                if result is None:
                    logger.info(
                        f"No data to upload to BigQuery table '{qualified_table}', "
                        "skipping."
                    )
                    continue
                logger.info(
                    f"Uploading file '{uri}' to BigQuery table '{qualified_table}'"
                )
                self._upload_single_uri_to_table(uri, qualified_table, client)
                logger.info(
                    f"File '{uri}' uploaded to BigQuery table "
                    f"'{qualified_table}' successfully"
                )
        except Exception:
            if qualified_table:
                logger.error(
                    f"Failed to upload data to BigQuery table '{qualified_table}'"
                )
            else:
                logger.error("Failed to upload data to BigQuery destination")
            raise
        finally:
            logger.info("Cleaning up auxiliary parquet files from GCS")
            for uri in auxiliary_uris:
                logger.debug(f"Deleting auxiliary file '{uri}' from GCS")
                try:
                    bucket_name, blob_name = uri.replace("gs://", "").split("/", 1)
                    bucket = gcs_client.bucket(bucket_name)
                    blob = bucket.blob(blob_name)
                    blob.delete()
                    logger.debug(
                        f"Auxiliary file '{uri}' deleted successfully from GCS"
                    )
                except Exception:
                    logger.warning(f"Failed to delete auxiliary file '{uri}' from GCS")

    def _fully_qualify_table(self, table: str) -> str:
        if table.count(".") == 2:
            if self.conn.project or self.conn.dataset:
                if self.conn.enforce_connection_params:
                    project, dataset, table_name = table.split(".")
                    self._enforce_configuration(table_name, project, dataset)
                else:
                    logger.warning(
                        f"Table '{table}' is already fully qualified, so the 'project' "
                        "and 'dataset' parameters in the connection will be ignored."
                    )
            return table
        elif table.count(".") == 1:
            if not self.conn.project:
                raise ValueError(
                    f"Table '{table}' is not fully qualified, so the 'project' "
                    "parameter must be provided in the connection."
                )
            if self.conn.dataset:
                if self.conn.enforce_connection_params:
                    dataset, table_name = table.split(".")
                    self._enforce_configuration(table_name, None, dataset)
                else:
                    logger.warning(
                        f"Table '{table}' includes the dataset, so the 'dataset' "
                        "parameter in the connection will be ignored."
                    )
            return f"{self.conn.project}.{table}"
        else:
            if not self.conn.project or not self.conn.dataset:
                raise ValueError(
                    f"Table '{table}' is not fully qualified, so both the 'project' "
                    "and 'dataset' parameters must be provided in the connection."
                )
            return f"{self.conn.project}.{self.conn.dataset}.{table}"

    def _enforce_configuration(
        self, table: str, project: str = None, dataset: str = None
    ):
        if project and self.conn.project and project != self.conn.project:
            raise ValueError(
                f"Table '{table}' specifies project '{project}', which "
                "does not match the 'project' parameter in the connection "
                f"('{self.conn.project}'), and 'enforce_configuration' is "
                "set to True. Either use the same project in the table "
                "and in the connection, remove the project from the "
                "table provided in the destination,or set "
                "'enforce_configuration' in the connection to False."
            )
        if dataset and self.conn.dataset and dataset != self.conn.dataset:
            raise ValueError(
                f"Table '{table}' specifies dataset '{dataset}', which "
                "does not match the 'dataset' parameter in the connection "
                f"('{self.conn.dataset}'), and 'enforce_configuration' is "
                "set to True. Either use the same dataset in the table "
                "and in the connection, remove the dataset from the "
                "table provided in the destination, or set "
                "'enforce_configuration' in the connection to False."
            )

    def _upload_single_uri_to_table(
        self,
        uri: str,
        table: str,
        client,
    ):
        logger.debug(f"Copying file '{uri}' to table '{table}'")

        from google.cloud import bigquery

        try:
            if self.if_table_exists == "replace":
                logger.debug(
                    "'if_table_exists' is set to 'replace', so the current "
                    f"data in table '{table}' will be truncated."
                )

                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                schema_update_options = []
            else:
                logger.debug(
                    "'if_table_exists' is set to 'append', so the data will be "
                    f"appended to table '{table}' if it already exists."
                )
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                if self.schema_strategy == "strict":
                    logger.debug(
                        "Schema strategy is 'strict', so the schema will not be "
                        "modified and the operation will fail if there are any "
                        "differences."
                    )
                    schema_update_options = []
                else:
                    logger.debug(
                        "Schema strategy is 'update', so the schema will be updated "
                        "with any new columns that might exist in the file."
                    )
                    schema_update_options = [
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                    ]
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
                schema_update_options=schema_update_options,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            logger.debug(f"Job configuration: {job_config}")
            logger.debug(f"Starting load job from '{uri}' to table '{table}'")
            load_job = client.load_table_from_uri(
                uri,
                table,
                job_config=job_config,
            )
            logger.debug("Waiting for load job to complete...")
            load_job.result()
            logger.debug("Load job completed successfully.")
            logger.debug(f"Loaded {load_job.output_rows} rows into table '{table}'.")
        except Exception:
            logger.error(f"Failed to copy file '{uri}' to table '{table}'")
            raise

    def _get_connections(self):
        # Note: we do the imports here instead of at the top to speed up the import
        # of the tabsdata package, and therefore the td CLI response time.
        logger.debug("Creating BigQuery and GCS clients from connection credentials.")
        from google.cloud import bigquery, storage

        if isinstance(self.conn.credentials, GCPServiceAccountKeyCredentials):
            logger.debug(
                "Using service account key credentials for BigQuery and GCS clients."
            )
            credentials: GCPServiceAccountKeyCredentials = self.conn.credentials
            service_account_key_string = credentials.service_account_key.secret_value
            import json

            service_account_key_dict = json.loads(service_account_key_string)
            client = bigquery.Client.from_service_account_info(
                service_account_key_dict,
                **self.conn.cx_dst_configs_bigquery,
            )
            gcs_client = storage.Client.from_service_account_info(
                service_account_key_dict,
                **self.conn.cx_dst_configs_gcs,
            )
        else:
            raise ValueError(
                "Unsupported credentials type for BigQuery connection: "
                f"{type(self.conn.credentials)}"
            )
        logger.debug("BigQuery and GCS clients created successfully.")
        return client, gcs_client
