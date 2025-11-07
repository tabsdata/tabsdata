#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import datetime
import logging
import os
import subprocess
import time
import uuid
from enum import Enum
from urllib.parse import urlparse, urlunparse

import polars as pl

from tabsdata._credentials import (
    AzureCredentials,
    GCPCredentials,
    S3AccessKeyCredentials,
    S3Credentials,
    build_credentials,
)
from tabsdata._format import (
    AVRO_EXTENSION,
    CSV_EXTENSION,
    NDJSON_EXTENSION,
    PARQUET_EXTENSION,
    TABSDATA_EXTENSION,
    AvroFormat,
    CSVFormat,
    FileFormat,
    NDJSONFormat,
    ParquetFormat,
    build_file_format,
    get_implicit_format_from_list,
)
from tabsdata._io.constants import (
    AZURE_SCHEME,
    FILE_SCHEME,
    GCS_SCHEME,
    S3_SCHEME,
    URI_INDICATOR,
    SupportedAWSS3Regions,
)
from tabsdata._io.outputs.shared_enums import (
    IfTableExistsStrategy,
    IfTableExistStrategySpec,
    SchemaStrategy,
    SchemaStrategySpec,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._secret import _recursively_evaluate_secret, _recursively_load_secret
from tabsdata._tabsserver.function.cloud_connectivity_utils import (
    SERVER_SIDE_AWS_ACCESS_KEY_ID,
    SERVER_SIDE_AWS_REGION,
    SERVER_SIDE_AWS_SECRET_ACCESS_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_KEY,
    SERVER_SIDE_AZURE_ACCOUNT_NAME,
    SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON,
    obtain_and_set_azure_credentials,
    obtain_and_set_gcp_credentials,
    obtain_and_set_s3_credentials,
    set_s3_region,
)
from tabsdata._tabsserver.function.global_utils import (
    CURRENT_PLATFORM,
    convert_path_to_uri,
)
from tabsdata._tabsserver.function.yaml_parsing import (
    InputYaml,
    TransporterAzure,
    TransporterEnv,
    TransporterGCS,
    TransporterLocalFile,
    TransporterS3,
    V1CopyFormat,
    store_copy_as_yaml,
)
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
)

logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

FRAGMENT_INDEX_PLACEHOLDER = "$FRAGMENT_IDX"


class Catalog:
    pass


class AWSGlue(Catalog):

    IDENTIFIER = "aws-glue-catalog"

    ALLOW_INCOMPATIBLE_CHANGES_KEY = "allow_incompatible_changes"
    AUTO_CREATE_AT_KEY = "auto_create_at"
    DEFINITION_KEY = "definition"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    PARTITIONED_TABLE_KEY = "partitioned_table"
    SCHEMA_STRATEGY_KEY = "schema_strategy"
    TABLES_KEY = "tables"

    AWS_GLUE_ACCESS_KEY_ID = "client.access-key-id"
    AWS_GLUE_REGION = "client.region"
    AWS_GLUE_SECRET_ACCESS_KEY = "client.secret-access-key"

    S3_ACCESS_KEY_ID = "s3.access-key-id"
    S3_REGION = "s3.region"
    S3_SECRET_ACCESS_KEY = "s3.secret-access-key"

    def __init__(
        self,
        definition: dict,
        tables: str | list[str],
        auto_create_at: list[str | None] | str | None = None,
        if_table_exists: IfTableExistStrategySpec = "append",
        partitioned_table: bool = False,
        schema_strategy: SchemaStrategySpec = "update",
        s3_credentials: S3Credentials = None,
        s3_region: str = None,
        **kwargs,
    ):
        self.definition = definition
        self.tables = tables
        self.if_table_exists = if_table_exists
        self.partitioned_table = partitioned_table
        self.allow_incompatible_changes = kwargs.get(
            "allow_incompatible_changes", False
        )
        self.auto_create_at = auto_create_at
        self.schema_strategy = schema_strategy
        self.s3_credentials = s3_credentials
        self.s3_region = s3_region

    @property
    def partitioned_table(self) -> bool:
        """
        bool: Whether the table is partitioned or not.
        """
        return self._partitioned_table

    @partitioned_table.setter
    def partitioned_table(self, partitioned_table: bool):
        """
        Sets whether the table is partitioned or not.

        Args:
            partitioned_table (bool): Whether the table is partitioned or not.
        """
        if not isinstance(partitioned_table, bool):
            raise DestinationConfigurationError(
                ErrorCode.DECE40, type(partitioned_table)
            )
        self._partitioned_table = partitioned_table
        if hasattr(self, "_if_table_exists"):
            if (
                self._if_table_exists == IfTableExistsStrategy.REPLACE.value
            ) and partitioned_table:
                raise DestinationConfigurationError(ErrorCode.DECE39)

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
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.
        """
        valid_values = [
            IfTableExistsStrategy.APPEND.value,
            IfTableExistsStrategy.REPLACE.value,
        ]
        if if_table_exists not in valid_values:
            raise DestinationConfigurationError(
                ErrorCode.DECE33, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists
        if hasattr(self, "_partitioned_table"):
            if (
                self._partitioned_table
                and if_table_exists == IfTableExistsStrategy.REPLACE.value
            ):
                raise DestinationConfigurationError(ErrorCode.DECE39)

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
            raise DestinationConfigurationError(
                ErrorCode.DECE41, valid_values, schema_strategy
            )
        self._schema_strategy = schema_strategy

    @property
    def definition(self) -> dict:
        definition = {**self._user_definition}  # Make a copy to avoid modifying the
        # original
        if hasattr(self, "s3_credentials"):
            if isinstance(self.s3_credentials, S3AccessKeyCredentials):
                credentials: S3AccessKeyCredentials = self.s3_credentials
                definition[self.AWS_GLUE_ACCESS_KEY_ID] = credentials.aws_access_key_id
                definition[self.AWS_GLUE_SECRET_ACCESS_KEY] = (
                    credentials.aws_secret_access_key
                )
        if hasattr(self, "s3_region"):
            if self.s3_region is not None:
                definition[self.AWS_GLUE_REGION] = self.s3_region
        return definition

    @definition.setter
    def definition(self, definition: dict):
        if not isinstance(definition, dict):
            raise DestinationConfigurationError(ErrorCode.DECE30, type(definition))
        self._user_definition = _recursively_load_secret(definition)
        self._verify_duplicate_s3_credentials()
        self._verify_duplicate_s3_region()

    @property
    def s3_credentials(self) -> S3Credentials | None:
        return self._s3_credentials

    @s3_credentials.setter
    def s3_credentials(self, s3_credentials: S3Credentials | None):
        if s3_credentials is None:
            self._s3_credentials = None
        else:
            credentials = build_credentials(s3_credentials)
            if not (isinstance(credentials, S3Credentials)):
                raise DestinationConfigurationError(ErrorCode.DECE47, type(credentials))
            self._s3_credentials = credentials
        self._verify_duplicate_s3_credentials()

    @property
    def s3_region(self) -> str | None:
        """
        str: The region where the S3 bucket is located.
        """
        return self._s3_region

    @s3_region.setter
    def s3_region(self, region: str | None):
        """
        Sets the region where the S3 bucket is located.

        Args:
            region (str): The region where the S3 bucket is located.
        """
        if region:
            if not isinstance(region, str):
                raise DestinationConfigurationError(ErrorCode.DECE48, type(region))
            supported_regions = [element.value for element in SupportedAWSS3Regions]
            if region not in supported_regions:
                logger.warning(
                    "The 'region' parameter for the AWSGlue object has value "
                    f"'{region}', which is not recognized in our current list of AWS "
                    f"regions: {supported_regions}. This could indicate a typo in the "
                    "region provided, but it could also occur because you are "
                    "using a recently created AWS region or a private AWS region. "
                    "You can continue using this region if you are sure it is available"
                    " for your AWS account, but if it isn't it will cause an error "
                    "during runtime."
                )
            self._s3_region = region
        else:
            self._s3_region = None
        self._verify_duplicate_s3_region()

    @property
    def tables(self) -> list[str]:
        return self._tables

    @tables.setter
    def tables(self, tables: str | list[str]):
        if isinstance(tables, str):
            self._tables = [tables]
        elif isinstance(tables, list):
            if not all(isinstance(single_table, str) for single_table in tables):
                raise DestinationConfigurationError(ErrorCode.DECE31)
            self._tables = tables
        else:
            raise DestinationConfigurationError(ErrorCode.DECE32, type(tables))
        if hasattr(self, "_auto_create_at") and len(self._auto_create_at) != len(
            self._tables
        ):
            raise DestinationConfigurationError(
                ErrorCode.DECE42, self._tables, self._auto_create_at
            )

    @property
    def auto_create_at(self) -> list[str | None]:
        return self._auto_create_at

    @auto_create_at.setter
    def auto_create_at(self, auto_create_at: list[str | None]):
        if auto_create_at is None:
            self._auto_create_at = [None] * len(self._tables)
        elif isinstance(auto_create_at, str):
            self._auto_create_at = [auto_create_at]
        elif isinstance(auto_create_at, list):
            for single_location in auto_create_at:
                if not (isinstance(single_location, str) or single_location is None):
                    raise DestinationConfigurationError(ErrorCode.DECE43)
            self._auto_create_at = auto_create_at
        else:
            raise DestinationConfigurationError(ErrorCode.DECE44, type(auto_create_at))
        if hasattr(self, "_tables") and len(self._tables) != len(self._auto_create_at):
            raise DestinationConfigurationError(
                ErrorCode.DECE42, self._tables, self._auto_create_at
            )

    def _to_dict(self) -> dict:
        # TODO: Right now, Secrets are stored as a secret object, and we rely on the
        #   json serializer to turn them into dictionaries when bundling. Once using
        #   description jsons becomes more usual, this will have to be revisited.
        return {
            self.IDENTIFIER: {
                self.AUTO_CREATE_AT_KEY: self.auto_create_at,
                self.ALLOW_INCOMPATIBLE_CHANGES_KEY: self.allow_incompatible_changes,
                self.DEFINITION_KEY: self.definition,
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
                self.PARTITIONED_TABLE_KEY: self.partitioned_table,
                self.SCHEMA_STRATEGY_KEY: self.schema_strategy,
                self.TABLES_KEY: self.tables,
            }
        }

    def __eq__(self, other):
        if not isinstance(other, AWSGlue):
            return False
        return self._to_dict() == other._to_dict()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(definition={self.definition}, tables="
            f"{self.tables})"
        )

    def _verify_duplicate_s3_credentials(self):
        if hasattr(self, "_user_definition") and hasattr(self, "_s3_credentials"):
            if self._s3_credentials is not None and (
                self._user_definition.get(self.AWS_GLUE_ACCESS_KEY_ID)
                or self._user_definition.get(self.AWS_GLUE_SECRET_ACCESS_KEY)
            ):
                raise DestinationConfigurationError(ErrorCode.DECE45)

    def _verify_duplicate_s3_region(self):
        if hasattr(self, "_user_definition") and hasattr(self, "_s3_region"):
            if self._s3_region is not None and self._user_definition.get(
                self.AWS_GLUE_REGION
            ):
                raise DestinationConfigurationError(ErrorCode.DECE46)


def build_catalog(catalog) -> AWSGlue:
    """
    Builds a Catalog object from a dictionary or a Catalog object.

    Args:
        catalog (dict | AWSGlue): The dictionary or Catalog object to build the
          Catalog object from.

    Returns:
        AWSGlue: The Catalog object built from the dictionary or Catalog object.
    """
    if isinstance(catalog, AWSGlue):
        return catalog
    elif not isinstance(catalog, dict):
        raise DestinationConfigurationError(ErrorCode.DECE34, type(catalog))
    elif len(catalog) != 1 or next(iter(catalog)) != AWSGlue.IDENTIFIER:
        raise DestinationConfigurationError(
            ErrorCode.DECE35, AWSGlue.IDENTIFIER, list(catalog.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(catalog.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise DestinationConfigurationError(
            ErrorCode.DECE36, identifier, type(configuration)
        )
    return AWSGlue(**configuration)


class AzureDestination(DestinationPlugin):
    """
    Class for managing the configuration of Azure-file-based data outputs.

    Attributes:
        format (FileFormat): The format of the file to be created. If not provided,
            it will be inferred from the file extension.
        uri (str | list[str]): The URI of the files with format: 'az://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (AzureCredentials): The credentials required to access Azure.
    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the AzureDestination.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | list[str],
        credentials: AzureCredentials,
        format: str | FileFormat = None,
    ):
        """
        Initializes the AzureDestination with the given URI and the credentials
            required to access Azure; and optionally a format.

        Args:
            uri (str | list[str]): The URI of the files to export with format:
                'az://path/to/files'. It can be a single URI or a list of URIs.
            credentials (AzureCredentials): The credentials required to access
                Azure. Must be an AzureCredentials object.
            format (str | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'jsonl'.

        Raises:
            OutputConfigurationError
            FormatConfigurationError
        """
        self.uri = uri
        self.format = format
        self.credentials = credentials

    @property
    def uri(self) -> str | list[str]:
        """
        str | list[str]: The URI of the files with format: 'az://path/to/files'.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str | list[str]):
        """
        Sets the URI of the files with format: 'az://path/to/files'.

        Args:
            uri (str | list[str]): The URI of the files with format:
                'az://path/to/files'. It can be a single URI or a list of URIs.
        """
        self._uri = uri
        if isinstance(uri, str):
            self._uri_list = [uri]
        elif isinstance(uri, list):
            self._uri_list = uri
            if not all(isinstance(single_uri, str) for single_uri in self._uri_list):
                raise DestinationConfigurationError(ErrorCode.DECE14, type(uri))
        else:
            raise DestinationConfigurationError(ErrorCode.DECE14, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme.lower() != AZURE_SCHEME:
                raise DestinationConfigurationError(
                    ErrorCode.DECE15,
                    parsed_uri.scheme,
                    AZURE_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

        if not self.allow_fragments:
            for uri in self._uri_list:
                if FRAGMENT_INDEX_PLACEHOLDER in uri:
                    raise DestinationConfigurationError(
                        ErrorCode.DECE38,
                        FRAGMENT_INDEX_PLACEHOLDER,
                        uri,
                    )

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file extension of the URI.
        """
        return self._format or build_file_format(self._implicit_format)

    @format.setter
    def format(self, format: str | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str | FileFormat): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat.
                Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'avro'.
        """
        if format is None:
            self._format = None
            # No format was provided, so we validate that self._implicit_format is valid
            self._verify_valid_format(build_file_format(self._implicit_format))
        else:
            format = build_file_format(format)
            self._verify_valid_format(format)
            self._format = format

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the AzureDestination

        Args:
            format (FileFormat): The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise DestinationConfigurationError(
                ErrorCode.DECE13, type(format), valid_output_formats
            )

    @property
    def credentials(self) -> AzureCredentials:
        """
        AzureCredentials: The credentials required to access Azure.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: AzureCredentials):
        """
        Sets the credentials required to access Azure.

        Args:
            credentials (AzureCredentials): The credentials required to access
                Azure. Must be an AzureCredentials object.
        """
        credentials = build_credentials(credentials)
        if not (isinstance(credentials, AzureCredentials)):
            raise DestinationConfigurationError(ErrorCode.DECE16, type(credentials))
        self._credentials = credentials

    @property
    def allow_fragments(self) -> bool:
        """
        bool: Whether to allow fragments in the output.
        """
        return False

    @property
    def _stream_require_ec(self) -> bool:
        """
        Indicates whether the stream method requires an execution context.

        Returns:
            bool: True if the stream method requires an execution context,
            False otherwise.
        """
        return True

    def stream(self, working_dir: str, *results):
        logger.debug(
            f"Beginning streaming for {self.__class__.__name__} with results"
            f" '{results}'"
        )
        intermediate_files = self.chunk(working_dir, *results)
        # We add logic here instead of implementing a write method because there
        # would be a missmatch in the expected signature of the write method and the
        # signature it would need, due to the special catalog logic.
        obtain_and_set_azure_credentials(self.credentials),
        _write_results_in_final_files(
            results, intermediate_files, self, working_dir, self._ec.request
        )

    def chunk(self, working_dir: str, *results):
        logger.debug(
            f"Beginning chunking for {self.__class__.__name__} with results '{results}'"
        )
        intermediate_files = _store_results_in_intermediate_files(
            results, self, working_dir
        )
        logger.debug(
            f"Chunking completed for {self.__class__.__name__} with results '"
            f"{results}' and intermediate files '{intermediate_files}'"
        )
        return intermediate_files

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uri})"


class GCSDestination(DestinationPlugin):
    """
    Class for managing the configuration of GCS-file-based data outputs.

    Attributes:
        format (FileFormat): The format of the file to be created. If not provided,
            it will be inferred from the file extension.
        uri (str | list[str]): The URI of the files with format: 'gs://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (GCPCredentials): The credentials required to access GCS.
    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the GCSDestination.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | list[str],
        credentials: GCPCredentials,
        format: str | FileFormat = None,
    ):
        """
        Initializes the GCSDestination with the given URI and the credentials
            required to access GCS; and optionally a format.

        Args:
            uri (str | list[str]): The URI of the files to export with format:
                'gs://path/to/files'. It can be a single URI or a list of URIs.
            credentials (GCPCredentials): The credentials required to access
                GCS. Must be a GCPCredentials object.
            format (str | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'jsonl'.

        Raises:
            OutputConfigurationError
            FormatConfigurationError
        """
        self.uri = uri
        self.format = format
        self.credentials = credentials

    @property
    def uri(self) -> str | list[str]:
        """
        str | list[str]: The URI of the files with format: 'gs://path/to/files'.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str | list[str]):
        """
        Sets the URI of the files with format: 'gs://path/to/files'.

        Args:
            uri (str | list[str]): The URI of the files with format:
                'gs://path/to/files'. It can be a single URI or a list of URIs.
        """
        self._uri = uri
        if isinstance(uri, str):
            self._uri_list = [uri]
        elif isinstance(uri, list):
            self._uri_list = uri
            if not all(isinstance(single_uri, str) for single_uri in self._uri_list):
                raise DestinationConfigurationError(ErrorCode.DECE50, type(uri))
        else:
            raise DestinationConfigurationError(ErrorCode.DECE50, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme.lower() != GCS_SCHEME:
                raise DestinationConfigurationError(
                    ErrorCode.DECE51,
                    parsed_uri.scheme,
                    GCS_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

        if not self.allow_fragments:
            for uri in self._uri_list:
                if FRAGMENT_INDEX_PLACEHOLDER in uri:
                    raise DestinationConfigurationError(
                        ErrorCode.DECE38,
                        FRAGMENT_INDEX_PLACEHOLDER,
                        uri,
                    )

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file extension of the URI.
        """
        return self._format or build_file_format(self._implicit_format)

    @format.setter
    def format(self, format: str | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str | FileFormat): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'avro'.
        """
        if format is None:
            self._format = None
            # No format was provided, so we validate that self._implicit_format is valid
            self._verify_valid_format(build_file_format(self._implicit_format))
        else:
            format = build_file_format(format)
            self._verify_valid_format(format)
            self._format = format

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the GCSDestination

        Args:
            format (FileFormat): The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise DestinationConfigurationError(
                ErrorCode.DECE13, type(format), valid_output_formats
            )

    @property
    def credentials(self) -> GCPCredentials:
        """
        GCPCredentials: The credentials required to access GCS.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: GCPCredentials):
        """
        Sets the credentials required to access GCS.

        Args:
            credentials (GCPCredentials): The credentials required to access
                GCS. Must be a GCPCredentials object.
        """
        if not (isinstance(credentials, GCPCredentials)):
            raise DestinationConfigurationError(ErrorCode.DECE52, type(credentials))
        self._credentials = credentials

    @property
    def allow_fragments(self) -> bool:
        """
        bool: Whether to allow fragments in the output.
        """
        return False

    @property
    def _stream_require_ec(self) -> bool:
        """
        Indicates whether the stream method requires an execution context.

        Returns:
            bool: True if the stream method requires an execution context,
            False otherwise.
        """
        return True

    def stream(self, working_dir: str, *results):
        logger.debug(
            f"Beginning streaming for {self.__class__.__name__} with results"
            f" '{results}'"
        )
        intermediate_files = self.chunk(working_dir, *results)
        obtain_and_set_gcp_credentials(self.credentials),
        _write_results_in_final_files(
            results, intermediate_files, self, working_dir, self._ec.request
        )

    def chunk(self, working_dir: str, *results):
        logger.debug(
            f"Beginning chunking for {self.__class__.__name__} with results '{results}'"
        )
        intermediate_files = _store_results_in_intermediate_files(
            results, self, working_dir
        )
        logger.debug(
            f"Chunking completed for {self.__class__.__name__} with results '"
            f"{results}' and intermediate files '{intermediate_files}'"
        )
        return intermediate_files

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uri})"


class LocalFileDestination(DestinationPlugin):

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the LocalFileDestination.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    def __init__(
        self,
        path: str | list[str],
        format: str | FileFormat = None,
    ):
        """
        Initializes the LocalFileDestination with the given path; and optionally a
        format.

        Args:
            path (str | list[str]): The path where the files must be stored. It can be a
                single path or a list of paths.
            format (str | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'jsonl'.

        Raises:
            OutputConfigurationError
            FormatConfigurationError
        """
        self.path = path
        self.format = format

    @property
    def path(self) -> str | list[str]:
        """
        str | list[str]: The path or paths to store the files.
        """
        return self._path

    @path.setter
    def path(self, path: str | list[str]):
        """
        Sets the path or paths to store the files.

        Args:
            path (str | list[str]): The path or paths to store the files.
        """
        self._path = path
        if isinstance(path, str):
            self._path_list = [path]
        elif isinstance(path, list):
            self._path_list = path
            if not all(isinstance(single_path, str) for single_path in self._path_list):
                raise DestinationConfigurationError(ErrorCode.DECE11, type(path))
        else:
            raise DestinationConfigurationError(ErrorCode.DECE11, type(path))

        for individual_path in self._path_list:
            if URI_INDICATOR in individual_path:
                parsed_path = urlparse(individual_path)
                if parsed_path.scheme != FILE_SCHEME:
                    raise DestinationConfigurationError(
                        ErrorCode.DECE12,
                        parsed_path.scheme,
                        FILE_SCHEME,
                        urlunparse(parsed_path),
                    )

        self._implicit_format_string = get_implicit_format_from_list(self._path_list)
        if hasattr(self, "_format") and self._format is None:
            # This check verifies that we are not in the __init__ function,
            # so we might have to check if the implicit format is valid or not.
            self._verify_valid_format(build_file_format(self._implicit_format_string))

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file or files. If not provided, it will be
            inferred from the file extension in the path.
        """
        return self._format or build_file_format(self._implicit_format_string)

    @format.setter
    def format(self, format: str | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str): The format of the file. If not
                provided, it will be inferred from the file extension.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'avro'.
        """
        if format is None:
            self._format = None
            # No format was provided, so we validate that self._implicit_format is valid
            self._verify_valid_format(build_file_format(self._implicit_format_string))
        else:
            # A format was provided
            format = build_file_format(format)
            self._verify_valid_format(format)
            self._format = format

    @property
    def allow_fragments(self) -> bool:
        """
        bool: Whether to allow fragments in the output.
        """
        return True

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the LocalFileDestination

        Args:
            format (FileFormat): The format to verify
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise DestinationConfigurationError(
                ErrorCode.DECE13, type(format), valid_output_formats
            )

    @property
    def _stream_require_ec(self) -> bool:
        """
        Indicates whether the stream method requires an execution context.

        Returns:
            bool: True if the stream method requires an execution context,
            False otherwise.
        """
        return True

    def stream(self, working_dir: str, *results):
        logger.debug(
            f"Beginning streaming for {self.__class__.__name__} with results"
            f" '{results}'"
        )
        intermediate_files = self.chunk(working_dir, *results)
        # We add logic here instead of implementing a write method because there
        # would be a missmatch in the expected signature of the write method and the
        # signature it would need, due to the special catalog logic.
        _write_results_in_final_files(
            results, intermediate_files, self, working_dir, self._ec.request
        )

    def chunk(self, working_dir: str, *results):
        logger.debug(
            f"Beginning chunking for {self.__class__.__name__} with results '{results}'"
        )
        intermediate_files = _store_results_in_intermediate_files(
            results, self, working_dir
        )
        logger.debug(
            f"Chunking completed for {self.__class__.__name__} with results '"
            f"{results}' and intermediate files '{intermediate_files}'"
        )
        return intermediate_files

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.path})"


class S3Destination(DestinationPlugin):
    """
    Class for managing the configuration of S3-file-based data outputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension.
        uri (str | list[str]): The URI of the files with format: 's3://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (S3Credentials): The credentials required to access the S3 bucket.
    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the S3Destination.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | list[str],
        credentials: S3Credentials,
        format: str | FileFormat = None,
        region: str = None,
        catalog: AWSGlue = None,
    ):
        """
        Initializes the S3Destination with the given URI and the credentials required to
            access the S3 bucket, and optionally a format and date and
            time after which the files were modified.

        Args:
            uri (str | list[str]): The URI of the files with format:
                's3://path/to/files'. It can be a single URI or a list of URIs.
            credentials (S3Credentials): The credentials required to access the
                S3 bucket. Must be a S3Credentials object.
            format (str | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'jsonl'.
            region (str, optional): The region where the S3 bucket is located. If not
                provided, the default AWS region will be used.

        Raises:
            OutputConfigurationError
            FormatConfigurationError
        """
        self.uri = uri
        self.format = format
        self.credentials = credentials
        self.region = region
        self.catalog = catalog

    @property
    def uri(self) -> str | list[str]:
        """
        str | list[str]: The URI of the files with format: 's3://path/to/files'.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str | list[str]):
        """
        Sets the URI of the files with format: 's3://path/to/files'.

        Args:
            uri (str | list[str]): The URI of the files with format:
                's3://path/to/files'. It can be a single URI or a list of URIs.
        """
        self._uri = uri
        if isinstance(uri, str):
            self._uri_list = [uri]
        elif isinstance(uri, list):
            self._uri_list = uri
            if not all(isinstance(single_uri, str) for single_uri in self._uri_list):
                raise DestinationConfigurationError(ErrorCode.DECE17, type(uri))
        else:
            raise DestinationConfigurationError(ErrorCode.DECE17, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme.lower() != S3_SCHEME:
                raise DestinationConfigurationError(
                    ErrorCode.DECE12,
                    parsed_uri.scheme,
                    S3_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

    @property
    def region(self) -> str | None:
        """
        str: The region where the S3 bucket is located.
        """
        return self._region

    @region.setter
    def region(self, region: str | None):
        """
        Sets the region where the S3 bucket is located.

        Args:
            region (str): The region where the S3 bucket is located.
        """
        if region:
            if not isinstance(region, str):
                raise DestinationConfigurationError(ErrorCode.DECE18, type(region))
            supported_regions = [element.value for element in SupportedAWSS3Regions]
            if region not in supported_regions:
                logger.warning(
                    "The 'region' parameter for the S3FileOutput object has value "
                    f"'{region}', which is not recognized in our current list of AWS "
                    f"regions: {supported_regions}. This could indicate a typo in the "
                    "region provided, but it could also occur because you are "
                    "using a recently created AWS region or a private AWS region. "
                    "You can continue using this region if you are sure it is available"
                    " for your AWS account, but if it isn't it will cause an error "
                    "during runtime."
                )
            self._region = region
        else:
            self._region = None

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file.
        """
        return self._format or build_file_format(self._implicit_format)

    @format.setter
    def format(self, format: str | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str | FileFormat): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'avro'.
        """
        if format is None:
            self._format = None
            # No format was provided, so we validate that self._implicit_format is valid
            self._verify_valid_format(build_file_format(self._implicit_format))
        else:
            format = build_file_format(format)
            self._verify_valid_format(format)
            self._format = format
            if (
                hasattr(self, "_catalog")
                and self._catalog is not None
                and not isinstance(self._format, ParquetFormat)
            ):
                raise DestinationConfigurationError(
                    ErrorCode.DECE37, ParquetFormat, self._format
                )

    @property
    def catalog(self) -> AWSGlue:
        """
        Catalog: The catalog to store the data in.
        """
        return self._catalog

    @catalog.setter
    def catalog(self, catalog: AWSGlue):
        """
        Sets the catalog to store the data in.

        Args:
            catalog (AWSGlue): The catalog to store the data in.
        """
        if catalog is None:
            self._catalog = None
        else:
            catalog = build_catalog(catalog)
            if hasattr(self, "_format") and not isinstance(self.format, ParquetFormat):
                raise DestinationConfigurationError(
                    ErrorCode.DECE37, ParquetFormat, self.format
                )
            self._catalog = catalog

    @property
    def allow_fragments(self) -> bool:
        """
        bool: Whether to allow fragments in the output.
        """
        return True

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the S3Destination

        Args:
            format (FileFormat): The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise DestinationConfigurationError(
                ErrorCode.DECE13, type(format), valid_output_formats
            )

    @property
    def credentials(self) -> S3Credentials | S3AccessKeyCredentials:
        """
        S3Credentials: The credentials required to access the S3 bucket.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: S3Credentials):
        """
        Sets the credentials required to access the S3 bucket.

        Args:
            credentials (S3Credentials): The credentials required to access the
                S3 bucket. Must be a S3Credentials object.
        """
        credentials = build_credentials(credentials)
        if not (isinstance(credentials, S3Credentials)):
            raise DestinationConfigurationError(ErrorCode.DECE19, type(credentials))
        self._credentials = credentials

    @property
    def _stream_require_ec(self) -> bool:
        """
        Indicates whether the stream method requires an execution context.

        Returns:
            bool: True if the stream method requires an execution context,
            False otherwise.
        """
        return True

    def stream(self, working_dir: str, *results):
        logger.debug(
            f"Beginning streaming for {self.__class__.__name__} with results"
            f" '{results}'"
        )
        intermediate_files = self.chunk(working_dir, *results)
        # We add logic here instead of implementing a write method because there
        # would be a missmatch in the expected signature of the write method and the
        # signature it would need, due to the special catalog logic.
        obtain_and_set_s3_credentials(self.credentials),
        set_s3_region(self.region),
        _write_results_in_final_files(
            results, intermediate_files, self, working_dir, self._ec.request
        )

    def chunk(self, working_dir: str, *results):
        logger.debug(
            f"Beginning chunking for {self.__class__.__name__} with results '{results}'"
        )
        intermediate_files = _store_results_in_intermediate_files(
            results, self, working_dir
        )
        logger.debug(
            f"Chunking completed for {self.__class__.__name__} with results '"
            f"{results}' and intermediate files '{intermediate_files}'"
        )
        return intermediate_files

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uri})"


def _write_results_in_final_files(
    results: tuple[pl.LazyFrame | None | list[pl.LazyFrame | None]],
    intermediate_files: list[str | None | list[str | None]],
    destination: (
        AzureDestination | GCSDestination | LocalFileDestination | S3Destination
    ),
    output_folder: str,
    request: InputYaml,
):
    logger.info(f"Storing results in file destination '{destination}'")

    destination_path = _obtain_destination_path_list(destination)

    for number, (result, destination_file, intermediate_file) in enumerate(
        zip(results, destination_path, intermediate_files)
    ):
        if result is None:
            logger.warning(f"Result is None. No data stored in '{destination_file}.")
        else:
            logger.debug(
                f"Storing result in destination file '{destination_file}' "
                f"from intermediate file '{intermediate_file}'"
            )
            if isinstance(intermediate_file, list):
                # If the intermediate file is a list, it means that the result is a
                # list of LazyFrames, so we need to store each LazyFrame in its own
                # final file.
                logger.debug(
                    "Intermediate file is a list, verifying if the "
                    "destination allows fragments."
                )
                _verify_fragment_destination(destination, destination_file)
                logger.debug("Verification completed, generating fragments.")
                destination_files = [
                    destination_file.replace(FRAGMENT_INDEX_PLACEHOLDER, str(index))
                    for index in range(len(intermediate_file))
                ]
                logger.debug(f"Destination files generated: {destination_files}")
            else:
                result = [result]
                destination_files = [destination_file]
                intermediate_file = [intermediate_file]

            # At this point, we should always have a list called result with all the
            # results (either a single one for a single LazyFrame or a list of
            # LazyFrames for a fragmented destination). The same should happen for
            # intermediate_files and destination_files

            # Destination files might be modified, for example if there are placeholders
            # to be replaced. The list below will store the final name of each
            # destination file.
            logger.debug(
                f"Looping over results '{result}' with destination files"
                f" '{destination_files}' and intermediate files"
                f" '{intermediate_files}'"
            )
            resolved_destination_files = []
            for (
                individual_result,
                individual_intermediate_file,
                individual_destination_file,
            ) in zip(result, intermediate_file, destination_files):
                if individual_result is None:
                    logger.warning(
                        "Individual result is None. No data stored:"
                        f" '{individual_intermediate_file}' -"
                        f" '{individual_destination_file}."
                    )
                    resolved_destination_file = None
                else:
                    logger.debug(
                        "Individual result is not None, storing it in "
                        f"{individual_destination_file} from intermediate "
                        f"file {individual_intermediate_file}"
                    )
                    resolved_destination_file = _store_result_using_transporter(
                        individual_destination_file,
                        individual_intermediate_file,
                        destination,
                        output_folder,
                        request,
                    )
                resolved_destination_files.append(resolved_destination_file)
            if hasattr(destination, "catalog") and destination.catalog is not None:
                logger.info("Storing file(s) in catalog")
                catalog = destination.catalog
                _store_file_in_catalog(
                    catalog,
                    resolved_destination_files,
                    catalog.tables[number],
                    result,
                    number,
                    destination,
                )
    logger.info(f"Results stored in file destination {destination} successfully.")


def _store_results_in_intermediate_files(
    results: tuple[pl.LazyFrame | None | list[pl.LazyFrame | None]],
    destination: (
        AzureDestination | GCSDestination | LocalFileDestination | S3Destination
    ),
    output_folder: str,
) -> list[str | None | list[str | None]]:
    logger.info(
        f"Storing results '{results}' in file destination '{destination}' "
        f"using output folder '{output_folder}'"
    )

    destination_path = _obtain_destination_path_list(destination)

    if len(results) != len(destination_path):
        logger.error(
            "The number of destination files does not match the number of results."
        )
        logger.error(f"Destination files: '{destination_path}'")
        logger.error(f"Results: '{results}'")
        logger.error(f"Number or results: {len(results)}")
        raise TypeError(
            "The number of destination tables does not match the number of results."
        )

    logger.debug(
        f"Pairing destination path '{destination_path}' with results '{results}'"
    )
    chunk_intermediate_files = []
    for result in results:
        if result is None:
            logger.warning("Result is None. No data stored in intermediate file.")
            intermediate_files = [None]
        else:
            logger.debug("Storing result in intermediate file.")
            intermediate_files, result = _pair_result_with_intermediate_file(
                output_folder, result, destination.format
            )
            if result is None:
                logger.warning("Individual result is None. No data stored.")
            elif isinstance(result, pl.LazyFrame):
                _sink_result_to_intermediate_file(
                    result,
                    intermediate_files,
                    destination.format,
                )
            else:
                for (
                    individual_result,
                    individual_intermediate_file,
                ) in zip(result, intermediate_files):
                    if individual_result is None:
                        logger.warning("Individual result is None. No data stored.")
                    else:
                        _sink_result_to_intermediate_file(
                            individual_result,
                            individual_intermediate_file,
                            destination.format,
                        )
        chunk_intermediate_files.append(intermediate_files)
    logger.info(
        f"Results {results} stored in intermediate files {chunk_intermediate_files}"
    )
    return chunk_intermediate_files


def _obtain_destination_path_list(destination):
    if isinstance(destination, LocalFileDestination):
        destination_path = destination.path
    elif isinstance(destination, (AzureDestination, GCSDestination, S3Destination)):
        destination_path = destination.uri
    else:
        logger.error(f"Storing results in destination '{destination}' not supported.")
        raise TypeError(
            f"Storing results in destination '{destination}' not supported."
        )

    if isinstance(destination_path, str):
        return [destination_path]
    elif isinstance(destination_path, list):
        return destination_path
    else:
        logger.error(
            "Parameter 'path' must be a string or a list of strings, got"
            f" '{type(destination_path)}' instead"
        )
        raise TypeError(
            "Parameter 'path' must be a string or a list of strings, got"
            f" '{type(destination_path)}' instead"
        )


def _pair_result_with_intermediate_file(
    output_folder: str,
    result: pl.LazyFrame | list[pl.LazyFrame | None] | None,
    format: AvroFormat | CSVFormat | ParquetFormat | NDJSONFormat | FileFormat,
):
    format_extension = "." + INPUT_FORMAT_CLASS_TO_EXTENSION[type(format)]
    if result is None:
        logger.warning("Result is None. No data will be stored for this TableFrame.")
        intermediate_files = None
        result = None
    elif isinstance(result, pl.LazyFrame):
        intermediate_files = os.path.join(
            output_folder, f"intermediate_{uuid.uuid4()}{format_extension}"
        )
        result = result
    elif isinstance(result, list):
        intermediate_files = []
        for fragment_number in range(len(result)):
            intermediate_file = os.path.join(
                output_folder,
                f"intermediate_{uuid.uuid4()}_with_fragment_"
                f"{fragment_number}{format_extension}",
            )
            intermediate_files.append(intermediate_file)
    else:
        logger.error(
            "The result of a registered function must be a TableFrame,"
            f" None or a list of TableFrames, got '{type(result)}' instead"
        )
        raise TypeError(
            "The result of a registered function must be a TableFrame,"
            f" None or a list of TableFrames, got '{type(result)}' instead"
        )
    return intermediate_files, result


INPUT_FORMAT_CLASS_TO_EXTENSION = {
    AvroFormat: AVRO_EXTENSION,
    CSVFormat: CSV_EXTENSION,
    NDJSONFormat: NDJSON_EXTENSION,
    ParquetFormat: PARQUET_EXTENSION,
}


def _store_result_using_transporter(
    destination_path: str,
    intermediate_file: str,
    destination: (
        AzureDestination | GCSDestination | LocalFileDestination | S3Destination
    ),
    output_folder: str,
    request: InputYaml,
) -> str:
    logger.info(
        f"Storing result in destination file '{destination_path}' from intermediate"
        f" file '{intermediate_file}'"
    )
    destination_path = _replace_placeholders_in_path(destination_path, request)
    transporter_origin_file = convert_path_to_uri(intermediate_file)
    origin = TransporterLocalFile(transporter_origin_file)
    logger.debug(f"Origin file for the transporter: {origin}")
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
    elif isinstance(destination, GCSDestination):
        destination = TransporterGCS(
            destination_path,
            service_account_key=TransporterEnv(SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON),
        )
    elif isinstance(destination, LocalFileDestination):
        destination = TransporterLocalFile(convert_path_to_uri(destination_path))
    else:
        logger.error(f"Storing results in destination '{destination}' not supported.")
        raise TypeError(
            f"Storing results in destination '{destination}' not supported."
        )
    logger.debug(f"Destination file for the transporter: {destination}")
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
        [binary] + arguments.split(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
    )
    if subprocess_result.returncode != 0:
        logger.error(f"Error exporting file: {subprocess_result.stderr}")
        raise Exception(f"Error exporting file: {subprocess_result.stderr}")
    else:
        logger.debug("Exported file successfully")

    return destination_path


def _sink_result_to_intermediate_file(
    result: pl.LazyFrame,
    intermediate_file: str,
    format: AvroFormat | CSVFormat | ParquetFormat | NDJSONFormat | FileFormat,
):
    logger.debug(f"Storing result in intermediate file '{intermediate_file}'")
    _store_polars_lf_in_file(result, intermediate_file, format)
    logger.debug(
        f"Result stored in intermediate file '{intermediate_file}' successfully"
    )
    return


def _store_file_in_catalog(
    catalog: AWSGlue,
    path_to_table_files: list[str],
    destination_table: str,
    lf_list: list[pl.LazyFrame],
    index: int,
    destination: S3Destination,
):

    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError

    logger.debug(f"Storing file in catalog '{catalog}'")
    definition = catalog.definition
    logger.debug(f"Catalog definition: {definition}")
    definition = _recursively_evaluate_secret(definition)
    if isinstance(destination, S3Destination):
        definition = _add_s3_credentials_to_catalog_definition(
            definition, catalog, destination
        )
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


def _add_s3_credentials_to_catalog_definition(
    definition: dict, catalog: AWSGlue, destination: S3Destination
):
    destination_credentials: S3AccessKeyCredentials = destination.credentials
    logger.debug("Adding S3 and AWS Glue credentials to catalog definition if missing")
    # Set S3 credentials in the catalog definition if not already set
    definition[catalog.S3_ACCESS_KEY_ID] = (
        definition.get(catalog.S3_ACCESS_KEY_ID)
        or destination_credentials.aws_access_key_id.secret_value
    )
    definition[catalog.S3_REGION] = (
        definition.get(catalog.S3_REGION) or destination.region
    )
    definition[catalog.S3_SECRET_ACCESS_KEY] = (
        definition.get(catalog.S3_SECRET_ACCESS_KEY)
        or destination_credentials.aws_secret_access_key.secret_value
    )
    # Set AWS Glue credentials in the catalog definition if not already set
    definition[catalog.AWS_GLUE_ACCESS_KEY_ID] = (
        definition.get(catalog.AWS_GLUE_ACCESS_KEY_ID)
        or destination_credentials.aws_access_key_id.secret_value
    )
    definition[catalog.AWS_GLUE_REGION] = (
        definition.get(catalog.AWS_GLUE_REGION) or destination.region
    )
    definition[catalog.AWS_GLUE_SECRET_ACCESS_KEY] = (
        definition.get(catalog.AWS_GLUE_SECRET_ACCESS_KEY)
        or destination_credentials.aws_secret_access_key.secret_value
    )
    return definition


def _verify_fragment_destination(
    destination: (
        AzureDestination | GCSDestination | LocalFileDestination | S3Destination
    ),
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


EXECUTION_ID_PLACEHOLDER = "$EXECUTION_ID"
EXPORT_TIMESTAMP_PLACEHOLDER = "$EXPORT_TIMESTAMP"
FUNCTION_RUN_ID_PLACEHOLDER = "$FUNCTION_RUN_ID"
SCHEDULER_TIMESTAMP_PLACEHOLDER = "$SCHEDULER_TIMESTAMP"
TRIGGER_TIMESTAMP_PLACEHOLDER = "$TRIGGER_TIMESTAMP"
TRANSACTION_ID_PLACEHOLDER = "$TRANSACTION_ID"


def _replace_placeholders_in_path(path: str, request: InputYaml) -> str:
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


FORMAT_TO_POLARS_WRITE_FUNCTION = {
    CSV_EXTENSION: pl.LazyFrame.sink_csv,
    NDJSON_EXTENSION: pl.LazyFrame.sink_ndjson,
    PARQUET_EXTENSION: pl.LazyFrame.sink_parquet,
    TABSDATA_EXTENSION: pl.LazyFrame.sink_parquet,
}


def _store_polars_lf_in_file(
    result: pl.LazyFrame,
    result_file: str | os.PathLike,
    format: AvroFormat | FileFormat | CSVFormat | ParquetFormat | NDJSONFormat = None,
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
    elif isinstance(format, AvroFormat):
        logger.debug(f"Writing result to file '{result_file}' using AVRO format")
        _store_polars_lf_to_avro(result, result_file, format)
        return
    else:
        logger.error(
            f"Writing output file with ending {file_ending} not supported "
            "with api Polars, as this is not a recognized file extension"
        )
        raise ValueError(
            f"Writing output file with ending {file_ending} not supported with "
            "api Polars, as this is not a recognized file extension"
        )


def _store_polars_lf_to_avro(
    result: pl.LazyFrame, result_file: str, format: AvroFormat
):
    """
    Stores a Polars LazyFrame in an AVRO file.

    Args:
        result (pl.LazyFrame): The Polars LazyFrame to store.
        result_file (str): The path to the AVRO file.
        format (AvroFormat): The AVRO format to use for storing the file.
    """

    import pyarrow.parquet as pq
    from fastavro import reader, writer

    logger.debug(f"Storing result in AVRO file '{result_file}'")
    if not isinstance(format, AvroFormat):
        raise TypeError("The format must be an instance of AVROFormat.")
    working_folder = os.path.dirname(result_file)
    aux_file = os.path.join(working_folder, f"aux_{uuid.uuid4().hex[:8]}.parquet")
    logger.debug(f"Storing result in auxiliary file '{aux_file}'")
    result.sink_parquet(aux_file)
    logger.debug(f"Result stored in auxiliary file '{aux_file}' successfully.")

    logger.debug("Obtaining the schema for the AVRO file")
    df = pl.scan_parquet(aux_file).limit(1).collect()
    file_to_get_schema = os.path.join(
        working_folder, f"schema_{uuid.uuid4().hex[:8]}.avro"
    )
    df.write_avro(file_to_get_schema)
    with open(file_to_get_schema, "rb") as f:
        # Create a reader to iterate over the file
        avro_reader = reader(f)
        schema = avro_reader.writer_schema
    logger.debug(f"Schema obtained for AVRO file: {schema}")

    chunk_size = format.chunk_size
    logger.debug(f"Chunk size for AVRO file: {chunk_size}")
    parquet_file = pq.ParquetFile(aux_file)
    with open(result_file, "wb") as out_file:
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            df = pl.from_arrow(batch)
            batch_dict = df.to_dicts()
            logger.debug(f"Writing batch of shape {df.shape} to file '{result_file}'")
            writer(out_file, schema, batch_dict)

    logger.debug(f"Result stored in AVRO file '{result_file}' successfully.")
