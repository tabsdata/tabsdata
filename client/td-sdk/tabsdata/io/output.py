#
# Copyright 2025 Tabs Data Inc.
#

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Literal
from urllib.parse import urlparse, urlunparse

from tabsdata.credentials import (
    AzureCredentials,
    S3Credentials,
    UserPasswordCredentials,
    build_credentials,
)
from tabsdata.exceptions import (
    ErrorCode,
    OutputConfigurationError,
)
from tabsdata.format import (
    CSVFormat,
    FileFormat,
    NDJSONFormat,
    ParquetFormat,
    build_file_format,
    get_implicit_format_from_list,
)
from tabsdata.io.constants import (
    AZURE_SCHEME,
    FILE_SCHEME,
    MARIADB_SCHEME,
    MYSQL_SCHEME,
    ORACLE_SCHEME,
    POSTGRES_SCHEMES,
    S3_SCHEME,
    URI_INDICATOR,
    SupportedAWSS3Regions,
)
from tabsdata.secret import _recursively_load_secret

logger = logging.getLogger(__name__)


class OutputIdentifiers(Enum):
    """
    Enum for the identifiers of the different types of data outputs.
    """

    AZURE = "azure-output"
    LOCALFILE = "localfile-output"
    MARIADB = "mariadb-output"
    MYSQL = "mysql-output"
    ORACLE = "oracle-output"
    POSTGRES = "postgres-output"
    S3 = "s3-output"
    TABLE = "table-output"


class IfTableExistsStrategy(Enum):
    """
    Enum for the strategies to follow when the table already exists.
    """

    APPEND = "append"
    REPLACE = "replace"


class Catalog:

    IDENTIFIER = "catalog"

    ALLOW_INCOMPATIBLE_CHANGES_KEY = "allow_incompatible_changes"
    DEFINITION_KEY = "definition"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    TABLES_KEY = "tables"

    def __init__(
        self,
        definition: dict,
        tables: str | List[str],
        allow_incompatible_changes: bool = False,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        self.definition = definition
        self.tables = tables
        self.if_table_exists = if_table_exists
        self.allow_incompatible_changes = allow_incompatible_changes

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE33, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    @property
    def definition(self) -> dict:
        return self._definition

    @definition.setter
    def definition(self, definition: dict):
        if not isinstance(definition, dict):
            raise OutputConfigurationError(ErrorCode.OCE30, type(definition))
        self._definition = _recursively_load_secret(definition)

    @property
    def tables(self) -> List[str]:
        return self._tables

    @tables.setter
    def tables(self, tables: str | List[str]):
        if isinstance(tables, str):
            self._tables = [tables]
        elif isinstance(tables, list):
            if not all(isinstance(single_table, str) for single_table in tables):
                raise OutputConfigurationError(ErrorCode.OCE31)
            self._tables = tables
        else:
            raise OutputConfigurationError(ErrorCode.OCE32, type(tables))

    def to_dict(self) -> dict:
        # TODO: Right now, Secrets are stored as a secret object, and we rely on the
        #   json serializer to turn them into dictionaries when bundling. Once using
        #   description jsons becomes more usual, this will have to be revisited.
        return {
            self.IDENTIFIER: {
                self.TABLES_KEY: self.tables,
                self.DEFINITION_KEY: self.definition,
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
                self.ALLOW_INCOMPATIBLE_CHANGES_KEY: self.allow_incompatible_changes,
            }
        }

    def __eq__(self, other):
        if not isinstance(other, Catalog):
            return False
        return self.to_dict() == other.to_dict()


def build_catalog(catalog) -> Catalog:
    """
    Builds a Catalog object from a dictionary or a Catalog object.

    Args:
        catalog (dict | Catalog): The dictionary or Catalog object to build the Catalog
            object from.

    Returns:
        Catalog: The Catalog object built from the dictionary or Catalog object.
    """
    if isinstance(catalog, Catalog):
        return catalog
    elif not isinstance(catalog, dict):
        raise OutputConfigurationError(ErrorCode.OCE34, type(catalog))
    elif len(catalog) != 1 or next(iter(catalog)) != Catalog.IDENTIFIER:
        raise OutputConfigurationError(
            ErrorCode.OCE35, Catalog.IDENTIFIER, list(catalog.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(catalog.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise OutputConfigurationError(ErrorCode.OCE36, identifier, type(configuration))
    return Catalog(**configuration)


class Output(ABC):
    """
    Abstract base class for managing data output configurations.
    """

    @abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the Output object to a dictionary with all
            the relevant information.

        Returns:
            dict: A dictionary with the relevant information of the Output
                object.
        """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Output):
            return False
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Output.

        Returns:
            str: A string representation of the Output.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


class AzureDestination(Output):
    """
    Class for managing the configuration of Azure-file-based data outputs.

    Attributes:
        format (FileFormat): The format of the file to be created. If not provided,
            it will be inferred from the file extension.
        uri (str | List[str]): The URI of the files with format: 'az://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (AzureCredentials): The credentials required to access Azure.

    Methods:
        to_dict(): Converts the AzureDestination object to a dictionary.
    """

    IDENTIFIER = OutputIdentifiers.AZURE.value

    CATALOG_KEY = "catalog"
    CREDENTIALS_KEY = "credentials"
    FORMAT_KEY = "format"
    URI_KEY = "uri"

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the AzureDestination.
        """

        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | List[str],
        credentials: dict | AzureCredentials,
        format: str | dict | FileFormat = None,
        catalog: dict | Catalog = None,
    ):
        """
        Initializes the AzureDestination with the given URI and the credentials
            required to access Azure; and optionally a format.

        Args:
            uri (str | List[str]): The URI of the files to export with format:
                'az://path/to/files'. It can be a single URI or a list of URIs.
            credentials (dict | AzureCredentials): The credentials required to access
                Azure. Can be a dictionary or a AzureCredentials object.
            format (str | dict | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'jsonl'.

        Raises:
            OutputConfigurationError
            FormatConfigurationError
        """
        self.uri = uri
        self.format = format
        self.credentials = credentials
        self.catalog = catalog

    @property
    def uri(self) -> str | List[str]:
        """
        str | List[str]: The URI of the files with format: 'az://path/to/files'.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str | List[str]):
        """
        Sets the URI of the files with format: 'az://path/to/files'.

        Args:
            uri (str | List[str]): The URI of the files with format:
                'az://path/to/files'. It can be a single URI or a list of URIs.
        """
        self._uri = uri
        if isinstance(uri, str):
            self._uri_list = [uri]
        elif isinstance(uri, list):
            self._uri_list = uri
            if not all(isinstance(single_uri, str) for single_uri in self._uri_list):
                raise OutputConfigurationError(ErrorCode.OCE14, type(uri))
        else:
            raise OutputConfigurationError(ErrorCode.OCE14, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme != AZURE_SCHEME:
                raise OutputConfigurationError(
                    ErrorCode.OCE15,
                    parsed_uri.scheme,
                    AZURE_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

    def to_dict(self) -> dict:
        """
        Converts the AzureDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the AzureDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.FORMAT_KEY: self.format.to_dict(),
                self.URI_KEY: self._uri_list,
                self.CREDENTIALS_KEY: self.credentials.to_dict(),
                self.CATALOG_KEY: self.catalog.to_dict() if self.catalog else None,
            }
        }

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file extension of the URI.
        """
        return self._format or build_file_format(self._implicit_format)

    @format.setter
    def format(self, format: str | dict | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str | dict | FileFormat): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'jsonl'.
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
                raise OutputConfigurationError(
                    ErrorCode.OCE37, ParquetFormat, self._format
                )

    @property
    def catalog(self) -> Catalog:
        """
        Catalog: The catalog to store the data in.
        """
        return self._catalog

    @catalog.setter
    def catalog(self, catalog: dict | Catalog):
        """
        Sets the catalog to store the data in.

        Args:
            catalog (dict | Catalog): The catalog to store the data in.
        """
        if catalog is None:
            self._catalog = None
        else:
            catalog = build_catalog(catalog)
            if hasattr(self, "_format") and not isinstance(self.format, ParquetFormat):
                raise OutputConfigurationError(
                    ErrorCode.OCE37, ParquetFormat, self.format
                )
            self._catalog = catalog

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the AzureDestination

        Args:
            format (FileFormat): The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise OutputConfigurationError(
                ErrorCode.OCE13, type(format), valid_output_formats
            )

    @property
    def credentials(self) -> AzureCredentials:
        """
        AzureCredentials: The credentials required to access Azure.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | AzureCredentials):
        """
        Sets the credentials required to access Azure.

        Args:
            credentials (dict | AzureCredentials): The credentials required to access
                Azure. Can be a dictionary or an AzureCredentials object.
        """
        credentials = build_credentials(credentials)
        if not (isinstance(credentials, AzureCredentials)):
            raise OutputConfigurationError(ErrorCode.OCE16, type(credentials))
        self._credentials = credentials


class LocalFileDestination(Output):
    IDENTIFIER = OutputIdentifiers.LOCALFILE.value

    CATALOG_KEY = "catalog"
    FORMAT_KEY = "format"
    PATH_KEY = "path"

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the LocalFileDestination.
        """

        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    def __init__(
        self,
        path: str | List[str],
        format: str | dict | FileFormat = None,
        catalog: dict | Catalog = None,
    ):
        """
        Initializes the LocalFileDestination with the given path; and optionally a
        format.

        Args:
            path (str | List[str]): The path where the files must be stored. It can be a
                single path or a list of paths.
            format (str | dict | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'jsonl'.

        Raises:
            OutputConfigurationError
            FormatConfigurationError
        """
        self.path = path
        self.format = format
        self.catalog = catalog

    @property
    def path(self) -> str | List[str]:
        """
        str | List[str]: The path or paths to store the files.
        """
        return self._path

    @path.setter
    def path(self, path: str | List[str]):
        """
        Sets the path or paths to store the files.

        Args:
            path (str | List[str]): The path or paths to store the files.
        """
        self._path = path
        if isinstance(path, str):
            self._path_list = [path]
        elif isinstance(path, list):
            self._path_list = path
            if not all(isinstance(single_path, str) for single_path in self._path_list):
                raise OutputConfigurationError(ErrorCode.OCE11, type(path))
        else:
            raise OutputConfigurationError(ErrorCode.OCE11, type(path))

        for individual_path in self._path_list:
            if URI_INDICATOR in individual_path:
                parsed_path = urlparse(individual_path)
                if parsed_path.scheme != FILE_SCHEME:
                    raise OutputConfigurationError(
                        ErrorCode.OCE12,
                        parsed_path.scheme,
                        FILE_SCHEME,
                        urlunparse(parsed_path),
                    )

        self._implicit_format_string = get_implicit_format_from_list(self._path_list)
        if hasattr(self, "_format") and self._format is None:
            # This check verifies that we are not in the __init__ function,
            # so we might have to check if the implicit format is valid or not.
            self._verify_valid_format(build_file_format(self._implicit_format_string))

    def to_dict(self) -> dict:
        """
        Converts the LocalFileDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the LocalFileDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.FORMAT_KEY: self.format.to_dict(),
                self.PATH_KEY: self._path_list,
                self.CATALOG_KEY: self.catalog.to_dict() if self.catalog else None,
            }
        }

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file or files. If not provided, it will be
            inferred from the file extension in the path.
        """
        return self._format or build_file_format(self._implicit_format_string)

    @format.setter
    def format(self, format: str | dict | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str): The format of the file. If not
                provided, it will be inferred from the file extension.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'log'.
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
            if (
                hasattr(self, "_catalog")
                and self._catalog is not None
                and not isinstance(self._format, ParquetFormat)
            ):
                raise OutputConfigurationError(
                    ErrorCode.OCE37, ParquetFormat, self._format
                )

    @property
    def catalog(self) -> Catalog:
        """
        Catalog: The catalog to store the data in.
        """
        return self._catalog

    @catalog.setter
    def catalog(self, catalog: dict | Catalog):
        """
        Sets the catalog to store the data in.

        Args:
            catalog (dict | Catalog): The catalog to store the data in.
        """
        if catalog is None:
            self._catalog = None
        else:
            catalog = build_catalog(catalog)
            if hasattr(self, "_format") and not isinstance(self.format, ParquetFormat):
                raise OutputConfigurationError(
                    ErrorCode.OCE37, ParquetFormat, self.format
                )
            self._catalog = catalog

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the LocalFileDestination

        Args:
            format (FileFormat): The format to verify
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise OutputConfigurationError(
                ErrorCode.OCE13, type(format), valid_output_formats
            )


class MariaDBDestination(Output):
    """
    Class for managing the configuration of MariaDB-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MariaDB database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.

    Methods:
        to_dict(): Converts the MariaDBDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.MARIADB.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the MariaDBDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MariaDB database. Can be a dictionary or a
                UserPasswordCredentials object.
            if_table_exists ({'append', 'replace'}, optional): The strategy to
                follow when the table already exists. Defaults to 'append'.
                - ‘replace’ will create a new database table, overwriting an existing
                one.
                - ‘append’ will append to an existing table.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE26, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def to_dict(self) -> dict:
        """
        Converts the MariaDBDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MariaDBDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not self._parsed_uri.scheme.startswith(MARIADB_SCHEME):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, MARIADB_SCHEME, self.uri
            )
        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise OutputConfigurationError(ErrorCode.OCE22, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the MariaDB
            database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the MariaDB database.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the MariaDB database. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE23, type(credentials))
            self._credentials = credentials


class MySQLDestination(Output):
    """
    Class for managing the configuration of MySQL-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MySQL database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.

    Methods:
        to_dict(): Converts the MySQLDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.MYSQL.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the MySQLDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MySQL database. Can be a dictionary or a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE27, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def to_dict(self) -> dict:
        """
        Converts the MySQLDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MySQLDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not self._parsed_uri.scheme.startswith(MYSQL_SCHEME):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, MYSQL_SCHEME, self.uri
            )
        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise OutputConfigurationError(ErrorCode.OCE8, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the MySQLDatabase.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the MySQLDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the MySQLDatabase. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE9, type(credentials))
            self._credentials = credentials


class OracleDestination(Output):
    """
    Class for managing the configuration of Oracle-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Oracle database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.

    Methods:
        to_dict(): Converts the OracleDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.ORACLE.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the OracleDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Oracle database. Can be a dictionary or a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE28, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def to_dict(self) -> dict:
        """
        Converts the OracleDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the OracleDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not self._parsed_uri.scheme.startswith(ORACLE_SCHEME):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, ORACLE_SCHEME, self.uri
            )
        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise OutputConfigurationError(ErrorCode.OCE24, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the Oracle
            database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the Oracle database.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the Oracle database. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE25, type(credentials))
            self._credentials = credentials


class PostgresDestination(Output):
    """
    Class for managing the configuration of Postgres-based data outputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Postgres database.
        destination_table (str | List[str]): The table(s) to create. If multiple tables
            are provided, they must be provided as a list.
        if_table_exists ({'append', 'replace'}): The strategy to
            follow when the table already exists.
            - ‘replace’ will create a new database table, overwriting an existing one.
            - ‘append’ will append to an existing table.
        uri (str): The URI of the database where the data is going to be stored.

    Methods:
        to_dict(): Converts the PostgresDestination object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.POSTGRES.value

    CREDENTIALS_KEY = "credentials"
    DESTINATION_TABLE_KEY = "destination_table"
    IF_TABLE_EXISTS_KEY = "if_table_exists"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        destination_table: List[str] | str,
        credentials: dict | UserPasswordCredentials = None,
        if_table_exists: Literal["append", "replace"] = "append",
    ):
        """
        Initializes the PostgresDestination with the given URI and destination table,
        and optionally connection credentials.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
            destination_table (List[str] | str): The tables to create. If multiple
                tables are provided, they must be provided as a list.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Postgres database. Can be a dictionary or a
                UserPasswordCredentials object.

        Raises:
            OutputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.destination_table = destination_table
        self.if_table_exists = if_table_exists

    @property
    def if_table_exists(self) -> Literal["append", "replace"]:
        """
        str: The strategy to follow when the table already exists.
        """
        return self._if_table_exists

    @if_table_exists.setter
    def if_table_exists(self, if_table_exists: Literal["append", "replace"]):
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
            raise OutputConfigurationError(
                ErrorCode.OCE29, valid_values, if_table_exists
            )
        self._if_table_exists = if_table_exists

    def to_dict(self) -> dict:
        """
        Converts the PostgresDestination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the PostgresDestination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.URI_KEY: self.uri,
                self.DESTINATION_TABLE_KEY: self.destination_table,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
                self.IF_TABLE_EXISTS_KEY: self.if_table_exists,
            }
        }

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is going to be stored.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is going to be stored.

        Args:
            uri (str): The URI of the database where the data is going to be stored.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if not any(
            [self._parsed_uri.scheme.startswith(scheme) for scheme in POSTGRES_SCHEMES]
        ):
            raise OutputConfigurationError(
                ErrorCode.OCE2, self._parsed_uri.scheme, POSTGRES_SCHEMES, self.uri
            )
        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

    @property
    def destination_table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._destination_table

    @destination_table.setter
    def destination_table(self, destination_table: List[str] | str):
        """
        Sets the table(s) to create.

        Args:
            destination_table (List[str] | str): The table(s) to create. If multiple
                tables are provided, they must be provided as a list.
        """
        if isinstance(destination_table, (list, str)):
            self._destination_table = destination_table
        else:
            raise OutputConfigurationError(ErrorCode.OCE20, type(destination_table))

    @property
    def credentials(self) -> UserPasswordCredentials:
        """
        UserPasswordCredentials: The credentials required to access the
            Postgres database.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the PostgresDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the PostgresDatabase. Can be a
                UserPasswordCredentials object, a dictionary or None if no
                credentials are needed.
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise OutputConfigurationError(ErrorCode.OCE21, type(credentials))
            self._credentials = credentials


class S3Destination(Output):
    """
    Class for managing the configuration of S3-file-based data outputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension.
        uri (str | List[str]): The URI of the files with format: 's3://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (S3Credentials): The credentials required to access the S3 bucket.

    Methods:
        to_dict(): Converts the S3Destination object to a dictionary.
    """

    IDENTIFIER = OutputIdentifiers.S3.value

    CATALOG_KEY = "catalog"
    CREDENTIALS_KEY = "credentials"
    FORMAT_KEY = "format"
    REGION_KEY = "region"
    URI_KEY = "uri"

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the S3Destination.
        """

        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | List[str],
        credentials: dict | S3Credentials,
        format: str | dict | FileFormat = None,
        region: str = None,
        catalog: dict | Catalog = None,
    ):
        """
        Initializes the S3Destination with the given URI and the credentials required to
            access the S3 bucket, and optionally a format and date and
            time after which the files were modified.

        Args:
            uri (str | List[str]): The URI of the files with format:
                's3://path/to/files'. It can be a single URI or a list of URIs.
            credentials (dict | S3Credentials): The credentials required to access the
                S3 bucket. Can be a dictionary or a S3Credentials object.
            format (str | dict | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
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
    def uri(self) -> str | List[str]:
        """
        str | List[str]: The URI of the files with format: 's3://path/to/files'.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str | List[str]):
        """
        Sets the URI of the files with format: 's3://path/to/files'.

        Args:
            uri (str | List[str]): The URI of the files with format:
                's3://path/to/files'. It can be a single URI or a list of URIs.
        """
        self._uri = uri
        if isinstance(uri, str):
            self._uri_list = [uri]
        elif isinstance(uri, list):
            self._uri_list = uri
            if not all(isinstance(single_uri, str) for single_uri in self._uri_list):
                raise OutputConfigurationError(ErrorCode.OCE17, type(uri))
        else:
            raise OutputConfigurationError(ErrorCode.OCE17, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme != S3_SCHEME:
                raise OutputConfigurationError(
                    ErrorCode.OCE12,
                    parsed_uri.scheme,
                    S3_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

    def to_dict(self) -> dict:
        """
        Converts the S3Destination object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the S3Destination
                object.
        """
        return {
            self.IDENTIFIER: {
                self.FORMAT_KEY: self.format.to_dict(),
                self.URI_KEY: self._uri_list,
                self.CREDENTIALS_KEY: self.credentials.to_dict(),
                self.REGION_KEY: self.region,
                self.CATALOG_KEY: self.catalog.to_dict() if self.catalog else None,
            }
        }

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
                raise OutputConfigurationError(ErrorCode.OCE18, type(region))
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
    def format(self, format: str | dict | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str | dict | FileFormat): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'log'.
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
                raise OutputConfigurationError(
                    ErrorCode.OCE37, ParquetFormat, self._format
                )

    @property
    def catalog(self) -> Catalog:
        """
        Catalog: The catalog to store the data in.
        """
        return self._catalog

    @catalog.setter
    def catalog(self, catalog: dict | Catalog):
        """
        Sets the catalog to store the data in.

        Args:
            catalog (dict | Catalog): The catalog to store the data in.
        """
        if catalog is None:
            self._catalog = None
        else:
            catalog = build_catalog(catalog)
            if hasattr(self, "_format") and not isinstance(self.format, ParquetFormat):
                raise OutputConfigurationError(
                    ErrorCode.OCE37, ParquetFormat, self.format
                )
            self._catalog = catalog

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the S3Destination

        Args:
            format (FileFormat): The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise OutputConfigurationError(
                ErrorCode.OCE13, type(format), valid_output_formats
            )

    @property
    def credentials(self) -> S3Credentials:
        """
        S3Credentials: The credentials required to access the S3 bucket.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | S3Credentials):
        """
        Sets the credentials required to access the S3 bucket.

        Args:
            credentials (dict | S3Credentials): The credentials required to access the
                S3 bucket. Can be a dictionary or a S3Credentials object.
        """
        credentials = build_credentials(credentials)
        if not (isinstance(credentials, S3Credentials)):
            raise OutputConfigurationError(ErrorCode.OCE19, type(credentials))
        self._credentials = credentials


class TableOutput(Output):
    """
    Class for managing the configuration of table-based data outputs.

    Attributes:
        table (str | List[str]): The table(s) to create. If multiple tables are
            provided, they must be provided as a list.

    Methods:
        to_dict(): Converts the TableOutput object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.TABLE.value

    TABLE_KEY = "table"

    def __init__(self, table: str | List[str]):
        """
        Initializes the TableOutput with the given table(s) to create.

        Args:
            table (str | List[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._table

    @table.setter
    def table(self, table: str | List[str]):
        """
        Sets the table(s) to create.

        Args:
            table (str | List[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self._table = table
        self._table_list = table if isinstance(table, list) else [table]
        for single_table in self._table_list:
            if not isinstance(single_table, str):
                raise OutputConfigurationError(
                    ErrorCode.OCE10, single_table, type(single_table)
                )

    def to_dict(self) -> dict:
        """
        Converts the TableOutput object to a dictionary with all the relevant
        information.
        """
        return {self.IDENTIFIER: {self.TABLE_KEY: self._table_list}}


# TODO: Explore unifying the build_input and build_output data into a single
#   function, make them use a common codebase or even create a BuildIO class to
#   encapsulate both of them. Waiting to see the development of both data to
#   decide.
#   https://tabsdata.atlassian.net/browse/TAB-47
def build_output(
    output: dict | Output | None,
) -> (
    Output
    | AzureDestination
    | LocalFileDestination
    | S3Destination
    | MySQLDestination
    | TableOutput
    | None
):
    """
    Builds an Output object.

    Args:
        output (dict | Output | None): A dictionary with the output information,
            or an Output object.

    Returns:
        Output: A Output object built from the output. That can be an Output object,
            or None if nothing was provided.

    Raises:
        OutputConfigurationError
    """
    if not output:
        return None
    elif isinstance(output, Output):
        return output
    elif isinstance(output, dict):
        return build_output_from_dict(output)
    else:
        raise OutputConfigurationError(ErrorCode.OCE7, type(output))


def build_output_from_dict(
    output: dict,
) -> (
    Output
    | AzureDestination
    | LocalFileDestination
    | S3Destination
    | MySQLDestination
    | TableOutput
    | None
):
    # The output dictionary must have exactly one key, which must be one of the
    # valid identifiers
    valid_identifiers = [element.value for element in OutputIdentifiers]
    if len(output) != 1 or next(iter(output)) not in valid_identifiers:
        raise OutputConfigurationError(
            ErrorCode.OCE3, valid_identifiers, list(output.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(output.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise OutputConfigurationError(ErrorCode.OCE4, identifier, type(configuration))
    existing_outputs = [
        MySQLDestination,
        TableOutput,
        LocalFileDestination,
        AzureDestination,
        S3Destination,
        PostgresDestination,
        MariaDBDestination,
        OracleDestination,
    ]
    for output_class in existing_outputs:
        if identifier == output_class.IDENTIFIER:
            return output_class(**configuration)
