#
# Copyright 2025 Tabs Data Inc.
#

import datetime
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List
from urllib.parse import urlparse, urlunparse

from tabsdata.credentials import (
    AzureCredentials,
    S3Credentials,
    UserPasswordCredentials,
    build_credentials,
)
from tabsdata.exceptions import (
    ErrorCode,
    InputConfigurationError,
)
from tabsdata.format import (
    CSVFormat,
    FileFormat,
    LogFormat,
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
from tabsdata.tableuri import build_table_uri_object

logger = logging.getLogger(__name__)


class InputIdentifiers(Enum):
    """
    Enum for the identifiers of the different types of data inputs.
    """

    AZURE = "azure-input"
    LOCALFILE = "localfile-input"
    MARIADB = "mariadb-input"
    MYSQL = "mysql-input"
    ORACLE = "oracle-input"
    POSTGRES = "postgres-input"
    S3 = "s3-input"
    TABLE = "table-input"


class Input(ABC):
    """
    Abstract base class for managing data input configurations.
    """

    @abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the Input object to a dictionary with all
            the relevant information.

        Returns:
            dict: A dictionary with the relevant information of the Input
                object.
        """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Input):
            return False
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


class AzureSource(Input):
    """
    Class for managing the configuration of Azure-file-based data inputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension of the data.
        uri (str | List[str]): The URI of the files with format: 'az://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (AzureCredentials): The credentials required to access Azure.
        initial_last_modified (str | datetime.datetime): If provided, only the files
            modified after this date and time will be considered.

    Methods:
        to_dict(): Converts the AzureSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.AZURE.value

    CREDENTIALS_KEY = "credentials"
    FORMAT_KEY = "format"
    LAST_MODIFIED_KEY = "initial_last_modified"
    URI_KEY = "uri"

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the AzureSource.
        """

        csv = CSVFormat
        ndjson = NDJSONFormat
        log = LogFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | List[str],
        credentials: dict | AzureCredentials,
        format: str | dict | FileFormat = None,
        initial_last_modified: str | datetime.datetime = None,
    ):
        """
        Initializes the AzureSource with the given URI and the credentials required to
            access Azure, and optionally a format and date and
            time after which the files were modified.

        Args:
            uri (str | List[str]): The URI of the files with format:
                'az://path/to/files'. It can be a single URI or a list of URIs.
            credentials (dict | AzureCredentials): The credentials required to access
                Azure. Can be a dictionary or a AzureCredentials object.
            format (str | dict | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'log'.
            initial_last_modified (str | datetime.datetime, optional): If provided,
                only the files modified after this date and time will be considered.
                The date and time can be provided as a string in
                [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) or as
                a datetime object. If no timezone is provided, UTC will be assumed.

        Raises:
            InputConfigurationError
            FormatConfigurationError
        """
        self.uri = uri
        self.format = format
        self.initial_last_modified = initial_last_modified
        self.credentials = credentials

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
                raise InputConfigurationError(ErrorCode.ICE28, type(uri))
        else:
            raise InputConfigurationError(ErrorCode.ICE28, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme != AZURE_SCHEME:
                raise InputConfigurationError(
                    ErrorCode.ICE29,
                    parsed_uri.scheme,
                    AZURE_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

    def to_dict(self) -> dict:
        """
        Converts the AzureSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the AzureSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.FORMAT_KEY: self.format.to_dict(),
                self.LAST_MODIFIED_KEY: self.initial_last_modified,
                self.URI_KEY: self._uri_list,
                self.CREDENTIALS_KEY: self.credentials.to_dict(),
            }
        }

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file extension of the data.
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

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the AzureSource

        Args:
            format (FileFormat): The format to verify.
        """
        valid_input_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_input_formats)):
            raise InputConfigurationError(
                ErrorCode.ICE4, type(format), valid_input_formats
            )

    @property
    def initial_last_modified(self) -> str:
        """
        str: The date and time after which the files were modified.
        """
        return (
            self._initial_last_modified.isoformat(timespec="microseconds")
            if self._initial_last_modified
            else None
        )

    @initial_last_modified.setter
    def initial_last_modified(self, initial_last_modified: str | datetime.datetime):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified (str | datetime.datetime): The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object. If no timezone is
                provided, UTC will be assumed.
        """
        if initial_last_modified:
            if isinstance(initial_last_modified, datetime.datetime):
                self._initial_last_modified = initial_last_modified
            else:
                try:
                    self._initial_last_modified = datetime.datetime.fromisoformat(
                        initial_last_modified
                    )
                except ValueError:
                    raise InputConfigurationError(ErrorCode.ICE5, initial_last_modified)
                except TypeError:
                    raise InputConfigurationError(
                        ErrorCode.ICE6, type(initial_last_modified)
                    )
        else:
            self._initial_last_modified = None

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
            raise InputConfigurationError(ErrorCode.ICE30, type(credentials))
        self._credentials = credentials


class LocalFileSource(Input):
    """
    Class for managing the configuration of local-file-based data inputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension of the data.
        path (str | List[str]): The path where the files can be found. It can be a
            single path or a list of paths.
        initial_last_modified (str | None): If not None, only the files modified after
            this date and time will be considered.

    Methods:
        to_dict(): Converts the LocalFileSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.LOCALFILE.value

    FORMAT_KEY = "format"
    LAST_MODIFIED_KEY = "initial_last_modified"
    PATH_KEY = "path"

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the LocalFileSource.
        """

        csv = CSVFormat
        ndjson = NDJSONFormat
        log = LogFormat
        parquet = ParquetFormat

    def __init__(
        self,
        path: str | List[str],
        format: str | dict | FileFormat = None,
        initial_last_modified: str | datetime.datetime = None,
    ):
        """
        Initializes the LocalFileSource with the given path, and optionally a format and
            a date and time after which the files were modified.

        Args:
            path (str | List[str]): The path where the files can be found. It can be a
                single path or a list of paths.
            format (str | dict | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format, a FileFormat object or a
                dictionary with the format as the 'type' key and any additional
                format-specific information. Currently supported formats are 'csv',
                'parquet', 'ndjson', 'jsonl' and 'log'.
            initial_last_modified (str | datetime.datetime, optional): If provided,
                only the files modified after this date and time will be considered.
                The date and time can be provided as a string in
                [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) or as
                a datetime object. If no timezone is provided, UTC will be assumed.

        Raises:
            InputConfigurationError
            FormatConfigurationError
        """
        self.path = path
        self.format = format
        self.initial_last_modified = initial_last_modified

    @property
    def path(self) -> str | List[str]:
        """
        str | List[str]: The path or paths to the files to load.
        """
        return self._path

    @path.setter
    def path(self, path: str | List[str]):
        """
        Sets the path or paths to the files to load.

        Args:
            path (str | List[str]): The path or paths to the files to load.
        """
        self._path = path
        if isinstance(path, str):
            self._path_list = [path]
        elif isinstance(path, list):
            self._path_list = path
            if not all(isinstance(single_path, str) for single_path in self._path_list):
                raise InputConfigurationError(ErrorCode.ICE13, type(path))
        else:
            raise InputConfigurationError(ErrorCode.ICE13, type(path))

        for individual_path in self._path_list:
            if URI_INDICATOR in individual_path:
                parsed_path = urlparse(individual_path)
                if parsed_path.scheme != FILE_SCHEME:
                    raise InputConfigurationError(
                        ErrorCode.ICE14,
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
        Converts the LocalFileSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the LocalFileSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.FORMAT_KEY: self.format.to_dict(),
                self.LAST_MODIFIED_KEY: self.initial_last_modified,
                self.PATH_KEY: self._path_list,
            }
        }

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file or files. If not provided, it will be
            inferred  from the file extension in the path.
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

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the LocalFileSource

        Args:
            format (FileFormat): The format to verify
        """
        valid_input_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_input_formats)):
            raise InputConfigurationError(
                ErrorCode.ICE4, type(format), valid_input_formats
            )

    @property
    def initial_last_modified(self) -> str:
        """
        str: The date and time after which the files were modified.
        """
        return (
            self._initial_last_modified.isoformat(timespec="microseconds")
            if self._initial_last_modified
            else None
        )

    @initial_last_modified.setter
    def initial_last_modified(self, initial_last_modified: str | datetime.datetime):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified (str | datetime.datetime): The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object. If no timezone is
                provided, UTC will be assumed.
        """
        if initial_last_modified:
            if isinstance(initial_last_modified, datetime.datetime):
                self._initial_last_modified = initial_last_modified
            else:
                try:
                    self._initial_last_modified = datetime.datetime.fromisoformat(
                        initial_last_modified
                    )
                except ValueError:
                    raise InputConfigurationError(ErrorCode.ICE5, initial_last_modified)
                except TypeError:
                    raise InputConfigurationError(
                        ErrorCode.ICE6, type(initial_last_modified)
                    )
        else:
            self._initial_last_modified = None


class MariaDBSource(Input):
    """
    Class for managing the configuration of MariaDB-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MariaDB database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.

    Methods:
        to_dict(): Converts the MariaDBSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.MARIADB.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the MariaDBSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MariaDB database. Can be a dictionary or a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme != MARIADB_SCHEME:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, MARIADB_SCHEME, self.uri
            )

    @property
    def initial_values(self) -> dict:
        """
        dict: The initial values for the parameters in the SQL queries.
        """
        return self._initial_values

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        """
        Sets the initial values for the parameters in the SQL queries.

        Args:
            initial_values (dict): The initial values for the parameters in the SQL
                queries.
        """
        if not initial_values:
            self._initial_values = {}
        elif not isinstance(initial_values, dict):
            raise InputConfigurationError(ErrorCode.ICE34, type(initial_values))
        else:
            self._initial_values = initial_values

    @property
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise InputConfigurationError(ErrorCode.ICE35, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE35, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            MariaDB. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access MariaDB.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access MariaDB. Can be a UserPasswordCredentials
                object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE36, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the MariaDBSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MariaDBSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class MySQLSource(Input):
    """
    Class for managing the configuration of MySQL-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            MySQL database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.

    Methods:
        to_dict(): Converts the MySQLSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.MYSQL.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the MySQLSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the MySQL database. Can be a dictionary or a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme != MYSQL_SCHEME:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, MYSQL_SCHEME, self.uri
            )

    @property
    def initial_values(self) -> dict:
        """
        dict: The initial values for the parameters in the SQL queries.
        """
        return self._initial_values

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        """
        Sets the initial values for the parameters in the SQL queries.

        Args:
            initial_values (dict): The initial values for the parameters in the SQL
                queries.
        """
        if not initial_values:
            self._initial_values = {}
        elif not isinstance(initial_values, dict):
            raise InputConfigurationError(ErrorCode.ICE12, type(initial_values))
        else:
            self._initial_values = initial_values

    @property
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise InputConfigurationError(ErrorCode.ICE19, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE19, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            MySQLDatabase. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the MySQLDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the MySQLDatabase. Can be a UserPasswordCredentials
                object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE22, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the MySQLSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the MySQLSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class OracleSource(Input):
    """
    Class for managing the configuration of Oracle-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Oracle database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.

    Methods:
        to_dict(): Converts the OracleSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.ORACLE.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the OracleSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Oracle database. Can be a dictionary or a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme != ORACLE_SCHEME:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, ORACLE_SCHEME, self.uri
            )

    @property
    def initial_values(self) -> dict:
        """
        dict: The initial values for the parameters in the SQL queries.
        """
        return self._initial_values

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        """
        Sets the initial values for the parameters in the SQL queries.

        Args:
            initial_values (dict): The initial values for the parameters in the SQL
                queries.
        """
        if not initial_values:
            self._initial_values = {}
        elif not isinstance(initial_values, dict):
            raise InputConfigurationError(ErrorCode.ICE37, type(initial_values))
        else:
            self._initial_values = initial_values

    @property
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise InputConfigurationError(ErrorCode.ICE38, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE38, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access
            Oracle. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access Oracle.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access Oracle. Can be a UserPasswordCredentials
                object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE39, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the OracleSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the OracleSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class PostgresSource(Input):
    """
    Class for managing the configuration of Postgres-based data inputs.

    Attributes:
        credentials (UserPasswordCredentials): The credentials required to access the
            Postgres database.
        initial_values (dict): The initial values for the parameters in the SQL queries.
        query (str | List[str]): The SQL query(s) to execute. If multiple queries are
            provided, they must be provided as a dictionary, with the parameter name in
            the registered function as the key and the SQL query as the value.
        uri (str): The URI of the database where the data is located.

    Methods:
        to_dict(): Converts the PostgresSource object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.POSTGRES.value

    CREDENTIALS_KEY = "credentials"
    INITIAL_VALUES_KEY = "initial_values"
    QUERY_KEY = "query"
    URI_KEY = "uri"

    def __init__(
        self,
        uri: str,
        query: str | List[str],
        credentials: dict | UserPasswordCredentials | None = None,
        initial_values: dict | None = None,
    ):
        """
        Initializes the PostgresSource with the given URI and query, and optionally
            connection credentials and initial values for the parameters in the SQL
            queries.

        Args:
            uri (str): The URI of the database where the data is located
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
            credentials (dict | UserPasswordCredentials, optional): The credentials
                required to access the Postgres database. Can be a dictionary or a
                UserPasswordCredentials object.
            initial_values (dict, optional): The initial values for the parameters in
                the SQL queries.

        Raises:
            InputConfigurationError
        """
        self.credentials = credentials
        self.uri = uri
        self.query = query
        self.initial_values = initial_values

    @property
    def uri(self) -> str:
        """
        str: The URI of the database where the data is located.
        """
        return self._uri

    @uri.setter
    def uri(self, uri: str):
        """
        Sets the URI of the database where the data is located.

        Args:
            uri (str): The URI of the database where the data is located.
        """
        self._uri = uri
        self._parsed_uri = urlparse(uri)
        if self._parsed_uri.scheme not in POSTGRES_SCHEMES:
            raise InputConfigurationError(
                ErrorCode.ICE2, self._parsed_uri.scheme, POSTGRES_SCHEMES, self.uri
            )

    @property
    def initial_values(self) -> dict:
        """
        dict: The initial values for the parameters in the SQL queries.
        """
        return self._initial_values

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        """
        Sets the initial values for the parameters in the SQL queries.

        Args:
            initial_values (dict): The initial values for the parameters in the SQL
                queries.
        """
        if not initial_values:
            self._initial_values = {}
        elif not isinstance(initial_values, dict):
            raise InputConfigurationError(ErrorCode.ICE31, type(initial_values))
        else:
            self._initial_values = initial_values

    @property
    def query(self) -> str | List[str]:
        """
        str | List[str]: The SQL query(s) to execute.
        """
        return self._query

    @query.setter
    def query(self, query: str | List[str]):
        """
        Sets the SQL query(s) to execute

        Args:
            query (str | List[str]): The SQL query(s) to execute. If multiple queries
                are provided, they must be provided as a list, and they will be
                mapped to the function inputs in the same order as they are defined.
        """
        if isinstance(query, str):
            self._query = query
        elif isinstance(query, list):
            self._query = query
            if not all(isinstance(single_query, str) for single_query in self._query):
                raise InputConfigurationError(ErrorCode.ICE32, type(query))
        else:
            raise InputConfigurationError(ErrorCode.ICE32, type(query))

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        UserPasswordCredentials | None: The credentials required to access the
            PostgresDatabase. If no credentials were provided, it will return None.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict | UserPasswordCredentials | None):
        """
        Sets the credentials to access the PostgresDatabase.

        Args:
            credentials (dict | UserPasswordCredentials | None): The credentials
                required to access the Postgres database. Can be a
                UserPasswordCredentials object, a dictionary or None
        """
        if not credentials:
            self._credentials = None
        else:
            credentials = build_credentials(credentials)
            if not (isinstance(credentials, UserPasswordCredentials)):
                raise InputConfigurationError(ErrorCode.ICE33, type(credentials))
            self._credentials = credentials

    def to_dict(self) -> dict:
        """
        Converts the PostgresSource object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the PostgresSource
                object.
        """
        return {
            self.IDENTIFIER: {
                self.INITIAL_VALUES_KEY: self.initial_values,
                self.QUERY_KEY: self.query,
                self.URI_KEY: self.uri,
                self.CREDENTIALS_KEY: (
                    self.credentials.to_dict() if self.credentials else None
                ),
            }
        }


class S3Source(Input):
    """
    Class for managing the configuration of S3-file-based data inputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension of the data.
        uri (str | List[str]): The URI of the files with format: 's3://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (S3Credentials): The credentials required to access the S3 bucket.
        initial_last_modified (str | datetime.datetime): If provided, only the files
            modified after this date and time will be considered.

    Methods:
        to_dict(): Converts the S3Source object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.S3.value

    CREDENTIALS_KEY = "credentials"
    FORMAT_KEY = "format"
    LAST_MODIFIED_KEY = "initial_last_modified"
    REGION_KEY = "region"
    URI_KEY = "uri"

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the S3Source.
        """

        csv = CSVFormat
        ndjson = NDJSONFormat
        log = LogFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | List[str],
        credentials: dict | S3Credentials,
        format: str | dict | FileFormat = None,
        initial_last_modified: str | datetime.datetime = None,
        region: str = None,
    ):
        """
        Initializes the S3Source with the given URI and the credentials required to
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
                'parquet', 'ndjson', 'jsonl' and 'log'.
            initial_last_modified (str | datetime.datetime, optional): If provided,
                only the files modified after this date and time will be considered.
                The date and time can be provided as a string in
                [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) or as
                a datetime object. If no timezone is provided, UTC will be assumed.
            region (str, optional): The region where the S3 bucket is located. If not
                provided, the default AWS region will be used.

        Raises:
            InputConfigurationError
            FormatConfigurationError
        """
        self.uri = uri
        self.format = format
        self.initial_last_modified = initial_last_modified
        self.credentials = credentials
        self.region = region

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
                raise InputConfigurationError(ErrorCode.ICE16, type(uri))
        else:
            raise InputConfigurationError(ErrorCode.ICE16, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme != S3_SCHEME:
                raise InputConfigurationError(
                    ErrorCode.ICE17,
                    parsed_uri.scheme,
                    S3_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

    def to_dict(self) -> dict:
        """
        Converts the S3Source object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the S3Source
                object.
        """
        return {
            self.IDENTIFIER: {
                self.FORMAT_KEY: self.format.to_dict(),
                self.LAST_MODIFIED_KEY: self.initial_last_modified,
                self.URI_KEY: self._uri_list,
                self.CREDENTIALS_KEY: self.credentials.to_dict(),
                self.REGION_KEY: self.region,
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
                raise InputConfigurationError(ErrorCode.ICE26, type(region))
            supported_regions = [element.value for element in SupportedAWSS3Regions]
            if region not in supported_regions:
                logger.warning(
                    "The 'region' parameter for the S3FileInput object has value "
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

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the S3Source

        Args:
            format (FileFormat): The format to verify.
        """
        valid_input_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_input_formats)):
            raise InputConfigurationError(
                ErrorCode.ICE4, type(format), valid_input_formats
            )

    @property
    def initial_last_modified(self) -> str:
        """
        str: The date and time after which the files were modified.
        """
        return (
            self._initial_last_modified.isoformat(timespec="microseconds")
            if self._initial_last_modified
            else None
        )

    @initial_last_modified.setter
    def initial_last_modified(self, initial_last_modified: str | datetime.datetime):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified (str | datetime.datetime): The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object. If no timezone is
                provided, UTC will be assumed.
        """
        if initial_last_modified:
            if isinstance(initial_last_modified, datetime.datetime):
                self._initial_last_modified = initial_last_modified
            else:
                try:
                    self._initial_last_modified = datetime.datetime.fromisoformat(
                        initial_last_modified
                    )
                except ValueError:
                    raise InputConfigurationError(ErrorCode.ICE5, initial_last_modified)
                except TypeError:
                    raise InputConfigurationError(
                        ErrorCode.ICE6, type(initial_last_modified)
                    )
        else:
            self._initial_last_modified = None

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
            raise InputConfigurationError(ErrorCode.ICE20, type(credentials))
        self._credentials = credentials


class TableInput(Input):
    """
    Class for managing the configuration of table-based data inputs.

    Attributes:
        table (str | List[str]): The table(s) to load.

    Methods:
        to_dict(): Converts the TableInput object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.TABLE.value

    TABLE_KEY = "table"

    def __init__(self, table: str | List[str]):
        """
        Initializes the TableInput with the given tables. If multiple tables are
            provided, they must be provided as a list.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to load.
        """
        return self._table

    @table.setter
    def table(self, table: str | List[str]):
        """
        Sets the table(s) to load.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list
        """
        self._table = table
        if isinstance(table, list):
            assert [build_table_uri_object(single_uri) for single_uri in table]
            self._table = table
            self._table_list = self._table
        else:
            assert build_table_uri_object(table)
            self._table = table
            self._table_list = [self._table]
        self._verify_valid_table_list()

    def _verify_valid_table_list(self):
        """
        Verifies that the tables in the list are valid.
        """
        for table in self._table_list:
            uri = build_table_uri_object(table)
            if not uri.table:
                raise InputConfigurationError(ErrorCode.ICE25, table)

    def to_dict(self) -> dict:
        """
        Converts the TableInput object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the Output
                object.
        """
        return {self.IDENTIFIER: {self.TABLE_KEY: self._table_list}}


def build_input(input: dict | Input | None) -> Input | None:
    """
    Builds an Input object.

    Args:
        input (dict | Input | None): A dictionary with the input information or an
            Input object.

    Returns:
        Input: A Input object built from the input.
            It can be a LocalFileSource, S3FileInput, MySQLSource, or TableInput
            object, or None if nothing was provided

    Raises:
        InputConfigurationError
    """
    if not input:
        return None
    elif isinstance(input, Input):
        return input
    elif isinstance(input, dict):
        return build_input_from_dict(input)
    else:
        raise InputConfigurationError(ErrorCode.ICE11, type(input))


def build_input_from_dict(input: dict) -> Input:
    valid_identifiers = [element.value for element in InputIdentifiers]
    # The input dictionary must have exactly one key, which must be one of the
    # valid identifiers
    if len(input) != 1 or next(iter(input)) not in valid_identifiers:
        raise InputConfigurationError(
            ErrorCode.ICE7, valid_identifiers, list(input.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(input.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise InputConfigurationError(ErrorCode.ICE8, identifier, type(configuration))
    existing_inputs = [
        LocalFileSource,
        S3Source,
        MySQLSource,
        TableInput,
        AzureSource,
        PostgresSource,
        MariaDBSource,
        OracleSource,
    ]
    for input_class in existing_inputs:
        if identifier == input_class.IDENTIFIER:
            return input_class(**configuration)
