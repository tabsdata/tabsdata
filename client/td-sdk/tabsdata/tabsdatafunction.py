#
# Copyright 2024 Tabs Data Inc.
#

import datetime
import inspect
import logging
import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, List, Literal, Type
from urllib.parse import urlparse, urlunparse

import pandas as pd
import polars as pl

import tabsdata.tableframe.lazyframe.frame as td_frame
import tabsdata.utils.tableframe._helpers as td_helpers
from tabsdata.credentials import (
    AzureCredentials,
    S3Credentials,
    UserPasswordCredentials,
    build_credentials,
)
from tabsdata.exceptions import (
    ErrorCode,
    FunctionConfigurationError,
    InputConfigurationError,
    OutputConfigurationError,
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
from tabsdata.plugin import DestinationPlugin, SourcePlugin
from tabsdata.tableuri import build_table_uri_object

logger = logging.getLogger(__name__)

TABLES_KEY = "tables"

AZURE_SCHEME = "az"
FILE_SCHEME = "file"
MARIADB_SCHEME = "mariadb"
MYSQL_SCHEME = "mysql"
ORACLE_SCHEME = "oracle"
POSTGRES_SCHEMES = ("postgres", "postgresql")
S3_SCHEME = "s3"

URI_INDICATOR = "://"


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


# TODO: Consider making this a list calculated at runtime from existing regions.
#   However, since they don't change that often, for now this should be good enough.
class SupportedAWSS3Regions(Enum):
    Ohio = "us-east-2"
    NorthVirginia = "us-east-1"
    NorthCalifornia = "us-west-1"
    Oregon = "us-west-2"
    CapeTown = "af-south-1"
    HongKong = "ap-east-1"
    Hyderabad = "ap-south-2"
    Jakarta = "ap-southeast-3"
    Malaysia = "ap-southeast-5"
    Melbourne = "ap-southeast-4"
    Mumbai = "ap-south-1"
    Osaka = "ap-northeast-3"
    Seoul = "ap-northeast-2"
    Singapore = "ap-southeast-1"
    Sydney = "ap-southeast-2"
    Tokyo = "ap-northeast-1"
    CanadaCentral = "ca-central-1"
    Calgary = "ca-west-1"
    Frankfurt = "eu-central-1"
    Ireland = "eu-west-1"
    London = "eu-west-2"
    Milan = "eu-south-1"
    Paris = "eu-west-3"
    Spain = "eu-south-2"
    Stockholm = "eu-north-1"
    Zurich = "eu-central-2"
    TelAviv = "il-central-1"
    Bahrain = "me-south-1"
    UAE = "me-central-1"
    SaoPaulo = "sa-east-1"
    GovCloudUSEast = "us-gov-east-1"
    GovCloudUSWest = "us-gov-west-1"


class IfTableExistsStrategy(Enum):
    """
    Enum for the strategies to follow when the table already exists.
    """

    APPEND = "append"
    REPLACE = "replace"


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

        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

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

        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

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

        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

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

        self.host, self.port = self._parsed_uri.netloc.split(":")
        self.database = self._parsed_uri.path[1:]

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


class TabsdataFunction:
    """
    Class to decorate a function with metadata and methods for use in a Tabsdata
        environment.

    Attributes:

    """

    def __init__(
        self,
        func: Callable,
        name: str | None,
        input: dict | Input | SourcePlugin = None,
        output: dict | Output | DestinationPlugin = None,
        trigger_by: str | List[str] | None = None,
    ):
        """
        Initializes the TabsDataFunction with the given function, input, output and
        trigger.

        Args:
            func (Callable): The function to decorate.
            name (str): The name with which the function will
                be registered. If None, the original_function name will be used.
            input (dict | Input | SourcePlugin, optional): The data to be used when
                running the function. Can be a dictionary or an instance of Input or
                SourcePlugin.
            output (dict | Output | DestinationPlugin, optional): The location where the
                function results will be saved when run.
            trigger_by (str | List[str], optional): The trigger(s) that will cause the
                function to execute. It can be a table in the system, a list of
                tables or None (in which case it will be inferred from the
                dependencies).

        Raises:
            FunctionConfigurationError
            InputConfigurationError
            OutputConfigurationError
            FormatConfigurationError
        """
        self.original_function = func
        self.output = output
        self.input = input
        self._func_original_folder, self._func_original_file = os.path.split(
            inspect.getfile(func)
        )
        self.trigger_by = trigger_by
        self.name = name

    def __repr__(self) -> str:
        """
        Returns a string representation of the TabsDataFunction.

        Returns:
            str: A string representation of the TabsDataFunction.
        """
        return (
            f"{self.__class__.__name__}({self._func.__name__})(input='{self.input}',"
            f" output='{self.output}', original_file='{self.original_file}',"
            f" original_folder='{self.original_folder}', trigger='{self.trigger_by}')"
        )

    def __call__(self, *args, **kwargs):
        """
        Calls the original function with the given arguments and keyword arguments.

        Args:
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            Any: The return value of the original function
        """
        new_args = _convert_recursively_to_tableframe(args)
        new_kwargs = _convert_recursively_to_tableframe(kwargs)
        result = self._func(*new_args, **new_kwargs)
        data_type = _recursively_obtain_datatype(args) or _recursively_obtain_datatype(
            kwargs
        )
        if data_type:
            return _clean_recursively_and_convert_to_datatype(result, data_type)
        else:
            return result

    @property
    def input(self) -> Input | SourcePlugin | None:
        """
        Input | SourcePlugin | None: The data to be used when running the function.
        """
        return self._input

    @input.setter
    def input(self, input: dict | Input | SourcePlugin | None):
        """
        Sets the input data for the function.

        Args:
            input (dict | Input | None): The data to be used when running the
                function. Can be a dictionary, an instance of Input, an instance of
                SourcePlugin or None.
        """
        if isinstance(input, SourcePlugin):
            self._input = input
        else:
            self._input = build_input(input)
        self._verify_valid_input_output()

    @property
    def original_folder(self) -> str:
        """
        str: The folder where the original function is defined, as a local path in the
            user's computer.
        """
        return self._func_original_folder

    @property
    def original_file(self):
        """
        str: The file where the original function is defined in the user's computer
        """
        return self._func_original_file

    @property
    def original_function(self) -> Callable:
        """
        Callable: The original function that was decorated, without any behaviour
            modifications.
        """
        return self._func

    @original_function.setter
    def original_function(self, func: Callable):
        """
        Sets the original function for the TabsDataFunction.

        Args:
            func (Callable): The original function that was decorated, without any
                behaviour modifications.
        """
        if not callable(func):
            raise FunctionConfigurationError(ErrorCode.FCE1, type(func))
        self._func = func

    @property
    def output(self) -> Output | DestinationPlugin | None:
        """
        dict: The location where the function results will be saved when run.
        """
        return self._output

    @output.setter
    def output(self, output: dict | Output | DestinationPlugin | None):
        """
        Sets the output location for the function.

        Args:
            output (dict | Output | DestinationPlugin | None): The location where the
                function results will be saved when run.
        """
        if isinstance(output, DestinationPlugin):
            self._output = output
        else:
            self._output = build_output(output)
        self._verify_valid_input_output()

    @property
    def name(self) -> str:
        """
        str: The name with which the function will be registered.
        """
        return self._name or self.original_function.__name__

    @name.setter
    def name(self, name: str | None):
        """
        Sets the name with which the function will be registered.

        Args:
            name (str | None): The name with which the function will be
                registered. If None, the original_function name will be used.
        """
        if isinstance(name, str) or name is None:
            self._name = name
        else:
            raise FunctionConfigurationError(ErrorCode.FCE6, type(name))

    @property
    def trigger_by(self) -> List[str] | None:
        """
        List[str]: The trigger(s) that will cause the function to execute. It must be
            another table or tables in the system.
        """
        return self._trigger_by

    @trigger_by.setter
    def trigger_by(self, trigger_by: str | List[str] | None):
        """
        Sets the trigger(s) that will cause the function to execute

        Args:
            trigger_by (str | List[str] | None): The trigger(s) that will
                cause the function to execute. It must be another table or tables in
                the system. If None, all the tables in the dependencies will be used.
        """
        if isinstance(trigger_by, str):
            trigger_by = [trigger_by]

        if trigger_by is None:
            self._trigger_by = None
            return
        elif isinstance(trigger_by, list):
            self._trigger_by = trigger_by
        else:
            raise FunctionConfigurationError(ErrorCode.FCE2, type(trigger_by))

        for trigger in self._trigger_by:
            if not isinstance(trigger, str):
                raise FunctionConfigurationError(ErrorCode.FCE2, type(trigger))
            trigger_uri = build_table_uri_object(trigger)
            if not trigger_uri.table:
                raise FunctionConfigurationError(ErrorCode.FCE3, trigger)

    def _verify_valid_input_output(self):
        """
        Verifies that the input and output are valid for the function.

        Raises:
            FunctionConfigurationError
        """
        if hasattr(self, "_input") and hasattr(self, "_output"):
            is_not_table_input = self.input and not isinstance(self.input, TableInput)
            is_not_table_output = self.output and not isinstance(
                self.output, TableOutput
            )
            if is_not_table_input and is_not_table_output:
                raise FunctionConfigurationError(
                    ErrorCode.FCE5, type(self.input), type(self.output)
                )


def _convert_recursively_to_tableframe(arguments: Any):
    if isinstance(arguments, dict):
        return {k: _convert_recursively_to_tableframe(v) for k, v in arguments.items()}
    elif isinstance(arguments, list):
        return [_convert_recursively_to_tableframe(v) for v in arguments]
    elif isinstance(arguments, tuple):
        return tuple(_convert_recursively_to_tableframe(v) for v in arguments)
    elif isinstance(arguments, td_frame.TableFrame):
        return arguments
    elif isinstance(arguments, pl.DataFrame):
        return td_frame.TableFrame.__build__(_add_dummy_required_columns(arguments))
    elif isinstance(arguments, pl.LazyFrame):
        return td_frame.TableFrame.__build__(_add_dummy_required_columns(arguments))
    elif isinstance(arguments, pd.DataFrame):
        return td_frame.TableFrame.__build__(
            _add_dummy_required_columns(pl.DataFrame(arguments))
        )
    return arguments


def _clean_recursively_and_convert_to_datatype(
    result,
    datatype: (
        Type[pl.DataFrame]
        | Type[pl.LazyFrame]
        | Type[td_frame.TableFrame]
        | Type[pd.DataFrame]
    ),
) -> Any:
    if isinstance(result, dict):
        return {
            k: _clean_recursively_and_convert_to_datatype(v, datatype)
            for k, v in result.items()
        }
    elif isinstance(result, list):
        return [_clean_recursively_and_convert_to_datatype(v, datatype) for v in result]
    elif isinstance(result, tuple):
        return tuple(
            _clean_recursively_and_convert_to_datatype(v, datatype) for v in result
        )
    elif isinstance(result, td_frame.TableFrame):
        try:
            if datatype == pl.DataFrame:
                return result._lf.drop(td_helpers.SYSTEM_COLUMNS).collect()
            elif datatype == pl.LazyFrame:
                return result._lf.drop(td_helpers.SYSTEM_COLUMNS)
            elif datatype == pd.DataFrame:
                return result._lf.drop(td_helpers.SYSTEM_COLUMNS).collect().to_pandas()
            else:
                return result
        except pl.exceptions.ColumnNotFoundError as e:
            raise ValueError(
                "Missing one of the following system columns"
                f" '{td_helpers.SYSTEM_COLUMNS}'. This indicates tampering in the data."
                " Ensure you are not modifying system columns in your data."
            ) from e
    else:
        return result


def _add_dummy_required_columns(
    lf: pl.LazyFrame | pl.DataFrame,
) -> pl.LazyFrame | pl.DataFrame:
    return lf.with_columns(
        [
            pl.lit("fake_value").alias(col_name)
            for col_name in td_helpers.SYSTEM_COLUMNS
            if col_name not in lf.collect_schema().names()
        ]
    )


def _recursively_obtain_datatype(
    arguments,
) -> (
    Type[pl.DataFrame]
    | Type[pd.DataFrame]
    | Type[pl.LazyFrame]
    | Type[td_frame.TableFrame]
    | None
):
    if isinstance(
        arguments, (pl.DataFrame, pl.LazyFrame, td_frame.TableFrame, pd.DataFrame)
    ):
        return type(arguments)
    elif not arguments:
        return None

    types = []
    if isinstance(arguments, dict):
        types = [_recursively_obtain_datatype(v) for v in arguments.values()]
    elif isinstance(arguments, (list, tuple)):
        types = [_recursively_obtain_datatype(v) for v in arguments]
    if pl.DataFrame in types:
        return pl.DataFrame
    elif pl.LazyFrame in types:
        return pl.LazyFrame
    elif td_frame.TableFrame in types:
        return td_frame.TableFrame
    elif pd.DataFrame in types:
        return pd.DataFrame
    else:
        return None
