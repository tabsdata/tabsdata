#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import subprocess
import uuid
from datetime import datetime, timezone
from enum import Enum
from urllib.parse import unquote, urlparse, urlunparse

from tabsdata._credentials import (
    AzureCredentials,
    GCPCredentials,
    S3Credentials,
    build_credentials,
)
from tabsdata._format import (
    AvroFormat,
    CSVFormat,
    FileFormat,
    LogFormat,
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
from tabsdata._io.plugin import SourcePlugin
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
from tabsdata._tabsserver.function.offset_utils import (
    OFFSET_LAST_MODIFIED_VARIABLE_NAME,
)
from tabsdata._tabsserver.function.yaml_parsing import (
    TransporterAvroFormat,
    TransporterAzure,
    TransporterCSVFormat,
    TransporterEnv,
    TransporterGCS,
    TransporterJsonFormat,
    TransporterLocalFile,
    TransporterLogFormat,
    TransporterParquetFormat,
    TransporterS3,
    V1ImportFormat,
    parse_import_report_yaml,
    store_import_as_yaml,
)
from tabsdata._tabsserver.utils import convert_uri_to_path
from tabsdata.exceptions import (
    ErrorCode,
    SourceConfigurationError,
)

logger = logging.getLogger(__name__)


class AzureSource(SourcePlugin):
    """
    Class for managing the configuration of Azure-file-based data inputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension of the data.
        uri (str | list[str]): The URI of the files with format: 'az://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (AzureCredentials): The credentials required to access Azure.
        initial_last_modified (str | datetime): If provided, only the files
            modified after this date and time will be considered.

    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the AzureSource.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        log = LogFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | list[str],
        credentials: AzureCredentials,
        format: str | FileFormat = None,
        initial_last_modified: str | datetime = None,
    ):
        """
        Initializes the AzureSource with the given URI and the credentials required to
            access Azure, and optionally a format and date and
            time after which the files were modified.

        Args:
            uri (str | list[str]): The URI of the files with format:
                'az://path/to/files'. It can be a single URI or a list of URIs.
            credentials (AzureCredentials): The credentials required to access
                Azure. Must be an AzureCredentials object.
            format (str | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object .
                Currently supported formats are 'csv',
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
            initial_last_modified (str | datetime, optional): If provided,
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
                raise SourceConfigurationError(ErrorCode.SOCE28, type(uri))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE28, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme.lower() != AZURE_SCHEME:
                raise SourceConfigurationError(
                    ErrorCode.SOCE29,
                    parsed_uri.scheme,
                    AZURE_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file extension of the data.
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
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
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
            raise SourceConfigurationError(
                ErrorCode.SOCE4, type(format), valid_input_formats
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
    def initial_last_modified(self, initial_last_modified: str | datetime):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified (str | datetime): The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object. If no timezone is
                provided, UTC will be assumed.
        """
        if initial_last_modified:
            if isinstance(initial_last_modified, datetime):
                processed_initial_last_modified = initial_last_modified
            else:
                try:
                    processed_initial_last_modified = datetime.fromisoformat(
                        initial_last_modified
                    )
                except ValueError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE5, initial_last_modified
                    )
                except TypeError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE6, type(initial_last_modified)
                    )
            _raise_exception_if_no_tzinfo(processed_initial_last_modified)
            self._initial_last_modified = processed_initial_last_modified
        else:
            self._initial_last_modified = None

    @property
    def initial_values(self) -> dict:
        if hasattr(self, "_initial_values"):
            return self._initial_values

        if self.initial_last_modified:
            return {
                OFFSET_LAST_MODIFIED_VARIABLE_NAME: None,
            }
        else:
            return {}

    @initial_values.setter
    def initial_values(self, new_values: dict | None):
        if not isinstance(new_values, dict) and new_values is not None:
            raise TypeError(
                "'initial_values' must be set to a dictionary or None, got"
                f" {type(new_values)} instead"
            )
        self._initial_values = new_values
        logger.info(f"Initial values updated to '{new_values}' successfully.")

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
            raise SourceConfigurationError(ErrorCode.SOCE30, type(credentials))
        self._credentials = credentials

    def chunk(self, working_dir: str) -> list[str | None | list[str | None]]:
        logger.debug(f"Triggering {self}")
        obtain_and_set_azure_credentials(self.credentials)
        local_sources = _execute_file_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        self._stream_ignore_working_dir = True
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}({self.uri})"


class GCSSource(SourcePlugin):
    """
    Class for managing the configuration of GCS-file-based data inputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension of the data.
        uri (str | list[str]): The URI of the files with format: 'gs://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (GCPCredentials): The credentials required to access GCS.
        initial_last_modified (str | datetime): If provided, only the files
            modified after this date and time will be considered.

    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the GCSSource.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        log = LogFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | list[str],
        credentials: GCPCredentials,
        format: str | FileFormat = None,
        initial_last_modified: str | datetime = None,
    ):
        """
        Initializes the GCSSource with the given URI and the credentials required to
            access GCS, and optionally a format and date and
            time after which the files were modified.

        Args:
            uri (str | list[str]): The URI of the files with format:
                'az://path/to/files'. It can be a single URI or a list of URIs.
            credentials (GCPCredentials): The credentials required to access
                GCS. Must be a GCPCredentials object.
            format (str | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
            initial_last_modified (str | datetime, optional): If provided,
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
                raise SourceConfigurationError(ErrorCode.SOCE43, type(uri))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE43, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme.lower() != GCS_SCHEME:
                raise SourceConfigurationError(
                    ErrorCode.SOCE44,
                    parsed_uri.scheme,
                    GCS_SCHEME,
                    urlunparse(parsed_uri),
                )

        self._implicit_format = get_implicit_format_from_list(self._uri_list)
        if hasattr(self, "_format") and self._format is None:
            self._verify_valid_format(build_file_format(self._implicit_format))

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file extension of the data.
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
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
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
        Verifies that the provided format is valid for the GCSSource

        Args:
            format (FileFormat): The format to verify.
        """
        valid_input_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_input_formats)):
            raise SourceConfigurationError(
                ErrorCode.SOCE4, type(format), valid_input_formats
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
    def initial_last_modified(self, initial_last_modified: str | datetime):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified (str | datetime): The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object. If no timezone is
                provided, UTC will be assumed.
        """
        if initial_last_modified:
            if isinstance(initial_last_modified, datetime):
                processed_initial_last_modified = initial_last_modified
            else:
                try:
                    processed_initial_last_modified = datetime.fromisoformat(
                        initial_last_modified
                    )
                except ValueError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE5, initial_last_modified
                    )
                except TypeError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE6, type(initial_last_modified)
                    )
            _raise_exception_if_no_tzinfo(processed_initial_last_modified)
            self._initial_last_modified = processed_initial_last_modified
        else:
            self._initial_last_modified = None

    @property
    def initial_values(self) -> dict:
        if hasattr(self, "_initial_values"):
            return self._initial_values

        if self.initial_last_modified:
            return {
                OFFSET_LAST_MODIFIED_VARIABLE_NAME: None,
            }
        else:
            return {}

    @initial_values.setter
    def initial_values(self, new_values: dict | None):
        if not isinstance(new_values, dict) and new_values is not None:
            raise TypeError(
                "'initial_values' must be set to a dictionary or None, got"
                f" {type(new_values)} instead"
            )
        self._initial_values = new_values
        logger.info(f"Initial values updated to '{new_values}' successfully.")

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
            raise SourceConfigurationError(ErrorCode.SOCE45, type(credentials))
        self._credentials = credentials

    def chunk(self, working_dir: str) -> list[str | None | list[str | None]]:
        logger.debug(f"Triggering {self}")
        obtain_and_set_gcp_credentials(self.credentials)
        local_sources = _execute_file_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        self._stream_ignore_working_dir = True
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}({self.uri})"


class LocalFileSource(SourcePlugin):
    """
    Class for managing the configuration of local-file-based data inputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension of the data.
        path (str | list[str]): The path where the files can be found. It can be a
            single path or a list of paths.
        initial_last_modified (str | None): If not None, only the files modified after
            this date and time will be considered.

    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the LocalFileSource.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        log = LogFormat
        parquet = ParquetFormat

    def __init__(
        self,
        path: str | list[str],
        format: str | FileFormat = None,
        initial_last_modified: str | datetime = None,
    ):
        """
        Initializes the LocalFileSource with the given path, and optionally a format and
            a date and time after which the files were modified.

        Args:
            path (str | list[str]): The path where the files can be found. It can be a
                single path or a list of paths.
            format (str | FileFormat, optional): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
            initial_last_modified (str | datetime, optional): If provided,
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
    def path(self) -> str | list[str]:
        """
        str | list[str]: The path or paths to the files to load.
        """
        return self._path

    @property
    def _uri_list(self) -> list[str]:
        """
        list[str]: The list of paths to the files to load.
        """
        return [unquote(convert_path_to_uri(path)) for path in self._path_list]

    @path.setter
    def path(self, path: str | list[str]):
        """
        Sets the path or paths to the files to load.

        Args:
            path (str | list[str]): The path or paths to the files to load.
        """
        self._path = path
        if isinstance(path, str):
            self._path_list = [path]
        elif isinstance(path, list):
            self._path_list = path
            if not all(isinstance(single_path, str) for single_path in self._path_list):
                raise SourceConfigurationError(ErrorCode.SOCE13, type(path))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE13, type(path))

        for individual_path in self._path_list:
            if URI_INDICATOR in individual_path:
                parsed_path = urlparse(individual_path)
                if parsed_path.scheme != FILE_SCHEME:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE14,
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
            inferred  from the file extension in the path.
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
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
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
            raise SourceConfigurationError(
                ErrorCode.SOCE4, type(format), valid_input_formats
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
    def initial_last_modified(self, initial_last_modified: str | datetime):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified (str | datetime): The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object. If no timezone is
                provided, UTC will be assumed.
        """
        if initial_last_modified:
            if isinstance(initial_last_modified, datetime):
                processed_initial_last_modified = initial_last_modified
            else:
                try:
                    processed_initial_last_modified = datetime.fromisoformat(
                        initial_last_modified
                    )
                except ValueError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE5, initial_last_modified
                    )
                except TypeError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE6, type(initial_last_modified)
                    )
            _raise_exception_if_no_tzinfo(processed_initial_last_modified)
            self._initial_last_modified = processed_initial_last_modified
        else:
            self._initial_last_modified = None

    @property
    def initial_values(self) -> dict:
        if hasattr(self, "_initial_values"):
            return self._initial_values

        if self.initial_last_modified:
            return {
                OFFSET_LAST_MODIFIED_VARIABLE_NAME: None,
            }
        else:
            return {}

    @initial_values.setter
    def initial_values(self, new_values: dict | None):
        if not isinstance(new_values, dict) and new_values is not None:
            raise TypeError(
                "'initial_values' must be set to a dictionary or None, got"
                f" {type(new_values)} instead"
            )
        self._initial_values = new_values
        logger.info(f"Initial values updated to '{new_values}' successfully.")

    def chunk(self, working_dir: str) -> list[str | None | list[str | None]]:
        logger.debug(f"Triggering {self}")
        local_sources = _execute_file_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        self._stream_ignore_working_dir = True
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}({self.path})"


class S3Source(SourcePlugin):
    """
    Class for managing the configuration of S3-file-based data inputs.

    Attributes:
        format (FileFormat): The format of the file. If not provided, it will be
            inferred from the file extension of the data.
        uri (str | list[str]): The URI of the files with format: 's3://path/to/files'.
            It can be a single URI or a list of URIs.
        credentials (S3Credentials): The credentials required to access the S3 bucket.
        initial_last_modified (str | datetime): If provided, only the files
            modified after this date and time will be considered.

    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the S3Source.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        log = LogFormat
        parquet = ParquetFormat

    def __init__(
        self,
        uri: str | list[str],
        credentials: S3Credentials,
        format: str | FileFormat = None,
        initial_last_modified: str | datetime = None,
        region: str = None,
    ):
        """
        Initializes the S3Source with the given URI and the credentials required to
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
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
            initial_last_modified (str | datetime, optional): If provided,
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
                raise SourceConfigurationError(ErrorCode.SOCE16, type(uri))
        else:
            raise SourceConfigurationError(ErrorCode.SOCE16, type(uri))

        self._parsed_uri_list = [urlparse(single_uri) for single_uri in self._uri_list]
        for parsed_uri in self._parsed_uri_list:
            if parsed_uri.scheme.lower() != S3_SCHEME:
                raise SourceConfigurationError(
                    ErrorCode.SOCE17,
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
                raise SourceConfigurationError(ErrorCode.SOCE26, type(region))
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
    def format(self, format: str | FileFormat):
        """
        Sets the format of the file.

        Args:
            format (str | FileFormat): The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'avro', 'ndjson', 'jsonl' and 'log'.
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
            raise SourceConfigurationError(
                ErrorCode.SOCE4, type(format), valid_input_formats
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
    def initial_last_modified(self, initial_last_modified: str | datetime):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified (str | datetime): The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object. If no timezone is
                provided, UTC will be assumed.
        """
        if initial_last_modified:
            if isinstance(initial_last_modified, datetime):
                processed_initial_last_modified = initial_last_modified
            else:
                try:
                    processed_initial_last_modified = datetime.fromisoformat(
                        initial_last_modified
                    )
                except ValueError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE5, initial_last_modified
                    )
                except TypeError:
                    raise SourceConfigurationError(
                        ErrorCode.SOCE6, type(initial_last_modified)
                    )
            _raise_exception_if_no_tzinfo(processed_initial_last_modified)
            self._initial_last_modified = processed_initial_last_modified
        else:
            self._initial_last_modified = None

    @property
    def initial_values(self) -> dict:
        if hasattr(self, "_initial_values"):
            return self._initial_values

        if self.initial_last_modified:
            return {
                OFFSET_LAST_MODIFIED_VARIABLE_NAME: None,
            }
        else:
            return {}

    @initial_values.setter
    def initial_values(self, new_values: dict | None):
        if not isinstance(new_values, dict) and new_values is not None:
            raise TypeError(
                "'initial_values' must be set to a dictionary or None, got"
                f" {type(new_values)} instead"
            )
        self._initial_values = new_values
        logger.info(f"Initial values updated to '{new_values}' successfully.")

    @property
    def credentials(self) -> S3Credentials:
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
            raise SourceConfigurationError(ErrorCode.SOCE20, type(credentials))
        self._credentials = credentials

    def chunk(self, working_dir: str) -> list[str | None | list[str | None]]:
        logger.debug(f"Triggering {self}")
        obtain_and_set_s3_credentials(self.credentials)
        set_s3_region(self.region)
        local_sources = _execute_file_importer(self, working_dir)
        logger.debug(f"Obtained local sources: '{local_sources}'")
        self._stream_ignore_working_dir = True
        return local_sources

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}({self.uri})"


def _execute_file_importer(
    source: AzureSource | GCSSource | LocalFileSource | S3Source,
    destination_folder: str,
) -> list:
    """
    Import files from a source to a destination. The source can be either a local file
        or an S3 bucket. The destination is always a local folder. The result is a list
        of files that were imported. Each element of the list is a list of paths to
        parquet files.
    :return: A list of files that were imported. Each element of the list is a list
        of paths to parquet files.
    """
    # noinspection PyProtectedMember
    location_list = source._uri_list
    destination_folder = (
        destination_folder
        if destination_folder.endswith(os.sep)
        else destination_folder + os.sep
    )
    last_modified = None
    lastmod_info = None
    if source.initial_last_modified:
        last_modified = source.initial_last_modified
        processed_initial_last_modified = datetime.fromisoformat(last_modified)
        logger.debug(
            f"Last modified time '{last_modified}' converted to "
            f"datetime object '{processed_initial_last_modified}'."
        )
        if processed_initial_last_modified.tzinfo is None:
            logger.error(
                f"Last modified time '{last_modified}', converted to "
                f"datetime object '{processed_initial_last_modified}' "
                "is not timezone-aware, but having a timezone is a "
                "requirement "
                "for initial_last_modified."
            )
            raise ValueError(
                f"Last modified time '{last_modified}', converted to "
                f"datetime object '{processed_initial_last_modified}' "
                "is not timezone-aware, but having a timezone is a "
                "requirement "
                "for initial_last_modified."
            )
        utc_initial_last_modified = processed_initial_last_modified.astimezone(
            timezone.utc
        )
        logger.debug(
            f"Last modified time '{last_modified}' converted to "
            f"UTC datetime object '{utc_initial_last_modified}'."
        )
        utc_last_modified_string = utc_initial_last_modified.isoformat(
            timespec="microseconds"
        )
        logger.debug(
            f"Last modified time '{last_modified}' converted to "
            f"UTC string '{utc_last_modified_string}'."
        )
        last_modified = utc_last_modified_string
        lastmod_info = source.initial_values.get(OFFSET_LAST_MODIFIED_VARIABLE_NAME)
        if lastmod_info:
            logger.debug("Using stored last modified value")
        else:
            logger.debug("Using decorator last modified value")
    logger.debug(f"Last modified: '{last_modified}'; lastmod_info: '{lastmod_info}'")
    source_list = []
    for location in location_list:
        sources, lastmod_info = _execute_single_file_import(
            origin_location_uri=location,
            destination_folder=destination_folder,
            file_format=source.format,
            initial_last_modified=last_modified,
            user_source=source,
            lastmod_info=lastmod_info,
        )
        source_list.append(sources)
    if source.initial_last_modified:
        logger.debug("Capturing new last modified information")
        source.initial_values = {OFFSET_LAST_MODIFIED_VARIABLE_NAME: lastmod_info}
    return source_list


# noinspection DuplicatedCode
def _execute_single_file_import(
    origin_location_uri: str,
    destination_folder: str,
    file_format: FileFormat,
    initial_last_modified: str | None,
    user_source: AzureSource | GCSSource | LocalFileSource | S3Source,
    lastmod_info: str = None,
) -> (list[str] | str, str | None):
    """
    Import a file from a location to a destination with a specific format. The file is
        imported using a binary, and the result returned is always a list of parquet
        files. If the location contained a wildcard for the files, the list might
        contain one or more elements.
    :return: list of imported files if using a wildcard pattern, single file if not.
    """
    transporter_import = _obtain_transporter_import(
        origin_location_uri,
        destination_folder,
        file_format,
        initial_last_modified,
        user_source,
        lastmod_info,
    )

    yaml_request_file = os.path.join(destination_folder, f"request_{uuid.uuid4()}.yaml")
    store_import_as_yaml(
        transporter_import,
        yaml_request_file,
    )

    binary = "transporter.exe" if CURRENT_PLATFORM.is_windows() else "transporter"
    report_file = os.path.join(destination_folder, f"report_{uuid.uuid4()}.yaml")
    arguments = f"--request {yaml_request_file} --report {report_file}"
    logger.debug(f"Importing files with command: {binary} {arguments}")
    subprocess_result = subprocess.run(
        [binary] + arguments.split(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
    )
    if subprocess_result.returncode != 0:
        logger.error(
            "Error importing file (return code "
            f"'{subprocess_result.returncode}'):"
            f" {subprocess_result.stderr}"
        )
        raise Exception(
            "Error importing file (return code "
            f"'{subprocess_result.returncode}'):"
            f" {subprocess_result.stderr}"
        )

    result = parse_import_report_yaml(report_file)
    files = result.files
    logger.debug(f"Parsed import report: {result}")
    if _is_wildcard_pattern(origin_location_uri):
        source_list = []
        if files:
            for dictionary in files:
                source_list.append(dictionary.get("to"))
            logger.info(f"Imported files to: '{source_list}'")
            if isinstance(file_format, AvroFormat):
                logger.debug("Converting AVRO files to Parquet format")
                new_source_list = []
                for source in source_list:
                    new_source_list.append(
                        _convert_avro_to_parquet(
                            source, destination_folder, file_format
                        )
                    )
                logger.debug(
                    f"Converted AVRO files to Parquet format: '{new_source_list}'"
                )
                source_list = new_source_list
        else:
            logger.info("No files imported")
    else:
        source_list = files[0].get("to") if files else None
        if not source_list:
            logger.info("No file imported")
        # If the data is not a wildcard pattern, the result is a single file
        else:
            logger.info(f"Imported file to: '{source_list}'")
            if isinstance(file_format, AvroFormat):
                logger.debug("Converting AVRO file to Parquet format")
                source_list = _convert_avro_to_parquet(
                    source_list, destination_folder, file_format
                )
                logger.debug(f"Converted AVRO file to Parquet format: '{source_list}'")
    logger.debug(f"New lastmod_info: '{result.lastmod_info}'")
    return source_list, result.lastmod_info


def _convert_avro_to_parquet(
    avro_file: str, destination_folder: str, file_format: AvroFormat
) -> str:
    import pandas as pd

    chunk_size = file_format.chunk_size
    logger.debug(
        f"Converting AVRO file '{avro_file}' to Parquet format with "
        f"chunk size {chunk_size}"
    )
    first_chunk = True
    uuid_string = uuid.uuid4().hex[:16]
    intermediate_file_name = f"from_avro_{uuid_string}.parquet"
    intermediate_file = os.path.join(destination_folder, intermediate_file_name)
    avro_file = convert_uri_to_path(avro_file)
    logger.debug(f"Using path for AVRO file '{avro_file}'")
    for chunk in _read_avro_in_chunks(avro_file, chunk_size):
        df = pd.DataFrame(chunk)
        df.to_parquet(
            intermediate_file,
            engine="fastparquet",
            index=False,
            append=(not first_chunk),
        )
        first_chunk = False
    logger.debug(
        f"AVRO file '{avro_file}' converted to Parquet file '"
        f"{intermediate_file}' successfully."
    )
    return intermediate_file


def _read_avro_in_chunks(avro_path, chunk_size):
    from fastavro import reader

    with open(avro_path, "rb") as f:
        reader = reader(f)
        batch = []
        for record in reader:
            batch.append(record)
            if len(batch) == chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch


def _obtain_source_object(user_source, origin_location_uri):
    if isinstance(user_source, S3Source):
        transporter_source = TransporterS3(
            origin_location_uri,
            access_key=TransporterEnv(SERVER_SIDE_AWS_ACCESS_KEY_ID),
            secret_key=TransporterEnv(SERVER_SIDE_AWS_SECRET_ACCESS_KEY),
            region=(
                TransporterEnv(SERVER_SIDE_AWS_REGION) if user_source.region else None
            ),
        )
    elif isinstance(user_source, GCSSource):
        transporter_source = TransporterGCS(
            origin_location_uri,
            service_account_key=TransporterEnv(SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON),
        )
    elif isinstance(user_source, AzureSource):
        transporter_source = TransporterAzure(
            origin_location_uri,
            account_name=TransporterEnv(SERVER_SIDE_AZURE_ACCOUNT_NAME),
            account_key=TransporterEnv(SERVER_SIDE_AZURE_ACCOUNT_KEY),
        )
    elif isinstance(user_source, LocalFileSource):
        transporter_source = TransporterLocalFile(origin_location_uri)
    else:
        logger.error(f"Importing from '{user_source}' not supported.")
        raise TypeError(f"Importing from '{user_source}' not supported.")
    logger.debug(f"Source config: {transporter_source}")
    return transporter_source


def _obtain_transporter_import(
    origin_location_uri: str,
    destination_folder: str,
    file_format: FileFormat,
    initial_last_modified: str | None,
    user_source: AzureSource | GCSSource | LocalFileSource | S3Source,
    lastmod_info: str = None,
):
    # Create the transporter source object
    transporter_source = _obtain_source_object(user_source, origin_location_uri)

    # Create the transporter format object
    if isinstance(file_format, CSVFormat):
        transporter_format = TransporterCSVFormat(file_format)
    elif isinstance(file_format, LogFormat):
        transporter_format = TransporterLogFormat()
    elif isinstance(file_format, NDJSONFormat):
        transporter_format = TransporterJsonFormat()
    elif isinstance(file_format, ParquetFormat):
        transporter_format = TransporterParquetFormat()
    elif isinstance(file_format, AvroFormat):
        transporter_format = TransporterAvroFormat()
    else:
        logger.error(f"Invalid file format: {type(file_format)}. No data imported.")
        raise TypeError(f"Invalid file format: {type(file_format)}. No data imported.")
    logger.debug(f"Format config: {transporter_format}")

    # Create transporter target object
    if not destination_folder.endswith(os.sep):
        logger.debug(f"Adding trailing separator to '{destination_folder}'")
        destination_folder = destination_folder + os.sep
        logger.debug(f"New destination folder: '{destination_folder}'")
    transporter_target = TransporterLocalFile(convert_path_to_uri(destination_folder))
    logger.debug(f"Target config: {transporter_target}")

    logger.debug(
        f"Using initial_lastmod: '{initial_last_modified}' "
        f"and lastmod_info: '{lastmod_info}'"
    )

    transporter_import = V1ImportFormat(
        source=transporter_source,
        target=transporter_target,
        format=transporter_format,
        initial_lastmod=initial_last_modified,
        lastmod_info=lastmod_info,
    )

    logger.debug(f"Transporter import config: {transporter_import}")
    return transporter_import


def _is_wildcard_pattern(pattern: str) -> bool:
    return any(char in pattern for char in "*?")


def _raise_exception_if_no_tzinfo(user_input_datetime: datetime):
    if user_input_datetime.tzinfo is None:
        raise SourceConfigurationError(ErrorCode.SOCE41, user_input_datetime)
