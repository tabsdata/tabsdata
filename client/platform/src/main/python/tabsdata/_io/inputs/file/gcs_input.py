#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from urllib.parse import quote

from pydantic import PositiveInt

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
from tabsdata._info import FileInfo, validate_file_info
from tabsdata._io.connections.file_connections.gcp_connection import GCSConn
from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.plugin import SourcePlugin
from tabsdata._tabsserver.function.cloud_connectivity_utils import (
    obtain_and_set_gcp_credentials,
)
from tabsdata._tabsserver.function.offset_utils import (
    OFFSET_LAST_MODIFIED_VARIABLE_NAME,
)
from tabsdata._utils.datetime import (
    NoTimezoneError,
    datetime_to_microseconds_iso_str,
    normalize_iso_datetime,
)
from tabsdata._utils.file_utils import (
    execute_file_copy,
    get_tableframes_from_file_sources,
)
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ErrorCode,
    SourceConfigurationError,
)
from tabsdata.tableframe.lazyframe.frame import TableFrame

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GCSSrc(SourcePlugin):
    """Read from Google Cloud Storage.

    Required: conn (GCSConn), path (str or list[str]).
    Source plugin for reading data from a GCS bucket.
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

    @_td_validate_call(ErrorCode.SOCE114)
    def __init__(
        self,
        conn: GCSConn | NamedConn,
        path: str | list[str],
        format: str | FileFormat | None = None,
        initial_last_modified: str | datetime | None = None,
        file_info: str | FileInfo | None = None,
        infer_schema_length: PositiveInt | None = 100,
    ):
        """
        Initialize a GCSSrc instance for reading data from a GCS bucket.

        Args:
            conn: The GCS connection configuration.
            path: The file path(s) within the GCS bucket with format 'path/to/file'. It
                can be a single path or a list of paths for multiple files.
            format: The format of the file. If not
                provided, it will be inferred from the file extension of the data.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'avro', 'ndjson', and 'log'.
            initial_last_modified: If provided,
                only the files modified after this date and time will be considered.
                The date and time can be provided as a string in
                [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) or as
                a datetime object.
            file_info: If provided, adds origin file information as system columns.
                'name' adds the file name, 'uri' adds the full URI. Default is None.
            infer_schema_length: The maximum number of rows to scan for schema
                inference. If set to None, the full data will be scanned.
        """
        self.conn = conn
        self.path = path
        self.format = format
        self.initial_last_modified = initial_last_modified
        self.file_info = file_info
        self.infer_schema_length = infer_schema_length

    @property
    def conn(self) -> GCSConn | NamedConn:
        """
        The GCS connection configuration for this source.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.SOCE114)
    def conn(self, conn: GCSConn | NamedConn):
        """
        Set the GCS connection configuration for this source.

        Args:
            conn: The GCS connection configuration to set.
        """
        self._conn = conn
        self._rebuild_uri_list()

    @property
    def path(self) -> list[str]:
        """
        The file path(s) within the GCS bucket where the data will be read from.
        """
        return self._path

    @path.setter
    @_td_validate_call(ErrorCode.SOCE114)
    def path(self, path: str | list[str]):
        """
        Set the file path(s) within the GCS bucket where the data will be read from.

        Args:
            path: The file path(s) within the GCS bucket with format 'path/to/file'. It
                can be a single path or a list of paths for multiple files.
        """
        if isinstance(path, str):
            self._path = [path]
        else:
            self._path = path

        self._rebuild_uri_list()

    def _rebuild_uri_list(self):
        """
        Rebuild the internal URI list from the current path and connection.

        The conn setter runs before the path setter during __init__, so _path may
        not exist yet on the first call. When the connection is a placeholder
        without a bucket_uri (e.g. a NamedConn not yet resolved), URIs will be
        built when the placeholder is replaced with a resolved GCSConn.
        """
        if not hasattr(self, "_path"):
            return
        if not hasattr(self._conn, "bucket_uri"):
            self._uri_list = []
            return
        self._uri_list = []
        for p in self._path:
            if p.startswith("/"):
                p = p[1:]
            full_uri = f"{self._conn.bucket_uri}/{quote(p, safe='/$*')}"
            self._uri_list.append(full_uri)

    @property
    def format(self) -> FileFormat:
        """
        The format of the file(s). If not provided, it will be inferred from
            the file extension of the path.
        """
        return self._format or build_file_format(self._implicit_format)

    @format.setter
    @_td_validate_call(ErrorCode.SOCE114)
    def format(self, format: str | FileFormat | None):
        """
        Set the format of the file(s).

        Args:
            format: The format to set. Can be either a string with the format or a
                FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'avro', 'ndjson', and 'log'. If not provided, it will be
                inferred from the file extension of the path.
        """
        if format is None:
            self._format = None
            self._implicit_format = get_implicit_format_from_list(self._path)
            self._verify_valid_format(build_file_format(self._implicit_format))
        else:
            format = build_file_format(format)
            self._verify_valid_format(format)
            self._format = format

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the GCSSrc.

        Args:
            format: The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise SourceConfigurationError(
                ErrorCode.SOCE4, type(format), valid_output_formats
            )

    @property
    def initial_last_modified(self) -> str:
        """
        str: The date and time after which the files were modified.
        """
        return datetime_to_microseconds_iso_str(self._initial_last_modified)

    @initial_last_modified.setter
    @_td_validate_call(ErrorCode.SOCE114)
    def initial_last_modified(self, initial_last_modified: str | datetime | None):
        """
        Sets the date and time after which the files were modified.

        Args:
            initial_last_modified: The date and time after
                which the files were modified. The date and time can be provided as a
                string in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
                or as a datetime object.
        """
        try:
            self._initial_last_modified = normalize_iso_datetime(initial_last_modified)
        except ValueError:
            raise SourceConfigurationError(ErrorCode.SOCE5, initial_last_modified)
        except TypeError:
            raise SourceConfigurationError(ErrorCode.SOCE6, type(initial_last_modified))
        except NoTimezoneError:
            raise SourceConfigurationError(ErrorCode.SOCE41, initial_last_modified)

    @property
    def initial_values(self) -> dict:
        """
        The initial values for the source.
        """
        if hasattr(self, "_initial_values"):
            return self._initial_values

        if self.initial_last_modified:
            return {
                OFFSET_LAST_MODIFIED_VARIABLE_NAME: None,
            }
        else:
            return {}

    @initial_values.setter
    @_td_validate_call(ErrorCode.SOCE114)
    def initial_values(self, new_values: dict | None):
        """
        Set the initial values for the source.

        Args:
            new_values: The initial values to set.
        """
        self._initial_values = new_values

    @property
    def file_info(self) -> FileInfo | None:
        """
        The file information to be added as system columns.
        """
        return self._file_info

    @file_info.setter
    @_td_validate_call(ErrorCode.SOCE114)
    def file_info(self, file_info: str | FileInfo | None):
        """
        Set the file information to be added as system columns.

        Args:
            file_info: The file information to set. 'name' adds the file name, 'uri'
                adds the full URI. Default is None.
        """
        self._file_info = validate_file_info(file_info)

    @property
    def infer_schema_length(self) -> PositiveInt | None:
        """
        The maximum number of rows to scan for schema inference. If set to None, the
            full data will be scanned.
        """
        return self._infer_schema_length

    @infer_schema_length.setter
    @_td_validate_call(ErrorCode.SOCE114)
    def infer_schema_length(self, infer_schema_length: PositiveInt | None):
        """
        Set the maximum number of rows to scan for schema inference.

        Args:
            infer_schema_length: The maximum number of rows to scan for schema
                inference. If set to None, the full data will be scanned.
        """
        self._infer_schema_length = infer_schema_length

    def stream(self, working_dir: str) -> list[TableFrame]:
        """
        Load the data from the GCS bucket based on the provided configuration and return
            it as a list of TableFrames.

        Args:
            working_dir: The directory where the parquet files will be saved.
        Returns:
            A list of TableFrames containing the data read from the GCS bucket.
        """
        logger.debug(f"Triggering {self} stream with working_dir: {working_dir}")

        obtain_and_set_gcp_credentials(self.conn.credentials)
        file_sources, from_paths = execute_file_copy(self, working_dir)
        logger.debug(f"Obtained file sources: '{file_sources}'")

        tableframes = get_tableframes_from_file_sources(
            file_sources,
            from_paths=from_paths,
            format=self.format,
            file_info=self.file_info,
            infer_schema_length=self.infer_schema_length,
        )
        logger.debug(f"Obtained {len(tableframes)} tableframes from {self}")
        return tableframes
