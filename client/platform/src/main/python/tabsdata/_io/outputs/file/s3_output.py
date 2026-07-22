#
# Copyright 2026 Tabsdata Inc.
#


import logging
from enum import Enum
from urllib.parse import quote

from tabsdata._format import (
    AvroFormat,
    CSVFormat,
    FileFormat,
    NDJSONFormat,
    ParquetFormat,
    build_file_format,
    get_implicit_format_from_list,
)
from tabsdata._io.connections.file_connections.s3_connection import S3Conn
from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.outputs.file_outputs import (
    AWSGlue,
    _store_results_in_intermediate_files,
    _write_results_in_final_files,
    build_catalog,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._tabsserver.function.cloud_connectivity_utils import (
    obtain_and_set_s3_credentials,
    set_s3_region,
)
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode

logger = logging.getLogger(__name__)


class S3Dest(DestinationPlugin):
    """Write to S3. Required: conn (S3Conn), path (str or list[str]).
    Destination plugin for writing data to an S3 bucket.
    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the S3Destination.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    @_td_validate_call(ErrorCode.DECE111)
    def __init__(
        self,
        conn: S3Conn | NamedConn,
        path: str | list[str],
        format: str | FileFormat | None = None,
        catalog: AWSGlue | None = None,
    ):
        """
        Initialize an S3Dest instance for writing data to an S3 bucket.

        Args:
            conn: The S3 connection configuration.
            path: The file path(s) within the S3 bucket with format 'path/to/files'. It
                can be a single path or a list of paths for multiple files.
            format: The format of the file. If not
                provided, it will be inferred from the file extension.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'avro'.
            catalog: The AWS Glue catalog configuration for this destination, if
                provided.
        """
        self.conn = conn
        self.path = path
        self.format = format
        self.catalog = catalog

    @property
    def conn(self) -> S3Conn | NamedConn:
        """
        The S3 connection configuration for this destination.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.DECE111)
    def conn(self, conn: S3Conn | NamedConn):
        """
        Set the S3 connection configuration for this destination.

        Args:
            conn: The S3 connection configuration to set.
        """
        self._conn = conn
        self._rebuild_full_uri()

    @property
    def path(self) -> list[str]:
        """
        The file path(s) within the S3 bucket where the data will be written.
        """
        return self._path

    @path.setter
    @_td_validate_call(ErrorCode.DECE111)
    def path(self, path: str | list[str]):
        """
        Set the file path(s) within the S3 bucket where the data will be written.

        Args:
            path: The file path(s) to set.
        """
        if isinstance(path, str):
            self._path = [path]
        else:
            self._path = path

        self._rebuild_full_uri()

    def _rebuild_full_uri(self):
        """
        Rebuild the internal URI list from the current path and connection.

        The conn setter runs before the path setter during __init__, so _path may
        not exist yet on the first call. When conn is a NamedConn placeholder, the
        bucket URI is not yet known; URIs will be built when the NamedConn is
        replaced with a resolved S3Conn.
        """
        if not hasattr(self, "_path"):
            return
        if not hasattr(self._conn, "bucket_uri"):
            self._full_uri = []
            return
        self._full_uri = []
        for p in self._path:
            if p.startswith("/"):
                p = p[1:]
            full_uri = f"{self._conn.bucket_uri}/{quote(p, safe='/$')}"
            self._full_uri.append(full_uri)

    @property
    def format(self) -> FileFormat:
        """
        The file format to use when writing data to the S3 bucket.
        """
        return self._format or build_file_format(self._implicit_format)

    def _validate_format(self, format: str | FileFormat | None) -> FileFormat | None:
        """
        Validate the file format provided for writing data to the S3 bucket.

        Args:
            format: The file format to validate.
        Returns:
            The validated file format as a FileFormat enum member.
        """
        if format is None:
            self._implicit_format = get_implicit_format_from_list(self._path)
            self._verify_valid_format(build_file_format(self._implicit_format))
        else:
            format = build_file_format(format)
            self._verify_valid_format(format)
        return format

    def _verify_valid_format(self, format: FileFormat):
        """
        Verifies that the provided format is valid for the S3Dest

        Args:
            format: The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise DestinationConfigurationError(
                ErrorCode.DECE13, type(format), valid_output_formats
            )

    @format.setter
    @_td_validate_call(ErrorCode.DECE111)
    def format(self, format: str | FileFormat | None):
        """
        Set the file format for this destination.

        Args:
            format: The file format to set, either as a string or a FileFormat enum
            member.
        """
        self._format = self._validate_format(format)

    @property
    def catalog(self) -> AWSGlue | None:
        """
        The AWS Glue catalog configuration for this destination, if provided.
        """
        return self._catalog

    @catalog.setter
    @_td_validate_call(ErrorCode.DECE111)
    def catalog(self, catalog: AWSGlue | None):
        """
        Set the AWS Glue catalog configuration for this destination.

        Args:
            catalog: The AWS Glue catalog configuration to set.
        """
        self._catalog = self._validate_catalog(catalog)

    def _validate_catalog(self, catalog: AWSGlue | None) -> AWSGlue | None:
        """
        Validate the AWS Glue catalog configuration for this destination.

        Args:
            catalog: The AWS Glue catalog configuration to validate.

        """
        if catalog is None:
            return None
        else:
            catalog = build_catalog(catalog)
            if not isinstance(self.format, ParquetFormat):
                raise DestinationConfigurationError(
                    ErrorCode.DECE37, ParquetFormat, self.format
                )
            return catalog

    @property
    def allow_fragments(self) -> bool:
        """
        bool: Whether to allow fragments in the output.
        """
        return True

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
        """
        Store the results in intermediate files, obtain and set S3 credentials, and
        write the results to the final files in the S3 bucket.
        """
        logger.debug(
            f"Triggering stream for {self} with working_dir: {working_dir} and results:"
            f" {results}"
        )

        intermediate_files = _store_results_in_intermediate_files(
            results, self, working_dir
        )

        obtain_and_set_s3_credentials(self.conn.credentials)
        set_s3_region(self.conn.region)
        _write_results_in_final_files(
            results, intermediate_files, self, working_dir, self._ec.request
        )
        logger.debug(f"Finished stream for {self}")

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(conn={self.conn}, path={self._path}, "
            f"format={self.format}, catalog={self.catalog})"
        )
