#
# Copyright 2026 Tabs Data Inc.
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
from tabsdata._io.connections.file_connections.azure_connection import AzureConn
from tabsdata._io.outputs.file_outputs import (
    FRAGMENT_INDEX_PLACEHOLDER,
    _store_results_in_intermediate_files,
    _write_results_in_final_files,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._tabsserver.function.cloud_connectivity_utils import (
    obtain_and_set_azure_credentials,
)
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import DestinationConfigurationError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class AzureDest(DestinationPlugin):
    """
    Destination plugin for writing data to an Azure Blob Storage container.
    """

    class SupportedFormats(Enum):
        """
        Enum for the supported formats for the AzureDest.
        """

        avro = AvroFormat
        csv = CSVFormat
        ndjson = NDJSONFormat
        parquet = ParquetFormat

    @_td_validate_call(ErrorCode.DECE116)
    def __init__(
        self,
        conn: AzureConn,
        path: str | list[str],
        format: str | FileFormat | None = None,
    ):
        """
        Initialize an AzureDest instance for writing data to Azure Blob Storage.

        Args:
            conn: The Azure connection configuration.
            path: The file path(s) within the Azure container with format
                'path/to/files'. It can be a single
                path or a list of paths for multiple files.
            format: The format of the file. If not
                provided, it will be inferred from the file extension.
                Can be either a string with the format or a FileFormat object.
                Currently supported formats are 'csv',
                'parquet', 'ndjson' and 'avro'.
        """

        self.conn = conn
        self.path = path
        self.format = format

    @property
    def conn(self) -> AzureConn:
        """
        The Azure connection configuration for this destination.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.DECE116)
    def conn(self, conn: AzureConn):
        """
        Set the Azure connection configuration for this destination.

        Args:
            conn: The Azure connection configuration to set.
        """
        self._conn = conn

    @property
    def path(self) -> list[str]:
        """
        The file path(s) within the Azure container where the data will be written.
        """
        return self._path

    @path.setter
    @_td_validate_call(ErrorCode.DECE116)
    def path(self, path: str | list[str]):
        """
        Set the file path(s) within the Azure container where the data will be written.

        Args:
            path: The file path(s) to set.
        """
        if isinstance(path, str):
            self._path = [path]
        else:
            self._path = path

        self._full_uri = []
        for p in self._path:
            if p.startswith("/"):
                p = p[1:]
            full_uri = f"{self.conn.container_uri}/{quote(p, safe='/$')}"
            self._full_uri.append(full_uri)

        if not self.allow_fragments:
            for p in self._path:
                if FRAGMENT_INDEX_PLACEHOLDER in p:
                    raise DestinationConfigurationError(
                        ErrorCode.DECE38,
                        FRAGMENT_INDEX_PLACEHOLDER,
                        p,
                    )

    @property
    def format(self) -> FileFormat:
        """
        FileFormat: The format of the file. If not provided, it will be inferred from
            the file extension of the path.
        """
        return self._format or build_file_format(self._implicit_format)

    @format.setter
    @_td_validate_call(ErrorCode.DECE116)
    def format(self, format: str | FileFormat | None):
        """
        Set the file format for this destination.

        Args:
            format: The file format to set. If not provided, the format will be
                inferred from the path extension.
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
        Verifies that the provided format is valid for the AzureDest.

        Args:
            format: The format to verify.
        """
        valid_output_formats = tuple(element.value for element in self.SupportedFormats)
        if not (isinstance(format, valid_output_formats)):
            raise DestinationConfigurationError(
                ErrorCode.DECE13, type(format), valid_output_formats
            )

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
        """
        Store the results in intermediate files, obtain and set Azure credentials, and
        write the results to the final files in Azure Blob Storage.

        Args:
            working_dir: The working directory to use for intermediate files.
            *results: The results to stream.
        """
        logger.debug(
            f"Triggering stream for {self} with working_dir: {working_dir} and results:"
            f" {results}"
        )
        intermediate_files = _store_results_in_intermediate_files(
            results, self, working_dir
        )

        obtain_and_set_azure_credentials(self.conn.credentials)

        _write_results_in_final_files(
            results, intermediate_files, self, working_dir, self._ec.request
        )
        logger.debug(f"Completed stream for {self}")

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(conn={self.conn}, path={self.path}, "
            f"format={self.format})"
        )
