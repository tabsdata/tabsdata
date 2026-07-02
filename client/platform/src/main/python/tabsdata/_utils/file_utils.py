#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import subprocess
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl

from tabsdata._format import (
    AvroFormat,
    CSVFormat,
    FileFormat,
    FileFormatIdentifier,
    LogFormat,
    NDJSONFormat,
    ParquetFormat,
)

if TYPE_CHECKING:
    from tabsdata._io.inputs.file.azure_input import AzureSrc
    from tabsdata._io.inputs.file.gcs_input import GCSSrc
    from tabsdata._io.inputs.file.s3_input import S3Src
from tabsdata._io.inputs.file_inputs import (
    LocalFileSource,
    _convert_avro_to_parquet,
    _is_wildcard_pattern,
    _obtain_source_object,
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
    TransporterCSVFormat,
    TransporterJsonFormat,
    TransporterLocalFile,
    TransporterLogFormat,
    TransporterParquetFormat,
    V1WildCardCopyFormat,
    parse_wildcard_copy_report_yaml,
    store_wildcard_copy_as_yaml,
)
from tabsdata._utils.tableframe.builders import from_polars
from tabsdata.tableframe.lazyframe.frame import TableFrame

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


FORMAT_TO_POLARS_SCAN_FUNCTION = {
    FileFormatIdentifier.CSV.value: pl.scan_csv,
    FileFormatIdentifier.LOG.value: pl.scan_csv,
    FileFormatIdentifier.NDJSON.value: pl.scan_ndjson,
    FileFormatIdentifier.PARQUET.value: pl.scan_parquet,
}

FORMAT_TO_TRANSPORTER_FORMAT = {
    FileFormatIdentifier.CSV.value: TransporterCSVFormat,
    FileFormatIdentifier.LOG.value: TransporterLogFormat,
    FileFormatIdentifier.NDJSON.value: TransporterJsonFormat,
    FileFormatIdentifier.PARQUET.value: TransporterParquetFormat,
    FileFormatIdentifier.AVRO.value: TransporterAvroFormat,
}


def _obtain_transporter_copy(
    origin_location_uri: str,
    destination_folder: str,
    initial_last_modified: str | None,
    user_source: LocalFileSource | S3Src | GCSSrc | AzureSrc,
    lastmod_info: str = None,
):
    # Create the transporter source object
    transporter_source = _obtain_source_object(user_source, origin_location_uri)

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

    transporter_copy = V1WildCardCopyFormat(
        source=transporter_source,
        target=transporter_target,
        initial_lastmod=initial_last_modified,
        lastmod_info=lastmod_info,
    )

    logger.debug(f"Transporter copy config: {transporter_copy}")
    return transporter_copy


def execute_file_copy(
    source: LocalFileSource | S3Src | GCSSrc | AzureSrc,
    destination_folder: str,
) -> tuple[list[str], list[str]]:
    """
    Copy files from a source to a destination. The source can be either a local file
        or an S3 bucket. The destination is always a local folder. The result is a list
        of files that were copied. Each element of the list is a list of paths to
        parquet files.
    :return: A list of files that were copied. Each element of the list is a list
        of paths to the files.
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
        last_modified_string = processed_initial_last_modified.isoformat(
            timespec="microseconds"
        )
        logger.debug(
            f"Last modified time '{last_modified}' converted to "
            f" string '{last_modified_string}'."
        )
        last_modified = last_modified_string
        lastmod_info = source.initial_values.get(OFFSET_LAST_MODIFIED_VARIABLE_NAME)
        if lastmod_info:
            logger.debug("Using stored last modified value")
        else:
            logger.debug("Using decorator last modified value")
    logger.debug(f"Last modified: '{last_modified}'; lastmod_info: '{lastmod_info}'")
    source_list = []
    from_list = []
    for location in location_list:
        sources, from_item, lastmod_info = _execute_single_file_copy(
            origin_location_uri=location,
            destination_folder=destination_folder,
            file_format=source.format,
            initial_last_modified=last_modified,
            user_source=source,
            lastmod_info=lastmod_info,
        )
        source_list.append(sources)
        from_list.append(from_item)
    if source.initial_last_modified:
        logger.debug(f"Capturing new last modified information: '{lastmod_info}'")
        source.initial_values = {OFFSET_LAST_MODIFIED_VARIABLE_NAME: lastmod_info}
    return source_list, from_list


def _execute_single_file_copy(
    origin_location_uri: str,
    destination_folder: str,
    file_format: FileFormat,
    initial_last_modified: str | None,
    user_source: LocalFileSource | S3Src | GCSSrc | AzureSrc,
    lastmod_info: str = None,
) -> tuple[
    list[str | list[str]],
    list[str | list[str]],
    str | None,
]:
    """
    Copy a file from a location to a destination with a specific format. The file is
        copied using a binary, and the result returned is always a list ofthe
        files. If the location contained a wildcard for the files, the list might
        contain one or more elements.
    :return: list of copied files if using a wildcard pattern, single file if not.
    """
    transporter_copy = _obtain_transporter_copy(
        origin_location_uri,
        destination_folder,
        initial_last_modified,
        user_source,
        lastmod_info,
    )

    yaml_request_file = os.path.join(destination_folder, f"request_{uuid.uuid4()}.yaml")
    store_wildcard_copy_as_yaml(
        transporter_copy,
        yaml_request_file,
    )

    binary = "td_transporter1.exe" if CURRENT_PLATFORM.is_windows() else "td_transporter1"
    report_file = os.path.join(destination_folder, f"report_{uuid.uuid4()}.yaml")
    arguments = f"--request {yaml_request_file} --report {report_file}"
    logger.debug(f"Copying files with command: {binary} {arguments}")
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

    result = parse_wildcard_copy_report_yaml(report_file)
    files = result.files
    logger.debug(f"Parsed copy report: {result}")
    if _is_wildcard_pattern(origin_location_uri):
        source_list = []
        from_list = []
        if files:
            for dictionary in files:
                source_list.append(dictionary.get("to"))
                from_list.append(dictionary.get("from"))
            logger.info(f"Copied files to: '{source_list}' from: '{from_list}'")
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
        from_list = files[0].get("from") if files else None
        if not source_list:
            logger.info("No file imported")
        # If the data is not a wildcard pattern, the result is a single file
        else:
            logger.info(f"Imported file to: '{source_list}' from: '{from_list}'")
            if isinstance(file_format, AvroFormat):
                logger.debug("Converting AVRO file to Parquet format")
                source_list = _convert_avro_to_parquet(
                    source_list, destination_folder, file_format
                )
                logger.debug(f"Converted AVRO file to Parquet format: '{source_list}'")
    logger.debug(f"New lastmod_info: '{result.lastmod_info}'")
    return source_list, from_list, result.lastmod_info


def _apply_log_transform(lf: pl.LazyFrame, from_path: str) -> pl.LazyFrame:
    """Transform a log LazyFrame to have 'file' and 'message' columns."""
    file_name = from_path.rsplit("/", 1)[-1]
    return lf.select([
        pl.lit(file_name).alias("file"),
        pl.nth(0).alias("message"),
    ])


def _add_file_info_columns(lf: pl.LazyFrame, from_path: str, file_info) -> pl.LazyFrame:
    """Add origin file info columns to a LazyFrame based on the file_info setting."""
    from tabsdata._info import FileInfo

    if file_info == FileInfo.NAME:
        file_value = from_path.rsplit("/", 1)[-1]
    elif file_info == FileInfo.URI:
        file_value = from_path
    else:
        return lf
    return lf.with_row_index("@td.origin.file.line", offset=1).with_columns(
        pl.lit(file_value).alias("@td.origin.file.path")
    )


def get_tableframes_from_file_sources(
    file_sources: list[list[str] | str],
    from_paths: list[list[str] | str],
    format: FileFormat | None = None,
    file_info=None,
    infer_schema_length: int | None = None,
) -> list[TableFrame | list[TableFrame]]:
    """
    Given a list of file input sources, where each source is a list of parquet file
    paths, concatenate the files for each source into a TableFrame.

    Args:
        file_sources (list[list[str] | str]): List of sources, each a list of parquet
            file paths.
        from_paths (list[list[str] | str]): List of original source URIs, parallel to
            file_sources.
        format (FileFormat | None): The file format to use for scanning.
        file_info (FileInfo | None): If set, adds @td.origin.file.path and
            @td.origin.file.line columns to each TableFrame.
        infer_schema_length (int | None): The maximum number of rows to scan for
            schema inference. If set to None, the full data will be scanned.
    Returns:
        list[TableFrame]: List of TableFrames, one per source.
    """
    table_frames: list[TableFrame | list[TableFrame]] = []
    scan_method = FORMAT_TO_POLARS_SCAN_FUNCTION.get(format.IDENTIFIER, pl.scan_parquet)
    format_options = get_format_options(format, infer_schema_length) if format else None

    for source, from_path in zip(file_sources, from_paths):
        logger.debug(f"Processing source: '{source}'")

        def _scan_and_transform(file_path: str, origin_path: str) -> pl.LazyFrame:
            lf = scan_method(
                file_path,
                **(format_options if format_options else {}),
            )
            if isinstance(format, LogFormat):
                lf = _apply_log_transform(lf, origin_path)
            if file_info:
                lf = _add_file_info_columns(lf, origin_path, file_info)
            return lf

        if isinstance(source, str):
            tf = from_polars(_scan_and_transform(source, from_path))
            table_frames.append(tf)
        elif isinstance(source, list):
            from_path_list = from_path if isinstance(from_path, list) else [from_path]
            lfs = [
                _scan_and_transform(f, fp)
                for f, fp in zip(source, from_path_list)
                if isinstance(f, str)
            ]
            table_frames.append([from_polars(lf) for lf in lfs])
        elif source is None:
            logger.debug("Source is None")
            table_frames.append(None)
        else:
            raise TypeError(
                f"Unexpected source type: {type(source)}. Source: '{source}'. "
                f"Expected str or list of str."
            )

    logger.debug(f"Returning {len(table_frames)} TableFrames")
    return table_frames


def get_format_options(
    file_format: FileFormat,
    infer_schema_length: int | None = None,
) -> dict:
    if isinstance(file_format, CSVFormat):
        format_options = {
            "separator": str(file_format.separator),
            "quote_char": str(file_format.quote_char),
            "eol_char": str(file_format.eol_char),
            "encoding": str(file_format.input_encoding).lower(),
            "null_values": file_format.input_null_values,
            "missing_utf8_is_empty_string": (
                (not file_format.input_missing_is_null)
                if file_format.input_missing_is_null is not None
                else True
            ),
            "truncate_ragged_lines": file_format.input_truncate_ragged_lines,
            "comment_prefix": file_format.input_comment_prefix,
            "try_parse_dates": file_format.input_try_parse_dates,
            "decimal_comma": file_format.input_decimal_comma,
            "has_header": file_format.input_has_header,
            "skip_rows": file_format.input_skip_rows,
            "skip_rows_after_header": file_format.input_skip_rows_after_header,
            "raise_if_empty": file_format.input_raise_if_empty,
            "ignore_errors": file_format.input_ignore_errors,
            "infer_schema_length": infer_schema_length,
        }
    elif isinstance(file_format, LogFormat):
        format_options = {
            "separator": "\0",
            "has_header": False,
            "quote_char": None,
            "infer_schema_length": infer_schema_length,
        }
    elif isinstance(file_format, NDJSONFormat):
        format_options = {
            "infer_schema_length": infer_schema_length,
        }
    elif isinstance(file_format, (ParquetFormat, AvroFormat)):
        format_options = {}
    else:
        raise TypeError(f"Unsupported file format: {file_format}")
    logger.debug(f"Converted file format '{file_format}' to dict '{format_options}'")
    return format_options
