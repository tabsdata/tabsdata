#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from tabsdata._io import constants as io_constants
from tabsdata._io.inputs.file_inputs import (
    AzureSource,
    GCSSource,
    LocalFileSource,
    S3Source,
)
from tabsdata._io.plugin import SourcePlugin
from tabsdata._tabsserver.function.global_utils import convert_uri_to_path
from tabsdata._tabsserver.function.lastmod_info_parsing import LastModInfo
from tabsdata._tabsserver.function.offset_utils import (
    OFFSET_LAST_MODIFIED_VARIABLE_NAME,
)
from tabsdata._utils.datetime import _convert_timestamp_to_string
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import (
    ErrorCode,
)

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata.tableframe.lazyframe.frame import TableFrame

logger = logging.getLogger(__name__)

STAGE_MANIFEST_FILE_EXTENSION = ".manifest.yaml"
STAGE_MANIFEST_FILE_PATTERN = f"*{STAGE_MANIFEST_FILE_EXTENSION}"
STAGE_MANIFEST_OFFSET_KEY = "stage_manifest_last_consumed"

STAGE_MANIFEST_VERSION_1 = 1
STAGE_MANIFEST_LATEST_VERSION = STAGE_MANIFEST_VERSION_1

STAGE_MANIFEST_KEY_VERSION = "version"
STAGE_MANIFEST_KEY_DATA = "data"
STAGE_MANIFEST_KEY_TABLE = "table"
STAGE_MANIFEST_KEY_FILES = "files"


@dataclass
class StageManifestEntry:
    table: str
    files: list[str] = field(default_factory=list)


@dataclass
class StageManifest:
    version: int = STAGE_MANIFEST_LATEST_VERSION
    entries: list[StageManifestEntry] = field(default_factory=list)


class Stage(SourcePlugin):
    """
    :categories: source

    Inputs that have been placed in a stage, usually from a StageTrigger.
    """

    @_td_validate_call(ErrorCode.SOCE110)
    def __init__(
        self,
        use_existing_data: bool = True,
    ):
        """
        Initializes the Stage plugin with the provided parameters.

        Args:
            use_existing_data: If and when the publisher is updated, whether to consume
                the files that existed in the stage before the update.
        """
        self.use_existing_data = use_existing_data

    @property
    def use_existing_data(self) -> bool:
        return self._use_existing_data

    @use_existing_data.setter
    @_td_validate_call(ErrorCode.SOCE110)
    def use_existing_data(self, value: bool):
        self._use_existing_data = value

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
        self, working_dir: str
    ) -> list[TableFrame | None | list[TableFrame | None]]:
        logger.debug(f"Triggering {self}")
        execution_context: ExecutionContext = self._ec
        uri = _obtain_uri(execution_context)
        configs = _obtain_configs(execution_context)
        registration_time = _obtain_registration_time(execution_context)
        if not uri.find(io_constants.URI_INDICATOR):
            raise ValueError(
                f"Received URI value {uri}, which does not contain the "
                f"uri indicator {io_constants.URI_INDICATOR} and is "
                "therefore not a valid URI. This is most likely due to "
                "an internal error of the Tabsdata server."
            )
        prefix = uri.split(io_constants.URI_INDICATOR)[0]
        match prefix:
            case io_constants.S3_SCHEME:
                auxiliary_plugin = _build_s3_source(uri, configs, registration_time)
            case io_constants.AZURE_SCHEME:
                auxiliary_plugin = _build_azure_source(uri, configs, registration_time)
            case io_constants.GCS_SCHEME:
                auxiliary_plugin = _build_gcs_source(uri, configs, registration_time)
            case io_constants.FILE_SCHEME:
                auxiliary_plugin = _build_local_source(uri, configs, registration_time)
            case _:
                raise ValueError(
                    f"Unsupported URI scheme: {prefix}. "
                    "Currently, the supported staging storage locations "
                    "are Azure, GCS, LocalFileSystem and S3."
                )
        logger.debug(f"Obtained auxiliary source {auxiliary_plugin}. Triggering it")
        lastmod_info_datetime = _obtain_lastmod_info_datetime(execution_context)
        if self.use_existing_data:
            logger.debug(
                "Stage configuration 'use_existing_data' is set to True, will use"
                f" registration time '{registration_time}' and last execution time"
                f" '{lastmod_info_datetime}'."
            )
        else:
            logger.debug(
                "Stage configuration 'use_existing_data' is set to False, "
                "calculating the time to use."
            )
            if lastmod_info_datetime is None:
                logger.debug(
                    "There is no previous execution recorded, "
                    f"so registration time {registration_time} will be used."
                )
                self._ec.status.offset.use_decorator_values = True
            else:
                if datetime.fromisoformat(
                    lastmod_info_datetime
                ) > datetime.fromisoformat(registration_time):
                    logger.debug(
                        f"The last execution time '{lastmod_info_datetime}' "
                        "is latter than the registration time '"
                        f"{registration_time}'. Therefore, the last "
                        "execution time will be used as the offset."
                    )
                    self._ec.status.offset.use_decorator_values = False
                else:
                    logger.debug(
                        f"The last execution time '{lastmod_info_datetime}' "
                        "is earlier than the registration time '"
                        f"{registration_time}'. Therefore, the registration "
                        "time will be used as the offset."
                    )
                    self._ec.status.offset.use_decorator_values = True
        params = auxiliary_plugin._run(execution_context)
        logger.debug(
            f"Copying final initial values from the auxiliary plugin {auxiliary_plugin}"
        )
        self.initial_values = auxiliary_plugin.initial_values
        logger.debug(f"Copied initial values {self.initial_values}")
        return params

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}(use_existing_data={self.use_existing_data})"


def _obtain_uri(execution_context: ExecutionContext) -> str:
    uri = execution_context.request.function_stage_uri
    if not uri:
        raise ValueError(
            "Error while trying to obtain the staging URI from the "
            f"request file. Obtained '{uri}' instead of a valid URI."
        )
    uri = uri if uri.endswith("/") else uri + "/"
    uri = uri + "*.parquet"
    return uri


def _obtain_configs(execution_context: ExecutionContext) -> dict:
    storage_options = {}
    location = execution_context.request.function_stage
    if prefix := location.env_prefix:
        logger.debug(f"Using prefix '{prefix}' for the staging options.")
        storage_options = execution_context.mount_options.get_options_for_prefix(prefix)
    return storage_options


def _obtain_registration_time(execution_context: ExecutionContext) -> str | None:
    registration_time = execution_context.request.function_defined_on
    if not registration_time:
        return None
    return _convert_timestamp_to_string(registration_time)


def _build_s3_source(uri: str, configs: dict, registration_time: str | None):
    raise NotImplementedError(
        "Staging files to S3 is currently not supported. The only "
        "supported staging storage is LocalFileSystem."
    )
    logger.debug(
        f"Staging location for {uri} identified as S3. Building auxiliary "
        "source to obtain the files."
    )
    aux_plugin = S3Source(uri=uri, initial_last_modified=registration_time)
    return aux_plugin


def _build_azure_source(uri: str, configs: dict, registration_time: str | None):
    raise NotImplementedError(
        "Staging files to Azure is currently not supported. The only "
        "supported staging storage is LocalFileSystem."
    )
    logger.debug(
        f"Staging location for {uri} identified as Azure. Building auxiliary "
        "source to obtain the files."
    )
    aux_plugin = AzureSource(uri=uri, initial_last_modified=registration_time)
    return aux_plugin


def _build_gcs_source(uri: str, configs: dict, registration_time: str | None):
    raise NotImplementedError(
        "Staging files to GCS is currently not supported. The only "
        "supported staging storage is LocalFileSystem."
    )
    logger.debug(
        f"Staging location for {uri} identified as GCS. Building auxiliary "
        "source to obtain the files."
    )
    aux_plugin = GCSSource(uri=uri, initial_last_modified=registration_time)
    return aux_plugin


def _build_local_source(uri: str, configs: dict, registration_time: str | None):
    logger.debug(
        f"Staging location for {uri} identified as LocalFileSystem. Building "
        "auxiliary source to obtain the files."
    )
    aux_plugin = LocalFileSource(path=uri, initial_last_modified=registration_time)
    return aux_plugin


def _obtain_lastmod_info_datetime(execution_context: ExecutionContext):
    logger.debug("Extracting the time of the last execution.")
    current_offset = execution_context.status.offset.current_offset
    lastmod_info = current_offset.get(OFFSET_LAST_MODIFIED_VARIABLE_NAME)
    if lastmod_info is None:
        return None
    lastmod_info = LastModInfo(lastmod_info)
    if len(lastmod_info.entries) > 1:
        raise ValueError(
            "Error trying to obtain the time of the last execution. The "
            f"JSON containing the information is {lastmod_info}"
        )
    elif len(lastmod_info.entries) == 0:
        logger.debug("No last offset found. Using the registration time as the offset.")
        return None
    current_entry = lastmod_info.entries[0]
    lastmod_info_str_datetime = current_entry.time
    logger.debug(
        f"Found last offset '{lastmod_info_str_datetime}' for entry {current_entry}"
    )
    return lastmod_info_str_datetime


def _read_manifest_file(manifest_path: str) -> str:
    """Reads a manifest YAML file and returns its contents as a string."""
    logger.debug(f"Reading manifest file from '{manifest_path}'")
    with open(manifest_path, "r") as f:
        return f.read()


def _write_manifest_file(manifest_path: str, content: str) -> None:
    """Writes a YAML string to a manifest file."""
    logger.debug(f"Writing manifest file to '{manifest_path}'")
    with open(manifest_path, "w") as f:
        f.write(content)


def _serialize_manifest(manifest: StageManifest) -> str:
    """
    Serializes a :class:`StageManifest` to a YAML string.

    Always writes using the latest version format.
    """
    return _serialize_manifest_v1(manifest)


def _deserialize_manifest(content: str) -> StageManifest:
    """
    Deserializes a YAML string into a :class:`StageManifest`.

    Dispatches to the appropriate version-specific deserializer based on
    the ``version`` key in the YAML. Content without a ``version`` key is
    treated as :data:`STAGE_MANIFEST_LATEST_VERSION`.
    """
    data = yaml.safe_load(content)
    if data is None:
        logger.debug("Manifest content is empty")
        return StageManifest()
    version = data.pop(
        STAGE_MANIFEST_KEY_VERSION,
        STAGE_MANIFEST_LATEST_VERSION,
    )
    match version:
        case 1:
            return _deserialize_manifest_v1(data)
        case _:
            raise ValueError(f"Unsupported manifest version: {version}")


def _serialize_manifest_v1(manifest: StageManifest) -> str:
    """Serializes a :class:`StageManifest` in version ``_MANIFEST_VERSION_1``
    format."""
    entries: dict = {}
    for counter, entry in enumerate(manifest.entries):
        entries[counter] = {
            STAGE_MANIFEST_KEY_TABLE: entry.table,
            STAGE_MANIFEST_KEY_FILES: entry.files,
        }
    data: dict = {
        STAGE_MANIFEST_KEY_VERSION: STAGE_MANIFEST_VERSION_1,
        STAGE_MANIFEST_KEY_DATA: entries,
    }
    return yaml.dump(data, default_flow_style=False, sort_keys=False)


def _deserialize_manifest_v1(data: dict) -> StageManifest:
    """
    Deserializes version ``_MANIFEST_VERSION_1`` format::

        version: <_MANIFEST_VERSION_1>
        data:
            0:
                table: schema.table_name
                files:
                    - path/to/file.parquet
            1:
                table: schema.other_table
                files:
                    - second/path/to/file.parquet
                    - third/path/to/file.parquet
    """
    numbered_entries = data.get(STAGE_MANIFEST_KEY_DATA, {})
    entries: list[StageManifestEntry] = []
    for key in sorted(numbered_entries.keys()):
        entry = numbered_entries[key]
        table = entry.get(STAGE_MANIFEST_KEY_TABLE, "")
        files = entry.get(STAGE_MANIFEST_KEY_FILES, [])
        if isinstance(files, str):
            files = [files]
        entries.append(StageManifestEntry(table=table, files=files))
    logger.debug(
        f"Manifest v{STAGE_MANIFEST_VERSION_1} contains {len(entries)} entry/entries"
    )
    return StageManifest(version=STAGE_MANIFEST_VERSION_1, entries=entries)


def _build_manifest_s3_source(file_paths: list[str], configs: dict):
    raise NotImplementedError(
        "ManifestStage with S3 staging is currently not supported. The only "
        "supported staging storage is LocalFileSystem."
    )


def _build_manifest_azure_source(file_paths: list[str], configs: dict):
    raise NotImplementedError(
        "ManifestStage with Azure staging is currently not supported. The only "
        "supported staging storage is LocalFileSystem."
    )


def _build_manifest_gcs_source(file_paths: list[str], configs: dict):
    raise NotImplementedError(
        "ManifestStage with GCS staging is currently not supported. The only "
        "supported staging storage is LocalFileSystem."
    )


def _build_manifest_local_source(file_paths: list[str], configs: dict):
    logger.debug(f"Building local source for {len(file_paths)} manifest file(s).")
    return LocalFileSource(path=file_paths)


def _parse_utc_datetime(dt_str: str) -> datetime:
    """Parses a datetime string into a timezone-aware UTC datetime.

    Handles both ``'Z'`` suffix (e.g. from ``_convert_timestamp_to_string``)
    and ``'+00:00'`` suffix (e.g. from ``datetime.isoformat()``).
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.fromisoformat(dt_str)


def _max_nullable_datetime(a: datetime | None, b: datetime | None) -> datetime | None:
    """Returns the maximum of two datetimes, treating None as absent."""
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


class ManifestStage(SourcePlugin):
    """
    :categories: source

    A source plugin that consumes files listed in a manifest file placed in
    the staging folder by a StageTrigger, rather than consuming all Parquet
    files in the folder.

    Each group of files corresponds to one input argument of the publisher
    function.
    """

    @_td_validate_call(ErrorCode.SOCE110)
    def __init__(
        self,
        use_existing_data: bool = True,
    ):
        """
        Initializes the ManifestStage plugin.

        Args:
            use_existing_data: If and when the publisher is updated, whether to
                consume the files that existed in the stage before the update.
        """
        self.use_existing_data = use_existing_data

    @property
    def use_existing_data(self) -> bool:
        return self._use_existing_data

    @use_existing_data.setter
    @_td_validate_call(ErrorCode.SOCE110)
    def use_existing_data(self, value: bool):
        self._use_existing_data = value

    @property
    def _stream_require_ec(self) -> bool:
        return True

    def stream(
        self, working_dir: str
    ) -> list[TableFrame | None | list[TableFrame | None]]:
        """
        Reads the manifest file from the staging folder, resolves the listed
        files, and returns them as TableFrames.

        Unlike :class:`Stage` which globs for all ``*.parquet`` files in the
        staging folder, ``ManifestStage`` only consumes the specific files
        referenced in the manifest.

        Args:
            working_dir: The working directory for intermediate files.

        Returns:
            A list of TableFrames (or None) corresponding to each positional
            group in the manifest.
        """
        logger.debug(f"Triggering {self}")
        execution_context: ExecutionContext = self._ec
        base_uri = execution_context.request.function_stage_uri
        if not base_uri:
            raise ValueError(
                "Error while trying to obtain the staging URI from the "
                f"request file. Obtained '{base_uri}' instead of a valid URI."
            )
        base_uri = base_uri if base_uri.endswith("/") else base_uri + "/"
        configs = _obtain_configs(execution_context)

        if not base_uri.find(io_constants.URI_INDICATOR):
            raise ValueError(
                f"Received URI value {base_uri}, which does not contain the "
                f"uri indicator {io_constants.URI_INDICATOR} and is "
                "therefore not a valid URI. This is most likely due to "
                "an internal error of the Tabsdata server."
            )
        prefix = base_uri.split(io_constants.URI_INDICATOR)[0]

        # Resolve the staging directory based on the storage scheme.
        # Currently only local file staging is supported.
        match prefix:
            case io_constants.FILE_SCHEME:
                staging_dir = convert_uri_to_path(base_uri.rstrip("/"))
            case _:
                raise NotImplementedError(
                    "ManifestStage currently only supports local file staging. "
                    f"Received URI scheme: {prefix}."
                )

        # Determine the effective offset for filtering manifest files.
        current_offset = execution_context.status.offset.current_offset
        last_consumed_time_str = current_offset.get(STAGE_MANIFEST_OFFSET_KEY)
        last_consumed_dt = (
            _parse_utc_datetime(last_consumed_time_str)
            if last_consumed_time_str
            else None
        )
        logger.debug(f"Stored manifest offset: {last_consumed_dt}")

        if self.use_existing_data:
            # use_existing_data=True: use stored offset only, ignore
            # registration time. On first run (None), all manifests
            # will be consumed.
            effective_offset = last_consumed_dt
            logger.debug(
                f"use_existing_data=True, effective offset: {effective_offset}"
            )
        else:
            # use_existing_data=False: use the maximum of the stored
            # offset and the registration time, so that manifests
            # predating the publisher registration are skipped.
            registration_time_str = _obtain_registration_time(execution_context)
            registration_dt = (
                _parse_utc_datetime(registration_time_str)
                if registration_time_str
                else None
            )
            effective_offset = _max_nullable_datetime(last_consumed_dt, registration_dt)
            logger.debug(
                f"use_existing_data=False, stored offset: "
                f"{last_consumed_dt}, registration time: "
                f"{registration_dt}, effective offset: "
                f"{effective_offset}"
            )

        # Find manifest files in the staging folder, sorted by mtime.
        staging_path = Path(staging_dir)
        all_manifests = sorted(
            staging_path.glob(STAGE_MANIFEST_FILE_PATTERN),
            key=lambda p: p.stat().st_mtime,
        )
        logger.debug(
            f"Found {len(all_manifests)} manifest file(s) in staging folder: "
            f"{all_manifests}"
        )

        # Filter: if we have an effective offset, only consume newer manifests.
        if effective_offset is not None:
            manifests = [
                m
                for m in all_manifests
                if datetime.fromtimestamp(m.stat().st_mtime, tz=timezone.utc)
                > effective_offset
            ]
        else:
            manifests = all_manifests

        if not manifests:
            logger.debug("No new manifest files to process")
            return []

        logger.debug(f"Processing {len(manifests)} new manifest file(s): {manifests}")

        # Aggregate file groups across all manifest files.
        # Each manifest maps positional indices to lists of file URIs.
        # We merge them so that all files for a given positional index
        # are combined across manifests.
        aggregated_groups: dict[int, list[str]] = {}
        for manifest_file in manifests:
            content = _read_manifest_file(str(manifest_file))
            manifest = _deserialize_manifest(content)
            for group_idx, entry in enumerate(manifest.entries):
                if group_idx not in aggregated_groups:
                    aggregated_groups[group_idx] = []
                aggregated_groups[group_idx].extend(entry.files)

        # Process each positional group.
        results = []
        for group_idx in sorted(aggregated_groups.keys()):
            group_files = aggregated_groups[group_idx]
            logger.debug(f"Group {group_idx}: {len(group_files)} file(s)")

            # Build the appropriate source plugin based on URI scheme.
            # The manifest already contains full URIs, so no path
            # resolution is needed.
            match prefix:
                case io_constants.S3_SCHEME:
                    auxiliary_plugin = _build_manifest_s3_source(group_files, configs)
                case io_constants.AZURE_SCHEME:
                    auxiliary_plugin = _build_manifest_azure_source(
                        group_files, configs
                    )
                case io_constants.GCS_SCHEME:
                    auxiliary_plugin = _build_manifest_gcs_source(group_files, configs)
                case io_constants.FILE_SCHEME:
                    auxiliary_plugin = _build_manifest_local_source(
                        group_files, configs
                    )
                case _:
                    raise ValueError(
                        f"Unsupported URI scheme: {prefix}. "
                        "Currently, the supported staging storage locations "
                        "are Azure, GCS, LocalFileSystem and S3."
                    )

            logger.debug(
                f"Running auxiliary plugin for group {group_idx}: {auxiliary_plugin}"
            )
            group_results = auxiliary_plugin._run(execution_context)

            # Each positional argument is always a list of TableFrames,
            # even for single-file groups (one-element list) or empty
            # results (empty list).
            if not group_results:
                results.append([])
            else:
                results.append(group_results)

        # Store the offset of the latest consumed manifest file as a
        # UTC datetime to ensure no issues with timezones.
        latest_mtime = datetime.fromtimestamp(
            manifests[-1].stat().st_mtime, tz=timezone.utc
        )
        latest_mtime_str = latest_mtime.isoformat()
        self.initial_values = {STAGE_MANIFEST_OFFSET_KEY: latest_mtime_str}
        logger.debug(f"Stored new manifest offset: {latest_mtime_str}")

        return results

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(use_existing_data={self.use_existing_data})"
