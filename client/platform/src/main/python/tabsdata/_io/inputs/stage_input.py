#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from tabsdata._io import constants as io_constants
from tabsdata._io.inputs.file_inputs import (
    AzureSource,
    GCSSource,
    LocalFileSource,
    S3Source,
)
from tabsdata._io.plugin import SourcePlugin
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
