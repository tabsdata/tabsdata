#
# Copyright 2025 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Callable

import cloudpickle

from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._io.stage_trigger import StageTrigger
from tabsdata._table_actions.actions import OnTablesAction
from tabsdata._tabsserver.function.configuration_utils import load_function_config
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.function.status import Status
from tabsdata._tabsserver.function.yaml_parsing import parse_request_yaml
from tabsdata._tdfunction import TabsdataFunction
from tabsdata._utils.bundle_utils import (
    ACTIONS_FOLDER,
    CODE_FOLDER,
    CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY,
    CONFIG_ENTRY_POINT_KEY,
    CONFIG_INPUT_ACTIONS_KEY,
    CONFIG_INPUTS_KEY,
    CONFIG_OUTPUT_ACTIONS_KEY,
    CONFIG_OUTPUT_KEY,
    CONFIG_TABSDATA_FUNCTION_FUNCTION_FILE_KEY,
    CONFIG_TABSDATA_FUNCTION_KEY,
    CONFIG_TRIGGERBY_KEY,
    INPUT_ACTIONS_KEY,
    OUTPUT_ACTIONS_KEY,
    PLUGINS_FOLDER,
    STAGE_TRIGGER_FOLDER,
    STAGE_TRIGGER_KEY,
)

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.yaml_parsing import FunctionYaml


logger = logging.getLogger(__name__)


class ExecutionContext:
    """
    Class to manage the execution context of the function.

    """

    def __init__(
        self,
        paths: ExecutionPaths,
        work: str,
        function_config: dict = None,
        request: FunctionYaml = None,
        status: Status = None,
        mount_options_dict: dict = None,
    ):
        self.paths = paths
        self.work = work
        self.paths.parent_context = self
        self.function_config = function_config
        self.request = request
        self.status = status
        self.logger = logger
        # Create the required folders for the function
        self.paths.create_required_folders()
        # Load the function configuration from the bundled information
        self.function_config = load_function_config(self.paths.bundle_folder)
        # Parse and load the information of the request file
        self.request = parse_request_yaml(self.paths.request_file)
        self.request.work = work
        self.mount_options = MountOptions(mount_options_dict or {})
        # Lazy-initialised by sink_lf_to_location; import kept local to
        # avoid circular dependency at module level.
        self.pending_cloud_copies = None

    @property
    def project_id(self) -> str:
        """The project of the run, from the request yaml — used to resolve
        project-scoped resources (e.g. named connection artifacts)."""
        return self.request.project_id

    @property
    def tabsdata_function(self) -> TabsdataFunction:
        if not hasattr(self, "_tabsdata_function"):
            if self.paths.tabsdata_function_file:
                with open(self.paths.tabsdata_function_file, "rb") as f:
                    self._tabsdata_function = cloudpickle.load(f)
            else:
                self._tabsdata_function = None
        return self._tabsdata_function

    @property
    def user_provided_function(self) -> Callable:
        if self.tabsdata_function is not None:
            return self.tabsdata_function.original_function
        else:
            if not hasattr(self, "_user_provided_function"):
                with open(self.paths.original_function_file, "rb") as f:
                    self._user_provided_function = cloudpickle.load(f)
            return self._user_provided_function

    @property
    def source(self) -> SourcePlugin | None:
        if self.tabsdata_function is not None:
            return self.tabsdata_function.input
        else:
            if not hasattr(self, "_source_plugin"):
                importer_plugin_file = self.function_config.input.get(
                    SourcePlugin.IDENTIFIER
                )
                if importer_plugin_file is None:
                    raise ValueError(
                        "Source plugin not found in function configuration"
                    )
                else:
                    with open(
                        os.path.join(self.paths.plugins_folder, importer_plugin_file),
                        "rb",
                    ) as f:
                        self._source_plugin = cloudpickle.load(f)
            return self._source_plugin

    @property
    def destination(self) -> DestinationPlugin | None:
        if self.tabsdata_function is not None:
            return self.tabsdata_function.output
        else:
            if not hasattr(self, "_destination_plugin"):
                destination_plugin_file = self.function_config.output.get(
                    DestinationPlugin.IDENTIFIER
                )
                if destination_plugin_file is None:
                    self._destination_plugin = None
                else:
                    with open(
                        os.path.join(
                            self.paths.plugins_folder, destination_plugin_file
                        ),
                        "rb",
                    ) as f:
                        self._destination_plugin = cloudpickle.load(f)
            return self._destination_plugin

    @property
    def input_actions(self) -> list[OnTablesAction] | None:
        # return self.tabsdata_function.input_actions Note: until input actions
        # exist, this will fail
        if self.tabsdata_function is not None:
            return []
        else:
            if not hasattr(self, "_input_actions"):
                input_actions_file = self.function_config.input_actions.get(
                    INPUT_ACTIONS_KEY
                )
                if input_actions_file is None:
                    self._input_actions = []
                else:
                    with open(
                        os.path.join(self.paths.actions_folder, input_actions_file),
                        "rb",
                    ) as f:
                        self._input_actions = cloudpickle.load(f)
            return self._input_actions if self._input_actions is not None else []

    @property
    def output_actions(self) -> list[OnTablesAction] | None:
        if self.tabsdata_function is not None:
            return self.tabsdata_function.output_actions
        else:
            if not hasattr(self, "_output_actions"):
                output_actions_file = self.function_config.output_actions.get(
                    OUTPUT_ACTIONS_KEY
                )
                if output_actions_file is None:
                    self._output_actions = []
                else:
                    with open(
                        os.path.join(self.paths.actions_folder, output_actions_file),
                        "rb",
                    ) as f:
                        self._output_actions = cloudpickle.load(f)
            return self._output_actions if self._output_actions is not None else []

    @property
    def trigger_by(self) -> StageTrigger | None:
        if self.tabsdata_function is not None:
            return self.tabsdata_function.trigger_by
        else:
            if not hasattr(self, "_stage_trigger"):
                stage_trigger_file = self.function_config.trigger_by.get(
                    STAGE_TRIGGER_KEY
                )
                if stage_trigger_file is None:
                    self._stage_trigger = None
                else:
                    with open(
                        os.path.join(
                            self.paths.stage_trigger_folder, stage_trigger_file
                        ),
                        "rb",
                    ) as f:
                        self._stage_trigger = cloudpickle.load(f)
            return self._stage_trigger

    @property
    def function_config(self) -> FunctionConfig:
        """
        Get the function configuration.
        """
        if self._function_config is None:
            self._function_config = FunctionConfig({})
        return self._function_config

    @function_config.setter
    def function_config(self, function_config: dict):
        """
        Set the function configuration.
        """
        if function_config is None:
            self._function_config = None
        elif isinstance(function_config, dict):
            self._function_config = FunctionConfig(function_config)
        elif isinstance(function_config, FunctionConfig):
            self._function_config = function_config
        else:
            raise TypeError(
                "'function_config' must be a dictionary, "
                "a FunctionConfig instance or None, "
                f"got {type(function_config)} instead"
            )

    @property
    def status(self) -> Status:
        """
        Get the status object.
        """
        if self._status is None:
            self._status = Status()
            self._status.load(self.request, self)
        return self._status

    @status.setter
    def status(self, status: Status):
        """
        Set the status object.
        """
        if status is None:
            self._status = None
        elif isinstance(status, Status):
            self._status = status
        else:
            raise TypeError(
                "'status' must be an instance of Status or None, "
                f"got {type(status)} instead"
            )

    def store_status(self) -> bool:
        """
        Store the status.
        """
        return self.status.store(self.request, self)


class FunctionConfig:
    """
    Class to manage the function configuration.

    """

    def __init__(self, function_config: dict):
        self.function_config = function_config

    @property
    def entry_point(self) -> dict:
        """
        Get the entry point of the function.
        """
        return self.function_config.get(CONFIG_ENTRY_POINT_KEY, {})

    @property
    def input(self) -> dict:
        """
        Get the input configuration.
        """
        return self.function_config.get(CONFIG_INPUTS_KEY, {})

    @property
    def output(self) -> dict:
        """
        Get the output configuration.
        """
        return self.function_config.get(CONFIG_OUTPUT_KEY, {})

    @property
    def input_actions(self) -> dict:
        """
        Get the input actions configuration.
        """
        return self.function_config.get(CONFIG_INPUT_ACTIONS_KEY, {})

    @property
    def output_actions(self) -> dict:
        """
        Get the output actions configuration.
        """
        return self.function_config.get(CONFIG_OUTPUT_ACTIONS_KEY, {})

    @property
    def trigger_by(self) -> dict:
        """
        Get the trigger by configuration.
        """
        return self.function_config.get(CONFIG_TRIGGERBY_KEY, {})

    @property
    def tabsdata_function(self) -> dict:
        """
        Get the tabsdata function.
        """
        return self.function_config.get(CONFIG_TABSDATA_FUNCTION_KEY, {})

    def __getitem__(self, item):
        """
        Get the item from the function configuration.
        """
        return self.function_config.get(item, {})

    def get(self, key, default=None):
        """
        Get the item from the function configuration.
        """
        return self.function_config.get(key, default)


class ExecutionPaths:
    """
    Class to manage the folders of the function.

    """

    def __init__(
        self,
        bundle_folder: str,
        response_folder: str,
        output_folder: str,
        request_file: str,
        parent_context: ExecutionContext = None,
    ):
        self.request_file = request_file
        self.bundle_folder = bundle_folder
        self.response_folder = response_folder
        self.output_folder = output_folder
        self.code_folder = os.path.join(bundle_folder, CODE_FOLDER)
        self.plugins_folder = os.path.join(bundle_folder, PLUGINS_FOLDER)
        self.actions_folder = os.path.join(bundle_folder, ACTIONS_FOLDER)
        self.stage_trigger_folder = os.path.join(bundle_folder, STAGE_TRIGGER_FOLDER)
        self.parent_context = parent_context

    @property
    def response_file(self) -> str:
        """
        Get the response file path.
        """
        return os.path.join(self.response_folder, RESPONSE_FILE_NAME)

    @property
    def original_function_file(self) -> str | None:
        """
        Get the function file path.
        """
        entry_point = self.parent_context.function_config.entry_point
        if not entry_point:
            return None
        return os.path.join(
            self.code_folder, entry_point[CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY]
        )

    @property
    def tabsdata_function_file(self) -> str | None:
        """
        Get the tabsdata function file path.
        """
        tabsdata_function = self.parent_context.function_config.tabsdata_function
        if not tabsdata_function:
            return None
        return os.path.join(
            self.code_folder,
            tabsdata_function[CONFIG_TABSDATA_FUNCTION_FUNCTION_FILE_KEY],
        )

    def create_required_folders(self):
        """
        Create the required folders for the function execution.
        """
        logger.info("Creating required folders")
        create_folders(
            self.response_folder,
            self.output_folder,
        )


def create_folders(*args):
    for folder in args:
        if folder is not None:
            logger.debug(f"Creating folder {folder}")
            os.makedirs(folder, exist_ok=True)


class MountOptions:
    """
    Class to manage the mount options for the function execution.
    """

    def __init__(self, options: dict):
        self.options = options

    def get_options_for_prefix(self, prefix: str):
        """
        Get a dictionary with all mount options that started with a specific prefix,
        with the prefix already removed.
        """
        return {
            k[len(prefix) + 1 :]: v
            for k, v in self.options.items()
            if k.startswith(prefix)
        }

    def __getitem__(self, item):
        """
        Get the mount option by key.
        """
        return self.options[item]
