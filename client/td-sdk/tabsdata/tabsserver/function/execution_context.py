#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Callable

import cloudpickle

from tabsdata.io.input import build_input
from tabsdata.io.output import build_output
from tabsdata.io.plugin import DestinationPlugin, SourcePlugin
from tabsdata.tabsserver.function.configuration_utils import load_function_config
from tabsdata.tabsserver.function.initial_values_utils import InitialValues
from tabsdata.tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata.tabsserver.function.yaml_parsing import parse_request_yaml
from tabsdata.utils.bundle_utils import (
    CODE_FOLDER,
    CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY,
    CONFIG_ENTRY_POINT_KEY,
    CONFIG_INPUTS_KEY,
    CONFIG_OUTPUT_KEY,
    PLUGINS_FOLDER,
)

if TYPE_CHECKING:
    from tabsdata.io.input import Input
    from tabsdata.io.output import Output
    from tabsdata.tabsserver.function.yaml_parsing import InputYaml


logger = logging.getLogger(__name__)


class ExecutionContext:
    """
    Class to manage the execution context of the function.

    """

    def __init__(
        self,
        paths: ExecutionPaths,
        function_config: dict = None,
        request: InputYaml = None,
        initial_values: InitialValues = None,
    ):
        self.paths = paths
        self.paths.parent_context = self
        self.function_config = function_config
        self.request = request
        self.initial_values = initial_values
        self.logger = logger
        # Create the required folders for the function
        self.paths.create_required_folders()
        # Load the function configuration from the bundled information
        self.function_config = load_function_config(self.paths.bundle_folder)
        # Parse and load the information of the request file
        self.request = parse_request_yaml(self.paths.request_file)

    @property
    def user_provided_function(self) -> Callable:
        if not hasattr(self, "_user_provided_function"):
            with open(self.paths.function_file, "rb") as f:
                self._user_provided_function = cloudpickle.load(f)
        return self._user_provided_function

    @property
    def source_plugin(self) -> SourcePlugin | None:
        if not hasattr(self, "_source_plugin"):
            importer_plugin_file = self.function_config.input.get(
                SourcePlugin.IDENTIFIER
            )
            if importer_plugin_file is None:
                self._source_plugin = None
            else:
                with open(
                    os.path.join(self.paths.plugins_folder, importer_plugin_file), "rb"
                ) as f:
                    self._source_plugin = cloudpickle.load(f)
        return self._source_plugin

    @property
    def non_plugin_source(self) -> Input | None:
        if not hasattr(self, "_non_plugin_source"):
            if self.source_plugin:
                # If a source plugin is provided, we don't have a non-plugin source
                self._non_plugin_source = None
            else:
                # If no source plugin is provided, we create a non-plugin source
                input_config = self.function_config.input
                self._non_plugin_source = build_input(input_config)
        return self._non_plugin_source

    @property
    def destination_plugin(self) -> DestinationPlugin | None:
        if not hasattr(self, "_destination_plugin"):
            destination_plugin_file = self.function_config.output.get(
                DestinationPlugin.IDENTIFIER
            )
            if destination_plugin_file is None:
                self._destination_plugin = None
            else:
                with open(
                    os.path.join(self.paths.plugins_folder, destination_plugin_file),
                    "rb",
                ) as f:
                    self._destination_plugin = cloudpickle.load(f)
        return self._destination_plugin

    @property
    def non_plugin_destination(self) -> Output | None:
        if not hasattr(self, "_non_plugin_destination"):
            if self.destination_plugin:
                # If a destination plugin is provided, we don't have a non-plugin
                # destination
                self._non_plugin_destination = None
            else:
                # If no destination plugin is provided, we create a non-plugin
                # destination
                output_config = self.function_config.output
                self._non_plugin_destination = build_output(output_config)
        return self._non_plugin_destination

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
    def initial_values(self) -> InitialValues:
        """
        Get the initial values object.
        """
        if self._initial_values is None:
            self._initial_values = InitialValues()
            self._initial_values.load_current_initial_values(self.request)
        return self._initial_values

    @initial_values.setter
    def initial_values(self, initial_values: InitialValues):
        """
        Set the initial values object.
        """
        if initial_values is None:
            self._initial_values = None
        elif isinstance(initial_values, InitialValues):
            self._initial_values = initial_values
        else:
            raise TypeError(
                "'initial_values' must be an instance of InitialValues or None, "
                f"got {type(initial_values)} instead"
            )

    def store_initial_values(self) -> bool:
        """
        Store the initial values.
        """
        return self.initial_values.store(self.request)


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
        self.parent_context = parent_context

    @property
    def response_file(self) -> str:
        """
        Get the response file path.
        """
        return os.path.join(self.response_folder, RESPONSE_FILE_NAME)

    @property
    def function_file(self) -> str:
        """
        Get the function file path.
        """
        entry_point = self.parent_context.function_config.entry_point
        if not entry_point:
            raise ValueError("Entry point not found in the function configuration")
        return os.path.join(
            self.code_folder, entry_point[CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY]
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
