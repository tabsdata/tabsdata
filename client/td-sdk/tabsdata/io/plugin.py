#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, List, Tuple, Union

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._generators as td_generators
from tabsdata.io.input import LocalFileSource
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tabsserver.function.execution_utils import trigger_non_plugin_source
from tabsdata.tabsserver.function.store_results_utils import (
    remove_system_columns_and_convert,
)

if TYPE_CHECKING:

    import polars as pl

    from tabsdata.tabsserver.function.execution_context import ExecutionContext
    from tabsdata.tabsserver.function.results_collection import ResultsCollection

    VALID_PLUGIN_RESULT = List[pl.LazyFrame | None] | pl.LazyFrame | None


@contextmanager
def td_context(
    plugin: SourcePlugin | DestinationPlugin, execution_context: ExecutionContext
):
    setattr(plugin, "_ec", execution_context)
    try:
        yield
    finally:
        delattr(plugin, "_ec")


class SourcePlugin:
    """
    Parent class for input plugins.

    Methods:
        chunk(working_dir: str) -> Union[str, Tuple[str, ...], List[str]]
            Trigger the import of the data. The method will receive a folder where it
            must store the data as parquet files, and return a list of the paths of
            the files created. This files will then be loaded and mapped to the
            dataset function in positional order, so if you want file.parquet to be
            the first argument of the dataset function, you must return it first. If
            you want a parameter to receive multiple files, return a list of the paths.
            For example, you would give the following return to provide a first argument
            with a single file and a second argument with two files:
            return ["file1.parquet", ["file2.parquet", "file3.parquet"]]
    """

    IDENTIFIER = "source-plugin"

    def _is_overridden(self, method_name: str) -> bool:
        class_method = getattr(SourcePlugin, method_name)
        object_method = getattr(self.__class__, method_name)
        return object_method is not class_method

    def _run(
        self, execution_context: ExecutionContext
    ) -> List[TableFrame | None | List[TableFrame | None]]:
        """
        Run the plugin. This method will be called by the framework and should not be
            called directly. It will call the stream method to import the data and
            return the result.

        Args:
            execution_context (ExecutionContext): The execution context of the function.

        Returns:
            Union[TableFrame, Tuple[TableFrame, ...], List[TableFrame]]
        """
        self._tabsdata_internal_logger = execution_context.logger
        logger = self._tabsdata_internal_logger
        destination_dir = execution_context.paths.output_folder
        logger.info(f"Importing files to '{destination_dir}'")
        # Add new value of initial values to plugin if provided
        initial_values_object = execution_context.status.offset
        if not initial_values_object.use_decorator_values:
            current_initial_values = initial_values_object.current_offset
            self.initial_values = current_initial_values
            logger.debug(f"Updated plugin initial values to: {current_initial_values}")
        logger.info("Starting plugin stream import")

        parameters = None
        # For a custom stream implementations, method is executed as is.
        if self._is_overridden("stream"):
            parameters = self.stream(destination_dir)
        else:
            # For the core stream implementations, method is executed as with the
            # execution context.
            with td_context(self, execution_context):
                parameters = self.stream(destination_dir)

        if self.initial_values:
            execution_context.status.offset.returns_values = True
        # Verify if the parameters are valid
        if not isinstance(parameters, list):
            logger.error(
                "The return value of the stream method of a plugin must be "
                f"a list of TableFrames or Nones, got {type(parameters)} "
                "instead"
            )
            raise TypeError(
                "The return value of the stream method of a plugin must be "
                f"a list of TableFrames or Nones, got {type(parameters)} "
                "instead"
            )
        for element in parameters:
            if isinstance(element, list):
                for single_element in element:
                    if (
                        not isinstance(single_element, TableFrame)
                        and single_element is not None
                    ):
                        logger.error(
                            "The return value of the stream method of a plugin"
                            " must be a list of TableFrames or Nones, got"
                            f" {type(single_element)} instead"
                        )
                        raise TypeError(
                            "The return value of the stream method of a plugin"
                            " must be a list of TableFrames or Nones, got"
                            f" {type(single_element)} instead"
                        )
            elif not isinstance(element, TableFrame) and element is not None:
                logger.error(
                    "The return value of the stream method of a plugin must be "
                    f"a list of TableFrames or Nones, got {type(element)} instead"
                )
                raise TypeError(
                    "The return value of the stream method of a plugin must be "
                    f"a list of TableFrames or Nones, got {type(element)} instead"
                )
        return parameters

    # ToDo: this must be refined to:
    #   - Expose a function that the user can apply to generate TableFrame's from data.
    #   - Ensure that, when stream is overridden, resulting TableFrames are repopulated
    #       with the correct metadata and system columns, and persisted in folder
    #       {function_data} before forwarding them the the user function.
    def stream(
        self,
        working_dir: str,
    ) -> List[TableFrame | None | List[TableFrame | None]]:
        # Default streaming implementation, delegates to chunking.
        # An implementation doing streaming should override this method.
        logger = self._tabsdata_internal_logger
        resulting_files = self.chunk(working_dir)
        logger.info(
            f"Imported files to '{working_dir}'. Resulting files: '{resulting_files}'"
        )

        idx = td_generators.IdxGenerator()
        if isinstance(resulting_files, str) or resulting_files is None:
            parameters = [
                _import_plugin_file_from_single_element(
                    resulting_files,
                    working_dir,
                    idx=idx,
                    execution_context=self._ec,
                )
            ]
        elif isinstance(resulting_files, (list, tuple)):
            parameters = []
            for element in resulting_files:
                if isinstance(element, (list, tuple)):
                    parameters.append(
                        [
                            _import_plugin_file_from_single_element(
                                single_element,
                                working_dir,
                                idx=idx,
                                execution_context=self._ec,
                            )
                            for single_element in element
                        ]
                    )
                elif isinstance(element, str) or element is None:
                    parameters.append(
                        _import_plugin_file_from_single_element(
                            element,
                            working_dir,
                            idx=idx,
                            execution_context=self._ec,
                        )
                    )
                else:
                    logger.error(
                        f"Invalid type for resulting files: {type(element)}. No data"
                        " imported."
                    )
                    raise TypeError(
                        f"Invalid type for resulting files: {type(element)}. No data"
                        " imported."
                    )
            logger.debug(
                f"List of parameters obtained after plugin import: {parameters}"
            )
        else:
            logger.error(
                f"Invalid type for resulting files: {type(resulting_files)}. No data"
                " imported."
            )
            raise TypeError(
                f"Invalid type for resulting files: {type(resulting_files)}. No data"
                " imported."
            )
        return parameters

    def chunk(self, working_dir: str) -> Union[str, Tuple[str, ...], List[str]]:
        """
        Trigger the import of the data. This must be implemented in any class that
            inherits from this class unless directly implementing streaming. The method
            will receive a folder where it must
            store the data as parquet files, and return a list of the paths of the
            files created. This files will then be loaded and mapped to the dataset
            function in positional order, so if you want file.parquet to be the first
            argument of the dataset function, you must return it first. If you want a
            parameter to receive multiple files, return a list of the paths.
            For example, you would give the following return to provide a first
            argument with a single file and a second argument with two files:
            return ["file1.parquet", ["file2.parquet", "file3.parquet"]]

        Args:
            working_dir (str): The folder where the files must be stored

        Returns:
            Union[str, Tuple[str, ...], List[str]]: The path of the file(s) created, in
                the order they must be mapped to the dataset function
        """
        raise NotImplementedError(
            "When implementing a SourcePlugin, either the 'stream' method or the "
            "'chunk' method must be overridden. The current plugin "
            f"'{self.__class__.__name__}' does not implement either of them."
        )

    def to_dict(self) -> dict:
        """
        Return a dictionary representation of the object. This is used to save the
            object in a file.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {self.IDENTIFIER: f"{self.__class__.__name__}.pkl"}

    @property
    def initial_values(self) -> dict:
        """
        Return a dictionary with the initial values to be stored after execution of
        the plugin. They will be accessible in the next execution of the plugin.
        The dictionary must have the parameter names as keys and the initial values
        as values, all the type string.

        Returns:
            dict: A dictionary with the initial values of the parameters of the plugin.
        """
        if hasattr(self, "_initial_values"):
            return self._initial_values
        return {}

    @initial_values.setter
    def initial_values(self, values: dict):
        """
        Set the initial values of the plugin. This method is used to set the initial
        values of the plugin after it is loaded from a file.

        Args:
            values (dict): A dictionary with the initial values of the parameters of
                the plugin.
        """
        self._initial_values = values


def _import_plugin_file_from_single_element(
    resulting_files: str | None,
    working_dir: str,
    idx: td_generators.IdxGenerator,
    execution_context: ExecutionContext,
) -> Union[TableFrame, None, List[TableFrame | None]]:
    if resulting_files is None:
        return None
    resulting_files_paths = os.path.join(working_dir, resulting_files)
    source_config = LocalFileSource(path=resulting_files_paths)
    return trigger_non_plugin_source(
        source_config,
        working_dir,
        execution_context=execution_context,
        idx=idx,
    )[0]


class DestinationPlugin:
    """
    Abstract class for output plugins.

    Methods:
        trigger_output(working_dir, *args, **kwargs)
            Trigger the exporting of the data. This function will receive the resulting
            data from the dataset function and must store it in the desired location.
    """

    IDENTIFIER = "destination-plugin"

    def _is_overridden(self, method_name: str) -> bool:
        class_method = getattr(DestinationPlugin, method_name)
        object_method = getattr(self.__class__, method_name)
        return object_method is not class_method

    def _run(self, execution_context: ExecutionContext, results: ResultsCollection):
        self._tabsdata_internal_logger = execution_context.logger
        logger = self._tabsdata_internal_logger
        logger.info(f"Exporting files with plugin '{str(self)}'")
        logger.debug("Processing results of the user-provided function")
        results_to_provide = []
        for result in results:
            result_value = result.value
            if isinstance(result_value, TableFrame):
                intermediate_result = remove_system_columns_and_convert(result_value)
            elif result_value is None:
                intermediate_result = None
            elif isinstance(result_value, list):
                intermediate_result = [
                    (
                        remove_system_columns_and_convert(single_result)
                        if isinstance(single_result, TableFrame)
                        else result
                    )
                    for single_result in result_value
                ]
            else:
                logger.error(
                    "The result of a registered function must be a TableFrame,"
                    f" None or a list of TableFrames, got '{type(result_value)}'"
                    " instead"
                )
                raise TypeError(
                    "The result of a registered function must be a TableFrame,"
                    f" None or a list of TableFrames, got '{type(result_value)}'"
                    " instead"
                )
            results_to_provide.append(intermediate_result)
        logger.info("Exporting files with plugin stream method")
        # For a custom stream implementations, method is executed as is.
        if self._is_overridden("stream"):
            self.stream(execution_context.paths.output_folder, *results_to_provide)
        else:
            # For the core stream implementations, method is executed as with the
            # execution context.
            with td_context(self, execution_context):
                self.stream(execution_context.paths.output_folder, *results_to_provide)
        logger.info("Exported files with plugin stream method successfully")

    def stream(self, working_dir: str, *results: VALID_PLUGIN_RESULT):
        """
        Trigger the exporting of the data. This method will receive the resulting data
            from the user function and must store it in the desired location.
            Note: this method *might* materialize the data provided in a single chunk
            generated by the chunk function if invoked, so chunks should be of an
            appropriate size.

        Args:
            working_dir (str): The folder where any intermediate files generated must
                be stored (this refers to temporary files that will be deleted after
                the execution of the plugin, not the final destination of the data)
            results: The data to be exported. It is a list of polars LazyFrames or None.

        Returns:
            None
        """
        files = self.chunk(working_dir, *results)
        self.write(files)

    def chunk(
        self, working_dir: str, *results: VALID_PLUGIN_RESULT
    ) -> List[str | List[str] | List[List[str]]]:
        """
        Trigger the exporting of the data to local parquet chunks. This method will
            receive the resulting data from the user function and must store it in
            the local system as parquet files, using the working_dir. Note: This
            method should *not* materialize the data, it should only store it in the
            local system.

        Args:
            working_dir (str): The folder where any files generated must
                be stored (this refers to temporary files that will be deleted after
                the execution of the plugin, not the final destination of the data)
            results: The data to be exported. It is a list of polars LazyFrames or None.

        Returns:
            A list of the intermediate files created
        """
        raise NotImplementedError(
            "When implementing a DestinationPlugin, either the 'stream' method or the "
            "'chunk' method must be overridden. The current plugin "
            f"'{self.__class__.__name__}' does not implement either of them."
        )

    def write(self, files: List[str | List[str] | List[List[str]]]):
        """
        Given a file or a list of files, write to the desired destination. Note: this
            method *might* materialize the data in the files it receives, so chunks
            should be of an appropriate size.

        Args:
            files (str): The file or files to be stored in the final destination.
        """

    def to_dict(self) -> dict:
        """
        Return a dictionary representation of the object. This is used to save the
            object in a file.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {self.IDENTIFIER: f"{self.__class__.__name__}.pkl"}
