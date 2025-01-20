#
# Copyright 2024 Tabs Data Inc.
#

from abc import ABC, abstractmethod
from typing import List, Tuple, Union


class SourcePlugin(ABC):
    """
    Abstract class for input plugins.

    Methods:
        trigger_input(working_dir: str) -> Union[str, Tuple[str, ...], List[str]]
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

    @abstractmethod
    def trigger_input(self, working_dir: str) -> Union[str, Tuple[str, ...], List[str]]:
        """
        Trigger the import of the data. This must be implemented in any class that
            inherits from this class. The method will receive a folder where it must
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


class DestinationPlugin(ABC):
    """
    Abstract class for output plugins.

    Methods:
        trigger_output(*args, **kwargs)
            Trigger the exporting of the data. This function will receive the resulting
            data from the dataset function and must store it in the desired location.
    """

    IDENTIFIER = "destination-plugin"

    @abstractmethod
    def trigger_output(self, *args, **kwargs):
        """
        Trigger the exporting of the data. This function will receive the resulting data
            from the dataset function and must store it in the desired location.

        Args:
            *args: The data to be exported
            **kwargs: Additional parameters to be used in the export

        Returns:
            None
        """

    def to_dict(self) -> dict:
        """
        Return a dictionary representation of the object. This is used to save the
            object in a file.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {self.IDENTIFIER: f"{self.__class__.__name__}.pkl"}
