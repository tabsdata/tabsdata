#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import importlib.util
import inspect
import os
import sys
import tempfile
from datetime import UTC, datetime, timedelta, timezone
from enum import Enum
from typing import Generator, List

import polars as pl
import requests

from tabsdata._io.inputs.table_inputs import TableInput
from tabsdata._io.outputs.sql_outputs import verify_output_sql_drivers
from tabsdata._io.outputs.table_outputs import TableOutput
from tabsdata._tabsdatafunction import TabsdataFunction
from tabsdata._utils.bundle_utils import create_bundle_archive
from tabsdata._utils.temps import tabsdata_temp_folder
from tabsdata.api.apiserver import APIServer, obtain_connection
from tabsdata.api.status_utils.data_version import data_version_status_to_mapping
from tabsdata.api.status_utils.execution import execution_status_to_mapping
from tabsdata.api.status_utils.function_run import function_run_status_to_mapping
from tabsdata.api.status_utils.transaction import transaction_status_to_mapping
from tabsdata.api.status_utils.worker import worker_status_to_mapping


class FunctionType(Enum):
    PUBLISHER = "P"
    SUBSCRIBER = "S"
    TRANSFORMER = "T"


FUNCTION_TYPE_MAPPING = {
    FunctionType.PUBLISHER.value: "Publisher",
    FunctionType.SUBSCRIBER.value: "Subscriber",
    FunctionType.TRANSFORMER.value: "Transformer",
}


def _function_type_to_mapping(function_type: str) -> str:
    """
    Function to convert a function type to a mapping. While currently it
    only accesses the dictionary and returns the corresponding value, it could get
    more difficult in the future.
    """
    return FUNCTION_TYPE_MAPPING.get(function_type, function_type)


class _LazyProperty:
    def __init__(self, data_key, attr_name=None, subordinate_time_string: bool = False):
        self.data_key = data_key
        self.attr_name = attr_name or "_" + data_key
        self.subordinate_time_string = subordinate_time_string

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = getattr(instance, self.attr_name, None)
        if value is None:
            if not self.subordinate_time_string:
                value = instance._data.get(self.data_key)
            else:
                value = _convert_timestamp_to_string(
                    getattr(instance, self.data_key[: -len("_str")])
                )
            setattr(instance, self.attr_name, value)
        return value

    def __set__(self, instance, value):
        setattr(instance, self.attr_name, value)


class Collection:
    """
    This class represents a collection in the TabsdataServer.

    Args:
        connection (APIServer): The connection to the server.
        name (str): The name of the collection.
        **kwargs: Additional keyword

    Attributes:
        created_on_str (str): The timestamp when the collection was created as a
            string.
    """

    created_by = _LazyProperty("created_by")
    created_on = _LazyProperty("created_on")
    created_on_str = _LazyProperty("created_on_str", subordinate_time_string=True)
    description = _LazyProperty("description")

    def __init__(
        self,
        connection: APIServer,
        name: str,
        description: str | None = None,
        **kwargs,
    ):
        """
        Initialize the Collection object.

        Args:
            connection (APIServer): The connection to the server.
            name (str): The name of the collection.
            **kwargs: Additional keyword arguments.
        """
        created_on = kwargs.get("created_on")
        created_by = kwargs.get("created_by")
        self.connection = connection
        self.name = name
        self.description = description
        self.created_on = created_on
        if created_on:
            self.created_on_str = _convert_timestamp_to_string(created_on)
        else:
            self.created_on_str = None
        self.created_by = created_by
        self._data = None
        self.kwargs = kwargs

    @property
    def _data(self):
        if self._data_dict is None:
            self._data = (
                self.connection.collection_get_by_name(self.name).json().get("data")
            )
        return self._data_dict

    @_data.setter
    def _data(self, data_dict):
        self._data_dict = data_dict

    @property
    def functions(self) -> List[Function]:
        return self.list_functions()

    @property
    def permissions(self) -> List[InterCollectionPermission]:
        return self.list_permissions()

    @property
    def tables(self) -> List[Table]:
        return self.list_tables()

    def create(self, raise_for_status: bool = True) -> Collection:
        description = self._description or self.name
        response = self.connection.collection_create(
            self.name, description, raise_for_status=raise_for_status
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def create_permission(
        self, to_collection: str | Collection, raise_for_status: bool = True
    ) -> InterCollectionPermission:
        if isinstance(to_collection, str):
            pass
        elif isinstance(to_collection, Collection):
            to_collection = to_collection.name
        else:
            raise TypeError(
                "The 'to_collection' parameter must be a string or a Collection "
                f"object; got {type(to_collection)} instead."
            )
        response = self.connection.authz_inter_coll_perm_create(
            self.name, to_collection, raise_for_status=raise_for_status
        )
        return InterCollectionPermission(
            self.connection, **response.json().get("data"), collection=self
        )

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.collection_delete(self.name, raise_for_status=raise_for_status)

    def delete_permission(
        self, permission: str | InterCollectionPermission, raise_for_status: bool = True
    ) -> InterCollectionPermission:
        """
        Delete an inter-collection permission by its ID.

        Args:
            permission (str | InterCollectionPermission): The ID of the
                inter-collection permission to delete or a InterCollectionPermission
                object.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.
        """
        perm = (
            InterCollectionPermission(self.connection, permission, self)
            if isinstance(permission, str)
            else permission
        )
        perm.delete(raise_for_status=raise_for_status)
        return perm

    def get_function(self, function_name: str) -> Function:
        function_definition = (
            self.connection.function_get(self.name, function_name).json().get("data")
        )
        function_definition.update({"connection": self.connection, "collection": self})
        return Function(**function_definition)

    def get_table(self, table_name: str) -> Table | None:
        for table in self.tables:
            if table.name == table_name:
                return table
        raise ValueError(f"Table {table_name} not found in collection {self.name}")

    def list_functions(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Function]:
        return list(
            self.list_functions_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_functions_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Function]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.function_in_collection_list(
                collection_name=self.name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_functions = data.get("data")
            for raw_function in raw_functions:
                raw_function = Function(self.connection, **raw_function)
                yield raw_function
            first_page = False

    def list_permissions(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[InterCollectionPermission]:
        return list(
            self.list_permissions_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_permissions_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[InterCollectionPermission]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.authz_inter_coll_perm_list(
                self.name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_permissions = data.get("data")
            for raw_permission in raw_permissions:
                built_permission = InterCollectionPermission(
                    self.connection, **raw_permission, collection=self
                )
                yield built_permission
            first_page = False

    def list_tables(
        self,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Table]:
        """
        List the tables in a collection.

        Args:

        Returns:
            List[Table]: The requested list of tables in the collection.
        """
        return list(
            self.list_tables_generator(
                at=at,
                at_trx=at_trx,
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_tables_generator(
        self,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Table]:
        provided = [x is not None for x in (at, at_trx)]
        if sum(provided) > 1:
            raise ValueError("Only one of 'at' or 'at_trx' can be provided at a time.")
        if at:
            at = _top_and_convert_to_timestamp(at)
        elif at_trx:
            if isinstance(at_trx, Transaction):
                transaction = at_trx
            else:
                transaction = Transaction(self.connection, at_trx)
            at = transaction.ended_on

        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.table_list(
                collection_name=self.name,
                at=at,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_tables = data.get("data")
            for raw_table in raw_tables:
                raw_table = Table(self.connection, self, **raw_table)
                yield raw_table
            first_page = False

    def read_function_run(
        self,
        function: Function | str,
        execution: Execution | str,
        raise_for_status=True,
    ) -> requests.Response:
        """
        Read the status of a function run.

        Args:
            function (Function | str): The function to read the status of.
            execution (Execution | str): The execution of the run.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.
        """
        function = (
            function
            if isinstance(function, Function)
            else Function(self.connection, self, function)
        )
        return function.read_run(execution, raise_for_status=raise_for_status)

    def refresh(self) -> Collection:
        self.description = None
        self._data = None
        self.created_by = None
        self.created_on = None
        self.created_on_str = None
        self.kwargs = None
        return self

    def register_function(
        self,
        function_path: str,
        description: str = None,
        path_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        function_name: str = None,
        reuse_tables: bool = False,
        raise_for_status: bool = True,
    ) -> Function:
        """
        Create a function in the server.

        Args:
            function_path (str): The path to the function. It should be in the form of
                /path/to/file.py::function_name.
            description (str, optional): The description of the function.
            path_to_bundle (str, optional): The path that has to be bundled and sent
                to the server. If None, the folder containing the function will be
                bundled.
            requirements (str, optional): Path to a custom requirements.yaml file
                with the packages, python version and other information needed to
                create the Python environment for the function to run in the backend.
                If not provided, this information will be inferred from the current
                execution session.
            local_packages (List[str] | str, optional): A list of paths to local
                Python packages that need to be included in the bundle. Each path
                must exist and be a valid Python package that can be installed by
                running `pip install /path/to/package`.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the function could not be created.
        """

        temporary_directory = tempfile.TemporaryDirectory(dir=tabsdata_temp_folder())
        valid_python_versions = (
            self.connection.runtime_info_get()
            .json()
            .get("data", {})
            .get("python_versions")
        )
        (
            tables,
            string_dependencies,
            trigger_by,
            function_snippet,
            context_location,
            decorator_function_name,
            decorator_type,
            source_or_destination_name,
        ) = _create_archive(
            function_path,
            temporary_directory,
            path_to_bundle,
            requirements,
            local_packages,
            valid_python_versions,
        )

        function_name = function_name or decorator_function_name

        description = description or function_name

        with open(context_location, "rb") as file:
            bundle = file.read()

        response = self.connection.function_upload_bundle(
            collection_name=self.name,
            bundle=bundle,
            raise_for_status=raise_for_status,
        )
        bundle_id = response.json().get("data").get("id")

        # TODO: Remove this once the parameter is optional
        runtime_values = "{}"

        self.connection.function_register(
            collection_name=self.name,
            function_name=function_name,
            description=description,
            tables=tables,
            dependencies=string_dependencies,
            trigger_by=trigger_by,
            function_snippet=function_snippet,
            bundle_id=bundle_id,
            runtime_values=runtime_values,
            reuse_frozen_tables=reuse_tables,
            decorator=decorator_type,
            plugin_name=source_or_destination_name,
            raise_for_status=raise_for_status,
        )
        return Function(self.connection, self, function_name)

    def update(
        self, name: str, description: str = None, raise_for_status: bool = True
    ) -> Collection:
        response = self.connection.collection_update(
            self.name,
            new_collection_name=name,
            description=description,
            raise_for_status=raise_for_status,
        )
        self.name = name
        self.refresh()
        self._data = response.json().get("data")
        return self

    def update_function(
        self,
        function_name: str,
        function_path: str,
        description: str,
        directory_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        new_function_name=None,
        reuse_tables: bool = False,
        raise_for_status: bool = True,
    ) -> Function:
        """
        Update a function in the server.

        Args:
            function_name (str): The name of the function.
            function_path (str): The path to the function. It should be in the form of
                /path/to/file.py::function_name.
            description (str): The new description of the function.
            directory_to_bundle (str, optional): The path that has to be bundled and
                sent to the server. If None, the folder containing the function will be
                bundled.
            requirements (str, optional): Path to a custom requirements.yaml file
                with the packages, python version and other information needed to
                create the Python environment for the function to run in the backend.
                If not provided, this information will be inferred from the current
                execution session.
            local_packages (List[str] | str, optional): A list of paths to local
                Python packages that need to be included in the bundle. Each path
                must exist and be a valid Python package that can be installed by
                running `pip install /path/to/package`.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the function could not be updated.
        """
        temporary_directory = tempfile.TemporaryDirectory(dir=tabsdata_temp_folder())
        valid_python_versions = (
            self.connection.runtime_info_get()
            .json()
            .get("data", {})
            .get("python_versions")
        )
        (
            tables,
            string_dependencies,
            trigger_by,
            function_snippet,
            context_location,
            decorator_new_function_name,
            decorator_type,
            source_or_destination_name,
        ) = _create_archive(
            function_path,
            temporary_directory,
            directory_to_bundle,
            requirements,
            local_packages,
            valid_python_versions,
        )

        with open(context_location, "rb") as file:
            bundle = file.read()

        response = self.connection.function_upload_bundle(
            collection_name=self.name,
            bundle=bundle,
            raise_for_status=raise_for_status,
        )
        bundle_id = response.json().get("data").get("id")

        # TODO: Remove this once the parameter is optional
        runtime_values = "{}"

        new_function_name = new_function_name or decorator_new_function_name
        self.connection.function_update(
            collection_name=self.name,
            function_name=function_name,
            new_function_name=new_function_name,
            description=description,
            tables=tables,
            dependencies=string_dependencies,
            trigger_by=trigger_by,
            function_snippet=function_snippet,
            decorator=decorator_type,
            bundle_id=bundle_id,
            runtime_values=runtime_values,
            reuse_frozen_tables=reuse_tables,
            plugin_name=source_or_destination_name,
            raise_for_status=raise_for_status,
        )

        return Function(self.connection, self, new_function_name)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Collection):
            return False
        return self.name == other.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!r}"


class DataVersion:
    """
    This class represents a data version of a table in the TabsdataServer.

    Args:
        id (str): The ID of the data version.
        execution_id (str): The ID of the execution.
        triggered_on (int): The timestamp when the data version was triggered.
        status (str): The status of the data version.
        function_id (str): The ID of the function.
        **kwargs: Additional keyword arguments.
    """

    column_count = _LazyProperty("column_count")
    created_at = _LazyProperty("created_at")
    created_at_str = _LazyProperty("created_at_str", subordinate_time_string=True)
    row_count = _LazyProperty("row_count")
    schema_hash = _LazyProperty("schema_hash")

    # TODO: Make a first class citizen, with links to transaction and execution
    def __init__(
        self,
        connection: APIServer,
        id: str,
        collection: str | Collection,
        table: str | Table,
        **kwargs,
    ):
        """
        Initialize the DataVersion object.

        Args:
            id (str): The ID of the data version.
            execution_id (str): The ID of the execution.
            triggered_on (int): The timestamp when the data version was triggered.
            status (str): The status of the data version.
            function_id (str): The ID of the function.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.collection = collection
        self.table = table

        self.execution = kwargs.get("execution_id")
        # TODO: Add lazy properties for status, triggered_on and triggered_on_str
        self.column_count = kwargs.get("column_count")
        self.created_at = kwargs.get("created_at")
        self.created_at_str = None
        self.row_count = kwargs.get("row_count")
        self.schema_hash = kwargs.get("schema_hash")
        status = kwargs.get("status")
        self.status = data_version_status_to_mapping(status) if status else None
        # Note: this might cause an inconsistency or a bug if function_id corresponds
        # to a different function than the one in the function attribute. Revisit this
        # if necessary.
        self.function = kwargs.get("function")
        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self):
        if self._data_dict is None:
            response = self.connection.dataversion_list(
                self.collection.name, self.table.name, request_filter=f"id:eq:{self.id}"
            )
            try:
                self._data = response.json().get("data").get("data")[0]
            except IndexError:
                raise ValueError(
                    f"Data version with ID {self.id} not found in collection "
                    f"{self.collection.name} for table {self.table.name}."
                )
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        else:
            raise TypeError(
                "Collection must be a string or a Collection object; got"
                f"{type(collection)} instead."
            )

    @property
    def execution(self) -> Execution:
        if self._execution is None:
            self.execution = self._data.get("execution_id")
        return self._execution

    @execution.setter
    def execution(self, execution: Execution | str | None):
        if isinstance(execution, Execution):
            self._execution = execution
        elif isinstance(execution, str):
            self._execution = Execution(self.connection, execution)
        elif execution is None:
            self._execution = None
        else:
            raise TypeError(
                "Execution must be an Execution object, a string or None; got"
                f"{type(execution)} instead."
            )

    @property
    def function(self) -> Function:
        if self._function is None:
            self.function = self._data.get("function")
        return self._function

    @function.setter
    def function(self, function: str | Function):
        if isinstance(function, str):
            self._function = Function(self.connection, self.collection, function)
        elif isinstance(function, Function):
            self._function = function
        elif function is None:
            self._function = None
        else:
            raise TypeError(
                "Function must be a string, a Function object or None; got"
                f"{type(function)} instead."
            )

    @property
    def table(self) -> Table:
        return self._table

    @table.setter
    def table(self, table: str | Table):
        if isinstance(table, str):
            self._table = Table(self.connection, self.collection, table)
        elif isinstance(table, Table):
            self._table = table
        else:
            raise TypeError(
                f"Table must be a string or a Table object; got{type(table)} instead."
            )

    def __eq__(self, other) -> bool:
        if not isinstance(other, DataVersion):
            return False
        return self.id == other.id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"

    def __str__(self) -> str:
        return f"ID: {self.id!r}"


class Execution:
    """
    This class represents an execution in the TabsdataServer.

    Args:
        id (str): The id of the execution.
        name (str): The name of the execution.
        collection (str): The collection where the execution is running.
        function (str): The function that was triggered to start the execution.
        triggered_by (str): The user that triggered the execution.
        triggered_on (int): The timestamp when the execution was triggered.
        ended_on (int): The timestamp when the execution ended.
        started_on (int): The timestamp when the execution started.
        status (str): The status of the execution.
        **kwargs: Additional keyword arguments.

    Attributes:
        triggered_on_str (str): The timestamp when the execution was triggered as a
            string.
        ended_on_str (str): The timestamp when the execution ended as a string.
        started_on_str (str): The timestamp when the execution started as a string.

    """

    ended_on = _LazyProperty("ended_on")
    ended_on_str = _LazyProperty("ended_on_str", subordinate_time_string=True)
    name = _LazyProperty("name")
    started_on = _LazyProperty("started_on")
    started_on_str = _LazyProperty("started_on_str", subordinate_time_string=True)
    triggered_by = _LazyProperty("triggered_by")
    triggered_on = _LazyProperty("triggered_on")
    triggered_on_str = _LazyProperty("triggered_on_str", subordinate_time_string=True)

    def __init__(
        self,
        connection: APIServer,
        id: str,
        **kwargs,
    ):
        """
        Initialize the Execution object.

        Args:
            id (str): The id of the execution.
            name (str): The name of the execution.
            collection (str): The collection where the execution is running.
            function (str): The function that was triggered to start the execution.
            triggered_by (str): The user that triggered the execution.
            triggered_on (int): The timestamp when the execution was triggered.
            ended_on (int): The timestamp when the execution ended.
            started_on (int): The timestamp when the execution started.
            status (str): The status of the execution.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.name = kwargs.get("name")
        self.collection = kwargs.get("collection")
        self.function = kwargs.get("function")
        self.triggered_on = kwargs.get("triggered_on")
        # This way it gets computed dynamically, and we don't need to replicate logic
        self.triggered_on_str = None
        self.status = kwargs.get("status")

        self.triggered_by = kwargs.get("triggered_by")
        self.ended_on = kwargs.get("ended_on")
        # This way it gets computed dynamically, and we don't need to replicate logic
        self.ended_on_str = None
        self.started_on = kwargs.get("started_on")
        # This way it gets computed dynamically, and we don't need to replicate logic
        self.started_on_str = None
        self.dot = kwargs.get("dot")
        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self):
        if self._data_dict is None:
            response = self.connection.execution_list(request_filter=f"id:eq:{self.id}")
            try:
                self._data = response.json().get("data").get("data")[0]
            except IndexError:
                raise ValueError(f"Execution with ID {self.id} not found.")
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        if self._collection is None:
            self.collection = self._data.get("collection")
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection | None):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        elif collection is None:
            self._collection = None
        else:
            raise TypeError(
                "Collection must be a string, a Collection object "
                f"or None; got {type(collection)} instead."
            )

    @property
    def function(self) -> Function | None:
        if self._function is None:
            self.function = self._data.get("function")
        return self._function

    @function.setter
    def function(self, function: str | Function | None):
        if isinstance(function, str):
            self._function = Function(self.connection, self.collection, function)
        elif isinstance(function, Function):
            self._function = function
        elif function is None:
            self._function = None
        else:
            raise TypeError(
                "Function must be a string, a Function object or None; got"
                f"{type(function)} instead."
            )

    @property
    def function_runs(self) -> list[FunctionRun]:
        raw_function_runs = (
            self.connection.function_list_runs(
                request_filter=f"execution_id:eq:{self.id}"
            )
            .json()
            .get("data")
            .get("data")
        )
        return [
            FunctionRun(**{**function_run, "connection": self.connection})
            for function_run in raw_function_runs
        ]

    @property
    def status(self) -> str:
        if self._status is None:
            self.status = self._data.get("status")
        return self._status

    @status.setter
    def status(self, status: str | None):
        if status is None:
            self._status = status
        else:
            self._status = execution_status_to_mapping(status)

    @property
    def transactions(self) -> List[Transaction]:
        tabsdata_server = TabsdataServer.__new__(TabsdataServer)
        tabsdata_server.connection = self.connection
        transactions = tabsdata_server.list_transactions(
            filter=f"execution_id:eq:{self.id}"
        )
        return transactions

    @property
    def workers(self) -> list[Worker]:
        raw_workers = (
            self.connection.workers_list(request_filter=f"execution_id:eq:{self.id}")
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker, "connection": self.connection})
            for worker in raw_workers
        ]

    def cancel(self) -> requests.Response:
        """
        Cancel an execution. This includes all transactions that are part of the
            execution.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_id is not found in the system.
        """
        response = self.connection.execution_cancel(self.id)
        self.refresh()
        return response

    def get_worker(self, worker_id: str):
        for worker in self.workers:
            if worker.id == worker_id:
                return worker
        raise ValueError(f"Worker {worker_id} not found for execution {self.id}")

    def recover(self) -> requests.Response:
        """
        Recover an execution. This includes all transactions that are part of the
            execution.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_id is not found in the system.
        """
        response = self.connection.execution_recover(self.id)
        self.refresh()
        return response

    def refresh(self) -> Execution:
        self.name = None
        self.collection = None
        self.function = None
        self.triggered_on = None
        self.triggered_on_str = None
        self.status = None
        self.triggered_by = None
        self.ended_on = None
        self.ended_on_str = None
        self.started_on = None
        self.started_on_str = None
        self.kwargs = None
        self._data = None
        return self

    def __eq__(self, other):
        if not isinstance(other, Execution):
            return False
        return self.id == other.id

    def __repr__(self) -> str:
        repr = f"{self.__class__.__name__}(id={self.id!r})"
        return repr

    def __str__(self) -> str:
        string = f"ID: {self.id!s}"
        return string


class Function:
    """
    This class represents a function in the TabsdataServer.

    Args:
        id (str): The ID of the function.
        triggers (List[str]): If not an empty list, the trigger(s) of
            the function.
        tables (List[str]): The tables generated the function.
        dependencies (List[str]): The dependencies of the function.
        name (str): The name of the function.
        description (str): The description of the function.
        defined_on (int): The timestamp when the function was defined.
        defined_by (str): The user that defined the function.
        **kwargs: Additional keyword arguments.

    Attributes:
        defined_on_str (str): The timestamp when the function was defined as a
            string.
    """

    defined_by = _LazyProperty("defined_by")
    defined_on = _LazyProperty("defined_on")
    defined_on_str = _LazyProperty("defined_on_str", subordinate_time_string=True)
    description = _LazyProperty("description")
    id = _LazyProperty("id")
    type = _LazyProperty("decorator")

    def __init__(
        self,
        connection: APIServer,
        collection: str | Collection,
        name: str,
        **kwargs,
    ):
        """
        Initialize the Function object.

        Args:
            id (str): The ID of the function.
            triggers (List[str]): If not an empty list, the trigger(s) of
                the function.
            tables (List[str]): The tables generated the function.
            dependencies (List[str]): The dependencies of the function.
            name (str): The name of the function.
            description (str): The description of the function.
            defined_on (int): The timestamp when the function was defined.
            defined_by (str): The user that defined the function.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.collection = collection
        self.name = name

        self.id = kwargs.get("id")
        self.triggers = kwargs.get("triggers")
        self.tables = kwargs.get("tables")
        self.dependencies = kwargs.get("dependencies")
        self.description = kwargs.get("description")
        self.defined_on = kwargs.get("defined_on")
        self.defined_on_str = None
        self.defined_by = kwargs.get("defined_by")
        self.type = kwargs.get("decorator")
        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            self._data = (
                self.connection.function_get(self.collection.name, self.name)
                .json()
                .get("data")
            )
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        else:
            raise TypeError(
                "Collection must be a string or a Collection object; got"
                f"{type(collection)} instead."
            )

    @property
    def dependencies(self) -> List[str]:
        # TODO Aleix: see if we can return something other than a string
        if self._dependencies is None:
            self.dependencies = self._data.get("dependencies")
        return self._dependencies

    @dependencies.setter
    def dependencies(self, dependencies: List[str] | None):
        self._dependencies = dependencies

    @property
    def history(self) -> List[Function]:
        return self.list_history()

    @property
    def runs(self):
        raw_runs = (
            self.connection.function_list_runs(
                request_filter=[
                    f"name:eq:{self.name}",
                    f"collection:eq:{self.collection.name}",
                ]
            )
            .json()
            .get("data")
            .get("data")
        )
        return [
            FunctionRun(**{**run, "connection": self.connection}) for run in raw_runs
        ]

    @property
    def tables(self) -> List[Table]:
        if self._tables is None:
            self.tables = self._data.get("tables")
        return self._tables

    @tables.setter
    def tables(self, tables: List[str | Table] | None):
        if tables is None:
            self._tables = None
        elif isinstance(tables, list):
            self._tables = [
                (
                    Table(self.connection, self.collection, table, function_name=self)
                    if isinstance(table, str)
                    else table
                )
                for table in tables
            ]
        else:
            raise TypeError(
                "Tables must be a list of strings or Table objects, or None; got"
                f"{type(tables)} instead."
            )

    @property
    def triggers(self) -> List[str]:
        # TODO Aleix: see if we can return something other than a string
        if self._triggers is None:
            self.triggers = self._data.get("triggers")
        return self._triggers

    @triggers.setter
    def triggers(self, triggers: List[str] | None):
        self._triggers = triggers

    @property
    def workers(self):
        raw_workers = (
            self.connection.workers_list(
                request_filter=[
                    f"function:eq:{self.name}",
                    f"collection:eq:{self.collection.name}",
                ]
            )
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker, "connection": self.connection})
            for worker in raw_workers
        ]

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.function_delete(
            self.collection.name, self.name, raise_for_status=raise_for_status
        )

    def get_table(self, table_name: str) -> Table:
        for table in self.tables:
            if table.name == table_name:
                return table
        raise ValueError(f"Table {table_name} not found for function {self.name}")

    def get_worker(self, worker_id: str):
        for worker in self.workers:
            if worker.id == worker_id:
                return worker
        raise ValueError(f"Worker {worker_id} not found for function {self.name}")

    def list_history(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Function]:
        return list(
            self.list_history_generator(
                filter=filter, order_by=order_by, raise_for_status=raise_for_status
            )
        )

    def list_history_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Function]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.function_list_history(
                self.collection.name,
                self.name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_functions = data.get("data")
            for raw_function in raw_functions:
                raw_function["collection"] = self.collection
                built_function = Function(
                    self.connection,
                    **raw_function,
                )
                yield built_function
            first_page = False

    def read_run(
        self, execution: Execution | str, raise_for_status: bool = True
    ) -> requests.Response:
        """
        Read the status of a function run.

        Args:
            execution (Execution | str): The execution of the run.

        """
        execution_id = execution if isinstance(execution, str) else execution.id
        return self.connection.execution_read_function_run(
            self.collection.name,
            self.name,
            execution_id,
            raise_for_status=raise_for_status,
        )

    def refresh(self) -> Function:
        self.id = None
        self.triggers = None
        self.tables = None
        self.dependencies = None
        self.description = None
        self.defined_on = None
        self.defined_on_str = None
        self.defined_by = None
        self.type = None
        self.kwargs = None
        self._data = None
        return self

    def register(
        self,
        function_path: str,
        description: str = None,
        path_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        reuse_tables: bool = False,
        raise_for_status: bool = True,
    ) -> Function:
        result = self.collection.register_function(
            function_path,
            description=description,
            path_to_bundle=path_to_bundle,
            requirements=requirements,
            local_packages=local_packages,
            function_name=self.name,
            reuse_tables=reuse_tables,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        return result

    def trigger(
        self,
        execution_name: str | None = None,
        raise_for_status: bool = True,
    ) -> Execution:
        """
        Trigger a function in the server.

        Args:
            execution_name (str, optional): The name of the execution.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            requests.Response: The response of the trigger request.

        Raises:
            APIServerError: If the function could not be triggered.
        """
        response = self.connection.function_execute(
            self.collection.name,
            self.name,
            execution_name=execution_name,
            raise_for_status=raise_for_status,
        )
        return Execution(
            self.connection,
            **{
                **response.json().get("data"),
                "collection": self.collection.name,
                "function": self.name,
            },
        )

    def update(
        self,
        function_path: str,
        description: str,
        directory_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        new_function_name: str = None,
        reuse_tables: bool = False,
        raise_for_status: bool = True,
    ) -> Function:
        collection = self.collection
        result = collection.update_function(
            self.name,
            function_path,
            description,
            directory_to_bundle=directory_to_bundle,
            requirements=requirements,
            local_packages=local_packages,
            new_function_name=new_function_name,
            reuse_tables=reuse_tables,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self.name = result.name
        self.collection = result.collection
        return result

    def __eq__(self, other):
        if not isinstance(other, Function):
            return False
        return self.name == other.name and self.collection == other.collection

    def __repr__(self) -> str:
        representation = (
            f"{self.__class__.__name__}(name={self.name!r}, "
            f"collection={self.collection!r})"
        )
        return representation

    def __str__(self) -> str:
        string_representation = f"Name: {self.name!s}, collection: {self.collection!s}"
        return string_representation


class FunctionRun:
    """
    This class represents a function run in the TabsdataServer.

    Args:
        id (str): The ID of the function run.
        collection (str): The collection of the function run.
        function (str): The function of the function run.
        function_id (str): The ID of the function of the function run.
        transaction_id (str): The ID of the transaction of the function run.
        execution (str): The execution of the function run.
        execution_id (str): The ID of the execution of the function run.
        status (str): The status of the associated data version.
        **kwargs: Additional keyword arguments.
    """

    ended_on = _LazyProperty("ended_on")
    ended_on_str = _LazyProperty("ended_on_str", subordinate_time_string=True)
    started_on = _LazyProperty("started_on")
    started_on_str = _LazyProperty("started_on_str", subordinate_time_string=True)

    def __init__(
        self,
        connection: APIServer,
        id: str,
        **kwargs,
    ):
        """
        Initialize the FunctionRun object.

        Args:
            id (str): The ID of the function run.
            collection (str): The collection of the function run.
            function (str): The function of the function run.
            function_id (str): The ID of the function of the function run.
            transaction_id (str): The ID of the transaction of the function run.
            execution (str): The execution of the function run.
            execution_id (str): The ID of the execution of the function run.
            data_version_id (str): The ID of the data version of the function run.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.collection = kwargs.get("collection")
        self.function = kwargs.get("name")
        self.transaction = kwargs.get("transaction_id")
        self.execution = kwargs.get("execution_id")
        self.status = kwargs.get("status")
        self.ended_on = kwargs.get("ended_on")
        self.ended_on_str = None
        self.started_on = kwargs.get("started_on")
        self.started_on_str = None
        self._data = None
        self.kwargs = kwargs

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            try:
                run = tabsdata_server.list_function_runs(filter=f"id:eq:{self.id}")[0]
            except IndexError:
                raise ValueError(f"Function run with ID {self.id} not found.")
            self._data = run.kwargs
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        if self._collection is None:
            self.collection = self._data.get("collection")
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection | None):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        elif collection is None:
            self._collection = None
        else:
            raise TypeError(
                "Collection must be a string or a Collection object; got"
                f"{type(collection)} instead."
            )

    @property
    def execution(self) -> Execution:
        if self._execution is None:
            self.execution = self._data.get("execution_id")
        return self._execution

    @execution.setter
    def execution(self, execution: str | Execution | None):
        if isinstance(execution, str):
            self._execution = Execution(self.connection, execution)
        elif isinstance(execution, Execution):
            self._execution = execution
        elif execution is None:
            self._execution = None
        else:
            raise TypeError(
                "Execution must be a string, an Execution object or None; got"
                f"{type(execution)} instead."
            )

    @property
    def function(self) -> Function:
        if self._function is None:
            self.function = self._data.get("name")
        return self._function

    @function.setter
    def function(self, function: str | Function | None):
        if isinstance(function, str):
            self._function = Function(self.connection, self.collection, function)
        elif isinstance(function, Function):
            self._function = function
        elif function is None:
            self._function = None
        else:
            raise TypeError(
                "Function must be a string or a Function object; got"
                f"{type(function)} instead."
            )

    @property
    def status(self) -> str:
        if self._status is None:
            self.status = self._data.get("status")
        return self._status

    @status.setter
    def status(self, status: str | None):
        if status is None:
            self._status = None
        else:
            self._status = function_run_status_to_mapping(status)

    @property
    def transaction(self) -> Transaction:
        if self._transaction is None:
            self.transaction = self._data.get("transaction_id")
        return self._transaction

    @transaction.setter
    def transaction(self, transaction: str | Transaction | None):
        if isinstance(transaction, str):
            self._transaction = Transaction(self.connection, transaction)
        elif isinstance(transaction, Transaction):
            self._transaction = transaction
        elif transaction is None:
            self._transaction = None
        else:
            raise TypeError(
                "Transaction must be a string, a Transaction object or None; got"
                f"{type(transaction)} instead."
            )

    def __eq__(self, other) -> bool:
        if not isinstance(other, FunctionRun):
            return False
        return self.id == other.id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"

    def __str__(self) -> str:
        return f"ID: {self.id!s}"


class InterCollectionPermission:

    granted_by = _LazyProperty("granted_by")
    granted_on = _LazyProperty("granted_on")
    granted_on_str = _LazyProperty("granted_on_str", subordinate_time_string=True)

    def __init__(
        self, connection: APIServer, id: str, collection: str | Collection, **kwargs
    ):
        """
        Initialize the InterCollectionPermission object.

        Args:
            connection (APIServer): The connection to the API server.
            id (str): The ID of the inter-collection permission.
            collection (str | Collection): The collection associated with the
                permission.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.collection = collection
        self.to_collection = kwargs.get("to_collection")
        self.granted_by = kwargs.get("granted_by")
        self.granted_on = kwargs.get("granted_on")
        self.granted_on_str = None

        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            try:
                inter_coll_perm = self.collection.list_permissions(
                    filter=f"id:eq:{self.id}"
                )[0]
            except IndexError:
                raise ValueError(
                    f"Inter collection permission with ID {self.id} "
                    f"for collection {self.collection.name} not found."
                )
            self._data = inter_coll_perm.kwargs
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        else:
            raise TypeError(
                "The 'collection' parameter must be a string or a Collection object; "
                f"got {type(collection)} instead."
            )

    @property
    def to_collection(self) -> Collection:
        """
        Get the collection to which this permission applies.

        Returns:
            Collection: The collection to which this permission applies.
        """
        if self._to_collection is None:
            self.to_collection = self._data.get("to_collection")
        return self._to_collection

    @to_collection.setter
    def to_collection(self, to_collection: str | Collection | None):
        if isinstance(to_collection, str):
            self._to_collection = Collection(self.connection, to_collection)
        elif isinstance(to_collection, Collection):
            self._to_collection = to_collection
        elif to_collection is None:
            self._to_collection = None
        else:
            raise TypeError(
                "The 'to_collection' parameter must be a string, a Collection object, "
                f"or None; got {type(to_collection)} instead."
            )

    def delete(self, raise_for_status: bool = True) -> None:
        """
        Delete the inter-collection permission.

        Args:
            raise_for_status (bool): Whether to raise an exception if the request
                was not successful.
        """
        self.connection.authz_inter_coll_perm_delete(
            self.collection.name, self.id, raise_for_status=raise_for_status
        )

    def refresh(self) -> InterCollectionPermission:
        """
        Refresh the inter-collection permission data.

        Returns:
            InterCollectionPermission: The refreshed inter-collection permission object.
        """
        self.to_collection = None
        self.granted_by = None
        self.granted_on = None
        self.granted_on_str = None
        self.kwargs = None
        self._data = None
        return self

    def __eq__(self, other):
        if not isinstance(other, InterCollectionPermission):
            return False
        return self.id == other.id

    def __repr__(self) -> str:
        representation = (
            f"{self.__class__.__name__}(id={self.id!r}, collection={self.collection!r})"
        )
        return representation

    def __str__(self) -> str:
        string_representation = f"ID: {self.id!s}, collection: {self.collection.name!s}"
        return string_representation


class Role:

    created_by = _LazyProperty("created_by")
    created_on = _LazyProperty("created_on")
    created_on_str = _LazyProperty("created_on_str", subordinate_time_string=True)
    description = _LazyProperty("description")
    fixed = _LazyProperty("fixed")
    id = _LazyProperty("id")
    modified_by = _LazyProperty("modified_by")
    modified_by_id = _LazyProperty("modified_by_id")
    modified_on = _LazyProperty("modified_on")
    modified_on_str = _LazyProperty("modified_on_str", subordinate_time_string=True)

    def __init__(
        self,
        connection: APIServer,
        name: str,
        **kwargs,
    ):
        """
        Initialize the Role object.

        Args:
            id (str): The ID of the role.
            name (str): The name of the role.
            **kwargs: Additional keyword arguments.
        """

        self.connection = connection
        self.name = name

        self.created_by = kwargs.get("created_by")
        self.created_on = kwargs.get("created_on")
        self.created_on_str = None
        self.description = kwargs.get("description")
        self.fixed = kwargs.get("fixed")
        self.id = kwargs.get("id")
        self.modified_by = kwargs.get("modified_by")
        self.modified_by_id = kwargs.get("modified_by_id")
        self.modified_on = kwargs.get("modified_on")
        self.modified_on_str = None

        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            self._data = self.connection.role_get_by_name(self.name).json().get("data")
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def permissions(self) -> list[RolePermission]:
        return self.list_permissions()

    @property
    def users(self) -> list[User]:
        return self.list_users()

    def add_user(self, user: str | User, raise_for_status: bool = True) -> User:
        """
        Add a user to the role.

        Args:
            user (str | User): The name of the user to delete or a User object.
            raise_for_status (bool): Whether to raise an exception if the request
                was not successful.
        """
        user = User(self.connection, user) if isinstance(user, str) else user
        user.add_role(self, raise_for_status=raise_for_status)
        return user

    def create(self, raise_for_status=True) -> Role:
        name = self.name
        description = self._description
        response = self.connection.role_create(
            name,
            description,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def create_permission(
        self,
        permission_type: str,
        entity: str | None = None,
        raise_for_status: bool = True,
    ) -> RolePermission:
        """
        Create a new permission.

        Args:
            permission_type (str): The type of the permission.
            entity (str | None): The entity associated with the permission.
            raise_for_status (bool): Whether to raise an exception if the request
                was not successful.
        Returns:
            RolePermission: The created permission object.
        """
        for name, value in VALID_PERMISSION_TYPES:
            if permission_type.lower() == name or permission_type.lower() == value:
                permission_type = value
                break
        else:
            raise ValueError(
                "Received an invalid value for the parameter 'permission_type':"
                f" {permission_type}. "
                "The valid values are: "
                f"{', '.join(str(p) for p in VALID_PERMISSION_TYPES)}."
            )
        response = self.connection.role_permission_create(
            self.name,
            permission_type,
            entity_name=entity,
            raise_for_status=raise_for_status,
        )
        return RolePermission(self.connection, **response.json().get("data"))

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.role_delete(self.name, raise_for_status=raise_for_status)

    def delete_permission(
        self, permission: str | RolePermission, raise_for_status: bool = True
    ) -> None:
        """
        Delete a permission by its ID.

        Args:
            permission (str | RolePermission): The ID of the permission to delete or a
                RolePermission object.
            raise_for_status (bool): Whether to raise an exception if the request
                was not successful.
        """
        permission = (
            RolePermission(self.connection, permission, self)
            if isinstance(permission, str)
            else permission
        )
        permission.delete(raise_for_status=raise_for_status)

    def delete_user(self, user: str | User, raise_for_status: bool = True) -> User:
        """
        Delete a user from the role.

        Args:
            user (str | User): The name of the user to delete or a User object.
            raise_for_status (bool): Whether to raise an exception if the request
                was not successful.
        """
        user = User(self.connection, user) if isinstance(user, str) else user
        user.delete_role(self, raise_for_status=raise_for_status)
        return user

    def get_permission(self, permission_id: str) -> RolePermission:
        """
        Get a permission by its ID.

        Args:
            permission_id (str): The ID of the permission to get.

        Returns:
            RolePermission: The permission object.
        """
        permissions = self.list_permissions(filter=f"id:eq:{permission_id}")
        if not permissions:
            raise ValueError(f"Permission with ID {permission_id} not found.")
        return permissions[0]

    def list_permissions(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[RolePermission]:
        return list(
            self.list_permissions_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_permissions_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[RolePermission]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.role_permission_list(
                self.name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_permissions = data.get("data")
            for raw_permission in raw_permissions:
                built_permission = RolePermission(
                    self.connection,
                    **raw_permission,
                )
                yield built_permission
            first_page = False

    def list_users(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[User]:
        return list(
            self.list_users_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_users_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[User]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.role_user_list(
                self.name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_users = data.get("data")
            for raw_user in raw_users:
                if "name" not in raw_user:
                    raw_user["name"] = raw_user.get("user")
                built_user = User(
                    self.connection,
                    **raw_user,
                )
                yield built_user
            first_page = False

    def refresh(self) -> Role:
        self.created_by = None
        self.created_on = None
        self.created_on_str = None
        self.description = None
        self.id = None
        self.fixed = None
        self.modified_by = None
        self.modified_by_id = None
        self.modified_on = None
        self.modified_on_str = None
        self.kwargs = None
        self._data = None
        return self

    def update(
        self,
        name: str = None,
        description: str = None,
        raise_for_status: bool = True,
    ) -> Role:
        response = self.connection.role_update(
            self.name,
            new_name=name,
            new_description=description,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self.name = response.json().get("data").get("name")
        return self

    def __eq__(self, other):
        if not isinstance(other, Role):
            return False
        return self.name == other.name

    def __repr__(self) -> str:
        representation = f"{self.__class__.__name__}(name={self.name!r})"
        return representation

    def __str__(self) -> str:
        string_representation = f"Name: {self.name!s}"
        return string_representation


class RolePermissionTypes(Enum):
    """
    Enum for permission types.
    """

    COLL_ADMIN = "ca"
    COLL_DEV = "cd"
    COLL_EXE = "cx"
    COLL_READ = "cr"
    SEC_ADMIN = "ss"
    SYS_ADMIN = "sa"


VALID_PERMISSION_TYPES = [
    (e.name.lower(), e.value.lower()) for e in RolePermissionTypes
]

PERMISSION_TYPES_WITH_ENTITY = [
    (
        RolePermissionTypes.COLL_ADMIN.name.lower(),
        RolePermissionTypes.COLL_ADMIN.value.lower(),
    ),
    (
        RolePermissionTypes.COLL_DEV.name.lower(),
        RolePermissionTypes.COLL_DEV.value.lower(),
    ),
    (
        RolePermissionTypes.COLL_EXE.name.lower(),
        RolePermissionTypes.COLL_EXE.value.lower(),
    ),
    (
        RolePermissionTypes.COLL_READ.name.lower(),
        RolePermissionTypes.COLL_READ.value.lower(),
    ),
]


class RolePermission:

    entity = _LazyProperty("entity")
    entity_id = _LazyProperty("entity_id")
    entity_type = _LazyProperty("entity_type")
    fixed = _LazyProperty("fixed")
    granted_by = _LazyProperty("granted_by")
    granted_on = _LazyProperty("granted_on")
    granted_on_str = _LazyProperty("granted_on_str", subordinate_time_string=True)
    permission_type = _LazyProperty("permission_type")

    def __init__(self, connection: APIServer, id: str, role: Role | str, **kwargs):
        """
        Initialize the Permission object.

        Args:
            connection (APIServer): The connection to the TabsdataServer.
            id (str): The ID of the permission.
            role (Role | str): The role associated with the permission.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.role = role
        self.entity = kwargs.get("entity")
        self.entity_id = kwargs.get("entity_id")
        self.entity_type = kwargs.get("entity_type")
        self.fixed = kwargs.get("fixed")
        self.granted_by = kwargs.get("granted_by")
        self.granted_on = kwargs.get("granted_on")
        self.granted_on_str = None
        self.permission_type = kwargs.get("permission_type")

        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            try:
                run = self.role.list_permissions(filter=f"id:eq:{self.id}")[0]
            except IndexError:
                raise ValueError(f"Role permission with ID {self.id} not found.")
            self._data = run.kwargs
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def role(self) -> Role:
        return self._role

    @property
    def type(self) -> str:
        """
        Get the type of the permission.

        Returns:
            str: The type of the permission.
        """
        type = self.permission_type
        for name, value in VALID_PERMISSION_TYPES:
            if type.lower() == name or type.lower() == value:
                type = name
                break
        return type

    @role.setter
    def role(self, role: str | Role):
        if isinstance(role, str):
            self._role = Role(self.connection, role)
        elif isinstance(role, Role):
            self._role = role
        else:
            raise TypeError(
                f"Role must be a string or a Role object; got{type(role)} instead."
            )

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.role_permission_delete(
            self.role.name, self.id, raise_for_status=raise_for_status
        )

    def refresh(self) -> RolePermission:
        self.entity = None
        self.entity_id = None
        self.entity_type = None
        self.fixed = None
        self.granted_by = None
        self.granted_on = None
        self.granted_on_str = None
        self.permission_type = None
        self.kwargs = None
        self._data = None
        return self

    def __eq__(self, other):
        if not isinstance(other, RolePermission):
            return False
        return self.id == other.id

    def __repr__(self) -> str:
        representation = (
            f"{self.__class__.__name__}(id={self.id!r}, role={self.role!r})"
        )
        return representation

    def __str__(self) -> str:
        string_representation = f"ID: {self.id!s}, role: {self.role.name!s}"
        return string_representation


class ServerStatus:
    """
    This class represents the status of the TabsdataServer.

    Args:
        status (str): The status of the server.
        latency_as_nanos (int): The latency of the server in nanoseconds.
        **kwargs: Additional keyword arguments.
    """

    def __init__(self, status: str, latency_as_nanos: int, **kwargs):
        """
        Initialize the ServerStatus object.

        Args:
            status (str): The status of the server.
            latency_as_nanos (int): The latency of the server in nanoseconds.
            **kwargs: Additional keyword arguments.
        """
        self.status = status
        self.latency_as_nanos = latency_as_nanos
        self.kwargs = kwargs

    def __eq__(self, other) -> bool:
        if not isinstance(other, ServerStatus):
            return False
        return self.status == other.status

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(status={self.status!r},"
            f" latency_as_nanos={self.latency_as_nanos!r})"
        )

    def __str__(self) -> str:
        latency_as_seconds = self.latency_as_nanos / float(10**9)
        formatted_latency = f"{latency_as_seconds:.6f}"
        return f"Status: {self.status!r} - Latency (s): {formatted_latency}"


class Table:
    """
    This class represents a table in the TabsdataServer.

    Args:
        collection (str | Collection): The collection where the table is stored.
        name (str): The name of the table.
        **kwargs: Additional keyword arguments.
    """

    def __init__(
        self, connection: APIServer, collection: str | Collection, name: str, **kwargs
    ):
        self.connection = connection
        self.collection = collection
        self.name = name

        self.id = kwargs.get("id")
        self.function = kwargs.get("function_name")
        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            try:
                self._data_dict = (
                    self.connection.table_list(
                        self.collection.name, request_filter=f"name:eq:{self.name}"
                    )
                    .json()
                    .get("data")
                    .get("data")[0]
                )
            except IndexError:
                raise ValueError(
                    f"Table with name {self.name} not found in collection "
                    f"{self.collection.name}."
                )
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        else:
            raise TypeError(
                "Collection must be a string or a Collection object; got"
                f"{type(collection)} instead."
            )

    @property
    def dataversions(self) -> List[DataVersion]:
        return self.list_dataversions()

    @property
    def function(self) -> Function | None:
        if self._function is None:
            self.function = self._data.get("function_name")
        return self._function

    @function.setter
    def function(self, function: str | Function | None):
        if isinstance(function, str):
            self._function = Function(self.connection, self.collection, function)
        elif isinstance(function, Function):
            self._function = function
        elif function is None:
            self._function = None
        else:
            raise TypeError(
                "Function must be a string, a Function object or None; got"
                f"{type(function)} instead."
            )

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.table_delete(
            self.collection.name, self.name, raise_for_status=raise_for_status
        )

    def download(
        self,
        destination_file: str,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        version: DataVersion | str | None = None,
        raise_for_status: bool = True,
    ):
        """
        Download a table for a given version as a parquet file. The version can
            be a fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            destination_file (str): The path to the destination file.
            commit (str, optional): The commit ID of the table to be downloaded.
            time (str, optional): If provided, the table version that was
                published last before that time will be downloaded.
            version (str, optional): The version of the table to be downloaded. The
                version can be a fixed version or a relative one (HEAD, HEAD^,
                and HEAD~## syntax). Defaults to "HEAD".
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the table could not be downloaded.
        """
        provided = [x is not None for x in (at, at_trx, version)]
        if sum(provided) > 1:
            raise ValueError(
                "Only one of 'at', 'at_trx' or 'version' can be provided at a time."
            )
        if at:
            at = _top_and_convert_to_timestamp(at)
        elif at_trx:
            if isinstance(at_trx, Transaction):
                transaction = at_trx
            else:
                transaction = Transaction(self.connection, at_trx)
            at = transaction.ended_on
        elif version:
            if isinstance(version, DataVersion):
                dataversion = version
            else:
                dataversion = DataVersion(
                    self.connection,
                    collection=self.collection,
                    table=self,
                    id=version,
                )
            at = dataversion.created_at
        response = self.connection.table_download(
            self.collection.name,
            self.name,
            at=at,
            raise_for_status=raise_for_status,
        )
        with open(destination_file, "wb") as file:
            file.write(response.content)

    def list_dataversions(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[DataVersion]:
        """
        List the data versions of a table in a collection.

        Args:

        Returns:
            List[DataVersion]: The list of data versions of the function.

        Raises:
            APIServerError: If the data versions could not be listed.
        """
        return list(
            self.list_dataversions_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_dataversions_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[DataVersion]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.dataversion_list(
                self.collection.name,
                self.name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_dataversions = data.get("data")
            for raw_dataversion in raw_dataversions:
                built_dataversion = DataVersion(
                    self.connection,
                    table=self,
                    **raw_dataversion,
                )
                yield built_dataversion
            first_page = False

    def get_schema(
        self,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        version: DataVersion | str | None = None,
    ) -> List[dict]:
        """
        Get the schema of a table for a given version. The version can be a
            fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:

        Returns:
            list: The schema of the table.

        Raises:
            APIServerError: If the schema could not be obtained.
        """
        provided = [x is not None for x in (at, at_trx, version)]
        if sum(provided) > 1:
            raise ValueError(
                "Only one of 'at', 'at_trx' or 'version' can be provided at a time."
            )
        if at:
            at = _top_and_convert_to_timestamp(at)
        elif at_trx:
            if isinstance(at_trx, Transaction):
                transaction = at_trx
            else:
                transaction = Transaction(self.connection, at_trx)
            at = transaction.ended_on
        elif version:
            if isinstance(version, DataVersion):
                dataversion = version
            else:
                dataversion = DataVersion(
                    self.connection,
                    collection=self.collection,
                    table=self,
                    id=version,
                )
            at = dataversion.created_at
        return (
            self.connection.table_get_schema(
                self.collection.name,
                self.name,
                at=at,
            )
            .json()
            .get("data")
        )["fields"]

    def refresh(self) -> Table:
        self.id = None
        self.function = None
        self.kwargs = None
        self._data = None
        return self

    def sample(
        self,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        version: DataVersion | str | None = None,
        offset: int = None,
        len: int = None,
    ) -> pl.DataFrame:
        """
        Get a sample of a table for a given version as a parquet file. The
            version can be a fixed version or a relative one (HEAD, HEAD^,
            and HEAD~## syntax).

        Args:
            offset (int, optional): The offset of the sample.
            len (int, optional): The length of the sample.
        Raises:
            APIServerError: If the sample could not be obtained.
        """
        provided = [x is not None for x in (at, at_trx, version)]
        if sum(provided) > 1:
            raise ValueError(
                "Only one of 'at', 'at_trx' or 'version' can be provided at a time."
            )
        if at:
            at = _top_and_convert_to_timestamp(at)
        elif at_trx:
            if isinstance(at_trx, Transaction):
                transaction = at_trx
            else:
                transaction = Transaction(self.connection, at_trx)
            at = transaction.ended_on
        elif version:
            if isinstance(version, DataVersion):
                dataversion = version
            else:
                dataversion = DataVersion(
                    self.connection,
                    collection=self.collection,
                    table=self,
                    id=version,
                )
            at = dataversion.created_at
        parquet_frame = self.connection.table_get_sample(
            self.collection.name,
            self.name,
            at=at,
            offset=offset,
            len=len,
        ).content
        return pl.read_parquet(parquet_frame)

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False
        return self.name == other.name and self.collection == other.collection

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!r}"


class Transaction:
    """
    This class represents a transaction in the TabsdataServer.

    Args:
        id (str): The ID of the transaction.
        execution_id (str): The ID of the execution.
        status (str): The status of the transaction.
        triggered_on (int): The timestamp when the transaction was triggered.
        ended_on (int): The timestamp when the transaction ended.
        started_on (int): The timestamp when the transaction started.
        **kwargs: Additional keyword arguments.

    Attributes:
        triggered_on_str (str): The timestamp when the transaction was triggered as a
            string.
        ended_on_str (str): The timestamp when the transaction ended as a string.
        started_on_str (str): The timestamp when the transaction started as a string.
    """

    ended_on = _LazyProperty("ended_on")
    ended_on_str = _LazyProperty("ended_on_str", subordinate_time_string=True)
    started_on = _LazyProperty("started_on")
    started_on_str = _LazyProperty("started_on_str", subordinate_time_string=True)
    triggered_by = _LazyProperty("triggered_by")
    triggered_on = _LazyProperty("triggered_on")
    triggered_on_str = _LazyProperty("triggered_on_str", subordinate_time_string=True)

    def __init__(
        self,
        connection: APIServer,
        id: str,
        **kwargs,
    ):
        self.id = id
        self.connection = connection

        self.collection = kwargs.get("collection")
        self.execution = kwargs.get("execution_id")
        self.status = kwargs.get("status")
        self.triggered_by = kwargs.get("triggered_by")
        self.triggered_on = kwargs.get("triggered_on")
        self.triggered_on_str = None
        self.ended_on = kwargs.get("ended_on")
        self.ended_on_str = None
        self.started_on = kwargs.get("started_on")
        self.started_on_str = None
        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            transaction = tabsdata_server.list_transactions(filter=f"id:eq:{self.id}")[
                0
            ]
            self._data = transaction.kwargs
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        if self._collection is None:
            self.collection = self._data.get("collection")
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection | None):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        elif collection is None:
            self._collection = None
        else:
            raise TypeError(
                "Collection must be a string or a Collection object; got"
                f"{type(collection)} instead."
            )

    @property
    def execution(self) -> Execution:
        if self._execution is None:
            self.execution = self._data.get("execution_id")
        return self._execution

    @execution.setter
    def execution(self, execution: str | Execution | None):
        if isinstance(execution, str):
            self._execution = Execution(self.connection, execution)
        elif isinstance(execution, Execution):
            self._execution = execution
        elif execution is None:
            self._execution = None
        else:
            raise TypeError(
                "Execution must be a string, an Execution object or None; got"
                f"{type(execution)} instead."
            )

    @property
    def function_runs(self) -> list[FunctionRun]:
        raw_function_runs = (
            self.connection.function_list_runs(
                request_filter=f"transaction_id:eq:{self.id}"
            )
            .json()
            .get("data")
            .get("data")
        )
        return [
            FunctionRun(**{**function_run, "connection": self.connection})
            for function_run in raw_function_runs
        ]

    @property
    def status(self) -> str:
        if self._status is None:
            self.status = self._data.get("status")
        return self._status

    @status.setter
    def status(self, status: str | None):
        if status is None:
            self._status = status
        else:
            self._status = transaction_status_to_mapping(status)

    @property
    def workers(self) -> list[Worker]:
        raw_workers = (
            self.connection.workers_list(request_filter=f"transaction_id:eq:{self.id}")
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker, "connection": self.connection})
            for worker in raw_workers
        ]

    def cancel(self) -> requests.Response:
        """
        Cancel a transaction. This includes all functions that are part of the
            transaction and all its dependants.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_id is not found in the system.
        """
        response = self.connection.transaction_cancel(self.id)
        self.refresh()
        return response

    def get_worker(self, worker_id: str):
        for worker in self.workers:
            if worker.id == worker_id:
                return worker
        raise ValueError(f"Worker {worker_id} not found for transaction {self.id}")

    def recover(self) -> requests.Response:
        """
        Recover a transaction. This includes all functions that are part of the
            transaction.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_id is not found in the system.
        """
        response = self.connection.transaction_recover(self.id)
        self.refresh()
        return response

    def refresh(self) -> Transaction:
        self.execution = None
        self.status = None
        self.triggered_by = None
        self.triggered_on = None
        self.triggered_on_str = None
        self.ended_on = None
        self.started_on = None
        self.started_on_str = None
        self.kwargs = None
        self._data = None
        return self

    def __eq__(self, other):
        if not isinstance(other, Transaction):
            return False
        return self.id == other.id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"

    def __str__(self) -> str:
        return f"ID: {self.id!r}"


class User:
    """
    This class represents a user in the TabsdataServer.

    Args:
        name (str): The name of the user.
        full_name (str): The full name of the user.
        email (str): The email of the user.
        enabled (bool): Whether the user is enabled or not.
        **kwargs: Additional keyword arguments.
    """

    email = _LazyProperty("email")
    enabled = _LazyProperty("enabled")
    full_name = _LazyProperty("full_name")

    def __init__(
        self,
        connection: APIServer,
        name: str,
        full_name: str = None,
        email: str = None,
        enabled: bool = None,
        **kwargs,
    ):
        """
        Initialize the User object.

        Args:
            name (str): The name of the user.
            full_name (str): The full name of the user.
            email (str): The email of the user.
            enabled (bool): Whether the user is enabled or not.
            **kwargs: Additional keyword arguments.
        """
        self.name = name
        self.connection = connection
        self.full_name = full_name
        self.email = email
        self.enabled = enabled
        self._data = None
        self.kwargs = kwargs

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            self._data = self.connection.users_get_by_name(self.name).json().get("data")
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    def add_role(self, role: str | Role, raise_for_status: bool = True) -> Role:
        """
        Add a role to the user.

        Args:
            role (str | Role): The role to add to the user.
            raise_for_status (bool): Whether to raise an exception if the request
                was not successful.
        """
        if isinstance(role, str):
            pass
        elif isinstance(role, Role):
            role = role.name
        else:
            raise TypeError(
                "The 'role' parameter must be a string or a Role object; got"
                f"{type(role)} instead."
            )
        self.connection.role_user_add(
            role, self.name, raise_for_status=raise_for_status
        )
        return Role(self.connection, role)

    def create(self, password: str, raise_for_status=True) -> User:
        full_name = self._full_name or self.name
        email = self._email
        # This logic might look confusing, but the reason why it is such is that
        # self._enabled can have 3 values:
        #     1. None: in this case, enabled must be True
        #     2. True: in this case, enabled must be True
        #     3. False: in this case, enabled must be False
        enabled = False if self._enabled is False else True
        response = self.connection.users_create(
            self.name,
            full_name,
            email,
            password,
            enabled,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.users_delete(self.name, raise_for_status=raise_for_status)

    def delete_role(self, role: str | Role, raise_for_status: bool = True) -> Role:
        """
        Delete a role from the user.

        Args:
            role (str | Role): The role to remove from the user.
            raise_for_status (bool): Whether to raise an exception if the request
                was not successful.
        """
        if isinstance(role, str):
            pass
        elif isinstance(role, Role):
            role = role.name
        else:
            raise TypeError(
                "The 'role' parameter must be a string or a Role object; got"
                f"{type(role)} instead."
            )
        self.connection.role_user_delete(
            role, self.name, raise_for_status=raise_for_status
        )
        return Role(self.connection, role)

    def read_role(self, role: str | Role, raise_for_status: bool = True) -> dict:
        if isinstance(role, str):
            pass
        elif isinstance(role, Role):
            role = role.name
        else:
            raise TypeError(
                "The 'role' parameter must be a string or a Role object; got"
                f"{type(role)} instead."
            )
        response = self.connection.role_user_read(
            role, self.name, raise_for_status=raise_for_status
        )
        return response.json().get("data")

    def refresh(self) -> User:
        self.full_name = None
        self.email = None
        self.enabled = None
        self._data = None
        self.kwargs = None
        return self

    def update(
        self,
        full_name: str = None,
        email: str = None,
        password: str = None,
        enabled: bool = None,
        raise_for_status: bool = True,
    ) -> User:
        response = self.connection.users_update(
            self.name,
            full_name=full_name,
            email=email,
            enabled=enabled,
            password=password,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def __eq__(self, other) -> bool:
        if not isinstance(other, User):
            return False
        return self.name == other.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!r}"


class Worker:
    """
    This class represents a worker in the TabsdataServer.

    Args:
        id (str): The ID of the worker.
        collection (str): The collection of the worker.
        function (str): The function of the worker.
        function_id (str): The ID of the function of the worker.
        transaction_id (str): The ID of the transaction of the worker.
        execution (str): The execution of the worker.
        execution_id (str): The ID of the execution of the worker.
        status (str): The status of the associated data version.
        **kwargs: Additional keyword arguments.
    """

    def __init__(
        self,
        connection: APIServer,
        id: str,
        **kwargs,
    ):
        """
        Initialize the Worker object.

        Args:
            id (str): The ID of the worker.
            collection (str): The collection of the worker.
            function (str): The function of the worker.
            function_id (str): The ID of the function of the worker.
            transaction_id (str): The ID of the transaction of the worker.
            execution (str): The execution of the worker.
            execution_id (str): The ID of the execution of the worker.
            data_version_id (str): The ID of the data version of the worker.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.collection = kwargs.get("collection")
        self.function = kwargs.get("function")
        self.transaction = kwargs.get("transaction_id")
        self.execution = kwargs.get("execution_id")
        self.status = kwargs.get("status")
        self._data = None
        self.kwargs = kwargs

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            try:
                worker = tabsdata_server.list_workers(filter=f"id:eq:{self.id}")[0]
            except IndexError:
                raise ValueError(f"Worker with ID {self.id} not found.")
            self._data = worker.kwargs
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def collection(self) -> Collection:
        if self._collection is None:
            self.collection = self._data.get("collection")
        return self._collection

    @collection.setter
    def collection(self, collection: str | Collection | None):
        if isinstance(collection, str):
            self._collection = Collection(self.connection, collection)
        elif isinstance(collection, Collection):
            self._collection = collection
        elif collection is None:
            self._collection = None
        else:
            raise TypeError(
                "Collection must be a string or a Collection object; got"
                f"{type(collection)} instead."
            )

    @property
    def execution(self) -> Execution:
        if self._execution is None:
            self.execution = self._data.get("execution_id")
        return self._execution

    @execution.setter
    def execution(self, execution: str | Execution | None):
        if isinstance(execution, str):
            self._execution = Execution(self.connection, execution)
        elif isinstance(execution, Execution):
            self._execution = execution
        elif execution is None:
            self._execution = None
        else:
            raise TypeError(
                "Execution must be a string, an Execution object or None; got"
                f"{type(execution)} instead."
            )

    @property
    def function(self) -> Function:
        if self._function is None:
            self.function = self._data.get("function")
        return self._function

    @function.setter
    def function(self, function: str | Function | None):
        if isinstance(function, str):
            self._function = Function(self.connection, self.collection, function)
        elif isinstance(function, Function):
            self._function = function
        elif function is None:
            self._function = None
        else:
            raise TypeError(
                "Function must be a string or a Function object; got"
                f"{type(function)} instead."
            )

    @property
    def log(self) -> str:
        """
        Get the logs of a worker in the server.

        Returns:
            str: The worker logs.
        """
        return self.connection.worker_log(self.id).text

    @property
    def status(self) -> str:
        if self._status is None:
            self.status = self._data.get("function_run_status")
        return self._status

    @status.setter
    def status(self, status: str | None):
        if status is None:
            self._status = None
        else:
            self._status = worker_status_to_mapping(status)

    @property
    def transaction(self) -> Transaction:
        if self._transaction is None:
            self.transaction = self._data.get("transaction_id")
        return self._transaction

    @transaction.setter
    def transaction(self, transaction: str | Transaction | None):
        if isinstance(transaction, str):
            self._transaction = Transaction(self.connection, transaction)
        elif isinstance(transaction, Transaction):
            self._transaction = transaction
        elif transaction is None:
            self._transaction = None
        else:
            raise TypeError(
                "Transaction must be a string, a Transaction object or None; got"
                f"{type(transaction)} instead."
            )

    def __eq__(self, other) -> bool:
        if not isinstance(other, Worker):
            return False
        return self.id == other.id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"

    def __str__(self) -> str:
        return f"ID: {self.id!s}"


class TabsdataServer:
    """
    This class represents the TabsdataServer.

    Args:
        url (str): The url of the server.
        username (str): The username of the user.
        password (str): The password of the user.
    """

    def __init__(
        self, url: str, username: str = None, password: str = None, role: str = None
    ):
        """
        Initialize the TabsdataServer object.

        Args:
            url (str): The url of the server.
            username (str): The username of the user.
            password (str): The password of the user.
        """
        self.connection = obtain_connection(url, username, password, role)

    @property
    def collections(self) -> List[Collection]:
        """
        Get the list of collections in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Collection]: The list of collections in the server.
        """
        return self.list_collections()

    @property
    def executions(self) -> List[Execution]:
        """
        Get the list of executions in the server. This list is obtained every time
            the property is accessed, so sequential accesses to this property in the
            same object might yield different results.

        Returns:
            List[Execution]: The list of executions in the server.
        """
        return self.list_executions()

    @property
    def roles(self) -> List[Role]:
        """
        Get the list of roles in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Role]: The list of roles in the server.
        """
        return self.list_roles()

    @property
    def status(self) -> ServerStatus:
        """
        Get the status of the server. This status is obtained every time the property is
            accessed, so sequential accesses to this property in the same object might
            yield different results.

        Returns:
            ServerStatus: The status of the server.
        """
        return ServerStatus(**self.connection.status_get().json().get("data"))

    @property
    def transactions(self) -> List[Transaction]:
        """
        Get the list of transactions in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Transaction]: The list of transactions in the server.
        """
        return self.list_transactions()

    @property
    def users(self) -> List[User]:
        """
        Get the list of users in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[User]: The list of users in the server.
        """
        return self.list_users()

    @property
    def valid_python_versions(self) -> list[str]:
        """
        Get the list of valid Python versions supported by the server.

        Returns:
            List[str]: The list of valid Python versions.
        """
        return (
            self.connection.runtime_info_get()
            .json()
            .get("data", {})
            .get("python_versions")
        )

    @property
    def workers(self) -> List[Worker]:
        """
        Get the list of workers in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Worker]: The list of workers in the server.
        """
        return self.list_workers()

    def create_collection(
        self, name: str, description: str = None, raise_for_status: bool = True
    ) -> Collection:
        """
        Create a collection in the server.

        Args:
            name (str): The name of the collection.
            description (str, optional): The description of the collection.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the collection could not be created.
        """
        collection = Collection(self.connection, name, description=description)
        collection.create(raise_for_status=raise_for_status)
        return collection

    def delete_collection(self, name: str, raise_for_status: bool = True) -> None:
        """
        Delete a collection in the server.

        Args:
            name (str): The name of the collection.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the collection could not be deleted.
        """
        collection = Collection(self.connection, name)
        collection.delete(raise_for_status=raise_for_status)

    def get_collection(self, name: str) -> Collection:
        """
        Get a collection in the server.

        Args:
            name (str): The name of the collection.

        Returns:
            Collection: The collection.

        Raises:
            APIServerError: If the collection could not be obtained.
        """
        collection_definition = self.connection.collection_get_by_name(name)
        return Collection(
            self.connection,
            **collection_definition.json().get("data"),
        )

    def list_collections(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Collection]:
        return list(
            self.list_collections_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_collections_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Collection]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.collection_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_collections = data.get("data")
            for raw_collection in raw_collections:
                built_collection = Collection(self.connection, **raw_collection)
                yield built_collection
            first_page = False

    def list_functions(
        self,
        collection_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Function]:
        """
        List the functions in a collection.

        Args:
            collection_name (str): The name of the collection.

        Returns:
            List[Function]: The list of functions in the collection.

        Raises:
            APIServerError: If the functions could not be listed.
        """
        return Collection(self.connection, collection_name).list_functions(
            filter=filter,
            order_by=order_by,
            raise_for_status=raise_for_status,
        )

    def update_collection(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        raise_for_status: bool = True,
    ) -> Collection:
        """
        Update a collection in the server.

        Args:
            name (str): The name of the collection.
            new_name (str, optional): The new name of the collection.
            new_description (str, optional): The new description of the collection.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the collection could not be updated.
        """
        collection = Collection(self.connection, name)
        return collection.update(
            name=new_name,
            description=new_description,
            raise_for_status=raise_for_status,
        )

    def list_dataversions(
        self,
        collection_name: str,
        table_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[DataVersion]:
        """
        List the data versions of a table in a collection.

        Args:
            collection_name (str): The name of the collection.

        Returns:
            List[DataVersion]: The list of data versions of the function.

        Raises:
            APIServerError: If the data versions could not be listed.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.list_dataversions(
            filter=filter, order_by=order_by, raise_for_status=raise_for_status
        )

    def cancel_execution(self, execution_id: str) -> requests.Response:
        """
        Cancel an execution. This includes all transactions that are part of the
            execution.

        Args:
            execution_id (str): The ID of the execution to cancel.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_id is not found in the system.
        """
        execution = Execution(self.connection, execution_id)
        return execution.cancel()

    def get_execution(self, execution_id: str) -> Execution:
        """
        Get an execution in the server.

        Args:
            execution_id (str): The ID of the execution.

        Returns:
            Execution: The execution.

        Raises:
            APIServerError: If the execution could not be obtained.
        """
        try:
            return self.list_executions(filter=f"id:eq:{execution_id}")[0]
        except IndexError:
            raise ValueError(f"Execution with ID {execution_id} not found.")

    def list_executions(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Execution]:
        return list(
            self.list_executions_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_executions_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Execution]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.execution_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_executions = data.get("data")
            for raw_execution in raw_executions:
                built_execution = Execution(self.connection, **raw_execution)
                yield built_execution
            first_page = False

    def recover_execution(self, execution_id: str) -> requests.Response:
        """
        Recover an execution. This includes all transactions that are part of the
            execution.

        Args:
            execution_id (str): The ID of the execution to recover.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_id is not found in the system.
        """
        execution = Execution(self.connection, execution_id)
        return execution.recover()

    def register_function(
        self,
        collection_name: str,
        function_path: str,
        description: str = None,
        path_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        function_name: str = None,
        reuse_tables: bool = False,
        raise_for_status: bool = True,
    ) -> Function:
        """
        Create a function in the server.

        Args:
            collection_name (str): The name of the collection.
            function_path (str): The path to the function. It should be in the form of
                /path/to/file.py::function_name.
            description (str, optional): The description of the function.
            path_to_bundle (str, optional): The path that has to be bundled and sent
                to the server. If None, the folder containing the function will be
                bundled.
            requirements (str, optional): Path to a custom requirements.yaml file
                with the packages, python version and other information needed to
                create the Python environment for the function to run in the backend.
                If not provided, this information will be inferred from the current
                execution session.
            local_packages (List[str] | str, optional): A list of paths to local
                Python packages that need to be included in the bundle. Each path
                must exist and be a valid Python package that can be installed by
                running `pip install /path/to/package`.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the function could not be created.
        """

        collection = Collection(self.connection, collection_name)
        return collection.register_function(
            function_path,
            description=description,
            path_to_bundle=path_to_bundle,
            requirements=requirements,
            local_packages=local_packages,
            function_name=function_name,
            reuse_tables=reuse_tables,
            raise_for_status=raise_for_status,
        )

    def delete_function(
        self, collection_name, function_name, raise_for_status: bool = True
    ) -> None:
        """
        Delete a function in the server.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the function could not be deleted.
        """
        function = Function(self.connection, collection_name, function_name)
        function.delete(raise_for_status=raise_for_status)

    def get_function(self, collection_name, function_name) -> Function:
        """
        Get a function in the server.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.

        Returns:
            Function: The function.

        Raises:
            APIServerError: If the function could not be obtained.
        """
        collection = self.get_collection(collection_name)
        return collection.get_function(function_name)

    def list_function_history(self, collection_name, function_name) -> List[Function]:
        """
        List the version history of a function.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.

        Returns:
            List[Function]: The list of versions of the function.

        Raises:
            APIServerError: If the data could not be listed.
        """
        function = Function(self.connection, collection_name, function_name)
        return function.history

    def get_function_run(self, function_run_id: str) -> FunctionRun:
        try:
            return self.list_function_runs(filter=f"id:eq:{function_run_id}")[0]
        except IndexError:
            raise ValueError(f"Function Run with ID {function_run_id} not found.")

    def list_function_runs(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[FunctionRun]:
        return list(
            self.list_function_runs_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_function_runs_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[FunctionRun]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.function_list_runs(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_function_runs = data.get("data")
            for raw_function_run in raw_function_runs:
                built_function_run = FunctionRun(self.connection, **raw_function_run)
                yield built_function_run
            first_page = False

    def read_function_run(
        self,
        collection: Collection | str,
        function: Function | str,
        execution: Execution | str,
        raise_for_status: bool = True,
    ) -> requests.Response:
        """
        Read the run of a function in the server.

        Args:
            collection(Collection | str): The name of the collection or a
                Collection object.
            function(Function | str): The name of the function or a Function object.
            execution(Execution | str): The ID of the execution or am
                Execution object.

        Raises:
            APIServerError: If the run could not be obtained.
        """
        function = (
            function
            if isinstance(function, Function)
            else Function(self.connection, collection, function)
        )
        return function.read_run(execution, raise_for_status=raise_for_status)

    def trigger_function(
        self,
        collection_name,
        function_name,
        execution_name: str | None = None,
        raise_for_status: bool = True,
    ) -> Execution:
        """
        Trigger a function in the server.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.
            execution_name (str, optional): The name of the execution.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            requests.Response: The response of the trigger request.

        Raises:
            APIServerError: If the function could not be triggered.
        """
        function = Function(self.connection, collection_name, function_name)
        return function.trigger(
            execution_name=execution_name, raise_for_status=raise_for_status
        )

    def update_function(
        self,
        collection_name: str,
        function_name: str,
        function_path: str,
        description: str,
        directory_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        new_function_name: str = None,
        reuse_tables: bool = False,
        raise_for_status: bool = True,
    ) -> Function:
        """
        Update a function in the server.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.
            function_path (str): The path to the function. It should be in the form of
                /path/to/file.py::function_name.
            description (str): The new description of the function.
            directory_to_bundle (str, optional): The path that has to be bundled and
                sent to the server. If None, the folder containing the function will be
                bundled.
            requirements (str, optional): Path to a custom requirements.yaml file
                with the packages, python version and other information needed to
                create the Python environment for the function to run in the backend.
                If not provided, this information will be inferred from the current
                execution session.
            local_packages (List[str] | str, optional): A list of paths to local
                Python packages that need to be included in the bundle. Each path
                must exist and be a valid Python package that can be installed by
                running `pip install /path/to/package`.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the function could not be updated.
        """
        collection = Collection(self.connection, collection_name)
        return collection.update_function(
            function_name,
            function_path,
            description,
            directory_to_bundle=directory_to_bundle,
            requirements=requirements,
            local_packages=local_packages,
            new_function_name=new_function_name,
            reuse_tables=reuse_tables,
            raise_for_status=raise_for_status,
        )

    def create_inter_coll_perm(
        self,
        collection: str | Collection,
        to_collection: str | Collection,
        raise_for_status: bool = True,
    ) -> InterCollectionPermission:
        """
        Create an inter-collection permission in the server.

        Args:
            collection (str | Collection): The name of the collection or a Collection
                object.
            to_collection (str | Collection): The name of the target collection or a
                Collection object.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            InterCollectionPermission: The created inter-collection permission.
        """
        collection = (
            collection
            if isinstance(collection, Collection)
            else (self.get_collection(collection))
        )
        return collection.create_permission(
            to_collection, raise_for_status=raise_for_status
        )

    def delete_inter_coll_perm(
        self, collection: str, permission: str, raise_for_status: bool = True
    ):
        """
        Delete an inter-collection permission in the server.

        Args:
            collection (str): The name of the collection.
            permission (str): The id of the
                inter-collection permission.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            InterCollectionPermission: The created inter-collection permission.
        """
        collection = self.get_collection(collection)
        collection.delete_permission(permission, raise_for_status=raise_for_status)

    def list_inter_coll_perm(
        self,
        collection: str | Collection,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> list[InterCollectionPermission]:
        """
        List the inter-collection permissions in the server.

        Returns:
            List[InterCollectionPermission]: The list of inter-collection permissions.
        """
        collection = (
            collection
            if isinstance(collection, Collection)
            else (self.get_collection(collection))
        )
        return collection.list_permissions(
            filter=filter, order_by=order_by, raise_for_status=raise_for_status
        )

    def login(self, username: str, password: str, role: str = None):
        self.connection.authentication_login(
            username,
            password,
            role=role,
        )

    def logout(self, raise_for_status: bool = True):
        return self.connection.authentication_logout(raise_for_status=raise_for_status)

    def change_password(
        self,
        username: str,
        old_password: str,
        new_password: str,
        raise_for_status: bool = True,
    ):
        self.connection.authentication_password_change(
            username, old_password, new_password, raise_for_status=raise_for_status
        )

    def change_role(self, role: str):
        self.connection.authentication_role_change(role)

    def auth_info(self) -> dict:
        data = self.connection.authentication_info().json().get("data")
        all_roles = data.get("user_roles", [])
        roles_by_name = [role["name"] for role in all_roles]
        current_role_id = data.get("current_role_id", "")
        current_role = next(
            (role["name"] for role in all_roles if role["id"] == current_role_id), ""
        )
        info = {
            "name": data.get("name"),
            "email": data.get("email"),
            "current_role": current_role,
            "roles": roles_by_name,
        }
        return info

    def create_role(
        self, name: str, description: str = None, raise_for_status: bool = True
    ) -> Role:
        """
        Create a role in the server.

        Args:
            name (str): The name of the role.
            description (str, optional): The description of the role.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the collection could not be created.
        """
        role = Role(self.connection, name, description=description)
        role.create(raise_for_status=raise_for_status)
        return role

    def delete_role(self, name: str, raise_for_status: bool = True) -> None:
        """
        Delete a role in the server.

        Args:
            name (str): The name of the role.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the collection could not be deleted.
        """
        role = Role(self.connection, name)
        role.delete(raise_for_status=raise_for_status)

    def get_role(self, name: str) -> Role:
        """
        Get a role in the server.

        Args:
            name (str): The name of the role.

        Returns:
            Role: The role.

        Raises:
            APIServerError: If the collection could not be obtained.
        """
        role_definition = self.connection.role_get_by_name(name)
        return Role(
            self.connection,
            **role_definition.json().get("data"),
        )

    def list_roles(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Role]:
        """
        List the roles in the server.

        Returns:
            List[Role]: The list of roles in the server.
        """
        return list(self.list_roles_generator(filter, order_by, raise_for_status))

    def list_roles_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Role]:
        """
        List the roles in the server.

        Returns:
            List[Role]: The list of roles in the server.
        """
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.role_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_roles = data.get("data")
            for raw_role in raw_roles:
                built_role = Role(self.connection, **raw_role)
                yield built_role
            first_page = False

    def update_role(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        raise_for_status: bool = True,
    ) -> Role:
        """
        Update a role in the server.

        Args:
            name (str): The name of the role.
            new_name (str, optional): The new name of the role.
            new_description (str, optional): The new description of the role.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the collection could not be updated.
        """
        role = Role(self.connection, name)
        return role.update(
            name=new_name,
            description=new_description,
            raise_for_status=raise_for_status,
        )

    def create_role_permission(
        self,
        role_name: str,
        permission_type: str,
        entity: str | None = None,
        raise_for_status: bool = True,
    ) -> RolePermission:
        """
        Create a permission for a role in the server.

        Args:
            role_name (str): The name of the role.
            permission_type (str): The permission type.
            entity (str | None): The entity to which the permission applies. If None,
                the permission applies to all entities.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the permission could not be created.
        """
        role = self.get_role(role_name)
        return role.create_permission(
            permission_type, entity=entity, raise_for_status=raise_for_status
        )

    def delete_role_permission(
        self, role_name: str, permission_id: str, raise_for_status: bool = True
    ) -> None:
        """
        Delete a permission for a role in the server.
        """
        role = self.get_role(role_name)
        role.delete_permission(permission_id, raise_for_status=raise_for_status)

    def list_role_permissions(
        self,
        role_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> list[RolePermission]:
        role = self.get_role(role_name)
        return role.list_permissions(
            filter=filter, order_by=order_by, raise_for_status=raise_for_status
        )

    def list_role_users(
        self,
        role_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> list[User]:
        role = self.get_role(role_name)
        return role.list_users(
            filter=filter, order_by=order_by, raise_for_status=raise_for_status
        )

    def delete_table(
        self, collection_name: str, table_name: str, raise_for_status: bool = True
    ) -> None:
        """
        Delete a table in the server.

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the table could not be deleted.
        """
        table = Table(self.connection, collection_name, table_name)
        table.delete(raise_for_status=raise_for_status)

    def download_table(
        self,
        collection_name: str,
        table_name: str,
        destination_file: str,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        version: DataVersion | str | None = None,
        raise_for_status: bool = True,
    ):
        """
        Download a table for a given version as a parquet file. The version can
            be a fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
            destination_file (str): The path to the destination file.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the table could not be downloaded.
        """
        table = Table(self.connection, collection_name, table_name)
        table.download(
            destination_file,
            at=at,
            at_trx=at_trx,
            version=version,
            raise_for_status=raise_for_status,
        )

    def get_table_schema(
        self,
        collection_name: str,
        table_name: str,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        version: DataVersion | str | None = None,
    ) -> List[dict]:
        """
        Get the schema of a table for a given version. The version can be a
            fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.

        Returns:
            list: The schema of the table.

        Raises:
            APIServerError: If the schema could not be obtained.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.get_schema(at=at, at_trx=at_trx, version=version)

    def list_tables(
        self,
        collection_name: str,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        filter: List[str] | str = None,
        order_by: str = None,
    ) -> List[Table]:
        """
        List the tables in a collection.

        Args:
            collection_name (str): The name of the collection.
            offset (int, optional): The offset of the list.
            len (int, optional): The length of the list.

        Returns:
            List[Table]: The requested list of tables in the collection.
        """
        collection = Collection(self.connection, collection_name)
        return collection.list_tables(
            at=at, at_trx=at_trx, filter=filter, order_by=order_by
        )

    def sample_table(
        self,
        collection_name: str,
        table_name: str,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        version: DataVersion | str | None = None,
        offset: int = None,
        len: int = None,
    ) -> pl.DataFrame:
        """
        Get a sample of a table for a given version as a parquet file. The
            version can be a fixed version or a relative one (HEAD, HEAD^,
            and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
            offset (int, optional): The offset of the sample.
            len (int, optional): The length of the sample.
        Raises:
            APIServerError: If the sample could not be obtained.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.sample(
            at=at,
            at_trx=at_trx,
            version=version,
            offset=offset,
            len=len,
        )

    def cancel_transaction(self, transaction_id: str) -> requests.Response:
        """
        Cancel a transaction. This includes all functions that are part of the
            transaction and all its dependants.

        Args:
            transaction_id (str): The ID of the transaction to cancel.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the transaction_id is not found in the system.
        """
        transaction = Transaction(self.connection, transaction_id)
        return transaction.cancel()

    def get_transaction(self, transaction_id: str) -> Transaction:
        """
        Get a transaction in the server.

        Args:
            transaction_id (str): The ID of the transaction.

        Returns:
            Transaction: The transaction.

        Raises:
            APIServerError: If the transaction could not be obtained.
        """
        try:
            return self.list_transactions(filter=f"id:eq:{transaction_id}")[0]
        except IndexError:
            raise ValueError(f"Transaction with ID {transaction_id} not found.")

    def list_transactions(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Transaction]:
        return list(
            self.list_transactions_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_transactions_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Transaction]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.transaction_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_transactions = data.get("data")
            for raw_transaction in raw_transactions:
                built_transaction = Transaction(self.connection, **raw_transaction)
                yield built_transaction
            first_page = False

    def recover_transaction(self, transaction_id: str) -> requests.Response:
        """
        Recover a transaction. This includes all functions that are part of the
            transaction.

        Args:
            transaction_id (str): The ID of the transaction to recover.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the transaction_id is not found in the system.
        """
        transaction = Transaction(self.connection, transaction_id)
        return transaction.recover()

    def add_user_to_role(
        self, name: str, role_name: str, raise_for_status: bool = True
    ) -> None:
        """
        Add a user to a role in the server.

        Args:
            name (str): The name of the user.
            role_name (str): The name of the role.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the user could not be added to the role.
        """
        user = self.get_user(name)
        role = self.get_role(role_name)
        user.add_role(role, raise_for_status=raise_for_status)

    def create_user(
        self,
        name: str,
        password: str,
        full_name: str = None,
        email: str = None,
        enabled: bool = True,
        raise_for_status: bool = True,
    ) -> User:
        """
        Create a user in the server.

        Args:
            name (str): The name of the user.
            password (str): The password of the user.
            full_name (str, optional): The full name of the user.
            email (str, optional): The email of the user.
            enabled (bool, optional): Whether the user is enabled or not.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the user could not be created.
        """
        full_name = full_name or name
        user = User(
            self.connection, name, full_name=full_name, email=email, enabled=enabled
        )
        return user.create(password, raise_for_status=raise_for_status)

    def delete_user(self, name: str, raise_for_status: bool = True) -> None:
        """
        Delete a user in the server.

        Args:
            name (str): The name of the user.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the user could not be deleted.
        """
        user = User(self.connection, name)
        user.delete(raise_for_status=raise_for_status)

    def delete_user_from_role(
        self, name: str, role_name: str, raise_for_status: bool = True
    ) -> None:
        """
        Delete a user from a role in the server.

        Args:
            name (str): The name of the user.
            role_name (str): The name of the role.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the user could not be added to the role.
        """
        user = self.get_user(name)
        role = self.get_role(role_name)
        user.delete_role(role, raise_for_status=raise_for_status)

    def get_user(self, name: str) -> User:
        """
        Get a user in the server.

        Args:
            name (str): The name of the user.

        Returns:
            User: The user.

        Raises:
            APIServerError: If the user could not be obtained.
        """
        user_definition = self.connection.users_get_by_name(name)
        return User(
            self.connection,
            **user_definition.json().get("data"),
        )

    def list_users(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[User]:
        return list(
            self.list_users_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_users_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[User]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.users_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_users = data.get("data")
            for raw_user in raw_users:
                built_user = User(self.connection, **raw_user)
                yield built_user
            first_page = False

    def update_user(
        self,
        name: str,
        full_name: str = None,
        email: str = None,
        enabled: bool = None,
        password: str = None,
        raise_for_status: bool = True,
    ) -> User:
        """
        Update a user in the server.

        Args:
            name (str): The name of the user.
            full_name (str, optional): The full name of the user.
            email (str, optional): The email of the user.
            enabled (bool, optional): Whether the user is enabled or not.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the user could not be updated.
        """
        user = User(self.connection, name)
        return user.update(
            full_name=full_name,
            email=email,
            enabled=enabled,
            password=password,
            raise_for_status=raise_for_status,
        )

    def get_worker_log(self, worker: str | Worker) -> str:
        """
        Get the logs of a worker in the server.

        Args:
            worker (str | Worker): The ID of the worker.

        Returns:
            str: The worker logs.
        """
        if isinstance(worker, str):
            worker = Worker(self.connection, worker)
        return worker.log

    def list_workers(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Worker]:
        return list(
            self.list_workers_generator(
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_workers_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Worker]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id and next_step):
            response = self.connection.workers_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_workers = data.get("data")
            for raw_worker in raw_workers:
                built_worker = Worker(self.connection, **raw_worker)
                yield built_worker
            first_page = False


def _convert_timestamp_to_string(timestamp: int | None) -> str:
    if not timestamp:
        return str(timestamp)
    return str(
        datetime.fromtimestamp(timestamp / 1e3, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def _create_archive(
    function_path,
    temporary_directory,
    path_to_bundle=None,
    requirements=None,
    local_packages=None,
    valid_python_versions: list[str] = None,
):
    function = _dynamic_import_function_from_path(function_path)
    function_name: str = function.name
    function_output = function.output
    if not requirements:
        verify_output_sql_drivers(function_output)
    tables: List[str] = (
        function_output._table_list if isinstance(function_output, TableOutput) else []
    )
    string_dependencies: List[str] = (
        function.input._table_list if isinstance(function.input, TableInput) else []
    )
    trigger_string_list = function.trigger_by
    try:
        function_snippet = inspect.getsource(function.original_function)
    except OSError:
        function_snippet = "Function source code not available"
    context_location = create_bundle_archive(
        function,
        save_location=temporary_directory.name,
        path_to_code=path_to_bundle,
        requirements=requirements,
        local_packages=local_packages,
        valid_python_versions=valid_python_versions,
    )

    function_type_to_api_type = {
        "publisher": "P",
        "subscriber": "S",
        "transformer": "T",
    }
    function_type = function_type_to_api_type.get(function.type, "U")  # Unknown type

    source_or_destination = None
    if function_type == "P":
        source_or_destination = function.input.__class__.__name__
    elif function_type == "S":
        source_or_destination = function.output.__class__.__name__

    return (
        tables,
        string_dependencies,
        trigger_string_list,
        function_snippet,
        context_location,
        function_name,
        function_type,
        source_or_destination,
    )


_UTC_FORMATS = [
    "%Y-%m-%dZ",
    "%Y-%m-%dT%HZ",
    "%Y-%m-%dT%H:%MZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
]

_LOCALIZED_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
]


def _top_and_convert_to_timestamp(incomplete_datetime: str | None) -> int | None:
    if not incomplete_datetime:
        return None
    try:
        # Check if the input is a valid integer (unix timestamp in milliseconds)
        timestamp = int(incomplete_datetime)
        if timestamp < 0:
            raise ValueError("Timestamp cannot be negative.")
        return timestamp  # Return as is, assuming it's already in milliseconds
    except ValueError:
        # If it fails to convert to an integer, it must be a datetime string
        pass
    # Define possible formats for incomplete datetime strings

    result = _complete_utc_datetime(incomplete_datetime)
    if result is not None:
        return result

    result = _complete_localized_datetime(incomplete_datetime)
    if result is not None:
        return result

    raise ValueError(
        f"Invalid datetime format string: {incomplete_datetime}. It should be either "
        "a unix timestamp "
        "(milliseconds since epoch) or one of one of the "
        "following:"
        f" {_UTC_FORMATS + _LOCALIZED_FORMATS}. "
        "A 'Z' character at the end of the datetime indicates UTC timezone, if it is "
        "not present the local timezone of the computer will be used."
    )


def _add_one_to_last_field(dt: datetime) -> datetime:
    if dt.microsecond != 0:
        return dt + timedelta(microseconds=1)
    elif dt.second != 0:
        return dt + timedelta(seconds=1)
    elif dt.minute != 0:
        return dt + timedelta(minutes=1)
    elif dt.hour != 0:
        return dt + timedelta(hours=1)
    else:
        return dt + timedelta(days=1)


def _complete_localized_datetime(incomplete_datetime: str | None) -> int | None:
    for fmt in _LOCALIZED_FORMATS:
        try:
            # Try to parse the incomplete datetime string
            dt = (
                datetime.strptime(incomplete_datetime, fmt)
                .replace(tzinfo=None)
                .astimezone()
            )
            dt = _add_one_to_last_field(dt)  # Add one to the last field
            # Format the datetime to the complete format
            return int(dt.timestamp() * 1000.0)  # Convert to milliseconds since epoch
        except ValueError:
            continue

    return None


def _complete_utc_datetime(incomplete_datetime: str | None) -> int | None:
    for fmt in _UTC_FORMATS:
        try:
            # Try to parse the incomplete datetime string
            dt = datetime.strptime(incomplete_datetime, fmt).replace(
                tzinfo=timezone.utc
            )
            dt = _add_one_to_last_field(dt)  # Add one to the last field
            # Format the datetime to the complete format
            return int(dt.timestamp() * 1000.0)  # Convert to milliseconds since epoch
        except ValueError:
            continue

    return None


def _dynamic_import_function_from_path(path: str) -> TabsdataFunction:  # noqa: C901
    """
    Dynamically import a function from a path in the form of 'path::function_name'.
    :param path:
    :return:
    """

    file_path, function_name = path.split("::")
    file_path = os.path.abspath(file_path)
    if not os.path.exists(file_path):
        if not file_path.endswith(".py"):
            raise FileNotFoundError(
                f"File not found: {file_path}. The .py extension may be missing."
            )
        else:
            raise FileNotFoundError(f"File not found: {file_path}")
    sys.path.insert(0, os.path.dirname(file_path))
    try:
        module_name = os.path.splitext(os.path.basename(file_path))[0]
    except ValueError:
        raise ValueError(
            f"Invalid file path: {file_path}. Expected format is "
            "'/path/to/file.py::function_name'."
        )
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None:
        raise ImportError(
            f"Failed to load module spec from {file_path}. "
            "The file may not be a valid Python module."
        )
    module = importlib.util.module_from_spec(spec)
    if module is None:
        raise ImportError(
            f"Failed to create module from spec for {file_path}. "
            "The file may not be a valid Python module."
        )
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise ImportError(f"Failed to execute module {file_path}: {str(e)}")
    try:
        function = getattr(module, function_name)
    except AttributeError:
        raise AttributeError(
            f"Function '{function_name}' not found in module {file_path}."
        )
    return function
