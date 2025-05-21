#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import datetime
import importlib.util
import inspect
import os
import sys
import tempfile
from typing import List

import polars as pl
import requests

from tabsdata.api.apiserver import APIServer, obtain_connection
from tabsdata.io.input import TableInput
from tabsdata.io.output import TableOutput
from tabsdata.tabsdatafunction import TabsdataFunction
from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsdata.utils.sql_utils import verify_output_sql_drivers

STATUS_MAPPING = {
    "C": "Canceled",
    "D": "Done",
    "F": "Failed",
    "H": "On Hold",
    "I": "Incomplete",
    "P": "Published",
    "R": "Running",
    "Rr": "Run Requested",
    "S": "Scheduled",
}


def status_to_mapping(status: str) -> str:
    """
    Function to convert a status to a mapping. While currently it
    only accesses the dictionary and returns the corresponding value, it could get
    more difficult in the future.
    """
    return STATUS_MAPPING.get(status, status)


class ExecutionPlan:
    """
    This class represents an execution plan in the TabsdataServer.

    Args:
        id (str): The id of the execution plan.
        name (str): The name of the execution plan.
        collection (str): The collection where the execution plan is running.
        dataset (str): The function where the execution plan is running,
            will eventually be changed to 'function'.
        triggered_by (str): The user that triggered the execution plan.
        triggered_on (int): The timestamp when the execution plan was triggered.
        ended_on (int): The timestamp when the execution plan ended.
        started_on (int): The timestamp when the execution plan started.
        status (str): The status of the execution plan.
        **kwargs: Additional keyword arguments.

    Attributes:
        triggered_on_str (str): The timestamp when the execution plan was triggered as a
            string.
        ended_on_str (str): The timestamp when the execution plan ended as a string.
        started_on_str (str): The timestamp when the execution plan started as a string.

    """

    def __init__(
        self,
        connection: APIServer,
        id: str,
        **kwargs,
    ):
        """
        Initialize the ExecutionPlan object.

        Args:
            id (str): The id of the execution plan.
            name (str): The name of the execution plan.
            collection (str): The collection where the execution plan is running.
            dataset (str): The dataset where the execution plan is running.
            triggered_by (str): The user that triggered the execution plan.
            triggered_on (int): The timestamp when the execution plan was triggered.
            ended_on (int): The timestamp when the execution plan ended.
            started_on (int): The timestamp when the execution plan started.
            status (str): The status of the execution plan.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.name = kwargs.get("name")
        self.collection = kwargs.get("collection")
        self.function = kwargs.get("dataset")
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
    def triggered_on_str(self) -> str:
        if self._triggered_on_str is None:
            self._triggered_on_str = convert_timestamp_to_string(self.triggered_on)
        return self._triggered_on_str

    @triggered_on_str.setter
    def triggered_on_str(self, triggered_on_str: str | None):
        self._triggered_on_str = triggered_on_str

    @property
    def triggered_by(self) -> str:
        if self._triggered_by is None:
            self.triggered_by = self._data.get("triggered_by")
        return self._triggered_by

    @triggered_by.setter
    def triggered_by(self, triggered_by: str | None):
        self._triggered_by = triggered_by

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
            self._status = status_to_mapping(status)

    @property
    def ended_on(self) -> int:
        if self._ended_on is None:
            self.ended_on = self._data.get("ended_on")
        return self._ended_on

    @ended_on.setter
    def ended_on(self, ended_on: int | None):
        self._ended_on = ended_on

    @property
    def ended_on_str(self) -> str:
        if self._ended_on_str is None:
            self._ended_on_str = convert_timestamp_to_string(self.ended_on)
        return self._ended_on_str

    @ended_on_str.setter
    def ended_on_str(self, ended_on_str: str | None):
        self._ended_on_str = ended_on_str

    @property
    def started_on(self) -> int:
        if self._started_on is None:
            self.started_on = self._data.get("started_on")
        return self._started_on

    @started_on.setter
    def started_on(self, started_on: int | None):
        self._started_on = started_on

    @property
    def started_on_str(self) -> str:
        if self._started_on_str is None:
            self._started_on_str = convert_timestamp_to_string(self.started_on)
        return self._started_on_str

    @started_on_str.setter
    def started_on_str(self, started_on_str: str | None):
        self._started_on_str = started_on_str

    @property
    def triggered_on(self) -> int:
        if self._triggered_on is None:
            self.triggered_on = self._data.get("triggered_on")
        return self._triggered_on

    @triggered_on.setter
    def triggered_on(self, triggered_on: int | None):
        self._triggered_on = triggered_on

    @property
    def name(self) -> str:
        if self._name is None:
            self.name = self._data.get("name")
        return self._name

    @name.setter
    def name(self, name: str | None):
        self._name = name

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
            # TODO: Eventually this will be .get("function")
            self.function = self._data.get("dataset")
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
    def dot(self) -> str:
        if self._dot is None:
            self.dot = self._data.get("dot")
        return self._dot

    @dot.setter
    def dot(self, dot: str | None):
        self._dot = dot

    @property
    def workers(self):
        raw_workers = (
            self.connection.workers_list(by_execution_plan_id=self.id)
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker, "connection": self.connection})
            for worker in raw_workers
        ]

    def get_worker(self, worker_id: str):
        for worker in self.workers:
            if worker.id == worker_id:
                return worker
        raise ValueError(f"Worker {worker_id} not found for execution plan {self.id}")

    def refresh(self) -> ExecutionPlan:
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
        self.dot = None
        return self

    @property
    def _data(self):
        if self._data_dict is None:
            # TODO: This is a costly workaround until information is added to
            #  execution_plan_get endpoint. Remove as soon as possible
            raw_execution_plans = (
                self.connection.execution_plan_list().json().get("data").get("data")
            )
            execution_plans = [
                ExecutionPlan(**{**execution_plan, "connection": self.connection})
                for execution_plan in raw_execution_plans
            ]
            for execution_plan in execution_plans:
                if execution_plan.id == self.id:
                    self._data_dict = execution_plan.kwargs
                    break
            # TODO: End of workaround
            self._data_dict.update(
                self.connection.execution_plan_read(self.id).json().get("data")
            )
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    def __repr__(self) -> str:
        repr = f"{self.__class__.__name__}(id={self.id!r})"
        return repr

    def __str__(self) -> str:
        string = f"ID: {self.id!s}"
        return string

    def __eq__(self, other):
        if not isinstance(other, ExecutionPlan):
            return False
        return self.id == other.id


class Function:
    """
    This class represents a function in the TabsdataServer.

    Args:
        id (str): The ID of the function.
        trigger_with_names (List[str]): If not an empty list, the trigger(s) of
            the function.
        tables (List[str]): The tables generated the function.
        dependencies_with_names (List[str]): The dependencies of the function.
        name (str): The name of the function.
        description (str): The description of the function.
        created_on (int): The timestamp when the function was created.
        created_by (str): The user that created the function.
        **kwargs: Additional keyword arguments.

    Attributes:
        created_on_string (str): The timestamp when the function was created as a
            string.
    """

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
            trigger_with_names (List[str]): If not an empty list, the trigger(s) of
                the function.
            tables (List[str]): The tables generated the function.
            dependencies_with_names (List[str]): The dependencies of the function.
            name (str): The name of the function.
            description (str): The description of the function.
            created_on (int): The timestamp when the function was created.
            created_by (str): The user that created the function.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.collection = collection
        self.name = name

        self.id = kwargs.get("id")
        self.trigger_with_names = kwargs.get("trigger_with_names")
        self.tables = kwargs.get("tables")
        self.dependencies_with_names = kwargs.get("dependencies_with_names")
        self.description = kwargs.get("description")
        self.created_on = kwargs.get("created_on")
        self.created_on_string = None
        self.created_by = kwargs.get("created_by")
        self.kwargs = kwargs
        self._data = None

    @property
    def history(self) -> List[Function]:
        raw_list_of_functions = (
            self.connection.function_list_history(self.collection.name, self.name)
            .json()
            .get("data")
            .get("data")
        )
        return [
            Function(
                **{
                    **function,
                    "connection": self.connection,
                    "collection": self.collection,
                }
            )
            for function in raw_list_of_functions
        ]

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
    def id(self) -> str:
        if self._id is None:
            self.id = self._data.get("id")
        return self._id

    @id.setter
    def id(self, id: str | None):
        self._id = id

    @property
    def trigger_with_names(self) -> List[str]:
        # TODO Aleix: see if we can return something other than a string
        if self._trigger_with_names is None:
            self.trigger_with_names = self._data.get("trigger_with_names")
        return self._trigger_with_names

    @trigger_with_names.setter
    def trigger_with_names(self, trigger_with_names: List[str] | None):
        self._trigger_with_names = trigger_with_names

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
                    Table(self.connection, self.collection, table, function=self)
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

    def get_table(self, table_name: str) -> Table:
        for table in self.tables:
            if table.name == table_name:
                return table
        raise ValueError(f"Table {table_name} not found for function {self.name}")

    @property
    def dependencies_with_names(self) -> List[str]:
        # TODO Aleix: see if we can return something other than a string
        if self._dependencies_with_names is None:
            self.dependencies_with_names = self._data.get("dependencies_with_names")
        return self._dependencies_with_names

    @dependencies_with_names.setter
    def dependencies_with_names(self, dependencies_with_names: List[str] | None):
        self._dependencies_with_names = dependencies_with_names

    @property
    def description(self) -> str:
        if self._description is None:
            self.description = self._data.get("description")
        return self._description

    @description.setter
    def description(self, description: str | None):
        self._description = description

    @property
    def created_on(self) -> int:
        if self._created_on is None:
            self.created_on = self._data.get("created_on")
        return self._created_on

    @created_on.setter
    def created_on(self, created_on: int | None):
        self._created_on = created_on

    @property
    def created_on_string(self) -> str:
        if self._created_on_string is None:
            self._created_on_string = convert_timestamp_to_string(self.created_on)
        return self._created_on_string

    @created_on_string.setter
    def created_on_string(self, created_on_string: str | None):
        self._created_on_string = created_on_string

    @property
    def created_by(self) -> str:
        if self._created_by is None:
            self.created_by = self._data.get("created_by")
        return self._created_by

    @created_by.setter
    def created_by(self, created_by: str | None):
        self._created_by = created_by

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
    def workers(self):
        raw_workers = (
            self.connection.workers_list(by_function_id=self.id)
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker, "connection": self.connection})
            for worker in raw_workers
        ]

    def get_worker(self, worker_id: str):
        for worker in self.workers:
            if worker.id == worker_id:
                return worker
        raise ValueError(f"Worker {worker_id} not found for function {self.name}")

    def refresh(self) -> Function:
        self.id = None
        self.trigger_with_names = None
        self.tables = None
        self.dependencies_with_names = None
        self.description = None
        self.created_on = None
        self.created_on_string = None
        self.created_by = None
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
        reuse_frozen_tables: bool = False,
        raise_for_status: bool = True,
    ) -> Function:
        result = self.collection.register_function(
            function_path,
            description=description,
            path_to_bundle=path_to_bundle,
            requirements=requirements,
            local_packages=local_packages,
            function_name=self.name,
            reuse_frozen_tables=reuse_frozen_tables,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        return result

    def update(
        self,
        function_path: str,
        description: str,
        directory_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        new_function_name=None,
        reuse_frozen_tables: bool = False,
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
            reuse_frozen_tables=reuse_frozen_tables,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self.name = result.name
        self.collection = result.collection
        return result

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.function_delete(
            self.collection.name, self.name, raise_for_status=raise_for_status
        )

    def __repr__(self) -> str:
        representation = (
            f"{self.__class__.__name__}(name={self.name!r}, "
            f"collection={self.collection!r})"
        )
        return representation

    def read_run(
        self, execution_plan: ExecutionPlan | str, raise_for_status: bool = True
    ) -> requests.Response:
        """
        Read the status of a function run.

        Args:
            execution_plan (ExecutionPlan | str): The execution plan of the run.

        """
        execution_plan_id = (
            execution_plan if isinstance(execution_plan, str) else execution_plan.id
        )
        return self.connection.execution_read_function_run(
            self.collection.name,
            self.name,
            execution_plan_id,
            raise_for_status=raise_for_status,
        )

    def trigger(
        self,
        execution_plan_name: str | None = None,
        raise_for_status: bool = True,
    ) -> ExecutionPlan:
        """
        Trigger a function in the server.

        Args:
            execution_plan_name (str, optional): The name of the execution plan.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            requests.Response: The response of the trigger request.

        Raises:
            APIServerError: If the function could not be triggered.
        """
        # TODO Aleix: Maybe return the execution plan
        response = self.connection.function_execute(
            self.collection.name,
            self.name,
            execution_plan_name=execution_plan_name,
            raise_for_status=raise_for_status,
        )
        return ExecutionPlan(
            self.connection,
            **{
                **response.json().get("data"),
                "collection": self.collection.name,
                "function": self.name,
            },
        )

    @property
    def data_versions(self) -> List[DataVersion]:
        return self.get_dataversions()

    def get_dataversions(
        self,
        offset: int = None,
        len: int = None,
    ) -> List[DataVersion]:
        """
        List the data versions of a function in a collection.

        Args:
            offset (int, optional): The offset of the data versions to list.
            len (int, optional): The number of data versions to list.

        Returns:
            List[DataVersion]: The list of data versions of the function.

        Raises:
            APIServerError: If the data versions could not be listed.
        """
        raw_list_of_data_versions = (
            self.connection.dataversion_list(
                self.collection.name, self.name, offset=offset, len=len
            )
            .json()
            .get("data")
            .get("data")
        )
        return [
            DataVersion(
                **{
                    **data_version,
                    "connection": self.connection,
                    "collection": self.collection,
                    "function": self,
                }
            )
            for data_version in raw_list_of_data_versions
        ]

    def __str__(self) -> str:
        string_representation = f"Name: {self.name!s}, collection: {self.collection!s}"
        return string_representation

    def __eq__(self, other):
        if not isinstance(other, Function):
            return False
        return self.name == other.name and self.collection == other.collection


class Commit:
    """
    This class represents a commit in the TabsdataServer.

    Args:
        id (str): The ID of the commit.
        execution_plan_id (str): The ID of the execution plan it is associated with.
        transaction_id (str): The ID of the transaction it is associated with.
        triggered_on (int): The timestamp when the commit was triggered.
        ended_on (int): The timestamp when the commit ended.
        started_on (int): The timestamp when the commit started.
        **kwargs: Additional keyword arguments.

    Attributes:
        triggered_on_str (str): The timestamp when the commit was triggered as a
            string.
        ended_on_str (str): The timestamp when the commit ended as a string.
        started_on_str (str): The timestamp when the commit started as a string.
    """

    # TODO Aleix: Add the rest of the attributes and link class
    def __init__(
        self,
        id: str,
        execution_plan_id: str,
        transaction_id: str,
        triggered_on: int,
        ended_on: int,
        started_on: int,
        **kwargs,
    ):
        self.id = id
        self.execution_plan_id = execution_plan_id
        self.transaction_id = transaction_id
        self.triggered_on = triggered_on
        self.triggered_on_str = convert_timestamp_to_string(triggered_on)
        self.ended_on = ended_on
        self.ended_on_str = convert_timestamp_to_string(ended_on)
        self.started_on = started_on
        self.started_on_str = convert_timestamp_to_string(started_on)
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(id={self.id!r},"
            f" execution_plan_id={self.execution_plan_id!r}, transaction_id"
            f"={self.transaction_id!r}, triggered_on={self.triggered_on_str!r},"
            f" ended_on={self.ended_on_str!r}, started_on={self.started_on_str!r})"
        )

    def __str__(self) -> str:
        return (
            f"ID: {self.id!s}, execution plan ID: {self.execution_plan_id!s}, "
            f"transaction ID : {self.transaction_id!s}, triggered on:"
            f" {self.triggered_on_str!s}, ended on: {self.ended_on_str!s},"
            f" started on: {self.started_on_str!s}"
        )


class DataVersion:
    """
    This class represents a data version of a table in the TabsdataServer.

    Args:
        id (str): The ID of the data version.
        execution_plan_id (str): The ID of the execution plan.
        triggered_on (int): The timestamp when the data version was triggered.
        status (str): The status of the data version.
        function_id (str): The ID of the function.
        **kwargs: Additional keyword arguments.
    """

    def __init__(
        self,
        connection: APIServer,
        id: str,
        collection: str | Collection,
        function: str | Function,
        **kwargs,
    ):
        """
        Initialize the DataVersion object.

        Args:
            id (str): The ID of the data version.
            execution_plan_id (str): The ID of the execution plan.
            triggered_on (int): The timestamp when the data version was triggered.
            status (str): The status of the data version.
            function_id (str): The ID of the function.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id
        self.collection = collection
        self.function = function

        self.execution_plan = kwargs.get("execution_plan_id")
        self.triggered_on = kwargs.get("triggered_on")
        triggered_on = kwargs.get("triggered_on")
        self.triggered_on_str = (
            convert_timestamp_to_string(self.triggered_on) if triggered_on else None
        )
        status = kwargs.get("status")
        self.status = status_to_mapping(status) if status else None
        # Note: this might cause an inconsistency or a bug if function_id corresponds
        # to a different function than the one in the function attribute. Revisit this
        # if necessary.
        self.function_id = kwargs.get("function_id")
        self.kwargs = kwargs
        # TODO Aleix: add _data logic once an endpoint is created to get a dataversion
        #   by ID
        self._data = None

    @property
    def execution_plan(self) -> ExecutionPlan:
        if self._execution_plan is None:
            self.execution_plan = self._data.get("execution_plan_id")
        return self._execution_plan

    @execution_plan.setter
    def execution_plan(self, execution_plan: ExecutionPlan | str | None):
        if isinstance(execution_plan, ExecutionPlan):
            self._execution_plan = execution_plan
        elif isinstance(execution_plan, str):
            self._execution_plan = ExecutionPlan(self.connection, execution_plan)
        elif execution_plan is None:
            self._execution_plan = None
        else:
            raise TypeError(
                "Execution plan must be an ExecutionPlan object, a string or None; got"
                f"{type(execution_plan)} instead."
            )

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
    def function(self) -> Function:
        return self._function

    @function.setter
    def function(self, function: str | Function):
        if isinstance(function, str):
            self._function = Function(self.connection, self.collection, function)
        elif isinstance(function, Function):
            self._function = function
        else:
            raise TypeError(
                "Function must be a string or a Function object; got"
                f"{type(function)} instead."
            )

    @property
    def workers(self):
        raw_workers = (
            self.connection.workers_list(by_data_version_id=self.id)
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker, "connection": self.connection})
            for worker in raw_workers
        ]

    def get_worker(self, worker_id: str):
        for worker in self.workers:
            if worker.id == worker_id:
                return worker
        raise ValueError(f"Worker {worker_id} not found for data version {self.id}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"

    def __str__(self) -> str:
        return f"ID: {self.id!r}"


class Collection:
    """
    This class represents a collection in the TabsdataServer.

    Args:
        connection (APIServer): The connection to the server.
        name (str): The name of the collection.
        **kwargs: Additional keyword

    Attributes:
        created_on_string (str): The timestamp when the collection was created as a
            string.
    """

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
            self.created_on_string = convert_timestamp_to_string(created_on)
        else:
            self.created_on_string = None
        self.created_by = created_by
        self._data = None
        self.kwargs = kwargs

    @property
    def description(self) -> str:
        if self._description is None:
            self.description = self._data.get("description")
        return self._description

    @description.setter
    def description(self, description: str | None):
        self._description = description

    @property
    def created_on(self) -> int:
        if self._created_on is None:
            self.created_on = self._data.get("created_on")
        return self._created_on

    @created_on.setter
    def created_on(self, created_on: int | None):
        self._created_on = created_on

    @property
    def created_by(self) -> str:
        if self._created_by is None:
            self.created_by = self._data.get("created_by")
        return self._created_by

    @created_by.setter
    def created_by(self, created_by: str | None):
        self._created_by = created_by

    @property
    def created_on_string(self) -> str:
        if self._created_on_string is None:
            self._created_on_string = convert_timestamp_to_string(self.created_on)
        return self._created_on_string

    @created_on_string.setter
    def created_on_string(self, created_on_string: str | None):
        self._created_on_string = created_on_string

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
        raw_list_of_functions = (
            self.connection.function_in_collection_list(self.name)
            .json()
            .get("data")
            .get("data")
        )
        # TODO Aleix: Once the endpoint returns the proper id, remove the "refresh()"
        #   workaround
        return [
            Function(
                **{**function, "connection": self.connection, "collection": self}
            ).refresh()
            for function in raw_list_of_functions
        ]

    @property
    def tables(self) -> List[Table]:
        return self.get_tables()

    def get_tables(self, offset: int = None, len: int = None) -> List[Table]:
        """
        List the tables in a collection.

        Args:
            offset (int, optional): The offset of the list.
            len (int, optional): The length of the list.

        Returns:
            List[Table]: The requested list of tables in the collection.
        """
        raw_tables = (
            self.connection.table_list(self.name, offset=offset, len=len)
            .json()
            .get("data")
            .get("data")
        )
        return [
            Table(**{**table, "connection": self.connection, "collection": self})
            for table in raw_tables
        ]

    def read_function_run(
        self,
        function: Function | str,
        execution_plan: ExecutionPlan | str,
        raise_for_status=True,
    ) -> requests.Response:
        """
        Read the status of a function run.

        Args:
            function (Function | str): The function to read the status of.
            execution_plan (ExecutionPlan | str): The execution plan of the run.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.
        """
        function = (
            function
            if isinstance(function, Function)
            else Function(self.connection, self, function)
        )
        return function.read_run(execution_plan, raise_for_status=raise_for_status)

    def refresh(self) -> Collection:
        self.description = None
        self._data = None
        self.created_by = None
        self.created_on = None
        self.created_on_string = None
        self.kwargs = None
        return self

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.collection_delete(self.name, raise_for_status=raise_for_status)

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

    def get_function(self, function_name: str) -> Function:
        function_definition = (
            self.connection.function_get(self.name, function_name).json().get("data")
        )
        function_definition.update({"connection": self.connection, "collection": self})
        return Function(**function_definition)

    def get_table(self, table_name: str) -> Table | None:
        # TODO: Change for the specific endpoint once implemented, for now iterating
        #  through all tables in the collection
        for table in self.tables:
            if table.name == table_name:
                return table
        raise ValueError(f"Table {table_name} not found in collection {self.name}")

    def register_function(
        self,
        function_path: str,
        description: str = None,
        path_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        function_name: str = None,
        reuse_frozen_tables: bool = False,
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

        temporary_directory = tempfile.TemporaryDirectory()
        (
            tables,
            string_dependencies,
            trigger_by,
            function_snippet,
            context_location,
            decorator_function_name,
            decorator_type,
        ) = create_archive(
            function_path,
            temporary_directory,
            path_to_bundle,
            requirements,
            local_packages,
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

        self.connection.function_create(
            collection_name=self.name,
            function_name=function_name,
            description=description,
            tables=tables,
            dependencies=string_dependencies,
            trigger_by=trigger_by,
            function_snippet=function_snippet,
            bundle_id=bundle_id,
            runtime_values=runtime_values,
            reuse_frozen_tables=reuse_frozen_tables,
            decorator=decorator_type,
            raise_for_status=raise_for_status,
        )
        return Function(self.connection, self, function_name)

    def update_function(
        self,
        function_name: str,
        function_path: str,
        description: str,
        directory_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        new_function_name=None,
        reuse_frozen_tables: bool = False,
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
        temporary_directory = tempfile.TemporaryDirectory()
        (
            tables,
            string_dependencies,
            trigger_by,
            function_snippet,
            context_location,
            decorator_new_function_name,
            decorator_type,
        ) = create_archive(
            function_path,
            temporary_directory,
            directory_to_bundle,
            requirements,
            local_packages,
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
            reuse_frozen_tables=reuse_frozen_tables,
            raise_for_status=raise_for_status,
        )

        return Function(self.connection, self, new_function_name)

    def create(self, raise_for_status: bool = True) -> Collection:
        description = self._description or self.name
        response = self.connection.collection_create(
            self.name, description, raise_for_status=raise_for_status
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!r}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, Collection):
            return False
        return self.name == other.name


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

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(status={self.status!r},"
            f" latency_as_nanos={self.latency_as_nanos!r})"
        )

    def __str__(self) -> str:
        return f"Status: {self.status!r} - Latency (ns): {self.latency_as_nanos!r}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, ServerStatus):
            return False
        return self.status == other.status


class Table:
    """
    This class represents a table in the TabsdataServer.

    Args:
        id (str): The ID of the table.
        name (str): The name of the table.
        function(str): The function that generated the table.
        **kwargs: Additional keyword arguments.
    """

    def __init__(
        self, connection: APIServer, collection: str | Collection, name: str, **kwargs
    ):
        self.connection = connection
        self.collection = collection
        self.name = name

        self.id = kwargs.get("id")
        self.function = kwargs.get("function")
        self.kwargs = kwargs

    @property
    def function(self) -> Function | None:
        if self._function is None:
            # TODO: Change this to a specific endpoint once implemented
            self._function = self.collection.get_table(self.name).function
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

    def get_schema(
        self,
        commit: str = None,
        time: str = None,
        version: str = None,
    ) -> dict:
        """
        Get the schema of a table for a given version. The version can be a
            fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            commit (str, optional): The commit ID of the table from which we will
                obtain the schema.
            time (str, optional): If provided, the table version that was
                published last before that time will be the one queried for its schema.
            version (str, optional): The version of the table to be queried for its
                schema. The version can be a fixed version or a relative one
                (HEAD, HEAD^, and HEAD~## syntax). Defaults to "HEAD".

        Returns:
            dict: The schema of the table.

        Raises:
            APIServerError: If the schema could not be obtained.
        """
        # TODO Aleix: Maybe version or commit can be an object apart from a string
        return (
            self.connection.table_get_schema(
                self.collection.name,
                self.name,
                commit=commit,
                time=time,
                version=version,
            )
            .json()
            .get("data")
        )

    def sample(
        self,
        commit: str = None,
        time: str = None,
        version: str = None,
        offset: int = None,
        len: int = None,
    ) -> pl.DataFrame:
        """
        Get a sample of a table for a given version as a parquet file. The
            version can be a fixed version or a relative one (HEAD, HEAD^,
            and HEAD~## syntax).

        Args:
            commit (str, optional): The commit ID of the table from which we will
                obtain the sample.
            time (str, optional): If provided, the table version that was
                published last before that time will be the one queried for a sample.
            version (str, optional): The version of the table to be queried for a
                sample. The version can be a fixed version or a relative one
                (HEAD, HEAD^, and HEAD~## syntax). Defaults to "HEAD".
            offset (int, optional): The offset of the sample.
            len (int, optional): The length of the sample.
        Raises:
            APIServerError: If the schema could not be obtained.
        """
        # TODO Aleix: Maybe version or commit can be an object apart from a string
        parquet_frame = self.connection.table_get_sample(
            self.collection.name,
            self.name,
            commit=commit,
            time=time,
            version=version,
            offset=offset,
            len=len,
        ).content
        return pl.read_parquet(parquet_frame)

    @property
    def _data(self) -> dict:
        return None
        # TODO: In the near future, the following code should be the one used to get
        #  the data for the table
        # if self._data_dict is None:
        #   self._data_dict = (
        #       self.connection.table_get(self.collection.name, self.name).json(
        #       ).get("data")
        #   )
        # return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    def refresh(self) -> Table:
        self.id = None
        self.function = None
        self.kwargs = None
        self._data = None
        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!r}"

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False
        return self.name == other.name and self.collection == other.collection

    def download(
        self,
        destination_file: str,
        commit: str = None,
        time: str = None,
        version: str = None,
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
            APIServerError: If the schema could not be obtained.
        """
        # TODO Aleix: Maybe version or commit can be an object apart from a string
        response = self.connection.table_get_data(
            self.collection.name,
            self.name,
            commit=commit,
            time=time,
            version=version,
            raise_for_status=raise_for_status,
        )
        with open(destination_file, "wb") as file:
            file.write(response.content)


class Transaction:
    """
    This class represents a transaction in the TabsdataServer.

    Args:
        id (str): The ID of the transaction.
        execution_plan_id (str): The ID of the execution plan.
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

    def __init__(
        self,
        connection: APIServer,
        id: str,
        **kwargs,
    ):
        self.id = id
        self.connection = connection

        self.execution_plan = kwargs.get("execution_plan_id")
        self.status = kwargs.get("status")
        self.triggered_on = kwargs.get("triggered_on")
        self.triggered_on_str = None
        self.ended_on = kwargs.get("ended_on")
        self.ended_on_str = None
        self.started_on = kwargs.get("started_on")
        self.started_on_str = None
        self.kwargs = kwargs
        self._data = None

    @property
    def execution_plan(self) -> ExecutionPlan:
        if self._execution_plan is None:
            self.execution_plan = self._data.get("execution_plan_id")
        return self._execution_plan

    @execution_plan.setter
    def execution_plan(self, execution_plan: str | ExecutionPlan | None):
        if isinstance(execution_plan, str):
            self._execution_plan = ExecutionPlan(self.connection, execution_plan)
        elif isinstance(execution_plan, ExecutionPlan):
            self._execution_plan = execution_plan
        elif execution_plan is None:
            self._execution_plan = None
        else:
            raise TypeError(
                "Execution plan must be a string, an ExecutionPlan object or None; got"
                f"{type(execution_plan)} instead."
            )

    @property
    def triggered_on_str(self) -> str:
        if self._triggered_on_str is None:
            self._triggered_on_str = convert_timestamp_to_string(self.triggered_on)
        return self._triggered_on_str

    @triggered_on_str.setter
    def triggered_on_str(self, triggered_on_str: str | None):
        self._triggered_on_str = triggered_on_str

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
            self._status = status_to_mapping(status)

    @property
    def ended_on(self) -> int:
        if self._ended_on is None:
            self.ended_on = self._data.get("ended_on")
        return self._ended_on

    @ended_on.setter
    def ended_on(self, ended_on: int | None):
        self._ended_on = ended_on

    @property
    def ended_on_str(self) -> str:
        if self._ended_on_str is None:
            self._ended_on_str = convert_timestamp_to_string(self.ended_on)
        return self._ended_on_str

    @ended_on_str.setter
    def ended_on_str(self, ended_on_str: str | None):
        self._ended_on_str = ended_on_str

    @property
    def started_on(self) -> int:
        if self._started_on is None:
            self.started_on = self._data.get("started_on")
        return self._started_on

    @started_on.setter
    def started_on(self, started_on: int | None):
        self._started_on = started_on

    @property
    def started_on_str(self) -> str:
        if self._started_on_str is None:
            self._started_on_str = convert_timestamp_to_string(self.started_on)
        return self._started_on_str

    @started_on_str.setter
    def started_on_str(self, started_on_str: str | None):
        self._started_on_str = started_on_str

    @property
    def triggered_on(self) -> int:
        if self._triggered_on is None:
            self.triggered_on = self._data.get("triggered_on")
        return self._triggered_on

    @triggered_on.setter
    def triggered_on(self, triggered_on: int | None):
        self._triggered_on = triggered_on

    @property
    def _data(self) -> dict:
        # TODO: This is an inefficient workaround, waiting for the endpoint to exist
        if self._data_dict is None:
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            for transaction in tabsdata_server.transactions:
                if transaction.id == self.id:
                    self._data = transaction.kwargs
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    def refresh(self) -> Transaction:
        self.execution_plan_id = None
        self.status = None
        self.triggered_on = None
        self.triggered_on_str = None
        self.ended_on = None
        self.started_on = None
        self.started_on_str = None
        self.kwargs = None
        self._data = None
        return self

    @property
    def workers(self):
        raw_workers = (
            self.connection.workers_list(by_transaction_id=self.id)
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker, "connection": self.connection})
            for worker in raw_workers
        ]

    def get_worker(self, worker_id: str):
        for worker in self.workers:
            if worker.id == worker_id:
                return worker
        raise ValueError(f"Worker {worker_id} not found for transaction {self.id}")

    def cancel(self) -> requests.Response:
        """
        Cancel an execution plan. This includes all functions that are part of the
            execution plan and all its dependants.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_plan_id is not found in the system.
        """
        return self.connection.transaction_cancel(self.id)

    def recover(self) -> requests.Response:
        """
        Recover an execution plan. This includes all functions that are part of the
            execution plan and all its dependants.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_plan_id is not found in the system.
        """
        return self.connection.transaction_recover(self.id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"

    def __str__(self) -> str:
        return f"ID: {self.id!r}"

    def __eq__(self, other):
        if not isinstance(other, Transaction):
            return False
        return self.id == other.id


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
    def full_name(self) -> str:
        if self._full_name is None:
            self.full_name = self._data.get("full_name")
        return self._full_name

    @full_name.setter
    def full_name(self, full_name: str | None):
        self._full_name = full_name

    @property
    def email(self) -> str:
        if self._email is None:
            self.email = self._data.get("email")
        return self._email

    @email.setter
    def email(self, email: str | None):
        self._email = email

    @property
    def enabled(self) -> bool:
        if self._enabled is None:
            self.enabled = self._data.get("enabled")
        return self._enabled

    @enabled.setter
    def enabled(self, enabled: bool | None):
        self._enabled = enabled

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            self._data = self.connection.users_get_by_name(self.name).json().get("data")
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    def refresh(self) -> User:
        self.full_name = None
        self.email = None
        self.enabled = None
        self._data = None
        self.kwargs = None
        return self

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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!r}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, User):
            return False
        return self.name == other.name


class Worker:
    """
    This class represents a worker in the TabsdataServer.

    Args:
        id (str): The ID of the worker.
        collection (str): The collection of the worker.
        function (str): The function of the worker.
        function_id (str): The ID of the function of the worker.
        transaction_id (str): The ID of the transaction of the worker.
        execution_plan (str): The execution plan of the worker.
        execution_plan_id (str): The ID of the execution plan of the worker.
        data_version_id (str): The ID of the data version of the worker.
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
        Initialize the WorkerMessage object.

        Args:
            id (str): The ID of the worker.
            collection (str): The collection of the worker.
            function (str): The function of the worker.
            function_id (str): The ID of the function of the worker.
            transaction_id (str): The ID of the transaction of the worker.
            execution_plan (str): The execution plan of the worker.
            execution_plan_id (str): The ID of the execution plan of the worker.
            data_version_id (str): The ID of the data version of the worker.
            **kwargs: Additional keyword arguments.
        """
        self.connection = connection
        self.id = id

        self.collection = kwargs.get("collection")
        self.function = kwargs.get("function")
        self.transaction = kwargs.get("transaction_id")
        self.execution_plan = kwargs.get("execution_plan_id")
        self.data_version_id = kwargs.get("data_version_id")
        started_on = kwargs.get("started_on")
        self.started_on = started_on
        self.started_on_str = (
            convert_timestamp_to_string(started_on) if started_on else None
        )
        self.status = kwargs.get("status")
        self._data = None
        self.kwargs = kwargs

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

    @property
    def execution_plan(self) -> ExecutionPlan:
        if self._execution_plan is None:
            self.execution_plan = self._data.get("execution_plan_id")
        return self._execution_plan

    @execution_plan.setter
    def execution_plan(self, execution_plan: str | ExecutionPlan | None):
        if isinstance(execution_plan, str):
            self._execution_plan = ExecutionPlan(self.connection, execution_plan)
        elif isinstance(execution_plan, ExecutionPlan):
            self._execution_plan = execution_plan
        elif execution_plan is None:
            self._execution_plan = None
        else:
            raise TypeError(
                "Execution plan must be a string, an ExecutionPlan object or None; got"
                f"{type(execution_plan)} instead."
            )

    @property
    def data_version_id(self) -> str:
        # TODO: To link the worker with the data version, we need a specific endpoint
        #   that given a data_version_id, returns the whole information of the
        #   data_version. For now leaving it as a string
        if self._data_version_id is None:
            self.data_version_id = self._data.get("data_version_id")
        return self._data_version_id

    @data_version_id.setter
    def data_version_id(self, data_version_id: str | None):
        self._data_version_id = data_version_id

    @property
    def started_on(self) -> int:
        if self._started_on is None:
            self.started_on = self._data.get("started_on")
        return self._started_on

    @started_on.setter
    def started_on(self, started_on: int | None):
        self._started_on = started_on

    @property
    def started_on_str(self) -> str:
        if self._started_on_str is None:
            self._started_on_str = convert_timestamp_to_string(self.started_on)
        return self._started_on_str

    @started_on_str.setter
    def started_on_str(self, started_on_str: str | None):
        self._started_on_str = started_on_str

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
            self._status = status_to_mapping(status)

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            # TODO: Improve this logic once we have a specific endpoint for it.
            #  Currently it is extremely inefficient
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            raw_list_of_execution_plans = tabsdata_server.execution_plans
            for plan in raw_list_of_execution_plans:
                plan_id = plan.id
                worker_list = tabsdata_server.worker_list(by_execution_plan_id=plan_id)
                for worker in worker_list:
                    if worker.id == self.id:
                        self._data = worker.kwargs
                        break
                if self._data_dict is not None:
                    break
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    @property
    def log(self) -> str:
        """
        Get the logs of a worker in the server.

        Returns:
            str: The worker logs.
        """
        return self.connection.worker_log(self.id).text

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id!r})"

    def __str__(self) -> str:
        return f"ID: {self.id!s}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, Worker):
            return False
        return self.id == other.id


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
    def commits(self) -> List[Commit]:
        """
        Get the list of commits in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Transaction]: The list of commits in the server.
        """
        # TODO Aleix
        raw_commits = self.connection.commit_list().json().get("data").get("data")
        return [Commit(**commit) for commit in raw_commits]

    @property
    def collections(self) -> List[Collection]:
        """
        Get the list of collections in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Collection]: The list of collections in the server.
        """
        raw_collections = (
            self.connection.collection_list().json().get("data").get("data")
        )
        return [
            Collection(**{**collection, "connection": self.connection})
            for collection in raw_collections
        ]

    @property
    def execution_plans(self) -> List[ExecutionPlan]:
        """
        Get the list of execution plans in the server. This list is obtained every time
            the property is accessed, so sequential accesses to this property in the
            same object might yield different results.

        Returns:
            List[ExecutionPlan]: The list of execution plans in the server.
        """
        raw_execution_plans = (
            self.connection.execution_plan_list().json().get("data").get("data")
        )
        return [
            ExecutionPlan(**{**execution_plan, "connection": self.connection})
            for execution_plan in raw_execution_plans
        ]

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
        raw_transactions = (
            self.connection.transaction_list().json().get("data").get("data")
        )
        return [
            Transaction(**{**transaction, "connection": self.connection})
            for transaction in raw_transactions
        ]

    @property
    def users(self) -> List[User]:
        """
        Get the list of users in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[User]: The list of users in the server.
        """
        raw_users = self.connection.users_list().json().get("data").get("data")
        return [User(**{**user, "connection": self.connection}) for user in raw_users]

    def collection_create(
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

    def collection_delete(self, name: str, raise_for_status: bool = True) -> None:
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

    def collection_get(self, name: str) -> Collection:
        """
        Get a collection in the server.

        Args:
            name (str): The name of the collection.

        Returns:
            Collection: The collection.

        Raises:
            APIServerError: If the collection could not be obtained.
        """
        return Collection(self.connection, name)

    def collection_list_functions(self, collection_name) -> List[Function]:
        """
        List the functions in a collection.

        Args:
            collection_name (str): The name of the collection.

        Returns:
            List[Function]: The list of functions in the collection.

        Raises:
            APIServerError: If the functions could not be listed.
        """
        return Collection(self.connection, collection_name).functions

    def collection_update(
        self,
        name: str,
        new_name=None,
        new_description: str = None,
        raise_for_status: bool = True,
    ) -> None:
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
        collection.update(
            name=new_name,
            description=new_description,
            raise_for_status=raise_for_status,
        )

    def dataversion_list(
        self,
        collection_name: str,
        function_name: str,
        offset: int = None,
        len: int = None,
    ) -> List[DataVersion]:
        """
        List the data versions of a function in a collection.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.
            offset (int, optional): The offset of the data versions to list.
            len (int, optional): The number of data versions to list.

        Returns:
            List[DataVersion]: The list of data versions of the function.

        Raises:
            APIServerError: If the data versions could not be listed.
        """
        function = Function(self.connection, collection_name, function_name)
        return function.get_dataversions(offset=offset, len=len)

    def execution_plan_read(self, execution_plan_id: str) -> str:
        """
        Read the execution plan in the server.

        Args:
            execution_plan_id (str): The ID of the execution plan.

        Returns:
            str: The execution plan.
        """
        execution_plan = ExecutionPlan(self.connection, execution_plan_id)
        return execution_plan.dot

    def function_create(
        self,
        collection_name: str,
        function_path: str,
        description: str = None,
        path_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        function_name: str = None,
        reuse_frozen_tables: bool = False,
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
            reuse_frozen_tables=reuse_frozen_tables,
            raise_for_status=raise_for_status,
        )

    def function_delete(
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

    def function_get(self, collection_name, function_name) -> Function:
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
        return Function(self.connection, collection_name, function_name)

    def function_list_history(self, collection_name, function_name) -> List[Function]:
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

    def function_read_run(
        self,
        collection: Collection | str,
        function: Function | str,
        execution_plan: ExecutionPlan | str,
        raise_for_status: bool = True,
    ) -> requests.Response:
        """
        Read the run of a function in the server.

        Args:
            collection(Collection | str): The name of the collection or a
                Collection object.
            function(Function | str): The name of the function or a Function object.
            execution_plan(ExecutionPlan | str): The name of the execution plan or a
                ExecutionPlan object.

        Raises:
            APIServerError: If the run could not be obtained.
        """
        function = (
            function
            if isinstance(function, Function)
            else Function(self.connection, collection, function)
        )
        return function.read_run(execution_plan, raise_for_status=raise_for_status)

    def function_trigger(
        self,
        collection_name,
        function_name,
        execution_plan_name: str | None = None,
        raise_for_status: bool = True,
    ) -> ExecutionPlan:
        """
        Trigger a function in the server.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.
            execution_plan_name (str, optional): The name of the execution plan.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            requests.Response: The response of the trigger request.

        Raises:
            APIServerError: If the function could not be triggered.
        """
        function = Function(self.connection, collection_name, function_name)
        return function.trigger(
            execution_plan_name=execution_plan_name, raise_for_status=raise_for_status
        )

    def function_update(
        self,
        collection_name: str,
        function_name: str,
        function_path: str,
        description: str,
        directory_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        new_function_name: str = None,
        reuse_frozen_tables: bool = False,
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
            reuse_frozen_tables=reuse_frozen_tables,
            raise_for_status=raise_for_status,
        )

    def login(self, username: str, password: str, role: str = None):
        self.connection.authentication_login(
            username,
            password,
            role=role,
        )

    def logout(self, raise_for_status: bool = True):
        return self.connection.authentication_logout(raise_for_status=raise_for_status)

    def password_change(
        self,
        username: str,
        old_password: str,
        new_password: str,
        raise_for_status: bool = True,
    ):
        self.connection.authentication_password_change(
            username, old_password, new_password, raise_for_status=raise_for_status
        )

    def role_change(self, role: str):
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

    def table_download(
        self,
        collection_name: str,
        table_name: str,
        destination_file: str,
        commit: str = None,
        time: str = None,
        version: str = None,
        raise_for_status: bool = True,
    ):
        """
        Download a table for a given version as a parquet file. The version can
            be a fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
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
            APIServerError: If the schema could not be obtained.
        """
        table = Table(self.connection, collection_name, table_name)
        table.download(
            destination_file,
            commit=commit,
            time=time,
            version=version,
            raise_for_status=raise_for_status,
        )

    def table_get_schema(
        self,
        collection_name: str,
        table_name: str,
        commit: str = None,
        time: str = None,
        version: str = None,
    ) -> dict:
        """
        Get the schema of a table for a given version. The version can be a
            fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
            commit (str, optional): The commit ID of the table from which we will
                obtain the schema.
            time (str, optional): If provided, the table version that was
                published last before that time will be the one queried for its schema.
            version (str, optional): The version of the table to be queried for its
                schema. The version can be a fixed version or a relative one
                (HEAD, HEAD^, and HEAD~## syntax). Defaults to "HEAD".

        Returns:
            dict: The schema of the table.

        Raises:
            APIServerError: If the schema could not be obtained.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.get_schema(commit=commit, time=time, version=version)

    def table_list(
        self, collection_name: str, offset: int = None, len: int = None
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
        return collection.get_tables(offset=offset, len=len)

    def table_sample(
        self,
        collection_name: str,
        table_name: str,
        commit: str = None,
        time: str = None,
        version: str = None,
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
            commit (str, optional): The commit ID of the table from which we will
                obtain the sample.
            time (str, optional): If provided, the table version that was
                published last before that time will be the one queried for a sample.
            version (str, optional): The version of the table to be queried for a
                sample. The version can be a fixed version or a relative one
                (HEAD, HEAD^, and HEAD~## syntax). Defaults to "HEAD".
            offset (int, optional): The offset of the sample.
            len (int, optional): The length of the sample.
        Raises:
            APIServerError: If the schema could not be obtained.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.sample(
            commit=commit,
            time=time,
            version=version,
            offset=offset,
            len=len,
        )

    def transaction_cancel(self, transaction_id: str) -> requests.Response:
        """
        Cancel an execution plan. This includes all functions that are part of the
            execution plan and all its dependants.

        Args:
            transaction_id (str): The ID of the execution plan to cancel.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_plan_id is not found in the system.
        """
        transaction = Transaction(self.connection, transaction_id)
        return transaction.cancel()

    def transaction_recover(self, transaction_id: str) -> requests.Response:
        """
        Recover an execution plan. This includes all functions that are part of the
            execution plan and all its dependants.

        Args:
            transaction_id (str): The ID of the execution plan to recover.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_plan_id is not found in the system.
        """
        transaction = Transaction(self.connection, transaction_id)
        return transaction.recover()

    def user_create(
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

    def user_delete(self, name: str, raise_for_status: bool = True) -> None:
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

    def user_get(self, name: str) -> User:
        """
        Get a user in the server.

        Args:
            name (str): The name of the user.

        Returns:
            User: The user.

        Raises:
            APIServerError: If the user could not be obtained.
        """
        return User(self.connection, name)

    def user_update(
        self,
        name: str,
        full_name: str = None,
        email: str = None,
        enabled: bool = None,
        password: str = None,
        raise_for_status: bool = True,
    ) -> None:
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
        user.update(
            full_name=full_name,
            email=email,
            enabled=enabled,
            password=password,
            raise_for_status=raise_for_status,
        )

    def worker_log(self, worker_id: str) -> str:
        """
        Get the logs of a worker in the server.

        Args:
            worker_id (str): The ID of the worker.

        Returns:
            str: The worker logs.
        """
        worker = Worker(self.connection, worker_id)
        return worker.log

    def worker_list(
        self,
        by_function_id: str = None,
        by_transaction_id: str = None,
        by_execution_plan_id: str = None,
        by_data_version_id: str = None,
    ):
        raw_worker_messages = (
            self.connection.workers_list(
                by_function_id,
                by_transaction_id,
                by_execution_plan_id,
                by_data_version_id,
            )
            .json()
            .get("data")
            .get("data")
        )
        return [
            Worker(**{**worker_message, "connection": self.connection})
            for worker_message in raw_worker_messages
        ]


def dynamic_import_function_from_path(path: str) -> TabsdataFunction:
    """
    Dynamically import a function from a path in the form of 'path::function_name'.
    :param path:
    :return:
    """
    file_path, function_name = path.split("::")
    sys.path.insert(0, os.path.dirname(file_path))
    module_name = os.path.splitext(os.path.basename(file_path))[0]

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    function = getattr(module, function_name)
    return function


def create_archive(
    function_path,
    temporary_directory,
    path_to_bundle=None,
    requirements=None,
    local_packages=None,
):
    function = dynamic_import_function_from_path(function_path)
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
    )

    function_type_to_api_type = {
        "publisher": "P",
        "subscriber": "S",
        "transformer": "T",
    }
    function_type = function_type_to_api_type.get(function.type, "U")  # Unknown type
    return (
        tables,
        string_dependencies,
        trigger_string_list,
        function_snippet,
        context_location,
        function_name,
        function_type,
    )


def convert_timestamp_to_string(timestamp: int | None) -> str:
    if not timestamp:
        return str(timestamp)
    return str(
        datetime.datetime.fromtimestamp(timestamp / 1e3, datetime.UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )
