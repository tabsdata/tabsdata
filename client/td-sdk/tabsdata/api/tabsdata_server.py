#
# Copyright 2024 Tabs Data Inc.
#

import datetime
import hashlib
import importlib.util
import inspect
import os
import sys
import tempfile
from typing import List

import polars as pl
import requests

from tabsdata.api.api_server import obtain_connection
from tabsdata.tabsdatafunction import TableInput, TableOutput, TabsdataFunction
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
    return STATUS_MAPPING.get(status, "Unrecognized status")


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
        id: str,
        name: str,
        collection: str,
        dataset: str,
        triggered_by: str,
        triggered_on: int,
        ended_on: int,
        started_on: int,
        status: str,
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
        self.id = id
        self.name = name
        self.collection = collection
        self.function = dataset
        self.triggered_by = triggered_by
        self.triggered_on = triggered_on
        self.triggered_on_str = convert_timestamp_to_string(self.triggered_on)
        self.raw_status = status
        self.ended_on = ended_on
        self.ended_on_str = convert_timestamp_to_string(self.ended_on)
        self.started_on = started_on
        self.started_on_str = convert_timestamp_to_string(self.started_on)
        self.status = status_to_mapping(status)
        self.kwargs = kwargs

    def __repr__(self) -> str:
        repr = (
            f"{self.__class__.__name__}(id={self.id!r},"
            f"name={self.name!r},"
            f"collection={self.collection!r},"
            f"function={self.function!r},"
            f"triggered_by={self.triggered_by!r},"
            f"triggered_on={self.triggered_on_str!r},"
            f"status={self.status!r}"
        )
        return repr

    def __str__(self) -> str:
        string = (
            f"ID: {self.id!s}, "
            f"name: {self.name!s}, "
            f"collection: {self.collection!s}, "
            f"function : {self.function!s}, "
            f"triggered by: '{self.triggered_by!s}', "
            f"triggered on: {self.triggered_on_str!s}, "
            f"status: {self.status!s}"
        )
        return string


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
        id: str,
        trigger_with_names: List[str] = None,
        tables: List[str] = None,
        dependencies_with_names: List[str] = None,
        name: str = None,
        description: str = None,
        created_on: int = None,
        created_by: str = None,
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
        self.id = id
        self.trigger_with_names = trigger_with_names
        self.tables = tables
        self.dependencies_with_names = dependencies_with_names
        self.name = name
        self.description = description
        self.created_on = created_on
        self.created_on_string = convert_timestamp_to_string(created_on)
        self.created_by = created_by
        self.kwargs = kwargs

    def __repr__(self) -> str:
        representation = f"{self.__class__.__name__}(id={self.id!r},"
        if self.name:
            representation += f"name={self.name!r},"
        if self.description:
            representation += f"description={self.description!r},"
        if self.created_on:
            representation += f"created_on={self.created_on_string!r},"
        if self.created_by:
            representation += f"created_by={self.created_by!r},"
        if self.dependencies_with_names:
            representation += (
                f"dependencies_with_names={self.dependencies_with_names!r},"
            )
        if self.trigger_with_names:
            representation += f"trigger_with_names={self.trigger_with_names!r},"
        if self.tables:
            representation += f"tables={self.tables!r}"
        return representation

    def __str__(self) -> str:
        string_representation = f"ID: {self.id!s}, "
        if self.name:
            string_representation += f"name: {self.name!s}, "
        if self.description:
            string_representation += f"description: '{self.description!s}', "
        if self.created_on:
            string_representation += f"created on: {self.created_on_string!s}, "
        if self.created_by:
            string_representation += f"created by: {self.created_by!s}, "
        if self.dependencies_with_names:
            string_representation += f"dependency: {self.dependencies_with_names!s}, "
        if self.trigger_with_names:
            string_representation += f"trigger: {self.trigger_with_names!s}, "
        if self.tables:
            string_representation += f"tables: {self.tables!s}"
        return string_representation


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
        id: str,
        execution_plan_id: str,
        triggered_on: int,
        status: str,
        function_id: str,
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

        self.id = id
        self.execution_plan_id = execution_plan_id
        self.triggered_on = triggered_on
        self.triggered_on_str = convert_timestamp_to_string(triggered_on)
        self.status = status_to_mapping(status)
        self.function_id = function_id
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(id={self.id!r},"
            f"execution_plan_id={self.execution_plan_id!r},"
            f"triggered_on={self.triggered_on_str!r},"
            f"status={self.status!r},"
            f"function_id={self.function_id!r})"
        )

    def __str__(self) -> str:
        return (
            f"ID: {self.id!r}, "
            f"execution plan ID: {self.execution_plan_id!r}, "
            f"triggered on: {self.triggered_on_str!r}, "
            f"status: {self.status!r}, "
            f"function ID: {self.function_id!r}"
        )


class Collection:
    """
    This class represents a collection in the TabsdataServer.

    Args:
        name (str): The name of the collection.
        id (str): The id of the collection.
        description (str): The description of the collection.
        created_on (int): The timestamp when the collection was created.
        created_by (str): The user that created the collection.
        **kwargs: Additional keyword

    Attributes:
        created_on_string (str): The timestamp when the collection was created as a
            string.
    """

    def __init__(
        self,
        name: str,
        description: str,
        created_on: int,
        created_by: str,
        **kwargs,
    ):
        """
        Initialize the Collection object.

        Args:
            name (str): The name of the collection.
            description (str): The description of the collection.
            created_on (int): The timestamp when the collection was created.
            created_by (str): The user that created the collection.
            **kwargs: Additional keyword arguments.
        """
        self.name = name
        self.description = description
        self.created_on = created_on
        self.created_on_string = convert_timestamp_to_string(created_on)
        self.created_by = created_by
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(name={self.name!r},"
            f"description={self.description!r},"
            f"created_on={self.created_on_string!r},"
            f"created_by={self.created_by!r})"
        )

    def __str__(self) -> str:
        return (
            f"Name: {self.name!r}, description: {self.description!r}, "
            f"created_on: {self.created_on_string!r}, created_by: {self.created_by!r}"
        )

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
        return f"Status: {self.status!r}, latency_as_nanos: {self.latency_as_nanos!r}"

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

    def __init__(self, id: str, name: str, function: str, **kwargs):
        self.id = id
        self.name = name
        self.function = function
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(id={self.id!r}, name={self.name!r},"
            f" function={self.function!r})"
        )

    def __str__(self) -> str:
        return f"ID: {self.id!r}, name: {self.name!r}, function: {self.function!r}"


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
        id: str,
        execution_plan_id: str,
        status: str,
        triggered_on: int,
        ended_on: int,
        started_on: int,
        **kwargs,
    ):
        self.id = id
        self.execution_plan_id = execution_plan_id
        self.status = status_to_mapping(status)
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
            f" execution_plan_id={self.execution_plan_id!r}, status={self.status!r},"
            f" triggered_on={self.triggered_on_str!r}, ended_on={self.ended_on_str!r},"
            f" started_on={self.started_on_str!r})"
        )

    def __str__(self) -> str:
        return (
            f"ID: {self.id!r}, execution plan ID: {self.execution_plan_id!r}, status:"
            f" {self.status!r}, triggered on: {self.triggered_on_str!r}, ended on:"
            f" {self.ended_on_str!r}, started on: {self.started_on_str!r}"
        )


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

    def __init__(self, name: str, full_name: str, email: str, enabled: bool, **kwargs):
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
        self.full_name = full_name
        self.email = email
        self.enabled = enabled
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(name={self.name!r},"
            f"full_name={self.full_name!r},"
            f"email={self.email!r},enabled={self.enabled!r})"
        )

    def __str__(self) -> str:
        return (
            f"Name: {self.name!r}, full name: {self.full_name!r}, email: "
            f"{self.email!r}, enabled: {self.enabled!r}"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, User):
            return False
        return self.name == other.name


class Worker:
    """
    This class represents a workerin the TabsdataServer.

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
        id: str,
        collection: str,
        function: str,
        function_id: str,
        transaction_id: str,
        execution_plan: str,
        execution_plan_id: str,
        data_version_id: str,
        started_on: int,
        status: str,
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
        self.id = id
        self.collection = collection
        self.function = function
        self.function_id = function_id
        self.transaction_id = transaction_id
        self.execution_plan = execution_plan
        self.execution_plan_id = execution_plan_id
        self.data_version_id = data_version_id
        self.started_on = started_on
        self.started_on_str = convert_timestamp_to_string(started_on)
        self.status = status_to_mapping(status)
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(id={self.id!r},"
            f"collection={self.collection!r}, function={self.function!r},"
            f"function_id={self.function_id!r}, transaction_id={self.transaction_id!r},"
            f"execution_plan={self.execution_plan!r},"
            f" execution_plan_id={self.execution_plan_id!r},"
            f"data_version_id={self.data_version_id!r},"
            f"started_on={self.started_on_str!r}),"
            f"status={self.status!r}"
        )

    def __str__(self) -> str:
        return (
            f"ID: {self.id!s}, collection: {self.collection!s}, function:"
            f" {self.function!s},"
            f"function ID: {self.function_id!s}, transaction ID:"
            f" {self.transaction_id!s},"
            f"execution plan: {self.execution_plan!s}, execution plan ID:"
            f" {self.execution_plan_id!s},"
            f"data version ID: {self.data_version_id!s},"
            f"started on: {self.started_on_str!s},"
            f"status: {self.status!r}"
        )


class TabsdataServer:
    """
    This class represents the TabsdataServer.

    Args:
        url (str): The url of the server.
        username (str): The username of the user.
        password (str): The password of the user.
    """

    def __init__(self, url: str, username: str, password: str):
        """
        Initialize the TabsdataServer object.

        Args:
            url (str): The url of the server.
            username (str): The username of the user.
            password (str): The password of the user.
        """
        self.connection = obtain_connection(url, username, password)

    @property
    def commits(self) -> List[Commit]:
        """
        Get the list of commits in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Transaction]: The list of commits in the server.
        """
        raw_commits = self.connection.commit_list().json().get("data")
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
        raw_collections = self.connection.collection_list().json().get("data")
        return [Collection(**collection) for collection in raw_collections]

    @property
    def execution_plans(self) -> List[ExecutionPlan]:
        """
        Get the list of execution plans in the server. This list is obtained every time
            the property is accessed, so sequential accesses to this property in the
            same object might yield different results.

        Returns:
            List[ExecutionPlan]: The list of execution plans in the server.
        """
        raw_execution_plans = self.connection.execution_plan_list().json().get("data")
        return [
            ExecutionPlan(**execution_plan) for execution_plan in raw_execution_plans
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
        return ServerStatus(
            **self.connection.status_get().json().get("database_status")
        )

    @property
    def transactions(self) -> List[Transaction]:
        """
        Get the list of transactions in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Transaction]: The list of transactions in the server.
        """
        raw_transactions = self.connection.transaction_list().json().get("data")
        return [Transaction(**transaction) for transaction in raw_transactions]

    @property
    def users(self) -> List[User]:
        """
        Get the list of users in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[User]: The list of users in the server.
        """
        raw_users = self.connection.users_list().json().get("data")
        return [User(**user) for user in raw_users]

    def collection_create(
        self, name: str, description: str = None, raise_for_status: bool = True
    ):
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
        description = description or name
        self.connection.collection_create(
            name, description, raise_for_status=raise_for_status
        )

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
        self.connection.collection_delete(name, raise_for_status=raise_for_status)

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
        return Collection(**self.connection.collection_get_by_name(name).json())

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
        raw_list_of_functions = (
            self.connection.function_in_collection_list(collection_name)
            .json()
            .get("data")
        )
        return [Function(**function) for function in raw_list_of_functions]

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
        self.connection.collection_update(
            name,
            new_collection_name=new_name,
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
        raw_list_of_data_versions = (
            self.connection.dataversion_list(
                collection_name, function_name, offset=offset, len=len
            )
            .json()
            .get("data")
        )
        return [
            DataVersion(**data_version) for data_version in raw_list_of_data_versions
        ]

    def execution_plan_read(self, execution_plan_id: str) -> str:
        """
        Read the execution plan in the server.

        Args:
            execution_plan_id (str): The ID of the execution plan.

        Returns:
            str: The execution plan.
        """
        return self.connection.execution_plan_read(execution_plan_id).json().get("dot")

    def function_create(
        self,
        collection_name: str,
        function_path: str,
        description: str = None,
        path_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        raise_for_status: bool = True,
    ) -> None:
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

        temporary_directory = tempfile.TemporaryDirectory()
        (
            bundle_hash,
            tables,
            string_dependencies,
            trigger_by,
            function_snippet,
            context_location,
            function_name,
        ) = create_archive_and_hash(
            function_path,
            temporary_directory,
            path_to_bundle,
            requirements,
            local_packages,
        )

        description = description or function_name

        response = self.connection.function_create(
            collection_name=collection_name,
            function_name=function_name,
            description=description,
            bundle_hash=bundle_hash,
            tables=tables,
            dependencies=string_dependencies,
            trigger_by=trigger_by,
            function_snippet=function_snippet,
            raise_for_status=raise_for_status,
        )
        current_function_id = response.json().get("current_function_id")
        with open(context_location, "rb") as file:
            bundle = file.read()

        self.connection.function_upload_bundle(
            collection_name=collection_name,
            function_name=function_name,
            function_id=current_function_id,
            bundle=bundle,
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
        self.connection.function_delete(
            collection_name, function_name, raise_for_status=raise_for_status
        )

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
        # TODO: For this and other function representations, decide if data and
        #  versions will be dynamically calculated like users, collections, etc. are
        #  for the TabsdataServer. Currently leaving it static due to time constraints.
        function_definition = self.connection.function_get(
            collection_name, function_name
        ).json()
        return Function(**function_definition)

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
        raw_list_of_functions = (
            self.connection.function_list_history(collection_name, function_name)
            .json()
            .get("data")
        )
        return [Function(**function) for function in raw_list_of_functions]

    def function_trigger(
        self,
        collection_name,
        function_name,
        execution_plan_name: str | None = None,
        raise_for_status: bool = True,
    ) -> requests.Response:
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
        return self.connection.function_execute(
            collection_name,
            function_name,
            execution_plan_name=execution_plan_name,
            raise_for_status=raise_for_status,
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
        raise_for_status: bool = True,
    ) -> None:
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
        temporary_directory = tempfile.TemporaryDirectory()
        (
            bundle_hash,
            tables,
            string_dependencies,
            trigger_by,
            function_snippet,
            context_location,
            new_function_name,
        ) = create_archive_and_hash(
            function_path,
            temporary_directory,
            directory_to_bundle,
            requirements,
            local_packages,
        )

        response = self.connection.function_update(
            collection_name=collection_name,
            function_name=function_name,
            new_function_name=new_function_name,
            description=description,
            bundle_hash=bundle_hash,
            tables=tables,
            dependencies=string_dependencies,
            trigger_by=trigger_by,
            function_snippet=function_snippet,
            raise_for_status=raise_for_status,
        )

        current_function_id = response.json().get("current_function_id")
        with open(context_location, "rb") as file:
            bundle = file.read()

        self.connection.function_upload_bundle(
            collection_name=collection_name,
            function_name=new_function_name or function_name,
            function_id=current_function_id,
            bundle=bundle,
            raise_for_status=raise_for_status,
        )

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
        response = self.connection.table_get_data(
            collection_name,
            table_name,
            commit=commit,
            time=time,
            version=version,
            raise_for_status=raise_for_status,
        )
        with open(destination_file, "wb") as file:
            file.write(response.content)

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
        return self.connection.table_get_schema(
            collection_name, table_name, commit=commit, time=time, version=version
        ).json()

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
        raw_tables = (
            self.connection.table_list(collection_name, offset=offset, len=len)
            .json()
            .get("data")
        )
        return [Table(**table) for table in raw_tables]

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
        parquet_frame = self.connection.table_get_sample(
            collection_name,
            table_name,
            commit=commit,
            time=time,
            version=version,
            offset=offset,
            len=len,
        ).content
        return pl.read_parquet(parquet_frame)

    def transaction_cancel(self, execution_plan_id: str) -> requests.Response:
        """
        Cancel an execution plan. This includes all functions that are part of the
            execution plan and all its dependants.

        Args:
            execution_plan_id (str): The ID of the execution plan to cancel.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_plan_id is not found in the system.
        """
        return self.connection.transaction_cancel(execution_plan_id)

    def transaction_recover(self, execution_plan_id: str) -> requests.Response:
        """
        Recover an execution plan. This includes all functions that are part of the
            execution plan and all its dependants.

        Args:
            execution_plan_id (str): The ID of the execution plan to recover.

        Returns:
            requests.Response: The response of the server to the request.

        Raises:
            APIServerError: If the execution_plan_id is not found in the system.
        """
        return self.connection.transaction_recover(execution_plan_id)

    def user_create(
        self,
        name: str,
        password: str,
        full_name: str = None,
        email: str = None,
        enabled: bool = True,
        raise_for_status: bool = True,
    ) -> None:
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
        self.connection.users_create(
            name, full_name, email, password, enabled, raise_for_status=raise_for_status
        )

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
        self.connection.users_delete(name, raise_for_status=raise_for_status)

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
        return User(**self.connection.users_get_by_name(name).json())

    def user_update(
        self,
        name: str,
        full_name: str = None,
        email: str = None,
        enabled: bool = None,
        raise_for_status: bool = True,
    ) -> None:
        # TODO: Implement change password logic, for now only full name, email
        #  and disabled are updated
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
        self.connection.users_update(
            name,
            full_name=full_name,
            email=email,
            enabled=enabled,
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
        return self.connection.worker_log(worker_id).text

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
        )
        return [Worker(**worker_message) for worker_message in raw_worker_messages]


def calculate_file_sha256(file_path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


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


def create_archive_and_hash(
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
    bundle_hash = calculate_file_sha256(context_location)
    return (
        bundle_hash,
        tables,
        string_dependencies,
        trigger_string_list,
        function_snippet,
        context_location,
        function_name,
    )


def convert_timestamp_to_string(timestamp: int | None) -> str:
    if not timestamp:
        return str(timestamp)
    return str(
        datetime.datetime.fromtimestamp(timestamp / 1e3, datetime.UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )
