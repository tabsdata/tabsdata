#
# Copyright 2026 Tabsdata Inc.
#

"""High-level user-facing SDK for talking to the tabsdata apiserver.

:class:`TabsdataServer` and its resource classes (:class:`Collection`,
:class:`Function`, :class:`FunctionRun`, :class:`Execution`,
:class:`Transaction`, :class:`Worker`, :class:`Table`, ...) wrap the
low-level HTTP client at :mod:`tabsdatak.sdk._apiserver` with friendly
method names.

Function registration in v2 uses the tabsdatak bundle producer:
``register_function`` / ``patch_function`` accept a
``PublisherInfo`` / ``TransformerInfo`` / ``SubscriberInfo`` (the
descriptor your ``@publisher``/``@transformer``/``@subscriber``
decorator produced), call :func:`tabsdatak.common._archive.bundle.build_bundle` to
produce the ``.tar.zst``, and POST it to the server. The legacy
``function_path``-based registration that re-imported the user's
module and assembled the bundle from scratch is gone — bring an
``Info`` object instead.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List, Union

import polars as pl
import requests

# noinspection PyProtectedMember
from tabsdatak import _connectors

# noinspection PyProtectedMember
from tabsdatak._api import qualified_class
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_STREAM_PUBLISHER_DEF,
    KIND_SUBSCRIBER_DEF,
    KIND_TRANSFORMER_DEF,
    PublisherInfo,
    StreamPublisherInfo,
    SubscriberInfo,
    TransformerInfo,
)
from tabsdatak.api import CronTrigger, TablesTrigger
from tabsdatak.common._archive.bundle import build_bundle, capture_source_snippet

if TYPE_CHECKING:
    from tabsdatak.common._actions.dataquality.processor import TrendTransformerInfo

# noinspection PyProtectedMember
from tabsdatak.common._datetime import _convert_timestamp_to_string

# noinspection PyProtectedMember
from tabsdatak.common._temps import tabsdata_temp_folder

# noinspection PyProtectedMember
from tabsdatak.sdk._apiserver import APIServer, obtain_connection
from tabsdatak.sdk.status_utils.data_version import data_version_status_to_mapping
from tabsdatak.sdk.status_utils.execution import (
    execution_status_to_mapping,
    execution_trigger_type_to_mapping,
)
from tabsdatak.sdk.status_utils.function_run import function_run_status_to_mapping
from tabsdatak.sdk.status_utils.transaction import transaction_status_to_mapping
from tabsdatak.sdk.status_utils.worker import worker_status_to_mapping

FunctionInfo = Union[
    PublisherInfo, StreamPublisherInfo, TransformerInfo, SubscriberInfo
]


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
        group: str | None = None,
        base_path: str | None = None,
        connection_document: str | None = None,
        secrets: list | None = None,
        **kwargs,
    ):
        """
        Initialize the Collection object.

        Args:
            connection (APIServer): The connection to the server.
            name (str): The name of the collection.
            description (str, optional): The description of the collection.
            group (str, optional): The name of the group to assign at
                creation time. Ignored when reading an existing collection.
            **kwargs: Additional keyword arguments.
        """
        created_on = kwargs.get("created_on")
        created_by = kwargs.get("created_by")
        self.connection = connection
        self.name = name
        self.description = description
        self._group = group
        # Source/destination collections are created WITH their connection;
        # only used at create time.
        self._connection_document = connection_document
        self._secrets = secrets
        # Logical storage prefix for the collection's artifacts. None at read time
        # falls back to the server value via the data dict / kwargs.
        self._base_path = base_path
        self.base_path = base_path if base_path is not None else kwargs.get("base_path")
        self.created_on = created_on
        if created_on:
            self.created_on_str = _convert_timestamp_to_string(created_on)
        else:
            self.created_on_str = None
        self.created_by = created_by
        # Owning project's name — populated on cross-project (wildcard) listings so
        # the CLI can show which project each collection belongs to.
        self.project = kwargs.get("project")
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
    def tables(self) -> List[Table]:
        return self.list_tables()

    def create(self, raise_for_status: bool = True) -> Collection:
        description = self._description or self.name
        response = self.connection.collection_create(
            self.name,
            description,
            group=self._group,
            base_path=self._base_path,
            connection=self._connection_document,
            secrets=self._secrets,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.collection_delete(self.name, raise_for_status=raise_for_status)

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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_functions = data.get("data") or []
            for raw_function in raw_functions:
                # The server payload carries a `connection` field (the Conn
                # FQN); set it to the APIServer here so it does not collide with
                # the `connection` positional when splatting (matches the
                # `get_function` pattern above).
                raw_function["connection"] = self.connection
                if "collection" not in raw_function:
                    raw_function["collection"] = self
                yield Function(**raw_function)
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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_tables = data.get("data") or []
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

    def _build_and_upload_bundle(
        self,
        info: FunctionInfo,
        project_root: str | Path,
        local_packages: list[str] | None,
        raise_for_status: bool,
        requirements: str | Path | None = None,
    ) -> tuple[_FunctionArchive, str]:
        """Bundle ``info`` + ``project_root`` and upload it to the
        apiserver. Returns the populated ``_FunctionArchive`` (carrying
        the metadata extracted from ``info``) and the ``bundle_id`` the
        server assigned to the upload.

        Shared by :meth:`register_function`, :meth:`update_function`,
        and :meth:`patch_function`. Each caller then issues its
        endpoint-specific ``function_register`` / ``function_update`` /
        ``function_patch`` call with the metadata it needs.

        If ``requirements`` is set, it must point to a
        ``requirements.yaml`` file the caller wants embedded verbatim
        in the bundle (overriding the auto-synthesised env snapshot).
        """
        temporary_directory = tempfile.TemporaryDirectory(dir=tabsdata_temp_folder())
        try:
            valid_python_versions = (
                self.connection
                .runtime_info_get()
                .json()
                .get("data", {})
                .get("python_versions")
            )
            archive = _create_archive(
                self.name,
                info,
                project_root,
                temporary_directory,
                local_packages=local_packages,
                valid_python_versions=valid_python_versions,
                requirements=requirements,
            )
            with open(archive.context_location, "rb") as file:
                bundle = file.read()
        finally:
            temporary_directory.cleanup()

        response = self.connection.function_upload_bundle(
            collection_name=self.name,
            bundle=bundle,
            raise_for_status=raise_for_status,
        )
        bundle_id = response.json().get("data").get("id")
        return archive, bundle_id

    def _trend_child_functions(
        self,
        trend_transformers: list[TrendTransformerInfo],
        local_packages: list[str] | None = None,
        requirements: str | Path | None = None,
        raise_for_status: bool = True,
    ) -> list[dict]:
        """Build and upload a bundle per declared trend transformer, and
        return the specs the owner's registration carries them in.
        """
        from tabsdatak.dataquality._symbols import FunctionType

        children = []
        for trend in trend_transformers:
            archive, bundle_id = self._build_and_upload_bundle(
                trend.info,
                Path(trend.project_root.name),
                local_packages,
                raise_for_status,
                requirements,
            )
            children.append({
                "name": trend.function_name,
                "function_type": FunctionType.DQSummaryTrends.value,
                "bundle_id": bundle_id,
                "snippet": archive.function_snippet,
                "dependencies": archive.string_dependencies,
                "tables": archive.tables,
                "table_types": trend.table_types,
            })
        return children

    def register_function(
        self,
        info: FunctionInfo,
        project_root: str | Path,
        *,
        description: str | None = None,
        local_packages: List[str] | None = None,
        function_name: str | None = None,
        reuse_tables: bool = False,
        enabled: bool = True,
        raise_for_status: bool = True,
        requirements: str | Path | None = None,
        base_path: str | None = None,
    ) -> Function:
        """Register a function in this collection from a tabsdatak ``FunctionInfo``.

        Args:
            info: The descriptor produced by ``@publisher`` /
                ``@stream_publisher`` / ``@transformer`` / ``@subscriber``
                (a :class:`PublisherInfo` / :class:`StreamPublisherInfo` /
                :class:`TransformerInfo` / :class:`SubscriberInfo`).
            project_root: Directory containing the user's project tree.
                Every non-ignored top-level entry is bundled under
                ``user_code/`` so the runner can resolve ``info.func``'s
                module path against it.
            description: Optional. Defaults to the function's name.
            local_packages: Optional list of local-package directories
                to stage under ``local_packages/<n>/`` in the bundle.
            function_name: Optional override of the name registered on
                the server. Defaults to ``info.func.__name__``.
            reuse_tables: When ``True``, frozen tables are reused
                across patches (see legacy semantics).
            enabled: Register in enabled state. Defaults to ``True``.
            raise_for_status: Raise on non-2xx responses. Defaults to
                ``True``.
            requirements: Optional path to a user-supplied
                ``requirements.yaml``. When set, the file is embedded
                in the bundle verbatim, overriding the env snapshot
                that ``build_bundle`` would otherwise synthesise from
                the active Python interpreter. Use this when the
                build env differs from the runtime env (e.g.
                cross-platform deployment).

        Returns:
            A :class:`Function` handle for the newly-registered function.

        Raises:
            APIServerError: If the function could not be created.
        """
        archive, bundle_id = self._build_and_upload_bundle(
            info,
            project_root,
            local_packages,
            raise_for_status,
            requirements,
        )
        try:
            function_name = function_name or archive.function_name
            description = description or function_name

            # TODO: Remove this once the parameter is optional
            runtime_values = "{}"

            child_functions = self._trend_child_functions(
                archive.trend_transformers_info or [],
                local_packages,
                requirements,
                raise_for_status,
            )

            self.connection.function_register(
                collection_name=self.name,
                function_name=function_name,
                description=description,
                tables=archive.tables,
                table_types=archive.table_types,
                parent_tables=archive.parent_tables or None,
                dependencies=archive.string_dependencies,
                trigger_by=archive.trigger_string_list,
                function_snippet=archive.function_snippet,
                bundle_id=bundle_id,
                runtime_values=runtime_values,
                reuse_frozen_tables=reuse_tables,
                decorator=archive.function_type,
                connector=archive.connector,
                connection=archive.connection,
                workers=archive.workers,
                enabled=enabled,
                base_path=base_path,
                child_functions=child_functions,
                raise_for_status=raise_for_status,
            )

            return Function(self.connection, self, function_name)
        finally:
            _cleanup_trend_roots(archive.trend_transformers_info)

    def patch_function(
        self,
        function_name: str,
        info: FunctionInfo,
        project_root: str | Path,
        patch_of: str,
        *,
        description: str | None = None,
        local_packages: List[str] | None = None,
        raise_for_status: bool = True,
        requirements: str | Path | None = None,
    ) -> dict:
        """Register a code-only patch of an existing function version.

        A patch shares the parent's topology (tables, dependencies,
        triggers, workers) — only the bundle / snippet / runtime
        values / description can be overridden. Patches are reachable
        only by explicit fvid via replay.

        Args:
            function_name: Name of the function to patch.
            info: The :class:`FunctionInfo` whose source is being
                patched in.
            project_root: Directory containing the user's project tree.
            patch_of: Function-version id (fvid) this patches.
            description: Optional new description.
            local_packages: Optional list of local-package directories
                to stage in the bundle.
            raise_for_status: Raise on non-2xx. Defaults to ``True``.
            requirements: Optional path to a user-supplied
                ``requirements.yaml`` to embed in the bundle verbatim
                (overriding the env-synthesised one). See
                :meth:`register_function` for details.

        Returns:
            The created FunctionPatchRecord (id, parent_function_version_id,
            bundle_id, snippet, defined_on, defined_by_id, ...).

        Raises:
            APIServerError: If the patch could not be created.
        """
        archive, bundle_id = self._build_and_upload_bundle(
            info, project_root, local_packages, raise_for_status, requirements
        )
        _cleanup_trend_roots(archive.trend_transformers_info)

        # TODO: Remove this once the parameter is optional
        runtime_values = "{}"

        response = self.connection.function_patch(
            collection_name=self.name,
            function_name=function_name,
            patch_of=patch_of,
            bundle_id=bundle_id,
            snippet=archive.function_snippet,
            runtime_values=runtime_values,
            description=description,
            raise_for_status=raise_for_status,
        )
        return response.json().get("data") or {}

    def list_function_patches(
        self,
        function_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[dict]:
        """
        List patch versions for a function (latest first).

        Returns:
            List[dict]: Each dict is a FunctionPatchRecord.
        """
        return list(
            self.list_function_patches_generator(
                function_name,
                filter=filter,
                order_by=order_by,
                raise_for_status=raise_for_status,
            )
        )

    def list_function_patches_generator(
        self,
        function_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[dict]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id is not None and next_step is not None):
            response = self.connection.function_list_patches(
                self.name,
                function_name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data") or {}
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            for raw_patch in data.get("data") or []:
                yield raw_patch
            first_page = False

    def update(
        self,
        name: str,
        description: str = None,
        base_path: str = None,
        raise_for_status: bool = True,
    ) -> Collection:
        response = self.connection.collection_update(
            self.name,
            new_collection_name=name,
            description=description,
            base_path=base_path,
            raise_for_status=raise_for_status,
        )
        self.name = name
        self.refresh()
        self._data = response.json().get("data")
        return self

    def upsert_connection(
        self,
        connection: str,
        secrets: list | None = None,
        raise_for_status: bool = True,
    ) -> Collection:
        self.connection.collection_connection_upsert(
            self.name,
            connection,
            secrets=secrets,
            raise_for_status=raise_for_status,
        )
        return self

    def get_connection(self, raise_for_status: bool = True) -> dict:
        response = self.connection.collection_connection_get(
            self.name, raise_for_status=raise_for_status
        )
        return response.json().get("data")

    def update_function(
        self,
        function_name: str,
        info: FunctionInfo,
        project_root: str | Path,
        *,
        description: str | None = None,
        local_packages: List[str] | None = None,
        new_function_name: str | None = None,
        reuse_tables: bool = False,
        enabled: bool = True,
        raise_for_status: bool = True,
        requirements: str | Path | None = None,
        base_path: str | None = None,
        reset_state: bool = False,
        reset_stage: bool = False,
    ) -> Function:
        """Update a function in this collection from a tabsdatak ``FunctionInfo``.

        Mirror of :meth:`register_function` for an already-registered
        function: builds a fresh bundle from ``info`` + ``project_root``,
        uploads it, and points the existing function at the new bundle.

        Args:
            function_name: The current name of the function to update.
            info: The descriptor produced by ``@publisher`` /
                ``@stream_publisher`` / ``@transformer`` /
                ``@subscriber`` (a :class:`PublisherInfo` /
                :class:`StreamPublisherInfo` / :class:`TransformerInfo`
                / :class:`SubscriberInfo`).
            project_root: Directory containing the user's project tree
                (see :meth:`register_function`).
            description: Optional new description.
            local_packages: Optional local-package directories to stage
                under ``local_packages/<n>/`` in the bundle.
            new_function_name: Optional rename; if omitted, the
                function's name (from ``info.func.__name__``) is used.
            reuse_tables: When ``True``, frozen tables are reused.
            enabled: New enabled state for the function.
            raise_for_status: Raise on non-2xx responses.
            requirements: Optional path to a user-supplied
                ``requirements.yaml`` to embed in the bundle verbatim.
                See :meth:`register_function` for details.

        Raises:
            APIServerError: If the function could not be updated.
        """
        archive, bundle_id = self._build_and_upload_bundle(
            info,
            project_root,
            local_packages,
            raise_for_status,
            requirements,
        )
        try:
            new_function_name = new_function_name or archive.function_name

            # TODO: Remove this once the parameter is optional
            runtime_values = "{}"

            child_functions = self._trend_child_functions(
                archive.trend_transformers_info or [],
                local_packages,
                requirements,
                raise_for_status,
            )

            self.connection.function_update(
                collection_name=self.name,
                function_name=function_name,
                new_function_name=new_function_name,
                description=description,
                tables=archive.tables,
                table_types=archive.table_types,
                parent_tables=archive.parent_tables or None,
                dependencies=archive.string_dependencies,
                trigger_by=archive.trigger_string_list,
                function_snippet=archive.function_snippet,
                decorator=archive.function_type,
                bundle_id=bundle_id,
                runtime_values=runtime_values,
                reuse_frozen_tables=reuse_tables,
                connector=archive.connector,
                connection=archive.connection,
                workers=archive.workers,
                enabled=enabled,
                base_path=base_path,
                reset_state=reset_state,
                reset_stage=reset_stage,
                child_functions=child_functions,
                raise_for_status=raise_for_status,
            )

            return Function(self.connection, self, new_function_name)
        finally:
            _cleanup_trend_roots(archive.trend_transformers_info)

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
            trigger_type: Type type of trigger for the execution.
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
        self.trigger_type = kwargs.get("trigger_type")
        self.status = kwargs.get("status")
        self.project = kwargs.get("project")

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
            with self.connection.spanning():
                response = self.connection.execution_list(
                    request_filter=f"id:eq:{self.id}"
                )
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
            self.connection
            .function_list_runs(request_filter=f"execution_id:eq:{self.id}")
            .json()
            .get("data")
            .get("data")
        )
        return [
            FunctionRun(**{**function_run, "connection": self.connection})
            for function_run in raw_function_runs
        ]

    @property
    def trigger_type(self) -> str:
        if self._trigger_type is None:
            self.trigger_type = self._data.get("trigger_type")
        return self._trigger_type

    @trigger_type.setter
    def trigger_type(self, trigger_type: str | None):
        if trigger_type is None:
            self._trigger_type = trigger_type
        else:
            self._trigger_type = execution_trigger_type_to_mapping(trigger_type)

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
            self.connection
            .workers_list(request_filter=f"execution_id:eq:{self.id}")
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
        self.trigger_type = None
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
        self.function_type = kwargs.get("function_type")
        self.dependencies = kwargs.get("dependencies")
        self.description = kwargs.get("description")
        self.defined_on = kwargs.get("defined_on")
        self.defined_on_str = None
        self.defined_by = kwargs.get("defined_by")
        self.type = kwargs.get("decorator")
        self.enabled = kwargs.get("enabled")
        self.patch_of = kwargs.get("patch_of")
        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            self._data = (
                self.connection
                .function_get(self.collection.name, self.name)
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
            self.connection
            .function_list_runs(
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
    def function_type(self) -> str:
        if self._function_type is None:
            self.function_type = self._data.get("function_type")
        return self._function_type

    @function_type.setter
    def function_type(self, function_type: str):
        self._function_type = function_type

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
            self.connection
            .workers_list(
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
        """Delete this function.

        The server deletes the functions declaring this one as their parent --
        the trend transformers among them -- along with it, in the same
        transaction.
        """
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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_functions = data.get("data") or []
            for raw_function in raw_functions:
                version_data = dict(raw_function)
                # See list_functions: set `connection` to the APIServer so the
                # server's `connection` (Conn FQN) field doesn't collide with
                # the positional when splatting.
                raw_function["connection"] = self.connection
                raw_function["collection"] = self.collection
                built_function = Function(**raw_function)
                # Each row is a past version; without its own payload `_data`
                # would lazily fetch the current one.
                built_function._data = version_data
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
        enabled: bool = True,
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
            enabled=enabled,
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
        enabled: bool = True,
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
            enabled=enabled,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self.name = result.name
        self.collection = result.collection
        return result

    def patch(
        self,
        function_path: str,
        patch_of: str,
        description: str = None,
        directory_to_bundle: str = None,
        requirements: str = None,
        local_packages: List[str] | str | None = None,
        raise_for_status: bool = True,
    ) -> dict:
        """
        Register a code-only patch of this function's `patch_of` version.

        See `Collection.patch_function` for argument semantics.
        """
        return self.collection.patch_function(
            self.name,
            function_path,
            patch_of=patch_of,
            description=description,
            path_to_bundle=directory_to_bundle,
            requirements=requirements,
            local_packages=local_packages,
            raise_for_status=raise_for_status,
        )

    @property
    def patches(self) -> List[dict]:
        return self.list_patches()

    def list_patches(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[dict]:
        return self.collection.list_function_patches(
            self.name,
            filter=filter,
            order_by=order_by,
            raise_for_status=raise_for_status,
        )

    def enable(self, enabled: bool = True, raise_for_status: bool = True) -> Function:
        collection = self.collection
        result = self.connection.function_enable(
            collection_name=collection.name,
            function_name=self.name,
            enabled=enabled,
            raise_for_status=raise_for_status,
        )
        self.refresh()
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

    def __hash__(self):
        """Generates a hash value for the object."""
        # The hash should be based on the same attributes used in __eq__
        return hash((self.name, self.collection.name))

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
        self.project = kwargs.get("project")
        self.ended_on = kwargs.get("ended_on")
        self.ended_on_str = None
        self.started_on = kwargs.get("started_on")
        self.started_on_str = None
        self._data = None
        self.kwargs = {"id": id, **kwargs}

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            try:
                with tabsdata_server._spanning():
                    run = tabsdata_server.list_function_runs(filter=f"id:eq:{self.id}")[
                        0
                    ]
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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_permissions = data.get("data") or []
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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_users = data.get("data") or []
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


class Group:
    """A group on the Tabsdata server.

    Groups are a lightweight tag attached to collections. System-seeded
    rows (`default`, `source`, `destination`) carry ``system = True``
    and reject update / delete.
    """

    created_by = _LazyProperty("created_by")
    created_on = _LazyProperty("created_on")
    created_on_str = _LazyProperty("created_on_str", subordinate_time_string=True)
    description = _LazyProperty("description")
    id = _LazyProperty("id")
    modified_by = _LazyProperty("modified_by")
    modified_by_id = _LazyProperty("modified_by_id")
    modified_on = _LazyProperty("modified_on")
    modified_on_str = _LazyProperty("modified_on_str", subordinate_time_string=True)
    system = _LazyProperty("system")

    def __init__(
        self,
        connection: APIServer,
        name: str,
        **kwargs,
    ):
        self.connection = connection
        self.name = name

        self.created_by = kwargs.get("created_by")
        self.created_on = kwargs.get("created_on")
        self.created_on_str = None
        self.description = kwargs.get("description")
        self.id = kwargs.get("id")
        self.modified_by = kwargs.get("modified_by")
        self.modified_by_id = kwargs.get("modified_by_id")
        self.modified_on = kwargs.get("modified_on")
        self.modified_on_str = None
        self.system = kwargs.get("system")

        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            self._data = self.connection.group_get_by_name(self.name).json().get("data")
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    def create(self, raise_for_status: bool = True) -> Group:
        name = self.name
        description = self._description
        response = self.connection.group_create(
            name,
            description,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.group_delete(self.name, raise_for_status=raise_for_status)

    def refresh(self) -> Group:
        self.created_by = None
        self.created_on = None
        self.created_on_str = None
        self.description = None
        self.id = None
        self.modified_by = None
        self.modified_by_id = None
        self.modified_on = None
        self.modified_on_str = None
        self.system = None
        self.kwargs = None
        self._data = None
        return self

    def update(
        self,
        name: str = None,
        description: str = None,
        raise_for_status: bool = True,
    ) -> Group:
        response = self.connection.group_update(
            self.name,
            new_name=name,
            new_description=description,
            raise_for_status=raise_for_status,
        )
        data = response.json().get("data")
        self.refresh()
        self.name = data.get("name")
        self._data = data
        return self

    def __eq__(self, other):
        if not isinstance(other, Group):
            return False
        return self.name == other.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!s}"


class Project:
    """A project on the Tabsdata server.

    A project is a tenant boundary: its collections, functions and data are
    isolated from other projects. Not every instance supports projects; where
    they are supported you can create and delete them freely.
    """

    created_by = _LazyProperty("created_by")
    created_on = _LazyProperty("created_on")
    created_on_str = _LazyProperty("created_on_str", subordinate_time_string=True)
    description = _LazyProperty("description")
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
        self.connection = connection
        self.name = name

        self.created_by = kwargs.get("created_by")
        self.created_on = kwargs.get("created_on")
        self.created_on_str = None
        self.description = kwargs.get("description")
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
            self._data = (
                self.connection.project_get_by_name(self.name).json().get("data")
            )
        return self._data_dict

    @_data.setter
    def _data(self, data_dict: dict | None):
        self._data_dict = data_dict

    def create(self, raise_for_status: bool = True) -> Project:
        response = self.connection.project_create(
            self.name,
            self._description,
            raise_for_status=raise_for_status,
        )
        self.refresh()
        self._data = response.json().get("data")
        return self

    def delete(self, raise_for_status: bool = True) -> None:
        self.connection.project_delete(self.name, raise_for_status=raise_for_status)

    def refresh(self) -> Project:
        self.created_by = None
        self.created_on = None
        self.created_on_str = None
        self.description = None
        self.id = None
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
    ) -> Project:
        response = self.connection.project_update(
            self.name,
            new_name=name,
            new_description=description,
            raise_for_status=raise_for_status,
        )
        data = response.json().get("data")
        self.refresh()
        self.name = data.get("name")
        self._data = data
        return self

    def __eq__(self, other):
        if not isinstance(other, Project):
            return False
        return self.name == other.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __str__(self) -> str:
        return f"Name: {self.name!s}"


class RolePermissionTypes(Enum):
    """
    Enum for permission types.
    """

    MANAGE_ACCESS = "manage_access"
    CREATE_PROJECTS = "create_projects"
    MANAGE_ROLES = "manage_roles"
    VIEW_PROJECT = "view_project"
    DEVELOP_PROJECT = "develop_project"
    MANAGE_PROJECT = "manage_project"


VALID_PERMISSION_TYPES = [
    (e.name.lower(), e.value.lower()) for e in RolePermissionTypes
]

PERMISSION_TYPES_WITH_ENTITY = [
    (
        RolePermissionTypes.VIEW_PROJECT.name.lower(),
        RolePermissionTypes.VIEW_PROJECT.value.lower(),
    ),
    (
        RolePermissionTypes.DEVELOP_PROJECT.name.lower(),
        RolePermissionTypes.DEVELOP_PROJECT.value.lower(),
    ),
    (
        RolePermissionTypes.MANAGE_PROJECT.name.lower(),
        RolePermissionTypes.MANAGE_PROJECT.value.lower(),
    ),
]


# --- Simple Security Mode permission facade ---------------------
# The permission-type constants (SIMPLE_*_PERMISSION_TYPES, VALID_*, ...) live in
# the lightweight `tabsdatak.sdk._permission_types` module so `tdk permission` can
# reference them in its option help without importing this (heavy) module.


class SimplePermission:
    """One row of a user's effective flat permission set
    (`tdk permission list --user X`).

    `project` is set for project-scoped permissions and ``None`` for
    system-scoped ones.
    """

    def __init__(self, permission: str, project: str = None, **kwargs):
        self.permission = permission
        self.project = project
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"SimplePermission(permission={self.permission!r}, "
            f"project={self.project!r})"
        )


class SimpleProjectAccess:
    """One row of "who has access to a project"
    (`tdk permission list --proj P`).

    `source` records what conferred the access: ``project (<rolename>)`` for
    per-project role memberships, ``system (manage_system)`` for system admins.
    """

    def __init__(self, user: str, permission: str, source: str, **kwargs):
        self.user = user
        self.permission = permission
        self.source = source
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return (
            f"SimpleProjectAccess(user={self.user!r}, "
            f"permission={self.permission!r}, source={self.source!r})"
        )


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
        self,
        connection: APIServer,
        collection_name: str | Collection,
        name: str,
        **kwargs,
    ):
        self.connection = connection
        self.collection = collection_name
        self.name = name

        self.id = kwargs.get("id")
        self.function = kwargs.get("function")
        self.kwargs = kwargs
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            try:
                self._data_dict = (
                    self.connection
                    .table_list(
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
        return self.list_dataversions(filter="status:eq:C")

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
    ) -> Path | None:
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

        Returns:
            Path | None: The path of the downloaded file, or None when the version
                holds no data, in which case no file is written.

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
        if not response.content:
            return None
        with open(destination_file, "wb") as file:
            file.write(response.content)
        return Path(destination_file)

    def list_dataversions(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
        at: int | None = None,
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
                at=at,
            )
        )

    def list_dataversions_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
        at: int | None = None,
    ) -> Generator[DataVersion]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id is not None and next_step is not None):
            response = self.connection.dataversion_list(
                self.collection.name,
                self.name,
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
                at=at,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_dataversions = data.get("data") or []
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
        extra_cols: str = None,
    ) -> List[dict]:
        """
        Get the schema of a table for a given version. The version can be a
            fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            extra_cols (str, optional): Which non-user columns to include on top
                of the user ones: 'none' (the server default), 'meta' (the
                '@td.' connector metadata), 'sys' (the '$td.' system columns)
                or 'all'.

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
            self.connection
            .table_get_schema(
                self.collection.name,
                self.name,
                at=at,
                extra_cols=extra_cols,
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
        sql: str = None,
        extra_cols: str = None,
    ) -> pl.DataFrame:
        """
        Get a sample of a table for a given version as a parquet file. The
            version can be a fixed version or a relative one (HEAD, HEAD^,
            and HEAD~## syntax).

        Args:
            offset (int, optional): The offset of the sample.
            len (int, optional): The length of the sample. Defaults to 100
                server-side, and cannot be greater than 1000.
            sql (str, optional): A SQL query run server-side on the table, which
                is the only queryable relation and is referenced by its own
                name. The offset is applied before the query and the length caps
                its result.
            extra_cols (str, optional): Which non-user columns to return on top
                of the user ones: 'none' (the server default), 'meta' (the
                '@td.' connector metadata), 'sys' (the '$td.' system columns)
                or 'all'.
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
            sql=sql,
            extra_cols=extra_cols,
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
        self.project = kwargs.get("project")
        self.triggered_by = kwargs.get("triggered_by")
        self.triggered_on = kwargs.get("triggered_on")
        self.triggered_on_str = None
        self.ended_on = kwargs.get("ended_on")
        self.ended_on_str = None
        self.started_on = kwargs.get("started_on")
        self.started_on_str = None
        self.kwargs = {"id": id, **kwargs}
        self._data = None

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            with tabsdata_server._spanning():
                try:
                    transaction = tabsdata_server.list_transactions(
                        filter=f"id:eq:{self.id}"
                    )[0]
                except IndexError:
                    raise ValueError(f"Transaction with ID {self.id} not found.")
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
            self.connection
            .function_list_runs(request_filter=f"transaction_id:eq:{self.id}")
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
            self.connection
            .workers_list(request_filter=f"transaction_id:eq:{self.id}")
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

    started_on = _LazyProperty("started_on")
    started_on_str = _LazyProperty("started_on_str", subordinate_time_string=True)
    ended_on = _LazyProperty("ended_on")
    ended_on_str = _LazyProperty("ended_on_str", subordinate_time_string=True)
    function_version_id = _LazyProperty("function_version_id")

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
        self.type = kwargs.get("worker_type")
        self.project = kwargs.get("project")
        self.started_on = kwargs.get("started_on")
        self.started_on_str = None
        self.ended_on = kwargs.get("ended_on")
        self.ended_on_str = None
        self.function_version_id = kwargs.get("function_version_id")
        self._data = None
        self.kwargs = {"id": id, **kwargs}

    @property
    def _data(self) -> dict:
        if self._data_dict is None:
            tabsdata_server = TabsdataServer.__new__(TabsdataServer)
            tabsdata_server.connection = self.connection
            try:
                with tabsdata_server._spanning():
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

    def __init__(self, url: str, username: str = None, password: str = None):
        """
        Initialize the TabsdataServer object.

        Args:
            url (str): The url of the server.
            username (str): The username of the user.
            password (str): The password of the user.
        """
        self.connection = obtain_connection(url, username, password)

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
    def groups(self) -> List[Group]:
        """
        Get the list of groups in the server. This list is obtained every time the
            property is accessed, so sequential accesses to this property in the same
            object might yield different results.

        Returns:
            List[Group]: The list of groups in the server.
        """
        return self.list_groups()

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
    def projects(self) -> List[Project]:
        """
        Get the list of projects in the server. This list is obtained every time
            the property is accessed, so sequential accesses to this property in the
            same object might yield different results.

        Returns:
            List[Project]: The list of projects in the server.
        """
        return self.list_projects()

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
            self.connection
            .runtime_info_get()
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
        self,
        name: str,
        description: str = None,
        group: str | None = None,
        base_path: str | None = None,
        connection: str | None = None,
        secrets: list | None = None,
        raise_for_status: bool = True,
    ) -> Collection:
        """
        Create a collection in the server.

        Args:
            name (str): The name of the collection.
            description (str, optional): The description of the collection.
            group (str, optional): The name of the group to assign. If
                omitted, the server assigns the default group.
            connection (str, optional): Dehydrated connection document as a YAML
                string — required when creating a source/destination collection
                (it is created WITH its connection, 1:1).
            secrets (list, optional): ``{key, value}`` covering every
                ``env:`` reference in the connection document.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the collection could not be created.
        """
        collection = Collection(
            self.connection,
            name,
            description=description,
            group=group,
            base_path=base_path,
            connection_document=connection,
            secrets=secrets,
        )
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
        data = collection_definition.json().get("data")
        collection = Collection(self.connection, **data)
        # Retain the full server DTO so `-o json/yaml` renders all fields
        # without a second fetch.
        collection._data = data
        return collection

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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_collections = data.get("data") or []
            for raw_collection in raw_collections:
                built_collection = Collection(self.connection, **raw_collection)
                # Retain the list item's full DTO so `-o json/yaml` renders all
                # fields without a per-item fetch.
                built_collection._data = raw_collection
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

    def list_functions_generator(
        self,
        collection_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Function]:
        """
        Yield the functions in a collection, fetching pages as they are consumed.

        Args:
            collection_name (str): The name of the collection.

        Returns:
            Generator[Function]: The functions in the collection.

        Raises:
            APIServerError: If the functions could not be listed.
        """
        return Collection(self.connection, collection_name).list_functions_generator(
            filter=filter,
            order_by=order_by,
            raise_for_status=raise_for_status,
        )

    def update_collection(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        new_base_path: str = None,
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
            base_path=new_base_path,
            raise_for_status=raise_for_status,
        )

    def update_collection_connection(
        self,
        name: str,
        connection: str,
        secrets: list | None = None,
        raise_for_status: bool = True,
    ) -> Collection:
        """
        Create or update the connection of a source/destination collection.

        The server exposes this as a dedicated endpoint (a collection's
        connection is not part of the collection-update payload), so it is a
        separate call from :meth:`update_collection`.

        Args:
            name (str): The name of the collection.
            connection (str): Dehydrated connection document as a YAML string
                (same wire form as create).
            secrets (list, optional): ``{key, value}`` covering every ``secret:``
                reference in the connection document.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the connection could not be updated.
        """
        collection = Collection(self.connection, name)
        return collection.upsert_connection(
            connection,
            secrets=secrets,
            raise_for_status=raise_for_status,
        )

    def get_collection_connection(
        self, name: str, raise_for_status: bool = True
    ) -> dict:
        """
        Read the connection of a source/destination collection.

        Returns the dehydrated connection document plus the NAMES (never the
        values) of its secrets.

        Args:
            name (str): The name of the collection.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            dict: ``{id, connection, secret_names}``.

        Raises:
            APIServerError: If the connection could not be read.
        """
        collection = Collection(self.connection, name)
        return collection.get_connection(raise_for_status=raise_for_status)

    def list_dataversions(
        self,
        collection_name: str,
        table_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
        at: int | None = None,
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
            filter=filter, order_by=order_by, raise_for_status=raise_for_status, at=at
        )

    def list_dataversions_generator(
        self,
        collection_name: str,
        table_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
        at: int | None = None,
    ) -> Generator[DataVersion]:
        """
        Yield the data versions of a table in a collection, fetching pages as
        they are consumed.

        Args:
            collection_name (str): The name of the collection.

        Returns:
            Generator[DataVersion]: The data versions of the table.

        Raises:
            APIServerError: If the data versions could not be listed.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.list_dataversions_generator(
            filter=filter, order_by=order_by, raise_for_status=raise_for_status, at=at
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

    def _spanning(self):
        """Suspend the active project for one call: by-id lookups resolve a
        globally unique id and must not be narrowed to the active project."""
        return self.connection.spanning()

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
            with self._spanning():
                return self.list_executions(filter=f"id:eq:{execution_id}")[0]
        except IndexError:
            raise ValueError(f"Execution with ID {execution_id} not found.")

    def get_execution_graph(self, execution_id: str) -> dict:
        """
        Get the plan graph of an execution.

        Args:
            execution_id (str): The ID of the execution.

        Returns:
            dict: The plan graph, including its `dot` rendering.
        """
        response = self.connection.execution_read(execution_id)
        return response.json().get("data") or {}

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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_executions = data.get("data") or []
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
        info: FunctionInfo,
        project_root: str | Path,
        *,
        description: str | None = None,
        local_packages: List[str] | None = None,
        function_name: str | None = None,
        reuse_tables: bool = False,
        enabled: bool = True,
        raise_for_status: bool = True,
        requirements: str | Path | None = None,
        base_path: str | None = None,
    ) -> Function:
        """Register a function in ``collection_name`` from a tabsdatak
        ``FunctionInfo``.

        See :meth:`Collection.register_function` for the full argument
        contract; this wrapper just resolves the collection by name and
        forwards.

        Args:
            collection_name: The name of the collection to register into.

        Raises:
            APIServerError: If the function could not be created.
        """
        collection = Collection(self.connection, collection_name)
        return collection.register_function(
            info,
            project_root,
            description=description,
            local_packages=local_packages,
            function_name=function_name,
            reuse_tables=reuse_tables,
            enabled=enabled,
            raise_for_status=raise_for_status,
            requirements=requirements,
            base_path=base_path,
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

    def download_function_bundle(
        self,
        function_version_id: str,
        destination_file: str,
        raise_for_status: bool = True,
    ) -> None:
        """
        Download the bundle of a function version as a tar.zst archive.

        The bundle is addressed by its function version id; only Active or
        Frozen versions are downloadable (a version whose function has been
        deleted is not found).

        Args:
            function_version_id (str): The function version id (fvid) whose
                bundle will be downloaded.
            destination_file (str): The path where the tar.zst bundle is written.
            raise_for_status (bool, optional): Whether to raise an exception if
                the request was not successful. Defaults to True.

        Raises:
            APIServerError: If the bundle could not be downloaded.
        """
        response = self.connection.function_bundle_download(
            function_version_id,
            raise_for_status=raise_for_status,
        )
        with open(destination_file, "wb") as file:
            file.write(response.content)

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
            with self._spanning():
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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_function_runs = data.get("data") or []
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
        info: FunctionInfo,
        project_root: str | Path,
        *,
        description: str | None = None,
        local_packages: List[str] | None = None,
        new_function_name: str | None = None,
        reuse_tables: bool = False,
        enabled: bool = True,
        raise_for_status: bool = True,
        requirements: str | Path | None = None,
        base_path: str | None = None,
        reset_state: bool = False,
        reset_stage: bool = False,
    ) -> Function:
        """Update a function in ``collection_name`` from a tabsdatak
        ``FunctionInfo``.

        See :meth:`Collection.update_function` for the full argument
        contract; this wrapper resolves the collection by name and
        forwards.

        Args:
            collection_name: The name of the collection.

        Raises:
            APIServerError: If the function could not be updated.
        """
        collection = Collection(self.connection, collection_name)
        return collection.update_function(
            function_name,
            info,
            project_root,
            description=description,
            local_packages=local_packages,
            new_function_name=new_function_name,
            reuse_tables=reuse_tables,
            enabled=enabled,
            raise_for_status=raise_for_status,
            requirements=requirements,
            base_path=base_path,
            reset_state=reset_state,
            reset_stage=reset_stage,
        )

    def patch_function(
        self,
        collection_name: str,
        function_name: str,
        info: FunctionInfo,
        project_root: str | Path,
        patch_of: str,
        *,
        description: str | None = None,
        local_packages: List[str] | None = None,
        raise_for_status: bool = True,
        requirements: str | Path | None = None,
    ) -> dict:
        """Register a code-only patch of an existing function version.

        The patch shares the parent's topology -- only bundle / snippet /
        runtime values / description can be overridden. Reachable only
        by explicit fvid via replay. See
        :meth:`Collection.patch_function` for the full argument contract;
        this wrapper just resolves the collection by name and forwards.

        Returns:
            dict: The created FunctionPatchRecord.
        """
        collection = Collection(self.connection, collection_name)
        return collection.patch_function(
            function_name,
            info,
            project_root,
            patch_of=patch_of,
            description=description,
            local_packages=local_packages,
            raise_for_status=raise_for_status,
            requirements=requirements,
        )

    def list_function_patches(
        self,
        collection_name: str,
        function_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[dict]:
        """List patch versions for a function (latest first)."""
        collection = Collection(self.connection, collection_name)
        return collection.list_function_patches(
            function_name,
            filter=filter,
            order_by=order_by,
            raise_for_status=raise_for_status,
        )

    def list_function_patches_generator(
        self,
        collection_name: str,
        function_name: str,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[dict]:
        """Yield patch versions for a function (latest first), fetching pages as
        they are consumed."""
        collection = Collection(self.connection, collection_name)
        return collection.list_function_patches_generator(
            function_name,
            filter=filter,
            order_by=order_by,
            raise_for_status=raise_for_status,
        )

    def replay_logical_function_run(
        self,
        logical_function_run_id: str,
        raise_for_status: bool = True,
    ) -> dict:
        """
        Re-execute a finished function run — mint a new concrete physical
        attempt against the latest patch (or head, if no patch exists). The
        pointer stays bound to the previous FR until the new attempt
        finishes successfully.

        Returns:
            dict: { logical_function_run_id, previous_function_run_id,
                    new_function_run_id, function_version_id }
        """
        response = self.connection.logical_function_run_replay(
            logical_function_run_id=logical_function_run_id,
            raise_for_status=raise_for_status,
        )
        return response.json().get("data") or {}

    def enable_function(
        self,
        collection_name: str,
        function_name: str,
        enabled: bool,
        raise_for_status: bool = True,
    ) -> Function:
        """
        Enable/Disable a function in the server.

        Args:
            collection_name (str): The name of the collection.
            function_name (str): The name of the function.
            enabled (bool): Whether to enable or disable the function.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Raises:
            APIServerError: If the function could not be updated.
        """
        function = Function(self.connection, collection_name, function_name)
        return function.enable(enabled=enabled, raise_for_status=raise_for_status)

    def login(self, username: str, password: str):
        self.connection.authentication_login(
            username,
            password,
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

    def auth_info(self) -> dict:
        data = self.connection.authentication_info().json().get("data")
        all_roles = data.get("user_roles", [])
        roles_by_name = [role["name"] for role in all_roles]
        info = {
            "name": data.get("name"),
            "email": data.get("email"),
            "roles": roles_by_name,
        }
        return info

    def auth_service_token(self) -> dict:
        """
        Information about the service token making this request.

        Returns:
            dict: The presenting token's slot, label, owner and expiry.
        """
        return self.connection.authentication_service_token().json().get("data")

    def create_service_token(
        self,
        slot: int,
        expires_in_days: int,
        comment: str = None,
        raise_for_status: bool = True,
    ) -> dict:
        """
        Create a service token in one of the caller's two slots.

        The token itself is in the response and is not recoverable
        afterwards: the server stores only a SHA-256 digest of it.

        Args:
            slot (int): Which slot to fill, 1 or 2.
            expires_in_days (int): Days until it expires, or -1 for never
                (only where the deployment permits it).
            comment (str, optional): A label, so the two slots tell
                themselves apart in a listing.
            raise_for_status (bool, optional): Whether to raise on a non-2xx
                response.

        Returns:
            dict: The created token, including its plaintext.
        """
        response = self.connection.service_token_create(
            slot,
            expires_in_days,
            comment=comment,
            raise_for_status=raise_for_status,
        )
        return response.json().get("data")

    def list_service_tokens(self, raise_for_status: bool = True) -> List[dict]:
        """
        The caller's own service tokens.

        Args:
            raise_for_status (bool, optional): Whether to raise on a non-2xx
                response.

        Returns:
            List[dict]: One entry per occupied slot.
        """
        response = self.connection.service_token_list(raise_for_status=raise_for_status)
        return response.json().get("data", {}).get("data", [])

    def revoke_service_token(self, slot: int, raise_for_status: bool = True) -> None:
        """
        Revoke the service token in one of the caller's slots, freeing
        the slot for the next rotation.

        Args:
            slot (int): Which slot to free, 1 or 2.
            raise_for_status (bool, optional): Whether to raise on a non-2xx
                response.
        """
        self.connection.service_token_revoke(slot, raise_for_status=raise_for_status)

    def create_group(
        self, name: str, description: str = None, raise_for_status: bool = True
    ) -> Group:
        """
        Create a group in the server.

        Args:
            name (str): The name of the group.
            description (str, optional): The description of the group.
            raise_for_status (bool, optional): Whether to raise on a non-2xx response.

        Returns:
            Group: The created group.
        """
        group = Group(self.connection, name, description=description)
        group.create(raise_for_status=raise_for_status)
        return group

    def delete_group(self, name: str, raise_for_status: bool = True) -> None:
        """Delete a group by name."""
        Group(self.connection, name).delete(raise_for_status=raise_for_status)

    def get_group(self, name: str) -> Group:
        """Get a group by name."""
        group_definition = self.connection.group_get_by_name(name)
        data = group_definition.json().get("data")
        group = Group(self.connection, **data)
        group._data = data
        return group

    def list_groups(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Group]:
        """List groups in the server."""
        return list(self.list_groups_generator(filter, order_by, raise_for_status))

    def list_groups_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Group]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id is not None and next_step is not None):
            response = self.connection.group_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_groups = data.get("data") or []
            for raw_group in raw_groups:
                built_group = Group(self.connection, **raw_group)
                built_group._data = raw_group
                yield built_group
            first_page = False

    def update_group(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        raise_for_status: bool = True,
    ) -> Group:
        """Update a group's name and/or description."""
        return Group(self.connection, name).update(
            name=new_name,
            description=new_description,
            raise_for_status=raise_for_status,
        )

    def metrics(
        self,
        dim: str | None = None,
        unit: str | None = None,
        align: str | None = None,
        from_ms: int | None = None,
        span: int | None = None,
        collection: List[str] | None = None,
        collection_exclude: List[str] | None = None,
        group: List[str] | None = None,
        group_exclude: List[str] | None = None,
        status: List[str] | None = None,
        raise_for_status: bool = True,
    ) -> dict:
        """Consolidated ``/metrics`` endpoint.

        Returns the raw response data
        (``{dim, unit, start_ms, end_ms, nodes, edges}``). All parameters
        are optional; the server defaults to
        ``dim=collection, unit=day, span=1, align=calendar``.

        Args:
            dim: ``"collection"`` (default) or ``"group"``.
            unit: window unit and GROUP BY grain
                (``hour|day|week|month|quarter|year``).
            align: ``"calendar"`` (default) snaps the start of the
                window to the calendar-unit boundary containing
                ``from``; ``"rolling"`` produces a ``span*unit``-long
                window ending exactly at ``from``.
            from_ms: right edge of the window (UTC epoch ms, exclusive).
                Defaults to server-now. Sent on the wire as ``from``.
            span: window width in ``unit`` increments — also the number of
                rows returned (``unit=hour&span=24`` → 24 rows).
            collection: include filter on collection ids.
            collection_exclude: exclude filter on collection ids.
            group: include filter on group ids.
            group_exclude: exclude filter on group ids.
            status: narrow to specific run statuses.
        """
        params: dict = {}
        if dim is not None:
            params["dim"] = dim
        if unit is not None:
            params["unit"] = unit
        if align is not None:
            params["align"] = align
        if from_ms is not None:
            params["from"] = from_ms
        if span is not None:
            params["span"] = span
        if collection:
            params["collection"] = collection
        if collection_exclude:
            params["collection_exclude"] = collection_exclude
        if group:
            params["group"] = group
        if group_exclude:
            params["group_exclude"] = group_exclude
        if status:
            params["status"] = status
        response = self.connection.metrics_get(
            params=params, raise_for_status=raise_for_status
        )
        return response.json().get("data") or {}

    def inflight(
        self,
        collection: List[str] | None = None,
        collection_exclude: List[str] | None = None,
        group: List[str] | None = None,
        group_exclude: List[str] | None = None,
        status: List[str] | None = None,
        raise_for_status: bool = True,
    ) -> dict:
        """Point-in-time `/inflight` — returns the raw response
        ``{rows}`` with non-terminal function run counts per
        ``(collection, group, status)``.
        """
        params: dict = {}
        if collection:
            params["collection"] = collection
        if collection_exclude:
            params["collection_exclude"] = collection_exclude
        if group:
            params["group"] = group
        if group_exclude:
            params["group_exclude"] = group_exclude
        if status:
            params["status"] = status
        response = self.connection.inflight_get(
            params=params, raise_for_status=raise_for_status
        )
        return response.json().get("data") or {}

    def grant_permission(
        self,
        user: str,
        permission: str,
        project: str = None,
        raise_for_status: bool = True,
    ) -> None:
        """
        Grant a flat permission to a user (Simple Security Mode facade).

        Args:
            user (str): The user to grant the permission to.
            permission (str): The flat permission to grant. One of
                ``VALID_SIMPLE_PERMISSION_TYPES``.
            project (str, optional): The project the permission applies to.
                Required for project-scoped permissions, forbidden for
                system-scoped ones.
            raise_for_status (bool, optional): Whether to raise an exception if
                the request was not successful. Defaults to True.

        Raises:
            APIServerError: If the permission could not be granted.
        """
        self.connection.permission_grant(
            user, permission, project, raise_for_status=raise_for_status
        )

    def revoke_permission(
        self,
        user: str,
        permission: str,
        project: str = None,
        raise_for_status: bool = True,
    ) -> None:
        """
        Revoke a flat permission from a user (Simple Security Mode facade).

        Args:
            user (str): The user to revoke the permission from.
            permission (str): The flat permission to revoke. One of
                ``VALID_SIMPLE_PERMISSION_TYPES``.
            project (str, optional): The project the permission applies to.
                Required for project-scoped permissions, forbidden for
                system-scoped ones.
            raise_for_status (bool, optional): Whether to raise an exception if
                the request was not successful. Defaults to True.

        Raises:
            APIServerError: If the permission could not be revoked.
        """
        self.connection.permission_revoke(
            user, permission, project, raise_for_status=raise_for_status
        )

    def list_permissions_by_user(
        self, user: str, raise_for_status: bool = True
    ) -> List[SimplePermission]:
        """
        List a user's effective flat permissions (Simple Security Mode facade).

        Args:
            user (str): The user whose permissions are listed.
            raise_for_status (bool, optional): Whether to raise an exception if
                the request was not successful. Defaults to True.

        Returns:
            List[SimplePermission]: The user's effective flat permissions.
        """
        response = self.connection.permission_list_by_user(
            user, raise_for_status=raise_for_status
        )
        data = response.json().get("data") or {}
        rows = data.get("data") or []
        return [SimplePermission(**row) for row in rows]

    def list_permissions_by_project(
        self, project: str, raise_for_status: bool = True
    ) -> List[SimpleProjectAccess]:
        """
        List everyone with authority on a project (Simple Security Mode facade).

        Args:
            project (str): The project whose access list is returned.
            raise_for_status (bool, optional): Whether to raise an exception if
                the request was not successful. Defaults to True.

        Returns:
            List[SimpleProjectAccess]: Everyone with authority on the project,
                with the source that conferred each grant.
        """
        response = self.connection.permission_list_by_project(
            project, raise_for_status=raise_for_status
        )
        data = response.json().get("data") or {}
        rows = data.get("data") or []
        return [SimpleProjectAccess(**row) for row in rows]

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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_roles = data.get("data") or []
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

    def create_project(
        self, name: str, description: str, raise_for_status: bool = True
    ) -> Project:
        """
        Create a project in the server.

        Args:
            name (str): The name of the project.
            description (str): The description of the project.
            raise_for_status (bool, optional): Whether to raise on a non-2xx response.

        Returns:
            Project: The created project.
        """
        project = Project(self.connection, name, description=description)
        project.create(raise_for_status=raise_for_status)
        return project

    def delete_project(self, name: str, raise_for_status: bool = True) -> None:
        """Delete a project by name."""
        Project(self.connection, name).delete(raise_for_status=raise_for_status)

    def get_project(self, name: str, raise_for_status: bool = True) -> Project:
        """Get a project by name."""
        project_definition = self.connection.project_get_by_name(
            name, raise_for_status=raise_for_status
        )
        data = project_definition.json().get("data")
        project = Project(self.connection, **data)
        project._data = data
        return project

    def list_projects(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> List[Project]:
        """List projects in the server."""
        return list(self.list_projects_generator(filter, order_by, raise_for_status))

    def list_projects_generator(
        self,
        filter: List[str] | str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ) -> Generator[Project]:
        first_page = True
        next_pagination_id = None
        next_step = None
        while first_page or (next_pagination_id is not None and next_step is not None):
            response = self.connection.project_list(
                request_filter=filter,
                order_by=order_by,
                pagination_id=next_pagination_id,
                next_step=next_step,
                raise_for_status=raise_for_status,
            )
            data = response.json().get("data")
            next_pagination_id = data.get("next_pagination_id")
            next_step = data.get("next")
            raw_projects = data.get("data") or []
            for raw_project in raw_projects:
                built_project = Project(self.connection, **raw_project)
                built_project._data = raw_project
                yield built_project
            first_page = False

    def update_project(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        raise_for_status: bool = True,
    ) -> Project:
        """Update a project's name and/or description."""
        return Project(self.connection, name).update(
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
    ) -> Path | None:
        """
        Download a table for a given version as a parquet file. The version can
            be a fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
            destination_file (str): The path to the destination file.
            raise_for_status (bool, optional): Whether to raise an exception if the
                request was not successful. Defaults to True.

        Returns:
            Path | None: The path of the downloaded file, or None when the version
                holds no data, in which case no file is written.

        Raises:
            APIServerError: If the table could not be downloaded.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.download(
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
        extra_cols: str = None,
    ) -> List[dict]:
        """
        Get the schema of a table for a given version. The version can be a
            fixed version or a relative one (HEAD, HEAD^, and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
            extra_cols (str, optional): Which non-user columns to include on top
                of the user ones: 'none' (the server default), 'meta' (the
                '@td.' connector metadata), 'sys' (the '$td.' system columns)
                or 'all'.

        Returns:
            list: The schema of the table.

        Raises:
            APIServerError: If the schema could not be obtained.
        """
        table = Table(self.connection, collection_name, table_name)
        return table.get_schema(
            at=at, at_trx=at_trx, version=version, extra_cols=extra_cols
        )

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

    def list_tables_generator(
        self,
        collection_name: str,
        at: int | str = None,
        at_trx: Transaction | str | None = None,
        filter: List[str] | str = None,
        order_by: str = None,
    ) -> Generator[Table]:
        """
        Yield the tables in a collection, fetching pages as they are consumed.

        Args:
            collection_name (str): The name of the collection.

        Returns:
            Generator[Table]: The tables in the collection.
        """
        collection = Collection(self.connection, collection_name)
        return collection.list_tables_generator(
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
        sql: str = None,
        extra_cols: str = None,
    ) -> pl.DataFrame:
        """
        Get a sample of a table for a given version as a parquet file. The
            version can be a fixed version or a relative one (HEAD, HEAD^,
            and HEAD~## syntax).

        Args:
            collection_name (str): The name of the collection.
            table_name (str): The name of the table.
            offset (int, optional): The offset of the sample.
            len (int, optional): The length of the sample. Defaults to 100
                server-side, and cannot be greater than 1000.
            sql (str, optional): A SQL query run server-side on the table, which
                is the only queryable relation and is referenced by its own
                name. The offset is applied before the query and the length caps
                its result.
            extra_cols (str, optional): Which non-user columns to return on top
                of the user ones: 'none' (the server default), 'meta' (the
                '@td.' connector metadata), 'sys' (the '$td.' system columns)
                or 'all'.
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
            sql=sql,
            extra_cols=extra_cols,
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
            with self._spanning():
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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_transactions = data.get("data") or []
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
        data = user_definition.json().get("data")
        user = User(self.connection, **data)
        user._data = data
        return user

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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_users = data.get("data") or []
            for raw_user in raw_users:
                built_user = User(self.connection, **raw_user)
                built_user._data = raw_user
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

    def get_worker(self, worker_id: str) -> Worker:
        """
        Get a worker in the server.

        Args:
            worker_id (str): The ID of the worker.

        Returns:
            Worker: The worker.
        """
        try:
            with self._spanning():
                return self.list_workers(filter=f"id:eq:{worker_id}")[0]
        except IndexError:
            raise ValueError(f"Worker with ID {worker_id} not found.")

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
        while first_page or (next_pagination_id is not None and next_step is not None):
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
            raw_workers = data.get("data") or []
            for raw_worker in raw_workers:
                built_worker = Worker(self.connection, **raw_worker)
                yield built_worker
            first_page = False


@dataclass
class _FunctionArchive:
    """Result of bundling a :class:`FunctionInfo` for registration.

    Mirrors the legacy ``TabsdataFunctionArchive`` shape so the
    ``function_register`` / ``function_patch`` apiserver calls keep
    their existing payload schema unchanged. The v2 difference: every
    field is derived from the :class:`FunctionInfo` rather than from
    legacy ``TabsdataFunction`` attributes, and the bundle itself is
    produced by :func:`tabsdatak.common._archive.bundle.build_bundle`.
    """

    tables: list[str]
    table_types: dict[str, str]
    parent_tables: dict[str, str]
    string_dependencies: list[str]
    trigger_string_list: list[str] | None
    function_snippet: str
    context_location: str
    function_name: str
    function_type: str
    connector: str | None
    connection: str | None
    workers: dict[str, str] | None
    trend_transformers_info: list[TrendTransformerInfo] | None = None


# Maps each ``FunctionInfo.__kind__`` to the API discriminator the
# server's ``function_register`` endpoint expects (P/S/T). Stream
# publishers register as publishers from the server's perspective
# (the runtime decides how to drive them based on the bundle's
# ``function_def.json``).
_KIND_TO_API_TYPE: dict[str, str] = {
    KIND_PUBLISHER_DEF: "P",
    KIND_STREAM_PUBLISHER_DEF: "P",
    KIND_TRANSFORMER_DEF: "T",
    KIND_SUBSCRIBER_DEF: "S",
}


def _workers(info: FunctionInfo) -> dict[str, str] | None:
    """Return the workers payload the server's ``function_register``
    endpoint stores on the function (later materialized into a row in
    ``workers`` by ``build_workers`` in
    ``td-services/.../function/layers/register.rs``):

    * ``CronTrigger`` → ``{"CT": json.dumps({"mask": ...})}``. Matches
      the ``CronTriggerConfig`` Rust struct in
      ``td-objects/.../dxo/worker.rs``; ``start``/``end`` are
      ``Option<DateTime>`` and omitted.
    * ``@stream_publisher`` (any) → ``{"ST": json.dumps({})}``. The
      ``StageTriggerConfig`` struct's fields all carry serde defaults
      (``TimeoutThresholdMin``/``SizeThreshold`` are typed-int wrappers
      with built-in defaults; ``start``/``end`` are ``Option``); an
      empty JSON object is enough to enqueue the long-running
      stream-publisher worker. The function-level batch settings
      live in the bundle's ``function_def.json`` and are consumed by
      ``run_streamer`` in the pod, not via this worker config.
    * Anything else → ``None`` (no orchestrator-managed worker).

    Stream-publisher MUST emit ``"ST"``; otherwise the apiserver never
    enqueues a ``WorkerType::StageTrigger`` row, the orchestrator's
    ``StageTriggersToSchedule`` filter finds nothing, and the
    long-running stream-publisher pod never gets scheduled (the
    per-trigger function-publisher pod still runs, but with an empty
    stage and the user function gets no rows).
    """
    trigger_by = getattr(info, "trigger_by", None)
    if isinstance(trigger_by, CronTrigger):
        return {"CT": json.dumps({"mask": trigger_by.mask})}
    if getattr(info, "__kind__", None) == KIND_STREAM_PUBLISHER_DEF:
        return {"ST": json.dumps({})}
    return None


# Kept under the old name so any out-of-tree caller (and the
# follow-on test file) keeps importing without a breaking rename.
_cron_workers = _workers


def _trigger_string_list(info: FunctionInfo) -> list[str] | None:
    """Extract the ``trigger_string_list`` field the legacy
    ``function_register`` payload expects.

    For :class:`TablesTrigger` we forward the explicit table list;
    for :class:`CronTrigger` (worker-style) and :class:`AllTablesTrigger`
    the list is ``None`` -- the server resolves the trigger semantics
    from the workers map (for cron) or the function's own input-table
    list (for all-tables)."""
    trigger_by = getattr(info, "trigger_by", None)
    if isinstance(trigger_by, TablesTrigger):
        return list(trigger_by.tables)
    return None


def _connector(info: FunctionInfo) -> str | None:
    """The function's source/destination class FQN -- the ``Src`` /
    ``StreamSrc`` / ``Dest`` the function binds to -- as a
    ``"module:qualname"`` string (e.g. ``"tabsdatak.conn.mysql:MySQLSrc"``).

    Discriminates on ``info.__kind__`` rather than ``isinstance`` so
    duck-typed Info objects (used in tests) work the same way. ``None`` for
    transformers (no connector).
    """
    kind = getattr(info, "__kind__", None)
    if kind in (KIND_PUBLISHER_DEF, KIND_STREAM_PUBLISHER_DEF):
        source = getattr(info, "source", None)
        return qualified_class(type(source)) if source is not None else None
    if kind == KIND_SUBSCRIBER_DEF:
        destination = getattr(info, "destination", None)
        return qualified_class(type(destination)) if destination is not None else None
    return None


def _connection(info: FunctionInfo) -> str | None:
    """The function's connection class FQN -- the concrete :class:`Conn`
    subclass its source/destination binds to -- as a ``"module:qualname"``
    string (e.g. ``"tabsdatak.conn.mysql:MySQLSrcConn"``).

    Resolved from the connector registry by the source/destination type
    (``def.conn``), *not* from a ``conn`` declared on the decorator. The
    server matches it against the collection's connection on register/update.
    ``None`` for transformers (no connection)."""
    kind = getattr(info, "__kind__", None)
    if kind == KIND_PUBLISHER_DEF:
        source = getattr(info, "source", None)
        conn_cls = (
            _connectors.get_src(type(source)).conn if source is not None else None
        )
    elif kind == KIND_STREAM_PUBLISHER_DEF:
        source = getattr(info, "source", None)
        conn_cls = (
            _connectors.get_stream_src(type(source)).conn
            if source is not None
            else None
        )
    elif kind == KIND_SUBSCRIBER_DEF:
        destination = getattr(info, "destination", None)
        conn_cls = (
            _connectors.get_dest(type(destination)).conn
            if destination is not None
            else None
        )
    else:
        conn_cls = None
    return qualified_class(conn_cls) if conn_cls is not None else None


def _input_tables(info: FunctionInfo) -> list[str]:
    """For transformers / subscribers, the cross-collection
    ``"collection/table"`` references. For publishers, an empty list."""
    return list(getattr(info, "input_tables", []) or [])


def _output_tables(info: FunctionInfo) -> list[str]:
    """For publishers / transformers, the declared output table
    names. For subscribers (no outputs), an empty list."""
    return list(getattr(info, "output_tables", []) or [])


def _dq_derived(
    collection: str, info: FunctionInfo
) -> tuple[list[str], dict[str, str], dict[str, str], list[TrendTransformerInfo]]:
    """Derive everything a function's data-quality actions imply.

    Returns the derived table names, their catalog type codes (so
    registration records each derived table under its ``TableType``), the
    output table each derived table derives from -- summaries and the trend
    tables alike, so registration parents them -- and the system transformers
    that maintain the summary trend tables. All four are empty when the
    function declares no data-quality action.

    One :class:`DataQualityProcessor` serves all four, so the action set is
    resolved and validated exactly once per registration.
    """
    from tabsdatak.common._actions.dataquality.processor import (
        DataQualityProcessor,
    )
    from tabsdatak.dataquality import DataQuality
    from tabsdatak.dataquality._tableinfo import TableKind

    on_tables = getattr(info, "on_tables", None) or []
    actions = [action for action in on_tables if isinstance(action, DataQuality)]
    if not actions:
        return [], {}, {}, []

    processor = DataQualityProcessor.from_actions(
        collection=collection,
        output_tables=_output_tables(info),
        actions=actions,
    )
    _, output_table_infos = processor.table_names()
    names = [table_info.table for table_info in output_table_infos]
    types = {
        table_info.table: table_info.kind.type.value
        for table_info in output_table_infos
    }
    trends = processor.build_trend_transformers_info()
    parents = {
        table_info.table: table_info.from_table
        for table_info in output_table_infos
        if table_info.kind is TableKind.DQ_SUMMARY
    } | {
        table: trend.parent_table
        for trend in trends
        for table in trend.info.output_tables
    }
    return names, types, parents, trends


def _cleanup_trend_roots(trend_transformers) -> None:
    """Discard the generated-module directories built for trend transformers.

    ``build_bundle`` copies each generated module into its bundle's
    ``user_code/`` during registration, so once registration has been attempted
    the directories are dead weight. Called from a ``finally`` so a failed
    registration leaves nothing behind either.
    """
    from tabsdatak.common._actions.dataquality.trend import cleanup_trend_tfr_mod

    for trend in trend_transformers or []:
        cleanup_trend_tfr_mod(trend.project_root)


def _create_archive(
    collection: str,
    info: FunctionInfo,
    project_root: str | Path,
    temporary_directory: tempfile.TemporaryDirectory | str,
    local_packages: list[str] | None = None,
    valid_python_versions: list[str] | None = None,
    requirements: str | Path | None = None,
) -> _FunctionArchive:
    """Bundle ``info`` (a :class:`FunctionInfo`) for registration.

    Calls :func:`tabsdatak.common._archive.bundle.build_bundle` to produce the
    ``.tar.zst`` and assembles the metadata fields the legacy
    ``function_register`` API still expects from the SDK side.

    Data-quality actions declared in ``info.on_tables`` contribute their
    derived tables to ``tables`` and their catalog kinds to ``table_types``
    (see :func:`_dq_tables`), so a data-quality function's derived outputs are
    registered under the right :class:`TableType`.

    .. note::

       One piece of legacy ``_create_archive`` behaviour is currently NOT
       carried over from the v1 SDK and remains as deferred work:

       * **StageTrigger workers payload.** The legacy code inspected
         ``function.trigger_by`` for a :class:`StageTrigger`
         instance and emitted ``workers={"ST": <json>}`` so the
         server would route the function via the stage-trigger
         apparatus. ``StageTrigger`` lives in the legacy
         ``tabsdata._io.stage_trigger`` module, which is in an
         excluded subtree, and there is no v2 tabsdatak equivalent
         yet. Functions whose v2 ``Info`` carries a stage trigger
         will register with ``workers=None`` until the v2 stage
         trigger story is designed. TODO(tabsdatak-migration).
    """
    save_location = (
        temporary_directory.name
        if isinstance(temporary_directory, tempfile.TemporaryDirectory)
        else temporary_directory
    )
    bundle_path = Path(save_location) / "bundle.tar.zst"

    build_bundle(
        info,
        Path(project_root),
        bundle_path,
        local_packages=[Path(p) for p in local_packages] if local_packages else None,
        valid_python_versions=valid_python_versions,
        requirements_yaml=Path(requirements) if requirements else None,
    )

    function_name = getattr(info.func, "__name__", "<anonymous>")
    function_type = _KIND_TO_API_TYPE.get(info.__kind__, "U")
    function_snippet = capture_source_snippet(info)

    dq_names, dq_types, dq_parents, trend_transformers = _dq_derived(collection, info)

    return _FunctionArchive(
        tables=_output_tables(info) + dq_names,
        table_types=dq_types,
        parent_tables=dq_parents,
        string_dependencies=_input_tables(info),
        trigger_string_list=_trigger_string_list(info),
        function_snippet=function_snippet,
        context_location=str(bundle_path),
        function_name=function_name,
        function_type=function_type,
        connector=_connector(info),
        connection=_connection(info),
        # ``_workers`` emits ``CT`` for ``CronTrigger`` and ``ST`` for
        # ``@stream_publisher``; ``None`` otherwise. The ``ST`` branch
        # is what makes the apiserver enqueue the long-running
        # stream-publisher worker on register (TD-2569). Without it
        # the orchestrator's ``StageTriggersToSchedule`` filter found
        # nothing and the stream-publisher pod never spawned.
        workers=_workers(info),
        trend_transformers_info=trend_transformers,
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
                datetime
                .strptime(incomplete_datetime, fmt)
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


# NOTE: the legacy ``_split_python_function_path`` and
# ``_dynamic_import_function_from_path`` helpers (which re-imported
# the user's decorated function from a "path/to/file.py::name" string
# to assemble the bundle) are gone in v2. Callers pass a ready-made
# ``FunctionInfo`` (PublisherInfo / TransformerInfo / SubscriberInfo)
# directly; the bundle is produced by ``tabsdatak.common._archive.bundle.build_bundle``
# and the runner reads ``function_def.json`` from the bundle root.
