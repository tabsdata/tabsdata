#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Self

from pydantic import ConfigDict, TypeAdapter, ValidationError

from tabsdatak.api import BasicTypeSpec, ContextKeySpec, FlowCtx, TransactionCtx
from tabsdatak.server._tdrunner.function._commons.document.message.request import Info

KEY_ADAPTER = TypeAdapter(ContextKeySpec, config=ConfigDict(strict=True))
VALUE_ADAPTER = TypeAdapter(BasicTypeSpec, config=ConfigDict(strict=True))


class ContextValueError(ValueError):
    pass


class FromFlowRequestCtx(FlowCtx):
    _work_folder: Path
    _project_id: str
    _project: str
    _collection_id: str
    _collection: str
    _function_id: str
    _function: str
    _function_version_id: str
    _attributes: dict[str, BasicTypeSpec]

    def __init__(
        self,
        work_folder: Path,
        project_id: str,
        project: str,
        collection_id: str,
        collection: str,
        function_id: str,
        function: str,
        function_version_id: str,
    ):
        self._work_folder = work_folder
        self._project_id = project_id
        self._project = project
        self._collection_id = collection_id
        self._collection = collection
        self._function_id = function_id
        self._function = function
        self._function_version_id = function_version_id
        self._attributes = {}

    @staticmethod
    def validate_key(key: Any) -> None:
        try:
            KEY_ADAPTER.validate_python(key)
        except ValidationError as error:
            raise ContextValueError(
                f"Key {key!r} is not a valid ContextKeySpec (must be a str)"
            ) from error

    @staticmethod
    def validate_value(key: str, value: Any) -> None:
        try:
            VALUE_ADAPTER.validate_python(value)
        except ValidationError as error:
            raise ContextValueError(
                f"Value for key {key!r} is not a valid BasicTypeSpec "
                f"(str/int/float/bool; got type {type(value).__name__!r})"
            ) from error

    @property
    def work_dir(self) -> Path:
        return self._work_folder

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def project(self) -> str:
        return self._project

    @property
    def collection_id(self) -> str:
        return self._collection_id

    @property
    def collection(self) -> str:
        return self._collection

    @property
    def function_id(self) -> str:
        return self._function_id

    @property
    def function(self) -> str:
        return self._function

    @property
    def function_version_id(self) -> str:
        return self._function_version_id

    def get_attr(self, attr: ContextKeySpec) -> BasicTypeSpec | None:
        return self._attributes.get(attr)

    # Values are checked when the state is written, so the None the SPI allows
    # still round-trips through the attribute store; keys fail here, at the
    # user's call site.
    def set_attr(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        self.validate_key(attr)
        self._attributes[attr] = val

    def add_metadata(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        pass

    def hydrate(self, initial: dict[str, BasicTypeSpec]) -> None:
        self._attributes.update(initial)

    def drain(self) -> dict[str, BasicTypeSpec]:
        return copy.deepcopy(self._attributes)


class FromRequestCtx(TransactionCtx, FromFlowRequestCtx):
    _execution_id: str
    _execution: str | None
    _transaction_id: str
    _logical_function_run_id: str
    _physical_function_run_id: str
    _triggered_on: datetime

    def __init__(
        self,
        work_folder: Path,
        project_id: str,
        project: str,
        collection_id: str,
        collection: str,
        function_id: str,
        function: str,
        function_version_id: str,
        execution_id: str,
        execution: str | None,
        transaction_id: str,
        logical_function_run_id: str,
        physical_function_run_id: str,
        triggered_on: datetime,
    ):
        super().__init__(
            work_folder=work_folder,
            project_id=project_id,
            project=project,
            collection_id=collection_id,
            collection=collection,
            function_id=function_id,
            function=function,
            function_version_id=function_version_id,
        )
        self._execution_id = execution_id
        self._execution = execution
        self._transaction_id = transaction_id
        self._logical_function_run_id = logical_function_run_id
        self._physical_function_run_id = physical_function_run_id
        self._triggered_on = triggered_on

    @classmethod
    def from_info(cls, work_folder: Path, info: Info) -> Self:
        return cls(
            work_folder=work_folder,
            project_id=str(info.project_id),
            project=info.project,
            collection_id=str(info.collection_id),
            collection=info.collection,
            function_id=str(info.function_id),
            function=info.function,
            function_version_id=str(info.function_version_id),
            execution_id=str(info.execution_id),
            execution=info.execution,
            transaction_id=str(info.transaction_id),
            logical_function_run_id=str(info.logical_function_run_id),
            physical_function_run_id=str(info.physical_function_run_id),
            triggered_on=info.triggered_on_datetime,
        )

    @property
    def execution_id(self) -> str:
        return self._execution_id

    @property
    def execution(self) -> str | None:
        return self._execution

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    @property
    def logical_function_run_id(self) -> str:
        return self._logical_function_run_id

    @property
    def physical_function_run_id(self) -> str:
        return self._physical_function_run_id

    @property
    def triggered_on(self) -> datetime:
        return self._triggered_on

    # TransactionCtx comes first in the MRO, so its abstract get_attr, set_attr
    # and add_metadata would shadow the concrete ones; these aliases credit them.
    get_attr = FromFlowRequestCtx.get_attr
    set_attr = FromFlowRequestCtx.set_attr
    add_metadata = FromFlowRequestCtx.add_metadata
