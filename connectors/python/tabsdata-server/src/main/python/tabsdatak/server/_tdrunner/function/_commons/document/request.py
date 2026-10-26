#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

from pydantic import model_validator

from tabsdatak.common._configuration.document.error import DocumentException
from tabsdatak.common._configuration.document.loader import DocumentModel, load
from tabsdatak.common._configuration.model.storage import StorageConfiguration
from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    MountDeclaration,
)
from tabsdatak.error import ErrorCode

RAW_SUFFIX = ".t"
MILLISECONDS_PER_SECOND = 1000

TABLE = "Table"
TABLE_VERSIONS = "TableVersions"
KNOWN_VARIANTS = (TABLE, TABLE_VERSIONS, "PartitionedTable", "PartitionedTableVersions")
SUPPORTED_VARIANTS = {
    "system_input": (TABLE,),
    "input": (TABLE, TABLE_VERSIONS),
    "system_output": (TABLE,),
    "output": (TABLE,),
}
VARIANT_SEPARATOR = ", "
SINGLE_ENVELOPE = "The request must hold exactly one of Function or Streamer"

Envelope = TypeVar("Envelope", bound=DocumentModel)


def variant(entry: object, supported: tuple[str, ...], where: str) -> object:
    if not isinstance(entry, dict) or len(entry) != 1:
        raise ValueError(f"Entry {where} must hold exactly one variant")
    [(tag, payload)] = entry.items()
    if tag in supported:
        return payload
    if tag in KNOWN_VARIANTS:
        raise ValueError(
            f"Variant {tag} at {where} is recognized but not yet supported",
        )
    raise ValueError(
        f"Variant {tag} at {where} is unknown; expected one of "
        f"{VARIANT_SEPARATOR.join(KNOWN_VARIANTS)}",
    )


def read(
    path: Path,
    envelope: type[Envelope],
    error: ErrorCode,
    strict: bool,
) -> Envelope:
    try:
        return load(path, envelope, strict=strict, resolve_secrets=False)
    except DocumentException as exception:
        raise error.exception(cause=exception, path=path, detail=str(exception))


class Location(DocumentModel):
    uri: str
    env_prefix: str | None = None

    def mount_declaration(self, storage_config: Path) -> MountDeclaration:
        if self.env_prefix is None:
            declaration = AnonymousMountDeclaration()
        else:
            configuration = load(storage_config, StorageConfiguration)
            declaration = configuration.mount(self.env_prefix).declaration()
        return declaration


class InputTable(DocumentModel):
    collection_id: str | int
    collection: str
    table_id: str | int
    table: str
    table_version_id: str | int
    execution_id: str | int | None = None
    transaction_id: str | int | None = None
    physical_function_run_id: str | int | None = None
    triggered_on: int | None = None
    logical_table_data_version_id: str | int | None = None
    physical_table_data_version_id: str | int | None = None
    location: Location | None = None
    input_idx: int
    table_pos: int
    version_pos: int

    @property
    def tables(self) -> tuple[InputTable, ...]:
        return (self,)


class InputTableVersions(DocumentModel):
    is_empty: bool
    versions: tuple[InputTable, ...]

    @property
    def tables(self) -> tuple[InputTable, ...]:
        return self.versions


class OutputTable(DocumentModel):
    collection_id: str | int
    collection: str
    table_id: str | int
    table: str
    table_version_id: str | int
    logical_table_data_version_id: str | int
    physical_table_data_version_id: str | int
    location: Location
    table_pos: int


class Info(DocumentModel):
    project_id: str | int
    project: str
    collection_id: str | int
    collection: str
    function_id: str | int
    function: str
    function_version_id: str | int
    function_defined_on: int
    function_bundle: Location
    function_stage: Location
    execution_id: str | int
    execution: str | None = None
    transaction_id: str | int
    logical_function_run_id: str | int
    physical_function_run_id: str | int
    triggered_on: int
    function_data: Location
    function_raw: Location | None = None
    scheduled_on: int

    @property
    def triggered_on_datetime(self) -> datetime:
        return datetime.fromtimestamp(
            self.triggered_on / MILLISECONDS_PER_SECOND,
            tz=timezone.utc,
        )


class Request(DocumentModel):
    info: Info
    system_input: tuple[InputTable, ...] = ()
    input: tuple[InputTable | InputTableVersions, ...] = ()
    system_output: tuple[OutputTable, ...] = ()
    output: tuple[OutputTable, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def unwrap(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        unwrapped = dict(data)
        for slot, supported in SUPPORTED_VARIANTS.items():
            entries = unwrapped.get(slot)
            if entries is None:
                continue
            if not isinstance(entries, list | tuple):
                raise ValueError(f"Slot {slot} must hold a list of tables")
            unwrapped[slot] = [
                entry
                if isinstance(entry, DocumentModel)
                else variant(entry, supported, f"{slot}[{index}]")
                for index, entry in enumerate(entries)
            ]
        return unwrapped

    @classmethod
    def load(cls, path: Path, error: ErrorCode) -> Request:
        return read(path, FunctionEnvelope, error, strict=True).Function


class FunctionEnvelope(DocumentModel):
    Function: Request


class StreamerInfo(DocumentModel):
    project_id: str | int
    project: str
    collection_id: str | int
    collection: str
    function_id: str | int
    function: str
    function_version_id: str | int
    function_defined_on: int
    function_bundle: Location
    function_stage: Location
    worker_id: str | int
    scheduled_on: int


class StreamerRequest(DocumentModel):
    info: StreamerInfo

    @classmethod
    def load(cls, path: Path, error: ErrorCode) -> StreamerRequest:
        return read(path, StreamerEnvelope, error, strict=True).Streamer


class StreamerEnvelope(DocumentModel):
    Streamer: StreamerRequest


class AnyInfo(DocumentModel):
    function_bundle: Location
    function_stage: Location


class AnyRequest(DocumentModel):
    info: AnyInfo

    @classmethod
    def load(cls, path: Path, error: ErrorCode) -> AnyRequest:
        return read(path, AnyEnvelope, error, strict=False).request


class AnyEnvelope(DocumentModel):
    Function: AnyRequest | None = None
    Streamer: AnyRequest | None = None

    @model_validator(mode="after")
    def single(self) -> AnyEnvelope:
        if (self.Function is None) == (self.Streamer is None):
            raise ValueError(SINGLE_ENVELOPE)
        return self

    @property
    def request(self) -> AnyRequest:
        return self.Function or self.Streamer
