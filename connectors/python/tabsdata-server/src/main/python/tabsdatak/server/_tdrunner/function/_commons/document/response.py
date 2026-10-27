#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow.parquet as pq
import yaml
from pydantic import model_validator

from tabsdatak.common._configuration.document.loader import DocumentModel, load
from tabsdatak.error import ErrorCode
from tabsdatak.server._tdrunner.function._commons.document.request import Request
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
)

SCHEMA_HASH_PREFIX = "sha256:"
PARQUET_SUFFIX = ".parquet"

SINGLE_WRITTEN_TABLE = "A written table must be exactly one of NoData or Data"

JSON_MODE = "json"
PYTHON_MODE = "python"


class TableInfo(DocumentModel):
    column_count: int
    row_count: int
    schema_hash: str
    data_bytes: int | None = None

    @classmethod
    def from_parquet(cls, path: Path) -> TableInfo:
        schema = pq.read_schema(path)
        schema_bytes = schema.serialize().to_pybytes()
        return cls(
            column_count=len(schema),
            row_count=int(pq.read_metadata(path).num_rows),
            schema_hash=SCHEMA_HASH_PREFIX + hashlib.sha256(schema_bytes).hexdigest(),
            data_bytes=path.stat().st_size,
        )


class TableEntry(DocumentModel):
    table: str
    info: TableInfo | None = None

    @classmethod
    def output(cls, entry: OutputEntry) -> TableEntry:
        if entry.file is None:
            return cls(table=entry.name)
        return cls(table=entry.name, info=TableInfo.from_parquet(Path(entry.file)))

    @classmethod
    def state(cls, name: str, state_dir: Path) -> TableEntry:
        parquet = state_dir / f"{name}{PARQUET_SUFFIX}"
        if not parquet.exists():
            return cls(table=name)
        return cls(table=name, info=TableInfo.from_parquet(parquet))


class Response(DocumentModel):
    output: tuple[TableEntry, ...]

    @classmethod
    def build(
        cls,
        request: Request,
        manifest: OutputManifest,
        state_dir: Path,
        error: ErrorCode,
    ) -> Response:
        produced = {entry.name: entry for entry in manifest.outputs}
        entries = []
        for table in request.output:
            if table.table not in produced:
                raise error.exception(table=table.table)
            entries.append(TableEntry.output(produced[table.table]))
        for table in request.system_output:
            entries.append(TableEntry.state(table.table, state_dir))
        return cls(output=tuple(entries))

    @classmethod
    def load(cls, path: Path) -> Response:
        return load(path, cls, strict=True, resolve_secrets=False)

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            yaml.safe_dump(
                self.model_dump(mode=PYTHON_MODE, exclude_none=True),
                sort_keys=False,
            ),
            encoding="utf-8",
        )

    def function_output(self) -> FunctionOutput:
        return FunctionOutput.of(self.output)


class WrittenNoData(DocumentModel):
    table: str


class WrittenData(DocumentModel):
    table: str
    info: TableInfo


class WrittenTable(DocumentModel):
    NoData: WrittenNoData | None = None
    Data: WrittenData | None = None

    @model_validator(mode="after")
    def single(self) -> WrittenTable:
        if (self.NoData is None) == (self.Data is None):
            raise ValueError(SINGLE_WRITTEN_TABLE)
        return self

    @classmethod
    def of(cls, entry: TableEntry) -> WrittenTable:
        if entry.info is None:
            return cls(NoData=WrittenNoData(table=entry.table))
        return cls(Data=WrittenData(table=entry.table, info=entry.info))


class FunctionOutput(DocumentModel):
    output: tuple[WrittenTable, ...]

    @classmethod
    def of(cls, entries: tuple[TableEntry, ...]) -> FunctionOutput:
        return cls(output=tuple(WrittenTable.of(entry) for entry in entries))

    def document(self) -> dict:
        return self.model_dump(mode=JSON_MODE, exclude_none=True)
