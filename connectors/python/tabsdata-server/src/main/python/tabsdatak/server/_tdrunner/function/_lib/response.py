#
# Copyright 2026 Tabs Data Inc.
#

"""Publisher `response.yaml` writer and reader.

Built by the `report-completion` command at the end of a publisher run.
The shape mirrors the legacy tabsdata response semantically — modified
output tables carry table metadata; not-modified tables carry just the
name — but uses plain YAML (no custom tags) so it can be parsed with
stock `yaml.safe_load` on the API-server side.

Schema:

    output:
      - table: customers
        info:
          column_count: 5
          row_count: 1000
          schema_hash: abc123
          data_bytes: 4096   # optional; bytes the parquet occupies on disk
      - table: orders   # no `info` → table was not modified
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow.parquet as pq
import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tabsdatak.error import ErrorCode
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
)
from tabsdatak.server._tdrunner.function._lib.request import Request


class InvalidResponseError(ValueError):
    """The response.yaml is malformed or missing required fields."""


class TableInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    column_count: int
    row_count: int
    schema_hash: str
    # Bytes the produced parquet file occupies on disk. Optional so
    # callers that can't / don't want to populate it can omit it; the
    # apiserver aggregates per-table values via sum() and treats a
    # missing field as "no contribution".
    data_bytes: int | None = None

    @classmethod
    def from_parquet(cls, path: Path) -> "TableInfo":
        """Read column/row counts, schema hash, and on-disk size from a parquet."""
        metadata = pq.read_metadata(path)
        schema = pq.read_schema(path)
        schema_bytes = schema.serialize().to_pybytes()
        return cls(
            column_count=len(schema),
            row_count=int(metadata.num_rows),
            schema_hash="sha256:" + hashlib.sha256(schema_bytes).hexdigest(),
            data_bytes=path.stat().st_size,
        )


class TableEntry(BaseModel):
    """An output-table entry in the response.

    `info` is set for tables the run modified, and `None` for tables
    that were declared as outputs in the request but not modified.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    table: str
    info: TableInfo | None = None

    @classmethod
    def from_manifest(cls, name: str, file: str | None) -> "TableEntry":
        """Entry for a declared output, resolved against the outputs manifest.

        A null `file` is the "user returned None" sentinel and yields a
        not-modified entry (`info=None`).
        """
        if file is None:
            return cls(table=name, info=None)
        return cls(table=name, info=TableInfo.from_parquet(Path(file)))

    @classmethod
    def from_state_dir(cls, name: str, state_dir: Path) -> "TableEntry":
        """Entry for a system-output table, resolved against `state_dir`.

        A missing `<state_dir>/<name>.parquet` yields a not-modified
        entry (`info=None`).
        """
        parquet = state_dir / f"{name}.parquet"
        if not parquet.exists():
            return cls(table=name, info=None)
        return cls(table=name, info=TableInfo.from_parquet(parquet))


class Response(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    output: tuple[TableEntry, ...]

    @classmethod
    def from_fn_response(
        cls,
        request: Request,
        manifest_entries: list[dict],
        state_dir: Path,
        *,
        mismatch_error_code: ErrorCode,
    ) -> "Response":
        """Build the response from a function run's outputs.

        `request.output` tables are resolved against the outputs manifest
        (authoritative for absolute file paths and for the "user returned
        None" sentinel via `file: null`). `request.system_output` tables
        are resolved against `state_dir/<name>.parquet`.

        Raises:
            The exception bound to `mismatch_error_code` when a declared
            `request.output` table has no entry in the manifest.
        """
        files_by_name: dict[str, str | None] = {
            entry[MANIFEST_NAME_KEY]: entry.get(MANIFEST_FILE_KEY)
            for entry in manifest_entries
        }
        entries: list[TableEntry] = []
        for table in request.output:
            if table.name not in files_by_name:
                raise mismatch_error_code.exception(table=table.name)
            entries.append(
                TableEntry.from_manifest(table.name, files_by_name[table.name])
            )
        for table in request.system_output:
            entries.append(TableEntry.from_state_dir(table.name, state_dir))
        return cls(output=tuple(entries))

    def to_worker_output(self) -> dict:
        """Render as the apiserver callback ``WorkerOutput`` context.

        Mirrors the Rust ``WorkerOutput::Function`` wrapping a
        ``FunctionOutput`` of externally-tagged ``WrittenTable`` entries:
        a modified table (``info`` present) becomes ``Data``, a
        not-modified table becomes ``NoData``. The flat on-disk
        ``response.yaml`` shape would not deserialize into this enum, so
        the tag must be added explicitly.
        """
        written: list[dict] = []
        for entry in self.output:
            if entry.info is None:
                written.append({"NoData": {"table": entry.table}})
            else:
                written.append({
                    "Data": {
                        "table": entry.table,
                        "info": entry.info.model_dump(mode="json", exclude_none=True),
                    }
                })
        return {"Function": {"output": written}}


def write_response(resp: Response, path: Path) -> None:
    """Serialise the response to plain YAML at `path`.

    Parent directories of `path` are created if missing — under the
    publisher init-container chain the `report-completion` main
    container writes to a subpath of the `shared/` emptyDir which
    starts out empty (the image-template-provided `shared/response/`
    subdir is overlaid by the emptyDir mount).

    Raises:
        OSError: if `path` is not writable (permission denied, target
            is a directory, etc.).
    """
    data = resp.model_dump(mode="python", exclude_none=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False)


def load_response(path: Path) -> Response:
    """Parse a response.yaml, primarily used for roundtrip testing.

    Raises:
        FileNotFoundError: if `path` does not exist.
        InvalidResponseError: if the YAML is malformed or the structure
            fails schema validation (missing required fields, wrong
            types, unknown fields).
    """
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidResponseError(f"malformed YAML in {path}: {exc}") from exc
    try:
        return Response.model_validate(raw)
    except ValidationError as exc:
        raise InvalidResponseError(
            f"invalid response structure in {path}: {exc}"
        ) from exc
