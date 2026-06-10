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

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError


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


class TableEntry(BaseModel):
    """An output-table entry in the response.

    `info` is set for tables the run modified, and `None` for tables
    that were declared as outputs in the request but not modified.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    table: str
    info: TableInfo | None = None


class Response(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    output: tuple[TableEntry, ...]


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
