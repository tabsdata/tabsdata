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
      - table: orders   # no `info` → table was not modified
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class InvalidResponseError(ValueError):
    """The response.yaml is malformed or missing required fields."""


@dataclass(frozen=True)
class TableInfo:
    column_count: int
    row_count: int
    schema_hash: str


@dataclass(frozen=True)
class TableEntry:
    """An output-table entry in the response.

    `info` is set for tables the run modified, and `None` for tables that
    were declared as outputs in the request but not modified.
    """

    table: str
    info: TableInfo | None = None


@dataclass(frozen=True)
class Response:
    output: tuple[TableEntry, ...]


def write_response(resp: Response, path: Path) -> None:
    """Serialise the response to plain YAML at `path`.

    Raises:
        OSError: if `path` is not writable (e.g. missing parent directory,
            permission denied, target is a directory).
    """
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(_to_dict(resp), fh, sort_keys=False)


def load_response(path: Path) -> Response:
    """Parse a response.yaml, primarily used for roundtrip testing.

    Raises:
        FileNotFoundError: if `path` does not exist.
        InvalidResponseError: if the YAML is malformed, the document is
            not a mapping, or the schema is invalid.
    """
    text = path.read_text(encoding="utf-8")
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise InvalidResponseError(f"malformed YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvalidResponseError("response must be a mapping")
    return _from_dict(raw)


def _to_dict(resp: Response) -> dict[str, Any]:
    output: list[dict[str, Any]] = []
    for entry in resp.output:
        item: dict[str, Any] = {"table": entry.table}
        if entry.info is not None:
            item["info"] = {
                "column_count": entry.info.column_count,
                "row_count": entry.info.row_count,
                "schema_hash": entry.info.schema_hash,
            }
        output.append(item)
    return {"output": output}


def _from_dict(raw: dict) -> Response:
    if "output" not in raw:
        raise InvalidResponseError("missing required field 'output'")
    output_raw = raw["output"]
    if not isinstance(output_raw, list):
        raise InvalidResponseError(
            f"'output' must be a list, got {type(output_raw).__name__}"
        )
    entries = tuple(_parse_entry(item, idx) for idx, item in enumerate(output_raw))
    return Response(output=entries)


def _parse_entry(raw: object, idx: int) -> TableEntry:
    if not isinstance(raw, dict):
        raise InvalidResponseError(
            f"output[{idx}] must be a mapping, got {type(raw).__name__}"
        )
    table = _require_field(raw, "table", str, f"output[{idx}].table")
    info_raw = raw.get("info")
    if info_raw is None:
        return TableEntry(table=table, info=None)
    if not isinstance(info_raw, dict):
        raise InvalidResponseError(
            f"output[{idx}].info must be a mapping or null, "
            f"got {type(info_raw).__name__}"
        )
    return TableEntry(table=table, info=_parse_info(info_raw, idx))


def _parse_info(raw: dict, idx: int) -> TableInfo:
    column_count = _require_field(
        raw, "column_count", int, f"output[{idx}].info.column_count"
    )
    row_count = _require_field(raw, "row_count", int, f"output[{idx}].info.row_count")
    schema_hash = _require_field(
        raw, "schema_hash", str, f"output[{idx}].info.schema_hash"
    )
    return TableInfo(
        column_count=column_count,
        row_count=row_count,
        schema_hash=schema_hash,
    )


def _require_field(parent: dict, key: str, type_: type, dotted_path: str) -> Any:
    if key not in parent:
        raise InvalidResponseError(f"missing required field {dotted_path!r}")
    val = parent[key]
    if not isinstance(val, type_):
        raise InvalidResponseError(
            f"field {dotted_path!r} must be {type_.__name__}, got {type(val).__name__}"
        )
    return val
