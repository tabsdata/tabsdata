#
# Copyright 2026 Tabsdata Inc.
#

"""Inputs-manifest schema + IO.

Shared by every runner command that produces or consumes the
inputs-manifest YAML (`prep-localize-state` and `run-source-plugin`
write it, `stamp-inputs` reads it and writes an enriched copy, and
`run-function` reads it). One source of truth for:

  * the YAML schema (top-level `inputs:` list of entries),
  * the reader (with shape validation), and
  * the writer.

Callers translate the exceptions raised here into their own
command-specific exit codes — the schema lives in this module, but
exit-code policy stays per-command.

YAML shape — one entry per positional argument of the user function;
each entry carries one table record per resolved version (a single
record for plain tables, several for a multi-version dependency,
ordered by `version_pos`):

    inputs:
      - name: <slot label>             # function parameter name, or a
                                       # caller-chosen identifier
        table_pos: <int>               # argument position (-1 when not
                                       # request-driven, e.g. publishers)
        is_empty: false                # true for an empty multi-version
                                       # dependency (slot value is [])
        tables:
          - file: <absolute path>      # null for a version member with
                                       # no physical data
            input_idx: <int>           # unique index;
                                       # request-assigned for native
                                       # inputs, stamped sequentially by
                                       # stamp-inputs for publishers
            version_pos: <int>         # order within the dependency
                                       # (-1 single-version sentinel)
            execution_id: <str|null>   # version metadata of the member
            transaction_id: <str|null>
            table_data_version_id: <str|null>
            triggered_on: <int|null>   # epoch milliseconds
      - name: <next slot>
        table_pos: 1
        is_empty: false
        tables: []                     # empty slots are explicit
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from tabsdatak.error import ErrorCode
from tabsdatak.spi import MetadataSpec


class InvalidManifestError(ValueError):
    """The inputs manifest is malformed or fails shape validation."""


# Public keys callers use when constructing entry dicts. The top-level
# `inputs:` key stays private — only the read/write helpers in this
# module wrap entries with it, so callers never type the literal.
MANIFEST_NAME_KEY = "name"
MANIFEST_TABLE_POS_KEY = "table_pos"
MANIFEST_IS_EMPTY_KEY = "is_empty"
MANIFEST_TABLES_KEY = "tables"
MANIFEST_FILE_KEY = "file"
MANIFEST_METADATA_KEY = "metadata"
MANIFEST_INPUT_IDX_KEY = "input_idx"
MANIFEST_VERSION_POS_KEY = "version_pos"
MANIFEST_EXECUTION_ID_KEY = "execution_id"
MANIFEST_TRANSACTION_ID_KEY = "transaction_id"
MANIFEST_VERSION_ID_KEY = "table_data_version_id"
MANIFEST_TRIGGERED_ON_KEY = "triggered_on"

_MANIFEST_INPUTS_KEY = "inputs"


def build_table_record(
    *,
    file: str | None,
    metadata: MetadataSpec | None,
    input_idx: int | None,
    version_pos: int,
    execution_id: str | None = None,
    transaction_id: str | None = None,
    table_data_version_id: str | None = None,
    triggered_on: int | None = None,
) -> dict[str, Any]:
    return {
        MANIFEST_FILE_KEY: file,
        MANIFEST_METADATA_KEY: metadata,
        MANIFEST_INPUT_IDX_KEY: input_idx,
        MANIFEST_VERSION_POS_KEY: version_pos,
        MANIFEST_EXECUTION_ID_KEY: execution_id,
        MANIFEST_TRANSACTION_ID_KEY: transaction_id,
        MANIFEST_VERSION_ID_KEY: table_data_version_id,
        MANIFEST_TRIGGERED_ON_KEY: triggered_on,
    }


def build_entry(
    *,
    name: str,
    table_pos: int,
    tables: list[dict[str, Any]],
    is_empty: bool = False,
) -> dict[str, Any]:
    return {
        MANIFEST_NAME_KEY: name,
        MANIFEST_TABLE_POS_KEY: table_pos,
        MANIFEST_IS_EMPTY_KEY: is_empty,
        MANIFEST_TABLES_KEY: tables,
    }


def load_inputs_manifest(path: Path) -> list[dict[str, Any]]:
    """Read the inputs manifest at `path` and return the `inputs:` list.

    Validates the top-level shape (mapping with an `inputs:` key whose
    value is a list); individual entries are returned as raw dicts for
    callers to inspect. Per-entry shape validation (the keys each
    entry must carry) is intentionally left to callers — different
    callers may tolerate different subsets.

    Raises:
        FileNotFoundError: `path` does not exist.
        InvalidManifestError: YAML is malformed, top-level isn't a
            mapping, `inputs:` is missing, or `inputs:` isn't a list.
    """
    if not path.is_file():
        raise FileNotFoundError(f"inputs manifest not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidManifestError(
            f"malformed YAML in inputs manifest {path}: {exc}"
        ) from exc
    if not isinstance(data, dict) or _MANIFEST_INPUTS_KEY not in data:
        raise InvalidManifestError(
            f"inputs manifest {path} missing {_MANIFEST_INPUTS_KEY!r} key"
        )
    entries = data[_MANIFEST_INPUTS_KEY]
    if not isinstance(entries, list):
        raise InvalidManifestError(
            f"{_MANIFEST_INPUTS_KEY!r} must be a list, got {type(entries).__name__}"
        )
    return entries


def load_inputs_manifest_or_raise(
    path: Path, error_code: ErrorCode
) -> list[dict[str, Any]]:
    """:func:`load_inputs_manifest` with a per-command typed-error wrap.

    Typed counterpart of :func:`load_inputs_manifest_or_fail`. Catches
    ``FileNotFoundError`` and :class:`InvalidManifestError` and
    re-raises via ``error_code.exception(cause=exc, path=path,
    detail=str(exc))``.

    Templates that bind ``error_code`` can reference ``{path}`` and
    ``{detail}`` placeholders.
    """
    try:
        return load_inputs_manifest(path)
    except (FileNotFoundError, InvalidManifestError) as exc:
        raise error_code.exception(cause=exc, path=path, detail=str(exc))


def write_inputs_manifest(entries: list[dict[str, Any]], path: Path) -> None:
    """Serialise `entries` as the inputs manifest at `path`.

    Wraps the list under the top-level `inputs:` key and writes plain
    YAML with key order preserved (so reading the file back gives a
    diff-friendly result).

    Raises:
        OSError: the file cannot be written (missing parent dir with
            no permission to create, target is a directory, etc.).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump({_MANIFEST_INPUTS_KEY: entries}, sort_keys=False),
        encoding="utf-8",
    )
