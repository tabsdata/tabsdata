#
# Copyright 2026 Tabs Data Inc.
#

"""Outputs-manifest schema + IO.

Written by `run-function`, consumed by `prep-upload-state`. Maps each
output table the user function declared (in `info.output_tables`
declaration order) to the single parquet file the function returned
for it — or to `null` when the function returned `None`, meaning the
table was not modified this run.

Schema:

    outputs:
      - name: <table_name>            # from info.output_tables[i]
        file: /absolute/path/to/<table_name>.parquet
      - name: <other_table>
        file: null                    # not modified this run

Two intentional differences from `inputs_manifest.py`:
  * `file:` is singular — each output table corresponds to at most
    one parquet (vs inputs, where an array-mode source may have many
    files per slot).
  * `null` is a meaningful value — distinguishes "not modified" from
    "modified and empty". Downstream (`prep-upload-state`) should
    treat null as "nothing to upload for this table".
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError


class InvalidOutputsManifestError(ValueError):
    """The outputs manifest is malformed or fails shape validation."""


# Public keys callers use when constructing entry dicts. The top-level
# `outputs:` key stays private — only the read/write helpers in this
# module wrap entries with it.
MANIFEST_NAME_KEY = "name"
MANIFEST_FILE_KEY = "file"

_MANIFEST_OUTPUTS_KEY = "outputs"


def load_outputs_manifest(path: Path) -> list[dict[str, Any]]:
    """Read the outputs manifest at `path` and return the `outputs:` list.

    Validates the top-level shape (mapping with an `outputs:` key whose
    value is a list); individual entries are returned as raw dicts.

    Raises:
        FileNotFoundError: `path` does not exist.
        InvalidOutputsManifestError: YAML is malformed, top-level isn't
            a mapping, `outputs:` is missing, or isn't a list.
    """
    if not path.is_file():
        raise FileNotFoundError(f"outputs manifest not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidOutputsManifestError(
            f"malformed YAML in outputs manifest {path}: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise InvalidOutputsManifestError(
            f"outputs manifest {path} top-level must be a mapping, "
            f"got {type(data).__name__}"
        )
    if _MANIFEST_OUTPUTS_KEY not in data:
        raise InvalidOutputsManifestError(
            f"outputs manifest {path} missing {_MANIFEST_OUTPUTS_KEY!r} key"
        )
    entries = data[_MANIFEST_OUTPUTS_KEY]
    if not isinstance(entries, list):
        raise InvalidOutputsManifestError(
            f"{_MANIFEST_OUTPUTS_KEY!r} must be a list, got {type(entries).__name__}"
        )
    return entries


def load_outputs_manifest_or_fail(path: Path, exit_code: int) -> list[dict[str, Any]]:
    """:func:`load_outputs_manifest` with the canonical command-side
    error wrap.

    Every command that takes ``--outputs-manifest`` had the same
    try/except boilerplate around :func:`load_outputs_manifest`. This
    helper folds it into one call; ``exit_code`` is passed by the
    caller so each command keeps its own exit-code numbering.
    """
    try:
        return load_outputs_manifest(path)
    except (FileNotFoundError, InvalidOutputsManifestError) as exc:
        raise CommandSpecificError(exit_code, str(exc)) from exc


def write_outputs_manifest(entries: list[dict[str, Any]], path: Path) -> None:
    """Serialise `entries` as the outputs manifest at `path`.

    Wraps the list under the top-level `outputs:` key and writes plain
    YAML with key order preserved.

    Raises:
        OSError: the file cannot be written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump({_MANIFEST_OUTPUTS_KEY: entries}, sort_keys=False),
        encoding="utf-8",
    )
