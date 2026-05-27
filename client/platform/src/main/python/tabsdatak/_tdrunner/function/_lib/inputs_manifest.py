#
# Copyright 2026 Tabs Data Inc.
#

"""Inputs-manifest schema + IO.

Shared by every runner command that produces or consumes the
inputs-manifest YAML (currently `run-source-plugin` writes it,
`stamp-inputs` reads it and writes an enriched copy, and
`run-function` will read it). One source of truth for:

  * the YAML schema (top-level `inputs:` list of `{name, files}` entries),
  * the reader (with shape validation), and
  * the writer.

Callers translate the exceptions raised here into their own
command-specific exit codes — the schema lives in this module, but
exit-code policy stays per-command.

YAML shape:

    inputs:
      - name: <slot label>             # function parameter name, or a
                                       # caller-chosen identifier
        files:
          - <absolute filesystem path>
          - ...
      - name: <next slot>
        files: []                      # empty slots are explicit
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError


class InvalidManifestError(ValueError):
    """The inputs manifest is malformed or fails shape validation."""


# Public keys callers use when constructing entry dicts. The top-level
# `inputs:` key stays private — only the read/write helpers in this
# module wrap entries with it, so callers never type the literal.
MANIFEST_NAME_KEY = "name"
MANIFEST_FILES_KEY = "files"

_MANIFEST_INPUTS_KEY = "inputs"


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


def load_inputs_manifest_or_fail(path: Path, exit_code: int) -> list[dict[str, Any]]:
    """:func:`load_inputs_manifest` with the canonical command-side error wrap.

    Every command that takes ``--inputs-manifest`` had the same try/except
    boilerplate around :func:`load_inputs_manifest`. This helper folds it
    into one call; ``exit_code`` is passed by the caller so each command
    keeps its own exit-code numbering (the convention is some
    ``EXIT_BAD_MANIFEST`` value, but the helper doesn't enforce a
    specific number).
    """
    try:
        return load_inputs_manifest(path)
    except (FileNotFoundError, InvalidManifestError) as exc:
        raise CommandSpecificError(exit_code, str(exc)) from exc


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
