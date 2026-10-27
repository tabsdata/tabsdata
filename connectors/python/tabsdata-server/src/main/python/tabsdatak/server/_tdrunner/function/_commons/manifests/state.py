#
# Copyright 2026 Tabsdata Inc.
#

"""State-manifest schema + IO.

Sibling of `inputs_manifest.py`. Carries the same per-entry shape
(`{name, files}`) but a distinct top-level key (`state:`), so the two
manifests stay separable on the wire even though their per-entry
schemas are identical.

`state-manifest` lists framework state-in tables — i.e. entries of
`request.system_input` after `prep-localize-state` has resolved them
to local paths. The `inputs-manifest` lists user-facing input tables
(from `request.input` for transformers, or from the source plugin for
publishers). The split keeps "function state the framework
round-trips" separate from "user input data the function consumes".

Writer is `prep-localize-state`. There is no consumer in the current
chain — this manifest exists so future steps (e.g. a manifest-driven
`prep-upload-state`, or a context loader that hands state to source
plugins) have a structured place to look. It is also useful as a
debugging breadcrumb.

YAML shape:

    state:
      - name: <slot label>             # mirrors `request.system_input` order
        files:
          - <absolute filesystem path> # where the transporter put the parquet
      - name: <next slot>
        files: []                      # entry with no allocated location
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# Per-entry keys are intentionally the same names as in
# `inputs_manifest.py` — callers building dicts use whichever
# constants are nearest their import site. The wire shape lines up
# so a future consolidation is mechanical.
MANIFEST_NAME_KEY = "name"
MANIFEST_FILES_KEY = "files"

_MANIFEST_STATE_KEY = "state"


class InvalidStateManifestError(ValueError):
    """The state manifest is malformed or fails shape validation."""


def load_state_manifest(path: Path) -> list[dict[str, Any]]:
    """Read the state manifest at `path` and return the `state:` list.

    Validates the top-level shape (mapping with a `state:` key whose
    value is a list); individual entries are returned as raw dicts.
    Per-entry shape validation is intentionally left to callers,
    matching the inputs-manifest contract.

    Raises:
        FileNotFoundError: `path` does not exist.
        InvalidStateManifestError: YAML is malformed, top-level isn't
            a mapping, `state:` is missing, or `state:` isn't a list.
    """
    if not path.is_file():
        raise FileNotFoundError(f"state manifest not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidStateManifestError(
            f"malformed YAML in state manifest {path}: {exc}"
        ) from exc
    if not isinstance(data, dict) or _MANIFEST_STATE_KEY not in data:
        raise InvalidStateManifestError(
            f"state manifest {path} missing {_MANIFEST_STATE_KEY!r} key"
        )
    entries = data[_MANIFEST_STATE_KEY]
    if not isinstance(entries, list):
        raise InvalidStateManifestError(
            f"{_MANIFEST_STATE_KEY!r} must be a list, got {type(entries).__name__}"
        )
    return entries


def write_state_manifest(entries: list[dict[str, Any]], path: Path) -> None:
    """Serialise `entries` as the state manifest at `path`.

    Wraps the list under the top-level `state:` key and writes plain
    YAML with key order preserved.

    Raises:
        OSError: the file cannot be written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump({_MANIFEST_STATE_KEY: entries}, sort_keys=False),
        encoding="utf-8",
    )
