#
# Copyright 2026 Tabs Data Inc.
#

"""run-env.yaml model.

The artifact `prepare-py-env` writes and downstream commands
(`run-source-plugin`, `run-function`, `run-destination-plugin`) consume
to describe the prepared Python runtime for a function run.

Schema:

    python_bin: /path/to/venv/bin/python      # required
    venv_dir:   /path/to/venv                 # required
    requirements_digest: "sha256:abcd..."     # required, "sha256:" prefix
    extra_env:                                # optional, defaults to {}
      PYTHONPATH: ""
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_DIGEST_PREFIX = "sha256:"


class InvalidRunEnvError(ValueError):
    """The run-env.yaml is malformed or missing required fields."""


@dataclass(frozen=True)
class RunEnv:
    """Prepared Python runtime descriptor for a function run."""

    python_bin: str
    venv_dir: str
    requirements_digest: str  # always `sha256:<hex>`
    extra_env: dict[str, str] = field(default_factory=dict)


def write_run_env(env: RunEnv, path: Path) -> None:
    """Serialise the run-env to plain YAML at `path`.

    Raises:
        OSError: if `path` is not writable (e.g. missing parent directory,
            permission denied, target is a directory).
    """
    body = {
        "python_bin": env.python_bin,
        "venv_dir": env.venv_dir,
        "requirements_digest": env.requirements_digest,
        "extra_env": dict(env.extra_env),
    }
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(body, fh, sort_keys=False)


def load_run_env(path: Path) -> RunEnv:
    """Load and validate a run-env.yaml.

    Raises:
        FileNotFoundError: if `path` does not exist.
        InvalidRunEnvError: if the YAML is malformed, the document is not
            a mapping, or the schema is invalid.
    """
    text = path.read_text(encoding="utf-8")
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise InvalidRunEnvError(f"malformed YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvalidRunEnvError("run-env must be a mapping")

    python_bin = _require(raw, "python_bin", str)
    venv_dir = _require(raw, "venv_dir", str)
    digest = _require(raw, "requirements_digest", str)
    if not digest.startswith(_DIGEST_PREFIX):
        raise InvalidRunEnvError(
            f"requirements_digest must start with {_DIGEST_PREFIX!r}, got {digest!r}"
        )

    return RunEnv(
        python_bin=python_bin,
        venv_dir=venv_dir,
        requirements_digest=digest,
        extra_env=_parse_extra_env(raw.get("extra_env")),
    )


def _parse_extra_env(raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise InvalidRunEnvError(
            f"'extra_env' must be a mapping, got {type(raw).__name__}"
        )
    parsed: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            raise InvalidRunEnvError(
                f"extra_env key {key!r} must be a string, got {type(key).__name__}"
            )
        if not isinstance(value, str):
            raise InvalidRunEnvError(
                f"extra_env[{key!r}] value must be a string, got {type(value).__name__}"
            )
        parsed[key] = value
    return parsed


def _require(parent: dict, key: str, type_: type) -> Any:
    if key not in parent:
        raise InvalidRunEnvError(f"missing required field {key!r}")
    val = parent[key]
    if not isinstance(val, type_):
        raise InvalidRunEnvError(
            f"field {key!r} must be {type_.__name__}, got {type(val).__name__}"
        )
    return val
