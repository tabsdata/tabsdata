#
# Copyright 2026 Tabs Data Inc.
#

"""run-env.yaml writer.

The artifact `prepare-py-env` writes; the pod-runner reads it once to
activate the venv before subsequent containers. tabsdatak only writes
it — no command consumes it as an input — so this module exposes
construction and serialisation only.

Schema:

    python_bin: /path/to/venv/bin/python      # required
    venv_dir:   /path/to/venv                 # required
    requirements_digest: "sha256:abcd..."     # required, "sha256:" prefix
    extra_env:                                # optional, defaults to {}
      PYTHONPATH: ""
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


class RunEnv(BaseModel):
    """Prepared Python runtime descriptor for a function run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    python_bin: str
    venv_dir: str
    requirements_digest: str = Field(pattern=r"^sha256:")
    extra_env: dict[str, str] = Field(default_factory=dict)


def write_run_env(env: RunEnv, path: Path) -> None:
    """Serialise the run-env to plain YAML at `path`.

    Raises:
        OSError: if `path` is not writable (e.g. missing parent directory,
            permission denied, target is a directory).
    """
    data = env.model_dump(mode="python")
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False)
