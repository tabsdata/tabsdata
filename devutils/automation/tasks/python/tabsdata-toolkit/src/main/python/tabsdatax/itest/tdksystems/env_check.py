#
# Copyright 2026 Tabsdata Inc.
#

"""Verify the active Python env satisfies the project's declared deps.

Reads runtime dependencies from the installed `tabsdata-toolkit` distribution
metadata (i.e. pyproject.toml's `[project] dependencies`) via
`importlib.metadata`, so there's a single source of truth.

Honours `; platform_machine != "aarch64"` markers (so ibm_db et al. don't
fail on Apple Silicon when those wheels aren't published). Skips
optional-dependency entries (`; extra == "..."`)."""

from __future__ import annotations

import importlib
import importlib.metadata
import platform
import sys

from .errors import DatagenError

DIST_NAME = "tabsdata-toolkit"

_IMPORT_OVERRIDES: dict[str, str] = {
    "py-cpuinfo": "cpuinfo",
    "pyyaml": "yaml",
    "psycopg2-binary": "psycopg2",
    "mysql-connector-python": "mysql.connector",
    "clickhouse-sqlalchemy": "clickhouse_sqlalchemy",
    "google-cloud-storage": "google.cloud.storage",
    "google-cloud-bigquery": "google.cloud.bigquery",
    "azure-storage-blob": "azure.storage.blob",
    "snowflake-connector-python": "snowflake.connector",
}


def _import_name(distribution: str) -> str:
    return _IMPORT_OVERRIDES.get(distribution, distribution.replace("-", "_"))


def _marker_ok(marker: str, machine: str) -> bool:
    parts = marker.strip().split()
    if len(parts) == 3 and parts[0] == "platform_machine":
        op, val = parts[1], parts[2].strip("'\"")
        if op == "==":
            return machine == val
        if op == "!=":
            return machine != val
    return True  # unknown marker → assume requirement applies


def _parse_requires(requires: list[str]) -> list[str]:
    """Filter and normalize a list of PEP 508 requirement strings into bare
    distribution names that should be importable in the current env."""
    machine = platform.machine()
    out: list[str] = []
    for raw in requires:
        line = raw.strip()
        if not line:
            continue
        if ";" in line:
            spec, marker = (s.strip() for s in line.split(";", 1))
            # Optional-dependency markers (e.g. `extra == "test"`) — skip:
            # we only check the unconditional, runtime deps here.
            if "extra ==" in marker or "extra==" in marker:
                continue
            if not _marker_ok(marker, machine):
                continue
        else:
            spec = line
        for sep in ("==", ">=", "<=", "~=", "!=", ">", "<"):
            if sep in spec:
                spec = spec.split(sep, 1)[0].strip()
                break
        # Strip any trailing extras `name[foo,bar]`.
        if "[" in spec:
            spec = spec.split("[", 1)[0].strip()
        if spec:
            out.append(spec)
    return out


def _requires_for(dist: str) -> list[str] | None:
    try:
        return importlib.metadata.requires(dist) or []
    except importlib.metadata.PackageNotFoundError:
        return None


def check_requirements() -> None:
    requires = _requires_for(DIST_NAME)
    if requires is None:
        # Distribution not installed — nothing to check (e.g. running from
        # a source checkout without `pip install -e .`).
        return
    deps = _parse_requires(requires)
    missing = [
        d
        for d in deps
        if importlib.util.find_spec(_import_name(d)) is None  # type: ignore[attr-defined]
    ]
    if missing:
        msg = (
            "required Python packages are missing from this env:\n  - "
            + "\n  - ".join(missing)
            + f"\nActive interpreter: {sys.executable}\n"
            f"Install: pip install -e ."
        )
        raise DatagenError(msg)
