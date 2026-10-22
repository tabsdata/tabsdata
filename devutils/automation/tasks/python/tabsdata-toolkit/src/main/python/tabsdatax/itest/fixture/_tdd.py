#
# Copyright 2026 Tabsdata Inc.
#

"""The tdd services the backend fixtures use: the store's names, a backend's
descriptor and the domain data verbs, each read from tdd's JSON events.

Only tdd knows the descriptors; the fixtures never open the store. tdd-exe is
found the way cargo make finds it (``$TDD_DIR``, else ``~/.tdd``) and reads its
key from ``$_TDD_CRYPTO_KEY``. ``store_dirs`` points the calls at another store
(``--sys-dir`` / ``--domain-dir``); tests set it to a tmp dir.
"""

from __future__ import annotations

import json
import os
import subprocess
import sysconfig
from pathlib import Path

SYSTEM = "system"
DOMAIN = "domain"

ENV_TDD_DIR = "TDD_DIR"
DEFAULT_TDD_DIR = "~/.tdd"

EVENT_OK = "ok"
EVENT_ERROR = "error"

_NAME_OPT = {SYSTEM: "--sys", DOMAIN: "--domain"}
_STORE_OPT = {SYSTEM: "--sys-dir", DOMAIN: "--domain-dir"}
_LISTING = {SYSTEM: "systems", DOMAIN: "domains"}

store_dirs: dict[str, Path | None] = {SYSTEM: None, DOMAIN: None}


class TddError(Exception):
    """A tdd command could not be run or did not end in an ok event."""


def exe() -> Path:
    tdd_dir = Path(os.environ.get(ENV_TDD_DIR) or DEFAULT_TDD_DIR).expanduser()
    venv = tdd_dir / "tdd-venvs" / "tdd"
    scripts = sysconfig.get_paths(
        scheme="venv", vars={"base": str(venv), "platbase": str(venv)}
    )["scripts"]
    return Path(scripts) / ("tdd-exe" + (sysconfig.get_config_var("EXE") or ""))


def store_args(family: str) -> list[str]:
    store = store_dirs[family]
    return [] if store is None else [_STORE_OPT[family], str(store)]


def run(family: str, verb: str, *args: str) -> dict:
    """Run ``tdd-exe <family> <verb> [args] --format json`` against the store and
    return its ok event."""
    path = exe()
    if not path.is_file():
        raise TddError(
            f"Tool tdd is not installed at {path}; run 'cargo make permit' to "
            f"install it."
        )
    argv = [str(path), family, verb, *args, *store_args(family), "--format", "json"]
    cp = subprocess.run(argv, capture_output=True, text=True)
    events = [
        json.loads(line) for line in cp.stdout.splitlines() if line.startswith("{")
    ]
    final = next(
        (e for e in reversed(events) if e.get("event") in (EVENT_OK, EVENT_ERROR)),
        None,
    )
    command = f"tdd {family} {verb}"
    if final is None:
        raise TddError(
            f"Command '{command}' ended without an ok or error event "
            f"(exit code {cp.returncode}):\n{cp.stderr.strip()}"
        )
    if final["event"] == EVENT_ERROR or cp.returncode != 0:
        raise TddError(
            f"Command '{command}' failed (exit code {cp.returncode}): "
            f"{final.get('message', '')}"
        )
    return final


def names(family: str) -> list[str]:
    """The names of the store's systems or domains."""
    return sorted(entry["name"] for entry in run(family, "list")[_LISTING[family]])


def descriptor(family: str, name: str) -> dict:
    """The descriptor of system or domain `name`, as ``info`` prints it."""
    [found] = run(family, "info", _NAME_OPT[family], name)["descriptors"]
    return found
