#
#  Copyright 2026. Tabs Data Inc.
#
"""Filesystem-backed :class:`ConnStore` implementation."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from tabsdata._cli.tdconn._internal._debug import log_call
from tabsdata._cli.tdconn._internal.store.conn_store import ConnStore
from tabsdata._cli.tdconn._internal.store.paths import instance_root
from tabsdata._io.connections.named_connection.named_connection import (
    _resolve_env_vars,
)


class FileConnStore(ConnStore):
    """Filesystem-backed store under ``<instance_root>/repository/connections/``.

    Files are named ``<name>.lower() + ".json"`` — matching the lookup the
    tabsdata-os execution workers perform at function-run time.
    """

    def __init__(self, instance: str | None = None):
        self.instance = instance
        self.root = instance_root(instance) / "repository" / "connections"

    def _path(self, name: str) -> Path:
        # Lowercased to match the execution-time filename convention.
        return self.root / f"{name.lower()}.json"

    def exists(self, name: str, debug: bool = False) -> bool:
        log_call(debug, f"FileConnStore.exists(name={name!r})")
        return self._path(name).is_file()

    def put(self, name: str, source_path: Path, debug: bool = False) -> None:
        log_call(
            debug,
            f"FileConnStore.put(name={name!r}, source_path={str(source_path)!r})",
        )
        self.root.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, self._path(name))

    def get(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> dict[str, Any]:
        log_call(
            debug,
            f"FileConnStore.get(name={name!r}, resolved_envs={resolved_envs})",
        )
        with self._path(name).open("r", encoding="utf-8") as f:
            cx_dict = json.load(f)
        if resolved_envs and isinstance(cx_dict.get("conn_configs"), dict):
            # Same env:ref substitution NamedConn._load performs at
            # function-run time — keeping it here means callers don't
            # have to re-run the loader just to get a usable payload.
            cx_dict["conn_configs"] = _resolve_env_vars(cx_dict["conn_configs"])
        return cx_dict

    def delete(self, name: str, debug: bool = False) -> None:
        log_call(debug, f"FileConnStore.delete(name={name!r})")
        self._path(name).unlink()

    def names(self, debug: bool = False) -> list[str]:
        log_call(debug, "FileConnStore.names()")
        if not self.root.is_dir():
            return []
        return sorted(
            p.stem for p in self.root.iterdir() if p.is_file() and p.suffix == ".json"
        )
