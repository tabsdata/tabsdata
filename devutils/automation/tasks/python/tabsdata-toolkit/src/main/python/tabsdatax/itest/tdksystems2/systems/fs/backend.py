#
# Copyright 2026 Tabsdata Inc.
#

"""Local-filesystem backend — domains are directories under a root path.

A ``fs`` system is a real directory (the staging root); each domain is a
subdirectory holding the staged data files. Destroy leaves the root in place
(it's externally owned); a force-recreate rmtree can never wipe a system path
because the root is validated (absolute, not under a system directory, >= 2
elements). Only the local-storage primitives live here — the rest is
:class:`FileBackend`.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems._common.files import FileBackend

_UNSAFE_ROOT_HEADS = frozenset({
    "usr",
    "opt",
    "etc",
    "var",
    "bin",
    "sbin",
    "lib",
    "boot",
    "dev",
    "proc",
    "sys",
    "root",
})


def validate_fs_root(root: str) -> None:
    if not root:
        raise TdxError("fs root path is empty")
    path = Path(root)
    if not path.is_absolute():
        raise TdxError(f"fs root path must be absolute (got: {root})")
    # Path elements below the anchor (drive/root); the leading "/" or "C:\\" is
    # excluded so the system-directory and depth checks compare real names.
    parts = [p for p in path.parts[1:] if p]
    if parts and parts[0].lower() in _UNSAFE_ROOT_HEADS:
        raise TdxError(f"fs root must not sit under a system directory (got: {root})")
    if len(parts) < 2:
        raise TdxError(f"fs root must have at least 2 path elements (got: {root})")


class Fs(FileBackend):
    slug = "fs"
    label = "fs"
    domain_word = "folder"

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option(
                "SYS_PATH", "sys-path", options.SYNTH, "target/backends/<name>"
            ),
        )

    def synth_default(self, name: str, sys_name: str) -> str:
        if name == "SYS_PATH":
            return f"target/backends/{sys_name}"
        raise TdxError(f"{self.slug}: no synth default for {name}")

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        o = overrides or {}
        root = str(Path(o.get("SYS_PATH") or f"target/backends/{name}").absolute())
        validate_fs_root(root)
        return {"SYS_PATH": root}

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        Path(meta["SYS_PATH"]).mkdir(parents=True, exist_ok=True)
        return {}

    def store_status(self, root: str) -> None:
        if not Path(root).is_dir():
            raise TdxError(f"root path does not exist: {root}")

    def store_exists(self, dest: str) -> bool:
        p = Path(dest)
        return p.is_dir() and any(p.iterdir())

    def store_children(self, root: str) -> list[str]:
        r = Path(root)
        return sorted(p.name for p in r.iterdir() if p.is_dir()) if r.is_dir() else []

    def store_clear(self, dest: str) -> None:
        p = Path(dest)
        if p.is_dir():
            shutil.rmtree(p)

    def store_upload(self, dest: str, local_dir: str) -> int:
        dst = Path(dest)
        dst.mkdir(parents=True, exist_ok=True)
        base = Path(local_dir)
        n = 0
        for src in base.rglob("*"):
            if src.is_file():
                out = dst / src.relative_to(base)
                out.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, out)
                n += 1
        return n
