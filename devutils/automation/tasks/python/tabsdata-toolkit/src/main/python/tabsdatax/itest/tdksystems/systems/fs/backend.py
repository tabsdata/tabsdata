"""Local-filesystem storage ops — the four the System contract needs."""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

from ...errors import DatagenError


class FsBackend:
    def __init__(self, values: dict[str, str]) -> None:
        self.root = values.get("SYS_PATH", "")

    def status(self) -> None:
        if not Path(self.root).is_dir():
            raise DatagenError(f"root path does not exist: {self.root}")

    def exists(self, prefix: str) -> bool:
        p = Path(prefix)
        return p.is_dir() and any(p.iterdir())

    def clear(self, prefix: str) -> None:
        p = Path(prefix)
        if p.is_dir():
            shutil.rmtree(p)

    def upload(self, prefix: str, local_paths: list[str]) -> int:
        from .._backend import walk_paths
        dst_base = Path(prefix)
        dst_base.mkdir(parents=True, exist_ok=True)
        n = 0
        for src, rel in walk_paths(local_paths):
            dst = dst_base / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            print(f"    {rel}", file=sys.stderr)
            n += 1
        return n
