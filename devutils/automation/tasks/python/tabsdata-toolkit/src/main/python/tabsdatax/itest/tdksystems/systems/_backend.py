"""Common helpers for cloud backends (walking local paths, normalising
prefixes for object listings)."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from ..errors import DatagenError


def normalise_prefix(p: str) -> str:
    """Strip leading slashes; ensure trailing slash for non-empty prefix."""
    p = (p or "").lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p


def walk_paths(paths: list[str]) -> Iterator[tuple[Path, str]]:
    """Yield (abs_src, dest_relpath) pairs for every file referenced.

    For a file path, dest_relpath is just the file's basename.
    For a directory, walks recursively and uses paths relative to the dir."""
    for path_str in paths:
        p = Path(path_str)
        if p.is_file():
            yield p, p.name
        elif p.is_dir():
            for f in sorted(p.rglob("*")):
                if f.is_file():
                    yield f, f.relative_to(p).as_posix()
        else:
            raise DatagenError(f"path is not a file or directory: {path_str}")
