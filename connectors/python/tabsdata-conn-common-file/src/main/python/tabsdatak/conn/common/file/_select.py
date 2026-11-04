#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable

from tabsdatak.conn.common._find import matches

CODECS = (".gz", ".zlib", ".zst", ".zstd", ".bz2", ".xz", ".lz4", ".br")
ARCHIVES = (".tar", ".zip")
WRAPPERS = CODECS + ARCHIVES


def ending(name: str, extensions: tuple[str, ...]) -> str | None:
    lowered = name.lower()
    return next(
        (extension for extension in extensions if lowered.endswith(extension)), None
    )


def codec(name: str) -> str | None:
    return ending(name, CODECS)


def archive(name: str) -> str | None:
    return ending(name, ARCHIVES)


def wrapper(name: str) -> str | None:
    return ending(name, WRAPPERS)


def plain(name: str) -> str:
    found = wrapper(name)
    return name if found is None else name[: -len(found)]


def accepts(pattern: str, include_compressed: bool) -> Callable[[str], bool]:
    match = matches(pattern)
    names_wrapper = wrapper(pattern) is not None

    def accept(name: str) -> bool:
        if wrapper(name) is None:
            return match(name)
        if not include_compressed:
            return False
        return match(name) or (not names_wrapper and match(plain(name)))

    return accept
