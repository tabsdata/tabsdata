#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable

from tabsdatak.conn.common._find import matches

CODECS = (".gz", ".zlib", ".zst", ".zstd", ".bz2", ".xz", ".lz4", ".br")


def codec(name: str) -> str | None:
    lowered = name.lower()
    return next(
        (extension for extension in CODECS if lowered.endswith(extension)), None
    )


def plain(name: str) -> str:
    found = codec(name)
    return name if found is None else name[: -len(found)]


def accepts(pattern: str, include_compressed: bool) -> Callable[[str], bool]:
    match = matches(pattern)
    names_codec = codec(pattern) is not None

    def accept(name: str) -> bool:
        if codec(name) is None:
            return match(name)
        if not include_compressed:
            return False
        return match(name) or (not names_codec and match(plain(name)))

    return accept
