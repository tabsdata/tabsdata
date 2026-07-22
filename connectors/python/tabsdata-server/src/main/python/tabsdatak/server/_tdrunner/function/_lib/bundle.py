#
# Copyright 2026 Tabsdata Inc.
#

"""Function-bundle tar extraction.

The function bundle is a `.tar.zst` (zstandard-compressed tar)
produced by tabsdata containing the user's code, `requirements.yaml`,
and any small assets. This module unpacks it into a target directory
using `tarfile`'s strict `data` filter (Python 3.12+), which rejects
path-traversal escapes, absolute paths, and links pointing outside
the destination.
"""

from __future__ import annotations

import tarfile
from pathlib import Path

import zstandard


def extract_bundle(archive: Path, dest_dir: Path) -> None:
    """Extract a `.tar.zst` bundle into `dest_dir`.

    Creates `dest_dir` (and any missing parents) if it does not exist.
    Existing contents of `dest_dir` are preserved; bundle members are
    written alongside them.

    The archive is decompressed through ``zstandard.ZstdDecompressor.
    stream_reader`` and the resulting tar stream is read in streaming
    mode (``mode="r|"``) so memory use stays bounded for large
    bundles.

    Raises:
        FileNotFoundError: if `archive` does not exist.
        zstandard.ZstdError: if `archive` is not valid zstandard
            data (corrupt header, truncated frame, …).
        tarfile.ReadError: if the inner tar stream (after successful
            decompression) is corrupt or not a tarball.
        tarfile.FilterError: if any member fails the `data`-filter safety
            checks (e.g. absolute path, parent-dir escape, link pointing
            outside the destination).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(archive, "rb") as raw:
        with zstandard.ZstdDecompressor().stream_reader(raw) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as tar:
                tar.extractall(dest_dir, filter="data")
