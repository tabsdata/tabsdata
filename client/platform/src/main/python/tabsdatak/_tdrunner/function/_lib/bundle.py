#
# Copyright 2026 Tabs Data Inc.
#

"""Function-bundle tar extraction.

The function bundle is a `.tar.gz` produced by tabsdata containing the
user's code, `requirements.txt`, and any small assets. This module
unpacks it into a target directory using `tarfile`'s strict `data` filter
(Python 3.12+), which rejects path-traversal escapes, absolute paths, and
links pointing outside the destination.
"""

from __future__ import annotations

import tarfile
from pathlib import Path


def extract_bundle(archive: Path, dest_dir: Path) -> None:
    """Extract a `.tar.gz` bundle into `dest_dir`.

    Creates `dest_dir` (and any missing parents) if it does not exist.
    Existing contents of `dest_dir` are preserved; bundle members are
    written alongside them.

    Raises:
        FileNotFoundError: if `archive` does not exist.
        tarfile.ReadError: if `archive` is corrupt or not a gzip-compressed
            tarball.
        tarfile.FilterError: if any member fails the `data`-filter safety
            checks (e.g. absolute path, parent-dir escape, link pointing
            outside the destination).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(dest_dir, filter="data")
