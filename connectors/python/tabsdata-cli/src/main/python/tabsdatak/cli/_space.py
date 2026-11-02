#
# Copyright 2026 Tabsdata Inc.
#

"""How much a download may write in a directory: what its filesystem has
free, less a margin, and the per-file size limit of the process
(``RLIMIT_FSIZE``), past which the kernel kills it with SIGXFSZ."""

from __future__ import annotations

import errno
import shutil
from dataclasses import dataclass

MARGIN_BYTES = 16 << 20
OUT_OF_SPACE = frozenset({errno.ENOSPC, errno.EFBIG})


@dataclass(frozen=True)
class Room:
    directory: str
    free: int
    per_file: int | None

    @property
    def limit(self) -> int:
        return self.free if self.per_file is None else min(self.free, self.per_file)


def _per_file() -> int | None:
    try:
        import resource
    except ImportError:
        return None
    soft, _ = resource.getrlimit(resource.RLIMIT_FSIZE)
    return None if soft == resource.RLIM_INFINITY else soft


def room(directory: str) -> Room:
    free = shutil.disk_usage(directory).free
    return Room(directory, max(0, free - MARGIN_BYTES), _per_file())
