#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

KIBIBYTE = 1024


def peak_bytes() -> int:
    """This process's own peak resident memory.

    On Linux ``ru_maxrss`` is not that: ``exec`` folds the high-water mark of
    the image it replaces into it, and a child started with ``vfork`` (what
    ``subprocess`` uses) replaces its parent's, so it starts at the pytest
    worker's peak. ``VmHWM`` is the high-water mark of this image alone.
    """
    if sys.platform == "win32":
        import psutil

        return psutil.Process().memory_info().peak_wset
    if sys.platform == "linux":
        status = Path("/proc/self/status").read_text(encoding="utf-8")
        for line in status.splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * KIBIBYTE
        raise RuntimeError("/proc/self/status has no VmHWM")
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


if __name__ == "__main__":
    print(peak_bytes())
