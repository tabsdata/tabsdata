#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys

KIBIBYTE = 1024


def peak_bytes() -> int:
    if sys.platform == "win32":
        import psutil

        return psutil.Process().memory_info().peak_wset
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * KIBIBYTE
