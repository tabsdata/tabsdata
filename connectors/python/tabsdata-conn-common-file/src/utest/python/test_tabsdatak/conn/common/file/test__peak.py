#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

MEBIBYTE = 1 << 20
PAGE = 4096
PEAK = Path(__file__).parent / "children" / "peak.py"


def test_a_child_reports_its_own_peak_not_the_one_of_its_parent():
    held = bytearray(512 * MEBIBYTE)
    for offset in range(0, len(held), PAGE):
        held[offset] = 1
    del held

    child = subprocess.run(
        [sys.executable, str(PEAK)],
        capture_output=True,
        encoding="utf-8",
        check=True,
    )

    assert int(child.stdout) < 256 * MEBIBYTE
