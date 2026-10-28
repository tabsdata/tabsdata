#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

EXAMPLES = "tabsdatax/pytesting/plugins/xdistgroups"


def run_grouped(pytester: pytest.Pytester, example: str, *arguments: str):
    copied = pytester.copy_example(f"{EXAMPLES}/{example}")
    return pytester.runpytest_subprocess(
        "-p",
        "no:cacheprovider",
        "--dist=loadgroup",
        *arguments,
        copied.name,
    )


def workers(pytester: pytest.Pytester, group: str) -> set[str]:
    return set(Path(pytester.path, f"{group}.log").read_text().split())


def test_a_group_runs_on_a_single_worker(pytester):
    run_grouped(pytester, "serial.py", "-n", "3").assert_outcomes(passed=8)
    assert len(workers(pytester, "serial")) == 1


def test_each_group_keeps_its_own_worker(pytester):
    run_grouped(pytester, "pair.py", "-n", "3").assert_outcomes(passed=16)
    assert len(workers(pytester, "first")) == 1
    assert len(workers(pytester, "second")) == 1


def test_grouped_ids_carry_the_group_once(pytester):
    result = run_grouped(pytester, "serial.py", "-n", "2", "-v")
    result.stdout.fnmatch_lines(["*PASSED serial.py::test_records_its_worker*@serial*"])
    result.stdout.no_fnmatch_line("*@serial@serial*")
