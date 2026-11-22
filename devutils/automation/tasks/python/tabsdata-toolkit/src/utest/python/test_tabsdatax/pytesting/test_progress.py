#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatax.pytesting.plugins.progress import format_seconds

pytestmark = [pytest.mark.unit]

EXAMPLE = "tabsdatax/pytesting/plugins/progress/outcomes.py"
FINAL = (
    "tests 7 | done 7 (100%) | running 0 | pending 0 | passed 4 | failed 1 | error 1 | "
    "skipped 1 | *"
)


def run(pytester: pytest.Pytester, *arguments: str):
    copied = pytester.copy_example(EXAMPLE)
    return pytester.runpytest_subprocess(
        "-p",
        "no:cacheprovider",
        "-p",
        "no:tdx-harvester",
        *arguments,
        copied.name,
    )


@pytest.mark.parametrize("workers", ["0", "2"])
def test_the_counters_replace_the_dots(pytester, workers):
    result = run(pytester, "--tdx-progress", "-n", workers)
    result.assert_outcomes(passed=4, failed=1, errors=1, skipped=1)
    result.stdout.fnmatch_lines([FINAL])
    result.stdout.no_fnmatch_line("*....*")
    result.stdout.no_fnmatch_line("*[[]100%[]]*")


def while_running(result: pytest.RunResult) -> list[str]:
    lines = result.stdout.lines
    final = next(
        i for i, line in enumerate(lines) if line.startswith("tests 7 | done 7")
    )
    return lines[:final]


@pytest.mark.parametrize("workers", ["0", "2"])
def test_each_failure_is_printed_as_it_happens(pytester, workers):
    result = run(pytester, "--tdx-progress", "-n", workers)
    assert {
        "FAILED outcomes.py::test_fails - AssertionError: one is not two",
        "ERROR outcomes.py::test_errors - RuntimeError: setup broke",
    } <= set(while_running(result))


def test_instafail_prints_the_failures_instead(pytester):
    result = run(pytester, "--tdx-progress", "--instafail", "--tb=line", "-n", "0")
    result.stdout.fnmatch_lines([FINAL])
    shown = while_running(result)
    assert not [line for line in shown if line.startswith("FAILED ")]
    assert [line for line in shown if line.endswith("AssertionError: one is not two")]


def test_without_the_option_the_dots_stay(pytester):
    result = run(pytester, "-n", "0")
    result.stdout.fnmatch_lines(["outcomes.py ....FEs*"])
    result.stdout.no_fnmatch_line("tests 7 |*")


def test_verbose_runs_keep_their_own_lines(pytester):
    result = run(pytester, "--tdx-progress", "-v", "-n", "0")
    result.stdout.fnmatch_lines(["*test_passes[[]0[]] PASSED*"])
    result.stdout.no_fnmatch_line("tests 7 |*")


@pytest.mark.parametrize(
    "seconds, shown",
    [
        (0, "00:00"),
        (59.9, "00:59"),
        (61, "01:01"),
        (3600, "1:00:00"),
        (3725, "1:02:05"),
    ],
)
def test_times_read_as_minutes_or_hours(seconds, shown):
    assert format_seconds(seconds) == shown
