#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.unit]

EXAMPLE = "tabsdatax/pytesting/plugins/loggerlevels/pinned.py"
PLUGIN = "tdx-logger-levels"
MODULE = "tabsdatax.pytesting.plugins.loggerlevels"


def run_in_order(pytester: pytest.Pytester, *plugins: str):
    copied = pytester.copy_example(EXAMPLE)
    return pytester.runpytest_subprocess(
        "-p", "no:cacheprovider", "-p", f"no:{PLUGIN}", *plugins, "-n", "0", copied.name
    )


def test_a_level_pinned_by_one_test_is_undone_before_the_next(pytester):
    run_in_order(pytester, "-p", MODULE).assert_outcomes(passed=2)


def test_without_the_plugin_the_pin_hides_the_next_tests_warnings(pytester):
    run_in_order(pytester).assert_outcomes(passed=1, failed=1)
