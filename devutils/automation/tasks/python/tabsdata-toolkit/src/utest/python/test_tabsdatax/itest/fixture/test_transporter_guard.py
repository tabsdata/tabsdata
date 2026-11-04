#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import subprocess
import sys

import pytest

from tabsdatax.itest.fixture.transporter import (
    TransporterUsedError,
    transporter_allowed,
    transporter_forbidden,
)

pytestmark = [pytest.mark.unit]


@pytest.fixture
def transporter(tmp_path):
    return str(tmp_path / "td_transporter2")


def test_starting_the_transporter_is_refused(transporter):
    with (
        transporter_forbidden(),
        pytest.raises(TransporterUsedError, match="td_transporter2"),
    ):
        subprocess.Popen([transporter, "copy"])


def test_running_the_transporter_is_refused(transporter):
    with transporter_forbidden(), pytest.raises(TransporterUsedError):
        subprocess.run([transporter, "copy"], check=False)


def test_a_transporter_command_line_string_is_refused(transporter):
    with transporter_forbidden(), pytest.raises(TransporterUsedError):
        subprocess.run(f"{transporter} copy", shell=True, check=False)


def test_other_programs_start():
    with transporter_forbidden():
        completed = subprocess.run(
            [sys.executable, "-c", "pass"], check=False, capture_output=True
        )
    assert completed.returncode == 0


def test_an_allowed_step_reaches_the_program(transporter):
    with transporter_forbidden(), transporter_allowed():
        with pytest.raises(FileNotFoundError):
            subprocess.Popen([transporter, "copy"])


def test_the_guard_is_lifted_afterwards(transporter):
    with transporter_forbidden():
        pass
    with pytest.raises(FileNotFoundError):
        subprocess.Popen([transporter, "copy"])
