#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for tabsdatax.itest.tdksystems.console — the brief-console / detailed-log
output split."""
from __future__ import annotations

import io
import sys

import pytest

from tabsdatax.itest.tdksystems import console


@pytest.fixture(autouse=True)
def _reset_console():
    """The console is a process-wide singleton; make sure each test starts (and
    leaves it) inactive so state can't leak between tests."""
    console.CONSOLE.active = False
    console.CONSOLE._term = None
    yield
    console.CONSOLE.active = False
    console.CONSOLE._term = None


def test_unique_log_path_avoids_clobber(tmp_path):
    p1 = console._unique_log_path(tmp_path)
    assert p1.parent == tmp_path and p1.suffix == ".log"
    p1.write_text("x")
    p2 = console._unique_log_path(tmp_path)
    assert p2 != p1 and not p2.exists()


def test_inactive_brief_prints_plain_to_stdout(capsys):
    console.out("neutral")
    console.good("success")
    console.bad("failure")
    console.warn("caution")
    out = capsys.readouterr().out
    assert all(w in out for w in ("neutral", "success", "failure", "caution"))
    assert "\x1b[" not in out  # capsys is not a TTY -> no color codes


def test_inactive_color_only_on_a_tty(monkeypatch):
    class _Tty(io.StringIO):
        def isatty(self) -> bool:
            return True

    buf = _Tty()
    monkeypatch.setattr(sys, "stdout", buf)
    console.good("ok")
    console.bad("no")
    text = buf.getvalue()
    assert "\x1b[32mok\x1b[0m" in text and "\x1b[31mno\x1b[0m" in text


def test_progress_line_format(capsys):
    console.progress(2, 3, True, "ok  ", "alpha", 1.23, "detail here")
    line = capsys.readouterr().out
    assert "[2/3]" in line and "alpha" in line and "1.2s" in line
    assert "detail here" in line


def test_activate_captures_detail_in_log_and_mirrors_brief(tmp_path, capfd):
    # The fd-level redirect fights pytest's own fd capture; disable it for the
    # window so activate can dup2 the real fds and restore them cleanly.
    with capfd.disabled():
        with console.activate(tmp_path) as con:
            log_path = con.log_path
            assert con.active is True
            print(">>> a detailed line")  # detail -> fd 1 -> log
            sys.stderr.write("stderr detail\n")  # detail -> fd 2 -> log
            console.out("a brief line")  # brief -> terminal + mirror to log
    assert console.CONSOLE.active is False  # restored on exit
    text = log_path.read_text()
    assert log_path.parent == tmp_path and log_path.suffix == ".log"
    assert ">>> a detailed line" in text
    assert "stderr detail" in text
    assert "a brief line" in text  # brief is mirrored into the log too


def test_reset_for_child_disables_active(capsys):
    console.CONSOLE.active = True
    console.CONSOLE.reset_for_child()
    assert console.CONSOLE.active is False
    console.out("post-fork line")  # falls back to print -> stdout
    assert "post-fork line" in capsys.readouterr().out
