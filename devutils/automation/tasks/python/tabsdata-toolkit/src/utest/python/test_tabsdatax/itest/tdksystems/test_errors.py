"""Tests for tabsdatax.itest.tdksystems.errors."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems.errors import DatagenError, run


def test_DatagenError_message_round_trips():
    err = DatagenError("oh no")
    assert str(err) == "oh no"


def test_run_exits_zero_on_success(capsys):
    with pytest.raises(SystemExit) as exc:
        run(lambda: 0)
    assert exc.value.code == 0


def test_run_exits_zero_when_main_returns_None(capsys):
    with pytest.raises(SystemExit) as exc:
        run(lambda: None)
    assert exc.value.code == 0


def test_run_exits_with_main_returncode():
    with pytest.raises(SystemExit) as exc:
        run(lambda: 7)
    assert exc.value.code == 7


def test_run_catches_DatagenError_with_clear_message(capsys):
    def boom() -> int:
        raise DatagenError("things went sideways")

    with pytest.raises(SystemExit) as exc:
        run(boom)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "ERROR: things went sideways" in err


def test_run_catches_unexpected_exception_with_traceback(capsys):
    def boom() -> int:
        raise RuntimeError("surprise")

    with pytest.raises(SystemExit) as exc:
        run(boom)
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "ERROR: RuntimeError: surprise" in err
    assert "Traceback" in err


def test_run_propagates_SystemExit_from_main():
    with pytest.raises(SystemExit) as exc:
        run(lambda: (_ for _ in ()).throw(SystemExit(42)))
    assert exc.value.code == 42


def test_red_colorizes_only_when_stderr_is_a_tty(monkeypatch):
    import tabsdatax.itest.tdksystems.errors as errors

    class _Err:
        def __init__(self, tty: bool) -> None:
            self._tty = tty

        def isatty(self) -> bool:
            return self._tty

    monkeypatch.setattr(errors.sys, "stderr", _Err(True))
    assert errors._red("ERROR: boom") == "\x1b[31mERROR: boom\x1b[0m"
    monkeypatch.setattr(errors.sys, "stderr", _Err(False))
    assert errors._red("ERROR: boom") == "ERROR: boom"
