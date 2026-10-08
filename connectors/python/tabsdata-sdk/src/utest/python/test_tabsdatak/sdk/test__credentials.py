#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

from tabsdatak.sdk import _credentials as c


def test_a_written_file_reads_back_and_leaves_no_temporary_file(tmp_path):
    path = str(tmp_path / "connection.json")
    c.write(path, {"bearer_token": "b1"})
    c.write(path, {"bearer_token": "b2"})
    assert c.read(path) == {"bearer_token": "b2"}
    assert os.listdir(tmp_path) == ["connection.json"]


def test_a_missing_file_reads_as_none(tmp_path):
    assert c.read(str(tmp_path / "absent.json")) is None


def test_a_replace_refused_while_another_process_holds_the_file_is_retried(
    tmp_path, monkeypatch
):
    path = str(tmp_path / "connection.json")
    real_replace = os.replace
    refusals = []

    def refusing_replace(src, dst):
        if len(refusals) < 3:
            refusals.append(dst)
            raise PermissionError(13, "The process cannot access the file")
        real_replace(src, dst)

    monkeypatch.setattr(c, "POLL_SECONDS", 0.001)
    monkeypatch.setattr(os, "replace", refusing_replace)
    c.write(path, {"bearer_token": "b"})
    assert len(refusals) == 3
    assert c.read(path) == {"bearer_token": "b"}
    assert os.listdir(tmp_path) == ["connection.json"]


def test_a_replace_refused_for_too_long_fails_and_cleans_up(tmp_path, monkeypatch):
    path = str(tmp_path / "connection.json")

    def refusing_replace(src, dst):
        raise PermissionError(13, "The process cannot access the file")

    monkeypatch.setattr(c, "IO_RETRY_SECONDS", 0.05)
    monkeypatch.setattr(c, "POLL_SECONDS", 0.001)
    monkeypatch.setattr(os, "replace", refusing_replace)
    with pytest.raises(PermissionError):
        c.write(path, {"bearer_token": "b"})
    assert os.listdir(tmp_path) == []


def test_a_read_refused_or_torn_for_a_moment_is_retried(tmp_path, monkeypatch):
    path = tmp_path / "connection.json"
    path.write_text('{"bearer_token": "b"}')
    real_open = open
    failures = [PermissionError(13, "locked"), ValueError("torn")]

    def flaky_open(file, *args, **kwargs):
        if str(file) == str(path) and failures:
            raise failures.pop(0)
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(c, "POLL_SECONDS", 0.001)
    monkeypatch.setattr("builtins.open", flaky_open)
    assert c.read(str(path)) == {"bearer_token": "b"}


def test_remove_forgets_the_file_and_tolerates_its_absence(tmp_path):
    path = str(tmp_path / "connection.json")
    c.write(path, {"bearer_token": "b"})
    c.remove(path)
    c.remove(path)
    assert c.read(path) is None


def test_the_lock_is_exclusive_and_gives_up_after_its_timeout(tmp_path):
    path = str(tmp_path / "connection.json")
    with c.locked(path, 1.0) as held:
        assert held
        with c.locked(path, 0.1) as second:
            assert not second
    with c.locked(path, 0.1) as again:
        assert again


def test_a_lock_that_cannot_be_created_is_not_taken(tmp_path):
    with c.locked(str(tmp_path / "missing" / "connection.json"), 0.1) as held:
        assert not held


def test_processes_writing_and_reading_at_once_never_see_a_torn_file(tmp_path):
    path = str(tmp_path / "connection.json")
    c.write(path, {"bearer_token": "seed", "pad": "x" * 4096})
    script = textwrap.dedent(
        """
        import sys
        from tabsdatak.sdk import _credentials as c
        path, me = sys.argv[1], sys.argv[2]
        for n in range(150):
            with c.locked(path, 30) as held:
                assert held
                c.write(path, {"bearer_token": f"{me}-{n}", "pad": "x" * 4096})
            data = c.read(path)
            assert data["pad"] == "x" * 4096, data
        """
    )
    workers = [
        subprocess.Popen([sys.executable, "-c", script, path, str(i)]) for i in range(6)
    ]
    assert [w.wait(timeout=120) for w in workers] == [0] * 6
    assert c.read(path)["pad"] == "x" * 4096
    assert sorted(os.listdir(tmp_path)) == ["connection.json", "connection.json.lock"]
