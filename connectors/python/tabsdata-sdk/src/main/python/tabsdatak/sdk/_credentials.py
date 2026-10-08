#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import time
from collections.abc import Iterator

LOCK_SUFFIX = ".lock"
IO_RETRY_SECONDS = 5.0
POLL_SECONDS = 0.05


def read(path: str) -> dict | None:
    deadline = time.monotonic() + IO_RETRY_SECONDS
    while True:
        try:
            with open(path, encoding="utf-8") as file:
                data = json.load(file)
            return data if isinstance(data, dict) else None
        except FileNotFoundError:
            return None
        except (PermissionError, ValueError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(POLL_SECONDS)


def write(path: str, data: dict) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    fd, temp = tempfile.mkstemp(
        prefix=f"{os.path.basename(path)}.", suffix=".tmp", dir=directory
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(data, file)
            file.flush()
            os.fsync(file.fileno())
        retrying(lambda: os.replace(temp, path))
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(temp)
        raise


def remove(path: str) -> None:
    def unlink() -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)

    retrying(unlink)


def retrying(operation) -> None:
    deadline = time.monotonic() + IO_RETRY_SECONDS
    while True:
        try:
            operation()
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(POLL_SECONDS)


@contextlib.contextmanager
def locked(path: str, timeout: float) -> Iterator[bool]:
    try:
        fd = os.open(path + LOCK_SUFFIX, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError:
        yield False
        return
    try:
        taken = acquire(fd, timeout)
        try:
            yield taken
        finally:
            if taken:
                release(fd)
    finally:
        os.close(fd)


def acquire(fd: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while True:
        if try_lock(fd):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(POLL_SECONDS)


if os.name == "nt":
    import msvcrt

    def try_lock(fd: int) -> bool:
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def release(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def try_lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True

    def release(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)
