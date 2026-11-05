#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

TRANSPORTER = "transporter"


class TransporterUsedError(AssertionError):
    pass


def program(args: Any) -> str:
    if isinstance(args, (str, bytes, os.PathLike)):
        words = os.fsdecode(args).split()
    elif isinstance(args, Sequence):
        words = [os.fsdecode(arg) for arg in args]
    else:
        words = []
    return Path(words[0]).name if words else ""


@contextmanager
def transporter_forbidden() -> Iterator[None]:
    original = subprocess.Popen

    class Guarded(subprocess.Popen[Any]):
        def __init__(self, args: Any, *rest: Any, **options: Any) -> None:
            name = program(args)
            if TRANSPORTER in name.lower():
                raise TransporterUsedError(
                    f"The transporter program '{name}' was started where data must "
                    "not go through the transporter"
                )
            super().__init__(args, *rest, **options)

    setattr(subprocess, "Popen", Guarded)
    try:
        yield
    finally:
        setattr(subprocess, "Popen", original)


@pytest.fixture
def no_transporter() -> Iterator[None]:
    with transporter_forbidden():
        yield
