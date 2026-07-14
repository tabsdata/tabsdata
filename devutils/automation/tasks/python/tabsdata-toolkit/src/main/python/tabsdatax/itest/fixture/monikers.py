#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import itertools
import os
import re
import threading
import uuid

import pytest

PYTEST_XDIST_WORKER = "PYTEST_XDIST_WORKER"

SMALL_UUID_LENGTH = 8

counter = itertools.count()
counter_lock = threading.Lock()

sanitizer = re.compile(r"[^a-z0-9]+")


def worker_number() -> str:
    worker = os.environ.get(PYTEST_XDIST_WORKER, "")
    digits = "".join(character for character in worker if character.isdigit())
    return digits or "0"


def next_sequence() -> int:
    with counter_lock:
        return next(counter)


def sanitize(text: str) -> str:
    return sanitizer.sub("-", text.lower()).strip("-")


def small_uuid() -> str:
    return uuid.uuid4().hex[:SMALL_UUID_LENGTH]


def test_name(request: pytest.FixtureRequest) -> str:
    return getattr(request.node, "originalname", None) or request.node.name


def moniker(request: pytest.FixtureRequest) -> str:
    return (
        f"{sanitize(test_name(request))}"
        f"-{worker_number()}"
        f"-{next_sequence()}"
        f"-{small_uuid()}"
    )
