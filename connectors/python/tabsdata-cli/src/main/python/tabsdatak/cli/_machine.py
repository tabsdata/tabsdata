#
# Copyright 2026 Tabsdata Inc.
#

"""A tdk invocation run by a program instead of a person, set for that
invocation by a :class:`tabsdatak.cli._registry.CliExtension`.

While one is active, commands prompt for nothing, show no hints or notes on
stderr, speak JSON only and hand their result to :attr:`Machine.emit` as one
document; the limits it carries replace the interactive defaults."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Machine:
    """``command`` is the invocation as the program names it, for resuming it;
    ``max_wait`` the longest a command waits and what sets that; ``max_result``
    the most rows a listing returns (0 for no cap); ``max_bytes`` the most
    UTF-8 bytes the document a command emits takes; ``connect_attempts`` how
    many times a refused or failed connection to the server is retried;
    ``retry_attempts`` how many times a busy or failing server is;
    ``confine_to`` the directory every local file must resolve under."""

    emit: Callable[[Any], None]
    command: list[str] = field(default_factory=list)
    request_timeout: tuple[float, float] | None = None
    wait_seconds: float | None = None
    max_wait: tuple[float, str] | None = None
    max_result: int | None = None
    max_bytes: int | None = None
    connect_attempts: int | None = None
    retry_attempts: int | None = None
    log_cap_bytes: int | None = None
    logs_budget_bytes: int | None = None
    confine_to: str | None = None


_current: Machine | None = None


@contextmanager
def running(machine: Machine) -> Iterator[Machine]:
    global _current
    previous, _current = _current, machine
    try:
        yield machine
    finally:
        _current = previous


def machine() -> Machine | None:
    return _current
