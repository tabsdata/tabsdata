#
#  Copyright 2026. Tabs Data Inc.
#
"""Tiny shared helper for the ``--debug`` trace plumbed through the stack.

Every store / logic / metadata method takes ``debug: bool = False``;
when true, it calls :func:`log_call` at entry so the user can see
what's happening at each layer. Routed to stderr so structured
stdout output from a command stays pipeline-clean.
"""

from __future__ import annotations

import sys


def log_call(debug: bool, msg: str) -> None:
    """Print *msg* to stderr as a ``[debug]`` line iff *debug* is ``True``.

    Consolidated in one helper so every call site reads the same way
    and the prefix stays consistent when logs from multiple layers
    interleave.
    """
    if debug:
        print(f"[debug] {msg}", file=sys.stderr)
