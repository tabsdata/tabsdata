#
# Copyright 2026 Tabsdata Inc.
#

"""Application errors and a top-level run() wrapper.

`DatagenError` is the application-level exception for expected, user-facing
failures. `run()` always prints `ERROR: <reason>` (with surrounding blank
lines) for any exception so nothing is silently swallowed — including
unexpected ones, which print a traceback for diagnosis.
"""

from __future__ import annotations

import sys
import traceback
from typing import Callable


class DatagenError(Exception):
    pass


def _red(msg: str) -> str:
    """Wrap `msg` in ANSI red — but only when stderr is a TTY, so piped or
    redirected output (e.g. setup.sh logs) stays plain text."""
    if sys.stderr.isatty():
        return f"\033[31m{msg}\033[0m"
    return msg


def run(main_fn: Callable[[], int | None]) -> None:
    try:
        rc = main_fn() or 0
    except DatagenError as exc:
        print("", file=sys.stderr)
        print(_red(f"ERROR: {exc}"), file=sys.stderr)
        print("", file=sys.stderr)
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("", file=sys.stderr)
        print(_red("ERROR: interrupted"), file=sys.stderr)
        print("", file=sys.stderr)
        sys.exit(130)
    except BaseException as exc:
        print("", file=sys.stderr)
        print(_red(f"ERROR: {exc.__class__.__name__}: {exc}"), file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        print("", file=sys.stderr)
        sys.exit(2)
    sys.exit(rc)
