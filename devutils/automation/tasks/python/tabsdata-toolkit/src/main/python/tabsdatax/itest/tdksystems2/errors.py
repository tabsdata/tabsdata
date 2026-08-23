#
# Copyright 2026 Tabsdata Inc.
#

"""Application errors and a top-level ``run()`` wrapper.

``TdxError`` is the application-level exception for expected, user-facing
failures. ``run()`` prints ``ERROR: <reason>`` for any exception so nothing is
silently swallowed — unexpected ones also print a traceback for diagnosis.
"""

from __future__ import annotations

import os
import sys
import traceback
from typing import Callable

from tabsdatax.itest.tdksystems2 import constants


class TdxError(Exception):
    pass


class DecryptError(TdxError):
    """A descriptor is one of ours (PEM armor present) but won't decrypt with the
    active ``$TD_BACKENDS_CRYPTO_KEY`` — the key is wrong, or the file is corrupt,
    or it carries a foreign PEM label. Distinct from TdxError so enumeration can
    surface a bad key instead of skipping the file as a non-descriptor."""


class LegacyDescriptorError(TdxError):
    """A descriptor is in the superseded ``TDX2`` format. It is intact and the key
    is fine — it just has to be converted by ``tdx-system upgrade`` /
    ``tdx-domain upgrade`` before anything will read it. Distinct from
    DecryptError so callers can tell "run the migration" from "your key is
    wrong"."""


def _red(msg: str) -> str:
    if sys.stderr.isatty():
        return f"\033[31m{msg}\033[0m"
    return msg


def run(main_fn: Callable[[], int | None]) -> None:
    try:
        rc = main_fn() or 0
    except TdxError as exc:
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
    except BaseException as exc:  # noqa: BLE001 - top-level: report, don't leak
        print("", file=sys.stderr)
        print(_red(f"ERROR: {exc.__class__.__name__}: {exc}"), file=sys.stderr)
        if os.environ.get(constants.ENV_DEBUG):
            traceback.print_exc(file=sys.stderr)
        else:
            print(
                f"(set {constants.ENV_DEBUG}=1 for the full traceback)", file=sys.stderr
            )
        print("", file=sys.stderr)
        sys.exit(2)
    sys.exit(rc)
