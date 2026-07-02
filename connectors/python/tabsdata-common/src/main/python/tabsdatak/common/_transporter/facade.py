#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.common._transporter.handler.handler2 import TransporterHandler2

TRANSPORTER_MODE_LEGACY = 1
TRANSPORTER_MODE_CURRENT = 2

TRANSPORTER_MODE = TRANSPORTER_MODE_CURRENT


class UnsupportedTransporterModeError(ValueError):
    pass


def make_handler(mode: int = TRANSPORTER_MODE) -> TransporterHandler:
    match mode:
        case 1:
            return TransporterHandler1()
        case 2:
            return TransporterHandler2()
        case _:
            raise UnsupportedTransporterModeError(
                f"The transporter mode '{mode}' is not supported."
            )
