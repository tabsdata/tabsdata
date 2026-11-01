#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.context.request import (
    FromRequestCtx,
)
from tabsdatak.spi import SrcPluginCtx


class SourceContext(FromRequestCtx, SrcPluginCtx):
    pass
