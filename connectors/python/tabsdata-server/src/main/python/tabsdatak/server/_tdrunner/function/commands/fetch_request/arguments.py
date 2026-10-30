#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    CommandArguments,
    option,
)


@dataclass(frozen=True, kw_only=True)
class FetchRequestArguments(CommandArguments):
    resolver_url: str = option(help="Resolver endpoint URL.")
    resolver_method: str = option(help="Resolver HTTP method.")
    resolver_authorization: str = option(help="Resolver Authorization header value.")
    request_file: Path = option(help="Path to write the fetched request body.")
