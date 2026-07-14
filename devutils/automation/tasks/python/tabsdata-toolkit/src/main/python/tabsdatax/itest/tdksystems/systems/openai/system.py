#
# Copyright 2026 Tabsdata Inc.
#

"""OpenAI service system — a keyed API account, credentials only.

The API key is 1Password-seeded as `$OPENAI__API_KEY`; there is nothing to
provision or tear down (see `_service.ServiceBase`)."""

from __future__ import annotations

from .._service import ServiceBase
from ..base import Option


class OpenAISystem(ServiceBase):
    slug = "openai"
    label = "OpenAI"
    options = (Option("API_KEY", "api-key", secret=True),)
