#
# Copyright 2026 Tabsdata Inc.
#

"""Anthropic service system — a keyed API account, credentials only.

The API key is 1Password-seeded as `$ANTHROPIC__API_KEY`; there is nothing to
provision or tear down (see `_service.ServiceBase`)."""

from __future__ import annotations

from .._service import ServiceBase
from ..base import Option


class AnthropicSystem(ServiceBase):
    slug = "anthropic"
    label = "Anthropic"
    options = (Option("API_KEY", "api-key", secret=True),)
