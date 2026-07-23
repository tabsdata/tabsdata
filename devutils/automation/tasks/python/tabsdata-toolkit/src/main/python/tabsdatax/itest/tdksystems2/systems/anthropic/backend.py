#
# Copyright 2026 Tabsdata Inc.
#

"""Anthropic — a credential-only service system (one API key)."""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.systems._common.service import ServiceBackend


class Anthropic(ServiceBackend):
    slug = "anthropic"
    label = "Anthropic"
    env_prefix = "ANTHROPIC"
    cred_keys = ("API_KEY",)
