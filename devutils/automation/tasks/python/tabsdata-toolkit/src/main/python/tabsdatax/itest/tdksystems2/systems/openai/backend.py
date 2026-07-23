#
# Copyright 2026 Tabsdata Inc.
#

"""OpenAI — a credential-only service system (one API key)."""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.systems._common.service import ServiceBackend


class Openai(ServiceBackend):
    slug = "openai"
    label = "OpenAI"
    env_prefix = "OPENAI"
    cred_keys = ("API_KEY",)
