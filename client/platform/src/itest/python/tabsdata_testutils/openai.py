#
# Copyright 2026 Tabs Data Inc.
#
"""OpenAI external system for live integration tests.

Gated by ``$OPENAI`` — tests run unless ``$OPENAI`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``OPENAI`` both as the
gate env var and as the base for every suffix below. Follows the
standard ``<PREFIX>__<SUFFIX>`` convention used by every other
system — the gate is intentionally a separate env var from the API
key itself so the test-harness gating surface stays uniform.
Conventional bundle that ``openai.sh`` exports:

* ``OPENAI__API_KEY``  — OpenAI API key
* ``OPENAI__MODEL``    — model id (tests fall back to ``gpt-4o-mini`` when absent)
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class OPENAI(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "OPENAI"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["API_KEY", "MODEL"]
