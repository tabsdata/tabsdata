#
# Copyright 2026 Tabsdata Inc.
#

"""Whether nobody is there to answer a prompt: ``TDK_AGENT=1`` or a coding
agent's marker variable. tdk then prompts for nothing, with or without an
extension installed."""

from __future__ import annotations

import os
from collections.abc import Mapping

UNATTENDED_ENV = "TDK_AGENT"
UNATTENDED_MARKERS = ("CLAUDECODE", "GEMINI_CLI", "CODEX_THREAD_ID")


def unattended_cause(env: Mapping[str, str] | None = None) -> str | None:
    """The variable that makes the session unattended, as the message naming
    it shows it; ``None`` when the session is attended."""
    env = os.environ if env is None else env
    if (env.get(UNATTENDED_ENV) or "").strip() == "1":
        return f"{UNATTENDED_ENV}=1"
    for marker in UNATTENDED_MARKERS:
        if (env.get(marker) or "").strip():
            return f"{marker} is set"
    return None
