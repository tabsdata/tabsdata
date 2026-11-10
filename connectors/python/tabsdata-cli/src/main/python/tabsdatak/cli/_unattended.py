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
# Each host sets its marker only in the shells its agent runs commands in.
UNATTENDED_MARKERS = (
    "CLAUDECODE",
    "CLAUDE_CODE",
    "GEMINI_CLI",
    "ANTIGRAVITY_AGENT",
    "CODEX_THREAD_ID",
    "CODEX_SANDBOX",
    "CODEX_CI",
    "CURSOR_AGENT",
    "VSCODE_AGENT",
    "COPILOT_CLI",
    "COPILOT_AGENT",
    "OPENCODE",
    "AUGMENT_AGENT",
    "JUNIE_DATA",
    "QWEN_CODE",
    "KILO",
    "CLINE_ACTIVE",
    "ROO_ACTIVE",
    "ROO_CLI_RUNTIME",
    "AI_AGENT",
    "AGENT",
)
_OFF = ("", "0", "false")


def coding_agent(env: Mapping[str, str] | None = None) -> str | None:
    """The marker variable of the coding agent this session runs in, if any;
    a marker set to ``0`` or ``false`` counts as unset."""
    env = os.environ if env is None else env
    return next(
        (
            marker
            for marker in UNATTENDED_MARKERS
            if (env.get(marker) or "").strip().lower() not in _OFF
        ),
        None,
    )


def unattended_cause(env: Mapping[str, str] | None = None) -> str | None:
    """The variable that makes the session unattended, as the message naming
    it shows it; ``None`` when the session is attended."""
    env = os.environ if env is None else env
    if (env.get(UNATTENDED_ENV) or "").strip() == "1":
        return f"{UNATTENDED_ENV}=1"
    marker = coding_agent(env)
    return None if marker is None else f"{marker} is set"
