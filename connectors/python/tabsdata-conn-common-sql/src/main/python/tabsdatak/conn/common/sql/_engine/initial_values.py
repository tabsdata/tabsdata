from __future__ import annotations

from typing import Any, Mapping

from tabsdatak.spi import SrcPluginCtx


def resolve_initial_values(
    ctx: SrcPluginCtx,
    seed: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Look up the active bind dict for this run.

    Walks ``seed`` (the user's first-run ``Src.initial_values``) bind
    name by bind name. For each, prefers ``ctx.get_attr(name)`` --
    that's where the previous run's persisted value lands -- and
    falls back to the seed when the ctx scratch has no entry for
    that name.

    Returns ``None`` when ``seed`` is None (no placeholders in any
    query), signalling no substitution is needed.

    Uses ``is not None`` rather than truthiness so a legitimately
    falsy value stored last run (``0``, ``False``, ``""``) isn't
    silently shadowed by the seed.
    """
    if seed is None:
        return None
    out: dict[str, Any] = {}
    for name, value in seed.items():
        from_ctx = ctx.get_attr(name)
        out[name] = from_ctx if from_ctx is not None else value
    return out
