#
# Copyright 2026 Tabsdata Inc.
#

"""Shared operations a domain fixture exposes by tapping the ``tdx-domain`` CLI.

A provisioned domain fixture (any :class:`..backend.Domain` subclass) can drive
the very ``tdx-domain`` subcommands a developer runs by hand — notably
``changes`` — against the exact domain the fixture resolved, without the test
re-deriving the store plumbing. The domain is addressed by name in the same
store the fixtures load from (``$TD_BACKENDS_DIR``, or ``domain_dir``); the
descriptor secret is read from the ambient ``$TD_BACKENDS_CRYPTO_KEY`` the CLI
already defaults to (the same one that provisioned the domain), so nothing
secret has to be threaded through here.

``run`` is the generic escape hatch (any verb, any flags); ``changes`` is the
typed convenience over ``tdx-domain changes`` covering all of its options.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

TDX_DOMAIN = "tdx-domain"


def run(
    verb: str,
    domain: str,
    *args: str,
    domain_dir: str | Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run ``tdx-domain <verb> --domain <domain> [args...]`` as a subprocess.

    ``domain_dir`` overrides the store dir (defaults to ``$TD_BACKENDS_DIR``,
    what the CLI and the fixtures already use); the descriptor secret is taken
    from the ambient ``$TD_BACKENDS_CRYPTO_KEY`` the CLI defaults to. With
    ``check`` the call raises with the captured output on a non-zero exit;
    either way the completed process (stdout / stderr captured as text) is
    returned.
    """
    argv = [TDX_DOMAIN, verb, "--domain", domain, *args]
    if domain_dir is not None:
        argv += ["--domain-dir", str(domain_dir)]
    cp = subprocess.run(argv, capture_output=True, text=True)
    if check and cp.returncode != 0:
        raise RuntimeError(
            f"`{' '.join(argv)}` failed (exit {cp.returncode}):\n{cp.stdout}{cp.stderr}"
        )
    return cp


def changes(
    domain: str,
    *,
    entities: str | list[str],
    count: int,
    ops: str = "mix",
    rows: int | None = None,
    seed: str | int | None = None,
    latch: str | Path | None = None,
    domain_dir: str | Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Apply deterministic INSERT/UPDATE/DELETE changes to a model-seeded domain
    via ``tdx-domain changes`` (drives CDC).

    Mirrors the CLI one-to-one: ``entities`` names the recipe entities (tables)
    to mutate — a comma string or a list; ``count`` is the number of
    transactions; ``ops`` is ``insert``/``update``/``delete``/``mix`` (default
    ``mix``); ``rows`` is the changes per listed entity per transaction (CLI
    default 1); ``seed`` is ``auto`` or a fixed integer (CLI default ``auto``);
    ``latch`` writes ``FILE.ok``/``FILE.err`` for a background waiter. Only
    works for a domain whose model ships a ``changes.py`` recipe.
    """
    names = entities if isinstance(entities, str) else ",".join(entities)
    args = ["--entities", names, "--count", str(count), "--ops", ops]
    if rows is not None:
        args += ["--rows", str(rows)]
    if seed is not None:
        args += ["--seed", str(seed)]
    if latch is not None:
        args += ["--latch", str(latch)]
    return run("changes", domain, *args, domain_dir=domain_dir, check=check)
