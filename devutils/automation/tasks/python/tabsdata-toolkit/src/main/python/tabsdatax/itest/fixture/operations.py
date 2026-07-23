#
# Copyright 2026 Tabsdata Inc.
#

"""Shared operations a domain fixture exposes by tapping the ``tdx-domain2`` CLI.

A provisioned domain fixture (any :class:`..backend.Domain` subclass) can drive
the very ``tdx-domain2`` subcommands a developer runs by hand — ``create-model``
/ ``create-data`` / ``modify-data`` — against the exact domain the fixture
resolved, without the test re-deriving the store plumbing. The domain is
addressed by name in the same store the fixtures load from (``$TD_BACKENDS_DIR``,
or ``domain_dir``); the descriptor secret is read from the ambient
``$TD_BACKENDS_CRYPTO_KEY`` the CLI already defaults to.

Each op runs the verb with ``--out json`` and parses its machine-readable
``{entity: {insert, update, delete}}`` report into a :class:`ChangeReport`.
``run`` is the generic escape hatch (any verb, any flags).
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

TDX_DOMAIN = "tdx-domain2"
OUT_JSON = "json"


@dataclass(frozen=True)
class EntityChange:
    """Rows inserted / updated / deleted on one entity by a command."""

    insert: int = 0
    update: int = 0
    delete: int = 0


@dataclass(frozen=True)
class ChangeReport:
    """A command's ``{entity: {insert, update, delete}}`` output as a dataclass."""

    entities: dict[str, EntityChange]

    @property
    def inserted(self) -> int:
        return sum(e.insert for e in self.entities.values())

    @property
    def updated(self) -> int:
        return sum(e.update for e in self.entities.values())

    @property
    def deleted(self) -> int:
        return sum(e.delete for e in self.entities.values())


def run(
    verb: str,
    domain: str,
    *args: str,
    domain_dir: str | Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run ``tdx-domain2 <verb> --domain <domain> [args...]`` as a subprocess.

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


def _parse(cp: subprocess.CompletedProcess[str]) -> ChangeReport:
    """Parse a verb's ``--out json`` report (on stdout) into a ChangeReport."""
    text = cp.stdout.strip()
    try:
        raw = json.loads(text)
    except ValueError as exc:  # the JSON object is the sole stdout content
        start = text.find("{")
        if start < 0:
            raise RuntimeError(f"no JSON report in output:\n{text}") from exc
        raw = json.loads(text[start:])
    return ChangeReport({
        entity: EntityChange(
            insert=int(counts.get("insert", 0)),
            update=int(counts.get("update", 0)),
            delete=int(counts.get("delete", 0)),
        )
        for entity, counts in raw.items()
    })


def create_model(
    domain: str,
    *,
    model: str,
    force: bool = False,
    domain_dir: str | Path | None = None,
    check: bool = True,
) -> ChangeReport:
    """Install `model`'s schema via ``tdx-domain2 create-model`` (entities at 0)."""
    args = ["--model", model, "--out", OUT_JSON]
    if force:
        args.append("--force")
    cp = run("create-model", domain, *args, domain_dir=domain_dir, check=check)
    return _parse(cp)


def create_data(
    domain: str,
    *,
    size: int = 1,
    force: bool = False,
    domain_dir: str | Path | None = None,
    check: bool = True,
) -> ChangeReport:
    """Load deterministic initial data via ``tdx-domain2 create-data`` (`size` x
    base row counts, all inserts)."""
    args = ["--size", str(size), "--out", OUT_JSON]
    if force:
        args.append("--force")
    return _parse(run("create-data", domain, *args, domain_dir=domain_dir, check=check))


def modify_data(
    domain: str,
    *,
    entity: str,
    sets: int,
    ops: str = "Mix",
    seed: str | int | None = None,
    domain_dir: str | Path | None = None,
    check: bool = True,
) -> ChangeReport:
    """Apply deterministic INSERT/UPDATE/DELETE changes via ``tdx-domain2
    modify-data``. ``entity`` is a single entity name or ``ALL``; ``sets`` is the
    number of change sets (for a single entity, the row count); ``ops`` is
    ``Insert``/``Update``/``Delete``/``Mix``; ``seed`` is ``AUTO`` or a fixed int.
    """
    args = ["--entity", entity, "--sets", str(sets), "--ops", ops, "--out", OUT_JSON]
    if seed is not None:
        args += ["--seed", str(seed)]
    return _parse(run("modify-data", domain, *args, domain_dir=domain_dir, check=check))
