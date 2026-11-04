#
# Copyright 2026 Tabsdata Inc.
#

"""Shared operations a domain fixture exposes by tapping the ``tdd domain`` CLI.

A provisioned domain fixture (any :class:`..backend.Domain` subclass) can drive
the very ``tdd domain`` verbs a developer runs by hand — ``create-model`` /
``create-data`` / ``modify-data`` — against the exact domain the fixture
resolved, without the test re-deriving the store plumbing. The domain is
addressed by name in the same store the fixtures load from; tdd decrypts with
the ambient ``$_TDD_CRYPTO_KEY``.

Each op parses the ``{entity: {insert, update, delete}}`` counts of the verb's
ok event into a :class:`ChangeReport`. ``run`` is the generic escape hatch (any
verb, any flags).
"""

from __future__ import annotations

from dataclasses import dataclass

from tabsdatax.itest.fixture import _tdd


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


def run(verb: str, domain: str, *args: str) -> dict:
    """Run ``tdd domain <verb> --domain <domain> [args...]`` and return its ok
    event; a failure raises :class:`._tdd.TddError` with tdd's message."""
    return _tdd.run(_tdd.DOMAIN, verb, "--domain", domain, *args)


def _parse(ok: dict) -> ChangeReport:
    """The ok event's ``entities`` counts as a ChangeReport."""
    return ChangeReport({
        entity: EntityChange(
            insert=int(counts.get("insert", 0)),
            update=int(counts.get("update", 0)),
            delete=int(counts.get("delete", 0)),
        )
        for entity, counts in ok["entities"].items()
    })


def create_model(domain: str, *, model: str, force: bool = False) -> ChangeReport:
    """Install `model`'s schema via ``tdd domain create-model`` (entities at 0)."""
    args = ["--model", model]
    if force:
        args.append("--force")
    return _parse(run("create-model", domain, *args))


def create_data(domain: str, *, size: int = 1, force: bool = False) -> ChangeReport:
    """Load deterministic initial data via ``tdd domain create-data`` (`size` x
    base row counts, all inserts)."""
    args = ["--size", str(size)]
    if force:
        args.append("--force")
    return _parse(run("create-data", domain, *args))


def modify_data(
    domain: str,
    *,
    entity: str,
    sets: int,
    ops: str = "Mix",
    seed: str | int | None = None,
) -> ChangeReport:
    """Apply deterministic INSERT/UPDATE/DELETE changes via ``tdd domain
    modify-data``. ``entity`` is a single entity name or ``ALL``; ``sets`` is the
    number of change sets (for a single entity, the row count); ``ops`` is
    ``Insert``/``Update``/``Delete``/``Mix``; ``seed`` is ``AUTO`` or a fixed int.
    """
    args = ["--entity", entity, "--sets", str(sets), "--ops", ops]
    if seed is not None:
        args += ["--seed", str(seed)]
    return _parse(run("modify-data", domain, *args))


def capture_table(domain: str, *, table: str, ddl: str | None = None) -> str:
    """Register `table` for change capture via ``tdd domain capture-table``,
    creating it from `ddl` first when given; returns its CD table."""
    args = ["--table", table]
    if ddl is not None:
        args += ["--ddl", ddl]
    return run("capture-table", domain, *args)["cd_table"]


def release_table(domain: str, *, table: str, drop: bool = False) -> None:
    """Undo :func:`capture_table` via ``tdd domain release-table``."""
    args = ["--table", table]
    if drop:
        args.append("--drop")
    run("release-table", domain, *args)
