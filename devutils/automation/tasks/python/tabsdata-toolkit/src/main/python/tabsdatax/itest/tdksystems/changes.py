#
# Copyright 2026 Tabsdata Inc.
#

"""Deterministic post-seed data changes for a model — the engine behind
`tdx-domain changes`.

A *change recipe* (``models/<name>/changes.py``) knows how to mutate the rows a
model seeds: which tables to touch, how to build a brand-new row, and how to
target an already-seeded row. This module turns a recipe plus a
:class:`ChangeSpec` (entities / ops / count / seed) into a deterministic list of
:class:`Transaction` s, and renders each :class:`Change` either to portable SQL
(for SQL domains) or to fresh rows (for file domains, which support inserts
only — a change is a new file).

The recipe is model-specific; the entity-selection / ops / count planning is
generic and lives here, so adding change support to a model is only writing its
:class:`Entity` s. The execution is system-specific and lives on the
``System`` backends (``apply_changes``), so adding a system that can take
changes is only implementing that one hook.

Determinism: the plan is a pure function of ``(recipe, spec)`` and the seeded
row counts. Re-running the same spec replays the same operations — updates and
tail-deletes are naturally idempotent; inserts that mint an explicit primary key
collide on a second run (recreate / reseed the domain between runs).
"""

from __future__ import annotations

import enum
import random
from dataclasses import dataclass, field
from pathlib import Path

from . import data_gen
from .emit import Column, Table, _sql_literal, _sql_quote_ident
from .errors import DatagenError


class OpSelector(str, enum.Enum):
    """The `--ops` selector — which DML verbs the run may emit."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    MIX = "mix"  # cycle insert -> update -> delete


class OpKind(str, enum.Enum):
    """A single change's verb."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


_MIX_CYCLE = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)

# A fixed `--seed N` reserves the insert-ordinal block [N*STRIDE, (N+1)*STRIDE),
# so distinct fixed seeds mint distinct inserts and a given seed reproduces the
# same ones. (`--seed auto` ignores this — it uses a compact persisted cursor.)
FIXED_SEED_INSERT_STRIDE = 1000

_B36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def base36(n: int, width: int = 0) -> str:
    """Non-negative int → base-36 string (``0-9a-z``), left-zero-padded to
    `width`. Recipes use it to pack a large insert ordinal into a short,
    unique CHAR key (e.g. a 4-char code)."""
    if n < 0:
        raise ValueError("base36 needs a non-negative int")
    out = ""
    while True:
        n, r = divmod(n, 36)
        out = _B36[r] + out
        if n == 0:
            return out.rjust(width, "0") if width else out


@dataclass
class Change:
    """One INSERT / UPDATE / DELETE against one table.

    `columns` are the table's column specs (for typed SQL literal rendering and
    for file emit). INSERT carries a full `row`; UPDATE carries `set_values`
    (column -> new value) and `where` (the primary-key predicate); DELETE
    carries `where`."""

    kind: OpKind
    table: str
    columns: list[Column]
    row: dict | None = None
    set_values: dict | None = None
    where: dict | None = None


# A transaction is an ordered list of changes applied atomically (one commit)
# on SQL systems. Parents come before children so foreign keys hold.
Transaction = list[Change]


@dataclass
class ChangeSpec:
    """The parsed `tdx-domain changes` request.

    `entities` names the recipe entities (tables) to mutate; every transaction
    applies `rows` changes to each of them, in the recipe's parents-first order.
    `seed` shifts UPDATE/DELETE targets so successive seeds touch different rows.
    `insert_base` is the starting ordinal for INSERTs: the `changes` verb passes
    a compact, persisted cursor for ``--seed auto`` (so inserts never collide
    across runs) or a seed-derived block for a fixed ``--seed N``. The recipe
    turns each ordinal into a non-colliding primary key."""

    entities: list[str]
    ops: OpSelector
    count: int  # number of transactions
    rows: int = 1  # changes per listed entity per transaction
    seed: int = 0
    insert_base: int = 0


# ======================================================================
# Recipe contract — implemented per model in models/<name>/changes.py
# ======================================================================


class Entity:
    """A model table `changes` may mutate.

    A recipe supplies one Entity per table it can change. `new_row` builds a
    deterministic brand-new row (with a non-colliding primary key) for INSERT
    #``i``; `target` returns the primary-key predicate of the ``i``-th seeded
    row (0-based, in primary-key order) for UPDATE / DELETE; `mutate` returns
    the column->new-value changes for an UPDATE of that row.

    Set `supports` to the subset of verbs the entity can take (e.g. a
    view-backed or append-only table may drop DELETE)."""

    table: str
    columns: list[Column]
    supports: tuple[OpKind, ...] = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)

    def new_row(self, rng: random.Random, i: int) -> dict:
        raise NotImplementedError

    def target(self, i: int, existing: int) -> dict:
        raise NotImplementedError

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        raise NotImplementedError


@dataclass
class ChangeModel:
    """A model's change recipe: the entities `changes` may mutate, ordered
    parents-first so a transaction that spans tables (and its inserts) satisfy
    foreign keys."""

    entities: list[Entity] = field(default_factory=list)


# ======================================================================
# Planning — generic (entities / ops / count), model-agnostic
# ======================================================================


def resolve_entities(model: ChangeModel, names: list[str]) -> list[Entity]:
    """Map the requested entity (table) `names` to the recipe's :class:`Entity`
    objects, in the recipe's own parents-first order (so a transaction that
    inserts across tables satisfies foreign keys) and de-duplicated. Fails if a
    name is not one the recipe defines."""
    if not names:
        raise DatagenError("changes: --entities must name at least one entity")
    known = {e.table: e for e in model.entities}
    unknown = [n for n in names if n not in known]
    if unknown:
        raise DatagenError(
            f"changes: unknown entit{'y' if len(unknown) == 1 else 'ies'} "
            f"{', '.join(unknown)} — the model's recipe defines: "
            f"{', '.join(known)}"
        )
    wanted = set(names)
    return [e for e in model.entities if e.table in wanted]


def plan(
    model: ChangeModel, spec: ChangeSpec, counts: dict[str, int]
) -> list[Transaction]:
    """Build `spec.count` transactions from `model`, mutating the entities named
    in `spec.entities` (`spec.rows` changes each, parents-first), using `counts`
    (the seeded per-table row counts, from the domain descriptor's `entities`) to
    bound UPDATE / DELETE targets. Deterministic for a given `spec.seed`."""
    if spec.count < 1:
        raise DatagenError("changes: --count must be >= 1")
    if spec.rows < 1:
        raise DatagenError("changes: --rows must be >= 1")
    entities = resolve_entities(model, spec.entities)
    rng = random.Random(spec.seed)
    planner = _Planner(model, spec, counts, rng, entities)
    txns: list[Transaction] = []
    for t in range(spec.count):
        txn = planner.build(t)
        if txn:
            txns.append(txn)
    if not txns:
        raise DatagenError(
            "changes: nothing to do — the requested entities / ops produced no "
            "operations (a domain seeded with --model but no --data has no rows "
            "to update/delete; use --ops insert or seed data first, or name "
            "entities that support the chosen ops)"
        )
    return txns


class _Planner:
    """Turns a transaction index into a transaction — `spec.rows` changes on each
    requested entity (parents-first) — walking a per-entity/per-kind cursor so
    successive transactions touch fresh rows."""

    def __init__(
        self,
        model: ChangeModel,
        spec: ChangeSpec,
        counts: dict[str, int],
        rng: random.Random,
        entities: list[Entity],
    ) -> None:
        self.model = model
        self.spec = spec
        self.counts = counts
        self.rng = rng
        self.entities = entities
        self._cursor: dict[tuple[str, OpKind], int] = {}
        self._inserts = 0  # global INSERT counter -> ordinal = insert_base + this
        self._mix = 0

    def _next_kind(self) -> OpKind:
        if self.spec.ops is OpSelector.MIX:
            k = _MIX_CYCLE[self._mix % len(_MIX_CYCLE)]
            self._mix += 1
            return k
        return OpKind(self.spec.ops.value)

    def _take(self, entity: Entity, kind: OpKind) -> Change | None:
        """Build the next change of `kind` on `entity`, or None if it can't
        (unsupported verb, or no seeded rows to target for update/delete)."""
        if kind not in entity.supports:
            return None
        if kind is OpKind.INSERT:
            # A globally-unique ordinal (compact for --seed auto, seed-blocked
            # for a fixed seed) that the recipe maps to a non-colliding key.
            i = self.spec.insert_base + self._inserts
            self._inserts += 1
            return Change(
                kind, entity.table, entity.columns, row=entity.new_row(self.rng, i)
            )
        existing = self.counts.get(entity.table, 0)
        if existing <= 0:
            return None
        key = (entity.table, kind)
        local = self._cursor.get(key, 0)
        self._cursor[key] = local + 1
        # Fold the seed in so successive seeds (--seed auto increments) touch
        # different rows; `% existing` keeps every target a valid seeded row.
        offset = (self.spec.seed + local) % existing
        if kind is OpKind.UPDATE:
            return Change(
                kind,
                entity.table,
                entity.columns,
                set_values=entity.mutate(self.rng, offset, existing),
                where=entity.target(offset, existing),
            )
        # DELETE walks in from the tail.
        idx = existing - 1 - offset
        return Change(
            kind, entity.table, entity.columns, where=entity.target(idx, existing)
        )

    def build(self, t: int) -> Transaction:
        """One transaction: `spec.rows` changes of this transaction's verb on
        each requested entity, parents-first. An entity that can't take the verb
        (or has no seeded row to target) contributes nothing."""
        kind = self._next_kind()
        out: Transaction = []
        for entity in self.entities:
            for _ in range(self.spec.rows):
                ch = self._take(entity, kind)
                if ch:
                    out.append(ch)
        return out


# ======================================================================
# Rendering — SQL (any dialect) and file rows
# ======================================================================


def render_sql(change: Change, quote: str = '"') -> str:
    """Render one change to a portable SQL statement (no trailing ``;``).

    `quote` quotes only non-bare identifiers (see `emit._sql_quote_ident`); the
    bundled models use bare lower-case names, so the default ANSI quote works on
    Postgres / Db2 / Oracle / MSSQL, and MySQL too (nothing gets quoted)."""
    cols = {c.name: c for c in change.columns}
    if change.kind is OpKind.INSERT:
        assert change.row is not None
        used = [c for c in change.columns if not c.auto and c.name in change.row]
        names = ",".join(_sql_quote_ident(c.name, quote) for c in used)
        vals = ",".join(_sql_literal(change.row.get(c.name), c.sql_type) for c in used)
        return f"INSERT INTO {change.table} ({names}) VALUES ({vals})"
    if change.kind is OpKind.UPDATE:
        assert change.set_values is not None and change.where is not None
        sets = ", ".join(
            f"{_sql_quote_ident(k, quote)} = {_sql_literal(v, cols[k].sql_type)}"
            for k, v in change.set_values.items()
        )
        where = _where_sql(change.where, cols, quote)
        return f"UPDATE {change.table} SET {sets} WHERE {where}"
    assert change.where is not None
    return f"DELETE FROM {change.table} WHERE {_where_sql(change.where, cols, quote)}"


def _where_sql(where: dict, cols: dict[str, Column], quote: str) -> str:
    return " AND ".join(
        f"{_sql_quote_ident(k, quote)} = {_sql_literal(v, cols[k].sql_type)}"
        for k, v in where.items()
    )


def insert_tables(txns: list[Transaction]) -> list[Table]:
    """Collect every INSERT change across `txns` into per-table :class:`Table`
    objects (recipe/plan order preserved) — for file domains, whose only change
    is a new file of new rows. Non-insert changes are ignored."""
    grouped: dict[str, Table] = {}
    for txn in txns:
        for ch in txn:
            if ch.kind is not OpKind.INSERT or ch.row is None:
                continue
            tbl = grouped.get(ch.table)
            if tbl is None:
                tbl = Table(name=ch.table, columns=ch.columns, rows=[])
                grouped[ch.table] = tbl
            tbl.rows.append(ch.row)
    return list(grouped.values())


def has_insert(txns: list[Transaction]) -> bool:
    return any(ch.kind is OpKind.INSERT for txn in txns for ch in txn)


def count_inserts(txns: list[Transaction]) -> int:
    """How many INSERT changes the plan holds — the amount to advance the
    domain's persisted insert cursor by after a ``--seed auto`` run."""
    return sum(1 for txn in txns for ch in txn if ch.kind is OpKind.INSERT)


# ======================================================================
# Recipe loading
# ======================================================================


def model_supports_changes(model: str) -> bool:
    """True if the model ships a `changes.py` recipe."""
    try:
        return (data_gen.model_dir(model) / "changes.py").is_file()
    except DatagenError:
        return False


def load_recipe(model: str) -> ChangeModel:
    """Import `models/<model>/changes.py` and return its :class:`ChangeModel`.

    The recipe module must expose ``recipe() -> ChangeModel``."""
    path = data_gen.model_dir(model) / "changes.py"
    if not path.is_file():
        raise DatagenError(
            f"model '{model}' has no changes.py — `tdx-domain changes` is not "
            f"supported for it"
        )
    module = _load_recipe_module(path)
    if not hasattr(module, "recipe"):
        raise DatagenError(f"{path} has no recipe() entry point")
    recipe = module.recipe()
    if not isinstance(recipe, ChangeModel):
        raise DatagenError(f"{path}: recipe() must return a ChangeModel")
    return recipe


def _load_recipe_module(path: Path):
    import importlib.util
    import sys

    mod_name = f"_changes_{path.parent.name.replace('-', '_')}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:
        raise DatagenError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(mod_name, None)
        raise
    return module
