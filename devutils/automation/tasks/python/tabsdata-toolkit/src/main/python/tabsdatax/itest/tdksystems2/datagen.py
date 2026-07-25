#
# Copyright 2026 Tabsdata Inc.
#

"""Generic deterministic data generation — initial data and post-seed deltas.

A *model* (``models/<name>/``) ships:

  * ``entities.json`` — the list of entity (table) names it generates.
  * ``datagenerator.py`` — a ``datagenerator() -> Model`` factory. The ``Model``
    knows how to build a deterministic **initial** dataset of a given size, and
    declares one ``Entity`` per table that supports **deltas** (which ops, how
    many rows per set, how to build/target/mutate a row).

This module owns the base size / multiplier, the deterministic delta *planner*
(entity-selection / ops / sets / seed, model-agnostic) and the SQL rendering.
Determinism: the plan is a pure function of ``(model, spec, seeded counts)``, so
the same call sequence with the same options reproduces the same data. A fixed
``--seed N`` reserves the insert-ordinal block ``[N*STRIDE, (N+1)*STRIDE)`` so
distinct seeds mint distinct inserts and each seed replays the same ones.
``--seed auto`` just uses the last persisted seed + 1 (same block rule), so the
seed sequence alone makes a run repeatable — no separate insert cursor is kept.
"""

from __future__ import annotations

import enum
import json
import random
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

from tabsdatax.itest.tdksystems2.emit import Column, Table, sql_literal, sql_quote_ident
from tabsdatax.itest.tdksystems2.errors import TdxError

# create-data --size is an integer multiplier: 1 is the base, N scales the
# sizeable entities to N * BASE_SIZE rows (fixed reference entities don't grow).
BASE_SIZE = 100

# A fixed `--seed N` reserves the insert-ordinal block [N*STRIDE, (N+1)*STRIDE).
FIXED_SEED_INSERT_STRIDE = 1000

# Initial data is seeded from this fixed base so a given size is reproducible.
INITIAL_SEED = 42

_B36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def base36(n: int, width: int = 0) -> str:
    """Non-negative int -> base-36 string (``0-9a-z``), left-zero-padded to
    `width`. Delta recipes use it to pack a large insert ordinal into a short,
    unique CHAR key."""
    if n < 0:
        raise ValueError("base36 needs a non-negative int")
    out = ""
    while True:
        n, r = divmod(n, 36)
        out = _B36[r] + out
        if n == 0:
            return out.rjust(width, "0") if width else out


class OpKind(str, enum.Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class OpSelector(str, enum.Enum):
    """The ``--ops`` selector — which verbs a modify-data run may emit."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    MIX = "mix"  # cycle insert -> update -> delete


_MIX_CYCLE = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)


@dataclass
class Change:
    """One INSERT / UPDATE / DELETE against one table."""

    kind: OpKind
    table: str
    columns: list[Column]
    row: dict | None = None
    set_values: dict | None = None
    where: dict | None = None


Transaction = list[Change]


# ======================================================================
# Model contract — implemented per model in models/<name>/datagenerator.py
# ======================================================================


class Entity:
    """A model table that ``modify-data`` may mutate.

    ``new_row`` builds a deterministic brand-new row (non-colliding PK) for
    INSERT ``#i``; ``target`` returns the PK predicate of the ``i``-th seeded row
    (0-based) for UPDATE / DELETE; ``mutate`` returns the column->new-value
    changes for an UPDATE. ``supports`` narrows the verbs an entity can take;
    ``rows_per_set`` is how many rows a ``--entity ALL`` run applies to this
    entity per set."""

    table: str
    columns: list[Column]
    supports: tuple[OpKind, ...] = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)
    rows_per_set: int = 1

    def new_row(self, rng: random.Random, i: int) -> dict:
        raise NotImplementedError

    def target(self, i: int, existing: int) -> dict:
        raise NotImplementedError

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        raise NotImplementedError


class Model:
    """A model: the delta entities plus a deterministic initial-data builder.

    ``entities`` is ordered parents-first so a transaction (and its inserts) that
    spans tables satisfies foreign keys. ``build_initial`` returns every
    generated table (a superset of the delta entities) in FK-safe load order."""

    entities: list[Entity] = []

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        raise NotImplementedError


@dataclass
class ChangeSpec:
    """A parsed ``modify-data`` request. ``entities`` names the entities to touch;
    ``rows_per_entity`` is how many changes each gets per set; ``sets`` is the
    number of transactions. ``seed`` shifts UPDATE/DELETE targets; ``insert_base``
    is the starting INSERT ordinal (the seed's own block, ``seed * STRIDE``)."""

    entities: list[str]
    ops: OpSelector
    sets: int
    rows_per_entity: dict[str, int]
    seed: int = 0
    insert_base: int = 0


# ======================================================================
# Planning — generic (entity-selection / ops / sets), model-agnostic
# ======================================================================


def resolve_entities(model: Model, names: list[str]) -> list[Entity]:
    """Map requested `names` to the model's Entity objects, in model
    (parents-first) order, de-duplicated. Fails on an unknown name."""
    if not names:
        raise TdxError("modify-data: --entity must name at least one entity")
    known = {e.table: e for e in model.entities}
    unknown = [n for n in names if n not in known]
    if unknown:
        raise TdxError(
            f"modify-data: unknown entit{'y' if len(unknown) == 1 else 'ies'} "
            f"{', '.join(unknown)} — this model changes: {', '.join(known)}"
        )
    wanted = set(names)
    return [e for e in model.entities if e.table in wanted]


def plan(model: Model, spec: ChangeSpec, counts: dict[str, int]) -> list[Transaction]:
    """Build ``spec.sets`` transactions, applying ``spec.rows_per_entity`` changes
    to each requested entity per transaction (parents-first). ``counts`` (the
    seeded per-table row counts) bounds UPDATE / DELETE targets. Deterministic
    for a given ``spec.seed`` / ``spec.insert_base``."""
    if spec.sets < 1:
        raise TdxError("modify-data: --sets must be >= 1")
    entities = resolve_entities(model, spec.entities)
    rng = random.Random(spec.seed)
    planner = _Planner(spec, counts, rng, entities)
    txns: list[Transaction] = []
    for t in range(spec.sets):
        txn = planner.build(t)
        if txn:
            txns.append(txn)
    if not txns:
        raise TdxError(
            "modify-data: nothing to do — the requested entities / ops produced no "
            "operations (a domain with no data has no rows to update/delete; use "
            "--ops Insert or create-data first, or name entities that support the "
            "chosen op)"
        )
    return txns


def op_counts(transactions: list[Transaction]) -> dict[str, dict[OpKind, int]]:
    """Per-table INSERT / UPDATE / DELETE counts a plan applies, in first-touched
    order. ``modify-data`` prints this so the user sees exactly what each table
    got. Every listed table maps all three ops (missing ones are 0)."""
    counts: dict[str, dict[OpKind, int]] = {}
    for txn in transactions:
        for ch in txn:
            per = counts.setdefault(
                ch.table, {OpKind.INSERT: 0, OpKind.UPDATE: 0, OpKind.DELETE: 0}
            )
            per[ch.kind] += 1
    return counts


def net_row_deltas(transactions: list[Transaction]) -> dict[str, int]:
    """Net per-table row-count change a plan applies: ``+1`` per INSERT, ``-1``
    per DELETE, ``0`` per UPDATE. ``modify-data`` adds this to the domain's
    recorded ``entities`` counts so ``info`` / ``status`` and the next run's
    UPDATE / DELETE bounds stay in step with the data."""
    deltas: dict[str, int] = {}
    for txn in transactions:
        for ch in txn:
            if ch.kind is OpKind.INSERT:
                deltas[ch.table] = deltas.get(ch.table, 0) + 1
            elif ch.kind is OpKind.DELETE:
                deltas[ch.table] = deltas.get(ch.table, 0) - 1
    return deltas


class _Planner:
    def __init__(
        self,
        spec: ChangeSpec,
        counts: dict[str, int],
        rng: random.Random,
        entities: list[Entity],
    ) -> None:
        self.spec = spec
        self.counts = counts
        self.rng = rng
        self.entities = entities
        self._cursor: dict[tuple[str, OpKind], int] = {}
        self._inserts = 0
        self._mix = 0

    def _next_kind(self) -> OpKind:
        if self.spec.ops is OpSelector.MIX:
            k = _MIX_CYCLE[self._mix % len(_MIX_CYCLE)]
            self._mix += 1
            return k
        return OpKind(self.spec.ops.value)

    def _take(self, entity: Entity, kind: OpKind) -> Change | None:
        if kind not in entity.supports:
            return None
        if kind is OpKind.INSERT:
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
        offset = (self.spec.seed + local) % existing
        if kind is OpKind.UPDATE:
            return Change(
                kind,
                entity.table,
                entity.columns,
                set_values=entity.mutate(self.rng, offset, existing),
                where=entity.target(offset, existing),
            )
        idx = existing - 1 - offset  # DELETE walks in from the tail
        return Change(
            kind, entity.table, entity.columns, where=entity.target(idx, existing)
        )

    def build(self, t: int) -> Transaction:
        kind = self._next_kind()
        out: Transaction = []
        for entity in self.entities:
            for _ in range(self.spec.rows_per_entity.get(entity.table, 1)):
                ch = self._take(entity, kind)
                if ch:
                    out.append(ch)
        return out


# ======================================================================
# Rendering — SQL
# ======================================================================


def render_sql(change: Change, quote: str = '"') -> str:
    """Render one change to a portable SQL statement (no trailing ``;``)."""
    cols = {c.name: c for c in change.columns}
    if change.kind is OpKind.INSERT:
        assert change.row is not None
        used = [c for c in change.columns if not c.auto and c.name in change.row]
        names = ",".join(sql_quote_ident(c.name, quote) for c in used)
        vals = ",".join(sql_literal(change.row.get(c.name), c.sql_type) for c in used)
        return f"INSERT INTO {change.table} ({names}) VALUES ({vals})"
    if change.kind is OpKind.UPDATE:
        assert change.set_values is not None and change.where is not None
        sets = ", ".join(
            f"{sql_quote_ident(k, quote)} = {sql_literal(v, cols[k].sql_type)}"
            for k, v in change.set_values.items()
        )
        where = _where_sql(change.where, cols, quote)
        return f"UPDATE {change.table} SET {sets} WHERE {where}"
    assert change.where is not None
    return f"DELETE FROM {change.table} WHERE {_where_sql(change.where, cols, quote)}"


def _where_sql(where: dict, cols: dict[str, Column], quote: str) -> str:
    return " AND ".join(
        f"{sql_quote_ident(k, quote)} = {sql_literal(v, cols[k].sql_type)}"
        for k, v in where.items()
    )


# ======================================================================
# Model discovery / loading
# ======================================================================


def _models_root() -> Path:
    return Path(__file__).parent / "models"


def list_models() -> list[str]:
    """All models on disk (dir with both ``datagenerator.py`` and
    ``entities.json``)."""
    root = _models_root()
    if not root.is_dir():
        return []
    out = []
    for child in sorted(root.iterdir()):
        if (child / "datagenerator.py").is_file() and (
            child / "entities.json"
        ).is_file():
            out.append(child.name)
    return out


def model_dir(name: str) -> Path:
    d = _models_root() / name
    if not (d / "datagenerator.py").is_file():
        raise TdxError(f"unknown model: {name} (known: {', '.join(list_models())})")
    return d


@dataclass
class EntityInfo:
    """One ``entities.json`` entry: the table `name` and whether it is
    `sizeable` — i.e. its row count grows with the ``--size`` multiplier
    (fixed reference entities, e.g. countries, are not sizeable)."""

    name: str
    sizeable: bool = True


def entity_specs(name: str) -> list[EntityInfo]:
    """The entities a model generates, in FK-safe order, with their sizeable
    markers, from ``entities.json`` (a list of ``{"name", "sizeable"}`` objects;
    a bare string is treated as sizeable for backward compatibility)."""
    path = model_dir(name) / "entities.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise TdxError(f"{path}: entities.json must be a list")
    out: list[EntityInfo] = []
    for e in data:
        if isinstance(e, str):
            out.append(EntityInfo(e, True))
        elif isinstance(e, dict) and isinstance(e.get("name"), str):
            out.append(EntityInfo(e["name"], bool(e.get("sizeable", True))))
        else:
            raise TdxError(f"{path}: each entity must be a name or {{name, sizeable}}")
    return out


def entities_of(name: str) -> list[str]:
    """The entity (table) names a model generates, from ``entities.json``."""
    return [e.name for e in entity_specs(name)]


def sizeable_entities(name: str) -> list[str]:
    """The names of the entities that grow with the ``--size`` multiplier."""
    return [e.name for e in entity_specs(name) if e.sizeable]


def load_model(name: str) -> Model:
    """Import ``models/<name>/datagenerator.py`` and return its ``Model``."""
    model_dir(name)  # validates existence
    module = import_module(f"{__package__}.models.{name}.datagenerator")
    if not hasattr(module, "datagenerator"):
        raise TdxError(f"model '{name}': datagenerator.py has no datagenerator() entry")
    model = module.datagenerator()
    if not isinstance(model, Model):
        raise TdxError(f"model '{name}': datagenerator() must return a Model")
    return model


def supported_ops(model: Model) -> set[OpKind]:
    ops: set[OpKind] = set()
    for e in model.entities:
        ops.update(e.supports)
    return ops


_OP_LETTER = {OpKind.INSERT: "I", OpKind.UPDATE: "U", OpKind.DELETE: "D"}
_OP_ORDER = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)


def ops_tag(supports: tuple[OpKind, ...]) -> str:
    """Compact ``I/U/D`` label for an entity's supported ``modify-data`` ops, in a
    stable insert/update/delete order."""
    return "/".join(_OP_LETTER[k] for k in _OP_ORDER if k in supports)


def op_applies(supports: tuple[OpKind, ...], ops: OpSelector) -> bool:
    """Whether an entity with `supports` takes at least one change under the `ops`
    selector — ``MIX`` acts if it supports any op; a single op must be supported."""
    if ops is OpSelector.MIX:
        return len(supports) > 0
    return OpKind(ops.value) in supports


def iud(insert: int = 0, update: int = 0, delete: int = 0) -> dict[str, int]:
    """The canonical per-entity counts object for ``--out json``:
    ``{"insert": .., "update": .., "delete": ..}``."""
    return {"insert": insert, "update": update, "delete": delete}
