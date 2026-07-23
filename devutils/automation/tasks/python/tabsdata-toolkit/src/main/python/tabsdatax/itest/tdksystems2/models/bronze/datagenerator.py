#
# Copyright 2026 Tabsdata Inc.
#

"""Data generator for the ``bronze`` model.

A medallion-bronze arrival-pattern simulator. Five tables, one per pattern a
source system uses to deliver data to a bronze layer:

  full_snapshot      — entire current state, every batch
  insup_soft_delete  — rows changed since last extract, with ``deleted`` flag
                           (insert + update + soft-delete)
  insup_no_delete    — rows changed since last extract, deletes invisible
                           (insert + update only)
  cdc_stream         — I/U/D operations following the ``@td.cdc.*`` contract
  append_events      — immutable events, unique key per row

Every table carries a monotonic ``sim_counter`` BIGINT (the only enforced PK).
The four non-CDC tables share a 10-column payload whose first two columns —
``tenant_id`` + ``entity_id`` — form a composite "business" key that is NOT
enforced by the schema. ``cdc_stream`` conforms to the ``@td.cdc.*`` CDC
contract: an ``op`` / ``tx`` / ``sq`` metadata triple plus an ``old`` + ``new``
pair per source column, so its identifiers carry ``@`` and ``.`` and must be
quoted. Reference-only: there is no delta recipe.

The initial size is the batch count (``n`` = the ``--batches`` prolific flag);
``--batch-size`` is pinned to 100 by the model sizing, so each table emits
``n * 100`` rows. Determinism replaces the v1 wall-clock timestamp and random
UUID with a fixed anchor and a seeded tx id.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta

from tabsdatax.itest.tdksystems2.datagen import Entity, Model
from tabsdatax.itest.tdksystems2.emit import Column, Table

_BASE_SEED = 42
_NOW = datetime(2025, 1, 1, 0, 0, 0)
_SPAN = timedelta(days=365)

TENANTS = ["T01", "T02", "T03"]
NAMES = [
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "iota",
    "kappa",
    "lambda",
    "mu",
    "nu",
    "xi",
    "omicron",
    "pi",
    "rho",
    "sigma",
    "tau",
    "upsilon",
    "phi",
    "chi",
    "psi",
    "omega",
]
STATUSES = ["active", "pending", "review", "archived", "draft"]
CATEGORIES = ["A", "B", "C", "D", "E"]


def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _rand_ts(now: datetime, span: timedelta) -> datetime:
    return now - timedelta(seconds=random.randint(0, int(span.total_seconds())))


def _payload(tenant_id: str, entity_id: int, now: datetime, span: timedelta) -> dict:
    """A source-row payload — the 10 'data' columns each arrival pattern emits,
    and the same shape that CDC tracks old/new for. is_active defaults to 1;
    soft-delete swaps it for ``deleted``."""
    return {
        "tenant_id": tenant_id,
        "entity_id": entity_id,
        "data_ts": _fmt_dt(_rand_ts(now, span)),
        "name": random.choice(NAMES),
        "status": random.choice(STATUSES),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(0, 10000), 2),
        "quantity": random.randint(0, 1000),
        "score": round(random.uniform(0, 1), 6),
        "is_active": 1,
    }


class _KeyMint:
    """Mints (tenant_id, entity_id) tuples where entity_id is unique only within
    its tenant — so the composite key is what disambiguates rows. Tracks a
    per-tenant high-water mark."""

    def __init__(self, tenants: list[str]):
        self._next: dict[str, int] = {t: 1 for t in tenants}
        self._tenants = list(tenants)

    def new(self) -> tuple[str, int]:
        t = random.choice(self._tenants)
        eid = self._next[t]
        self._next[t] += 1
        return (t, eid)


def _gen_full_snapshot(batches, batch_size, now, span):
    mint = _KeyMint(TENANTS)
    pool = [mint.new() for _ in range(batch_size)]
    rows, counter = [], 0
    churn = max(1, batch_size // 5) if batch_size > 1 else 0
    for b in range(batches):
        if b > 0 and churn:
            for i in random.sample(range(batch_size), min(churn, batch_size)):
                pool[i] = mint.new()
        for tid, eid in pool:
            counter += 1
            r = _payload(tid, eid, now, span)
            r["sim_counter"] = counter
            rows.append(r)
    return rows


def _gen_insup_soft_delete(batches, batch_size, now, span):
    mint = _KeyMint(TENANTS)
    pool: list[tuple[str, int]] = []
    rows, counter = [], 0
    for _ in range(batches):
        for _ in range(batch_size):
            counter += 1
            roll = random.random()
            if not pool or roll < 0.4:
                key = mint.new()
                pool.append(key)
                deleted = 0
            elif roll < 0.8:
                key = random.choice(pool)
                deleted = 0
            else:
                key = pool.pop(random.randrange(len(pool)))
                deleted = 1
            r = _payload(key[0], key[1], now, span)
            r.pop("is_active")
            r["deleted"] = deleted
            r["sim_counter"] = counter
            rows.append(r)
    return rows


def _gen_insup_no_delete(batches, batch_size, now, span):
    mint = _KeyMint(TENANTS)
    pool: list[tuple[str, int]] = []
    rows, counter = [], 0
    for _ in range(batches):
        for _ in range(batch_size):
            counter += 1
            if not pool or random.random() < 0.5:
                key = mint.new()
                pool.append(key)
            else:
                key = random.choice(pool)
            r = _payload(key[0], key[1], now, span)
            r["sim_counter"] = counter
            rows.append(r)
    return rows


def _gen_append_events(batches, batch_size, now, span):
    """Each row gets a fresh composite key — entity_id is the per-tenant event
    sequence number."""
    mint = _KeyMint(TENANTS)
    rows, counter = [], 0
    for _ in range(batches):
        for _ in range(batch_size):
            counter += 1
            tid, eid = mint.new()
            r = _payload(tid, eid, now, span)
            r["sim_counter"] = counter
            rows.append(r)
    return rows


SOURCE_COLS: list[tuple[str, str]] = [
    ("tenant_id", "varchar(20)"),
    ("entity_id", "bigint"),
    ("data_ts", "datetime"),
    ("name", "varchar(80)"),
    ("status", "varchar(20)"),
    ("category", "varchar(20)"),
    ("amount", "decimal(12,2)"),
    ("quantity", "int"),
    ("score", "double"),
    ("is_active", "smallint"),
]


def _new_tx_id() -> str:
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def _gen_cdc_stream(batches, batch_size, now, span):
    """Emit I/U/D rows following the @td.cdc.* contract.

    Consecutive rows occasionally share a tx id (runs of 1-3) so the consumer
    sees changes grouped into transactions. Per-key state is kept so updates
    emit a real before-image. Per spec, deletes emit the row-state in ``new``
    and NULL in ``old``."""
    mint = _KeyMint(TENANTS)
    live: dict[tuple[str, int], dict] = {}
    rows, counter = [], 0
    tx_id, tx_seq, tx_left = None, 0, 0
    for _ in range(batches):
        for _ in range(batch_size):
            counter += 1
            if tx_left == 0:
                tx_id, tx_seq, tx_left = _new_tx_id(), 1, random.randint(1, 3)
            else:
                tx_seq += 1
            tx_left -= 1

            roll = random.random()
            if not live or roll < 0.4:
                op = "i"
                key = mint.new()
                new_state = _payload(key[0], key[1], now, span)
                live[key] = new_state
                old_state = None
            elif roll < 0.8:
                op = "u"
                key = random.choice(list(live.keys()))
                old_state = live[key]
                new_state = _payload(key[0], key[1], now, span)
                live[key] = new_state
            else:
                op = "d"
                key = random.choice(list(live.keys()))
                new_state = live.pop(key)
                old_state = None

            r: dict = {
                "sim_counter": counter,
                "@td.cdc.meta.op": op,
                "@td.cdc.meta.tx": tx_id,
                "@td.cdc.meta.sq": tx_seq,
            }
            for src_col, _ in SOURCE_COLS:
                r[f"@td.cdc.data.col.old.{src_col}"] = (
                    old_state[src_col] if old_state else None
                )
                r[f"@td.cdc.data.col.new.{src_col}"] = (
                    new_state[src_col] if new_state else None
                )
            rows.append(r)
    return rows


def _data_cols(flag_name: str) -> list[Column]:
    """10 payload columns for the non-CDC tables. The first two — tenant_id +
    entity_id — form a composite business key, intentionally NOT enforced. The
    soft-delete table swaps ``is_active`` for ``deleted``."""
    return [
        Column("tenant_id", "varchar(20)", nullable=False),
        Column("entity_id", "bigint", nullable=False),
        Column("data_ts", "datetime", nullable=False),
        Column("name", "varchar(80)"),
        Column("status", "varchar(20)"),
        Column("category", "varchar(20)"),
        Column("amount", "decimal(12,2)"),
        Column("quantity", "int"),
        Column("score", "double"),
        Column(flag_name, "smallint", nullable=False),
    ]


def _cdc_cols() -> list[Column]:
    cols: list[Column] = [
        Column("sim_counter", "bigint", nullable=False),
        Column("@td.cdc.meta.op", "char(1)", nullable=False),
        Column("@td.cdc.meta.tx", "varchar(36)", nullable=False),
        Column("@td.cdc.meta.sq", "int", nullable=False),
    ]
    for src_col, src_type in SOURCE_COLS:
        cols.append(Column(f"@td.cdc.data.col.old.{src_col}", src_type))
        cols.append(Column(f"@td.cdc.data.col.new.{src_col}", src_type))
    return cols


SIM_COUNTER = Column("sim_counter", "bigint", nullable=False)


class _Bronze(Model):
    entities: list[Entity] = []  # reference-only: no deltas

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        random.seed(_BASE_SEED)
        batches = n
        batch_size = 100  # sizing.json pins --batch-size (default=min=cap=100)
        now, span = _NOW, _SPAN
        return [
            Table(
                "full_snapshot",
                [SIM_COUNTER, *_data_cols("is_active")],
                _gen_full_snapshot(batches, batch_size, now, span),
            ),
            Table(
                "insup_soft_delete",
                [SIM_COUNTER, *_data_cols("deleted")],
                _gen_insup_soft_delete(batches, batch_size, now, span),
            ),
            Table(
                "insup_no_delete",
                [SIM_COUNTER, *_data_cols("is_active")],
                _gen_insup_no_delete(batches, batch_size, now, span),
            ),
            Table(
                "cdc_stream",
                _cdc_cols(),
                _gen_cdc_stream(batches, batch_size, now, span),
            ),
            Table(
                "append_events",
                [SIM_COUNTER, *_data_cols("is_active")],
                _gen_append_events(batches, batch_size, now, span),
            ),
        ]


def datagenerator() -> Model:
    return _Bronze()
