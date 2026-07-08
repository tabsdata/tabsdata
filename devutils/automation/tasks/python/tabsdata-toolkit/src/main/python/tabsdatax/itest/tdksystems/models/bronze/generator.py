#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#

"""Bronze medallion arrival-pattern simulator.

Five tables, one per pattern a source system uses to deliver data to
a medallion-bronze layer:

  brz_full_snapshot      — entire current state, every batch
  brz_insup_soft_delete  — rows changed since last extract, with `deleted` flag
                           (insert + update + soft-delete)
  brz_insup_no_delete    — rows changed since last extract, deletes invisible
                           (insert + update only)
  brz_cdc_stream         — I/U/D operations following the @td.cdc.* contract
  brz_append_events      — immutable events, unique key per row

Every table carries a monotonic ``sim_counter`` BIGINT (the only enforced
PK) so a consumer can replay the next arrival with::

    SELECT ... FROM <tbl> WHERE sim_counter > :last
    ORDER BY sim_counter LIMIT :batch_size

The four non-CDC tables share a 10-column payload whose first two
columns — ``tenant_id`` (VARCHAR) and ``entity_id`` (BIGINT) — form a
composite "business" key that is *NOT* enforced by the schema;
consumers have to exercise composite-key dedup / upsert logic. The
soft-delete table swaps ``is_active`` for ``deleted``.

``brz_cdc_stream`` conforms to the ``@td.cdc.*`` CDC contract:

  Metadata columns
    "@td.cdc.meta.op"  CHAR(1)      'i' | 'u' | 'd'
    "@td.cdc.meta.tx"  VARCHAR(36)  source transaction id (UUID-ish)
    "@td.cdc.meta.sq"  INT          per-tx sequence number

  Per source column — one "old" + one "new":
    "@td.cdc.data.col.old.<COL>"
    "@td.cdc.data.col.new.<COL>"

  Semantics (values_format=columns, flatten_values=False):
    Insert (i)  : old = NULL,        new = inserted data
    Update (u)  : old = before-image, new = after-image
    Delete (d)  : old = NULL,        new = deleted data

Those column names contain `@` and `.` — not valid in a bare SQL
identifier — so DDLs and INSERTs use quoted identifiers. The quote
char is engine-specific (backticks for MySQL, double quotes for
Postgres / MSSQL / Oracle / Db2). The generator reads the target
engine slug from the ``TDX_ENGINE`` env var (set by
the data load when loading into a SQL container domain).

Usage:
    python generator.py --batches N [--batch-size M] [options]

    --batches N      Number of batches per table  (required)
    --batch-size M   Rows per batch               (default: 100)
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables

BASE_SEED = 42
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

HERE = Path(__file__).resolve().parent


def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _rand_ts(now: datetime, span: timedelta) -> datetime:
    return now - timedelta(seconds=random.randint(0, int(span.total_seconds())))


def _payload(tenant_id: str, entity_id: int, now: datetime, span: timedelta) -> dict:
    """A source-row payload — the 10 'data' columns each arrival
    pattern emits, and the same shape that CDC tracks old/new for.
    is_active defaults to 1; soft-delete swaps it for `deleted`."""
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
    """Mints (tenant_id, entity_id) tuples where entity_id is unique
    only within its tenant — so the composite key is what disambiguates
    rows. Tracks a per-tenant high-water mark."""

    def __init__(self, tenants: list[str]):
        self._next: dict[str, int] = {t: 1 for t in tenants}
        self._tenants = list(tenants)

    def new(self) -> tuple[str, int]:
        t = random.choice(self._tenants)
        eid = self._next[t]
        self._next[t] += 1
        return (t, eid)


# ---------------------------------------------------------------------------
# Per-pattern row generators
# ---------------------------------------------------------------------------


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
    """Each row gets a fresh composite key — entity_id is the per-tenant
    event sequence number."""
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


# ---------------------------------------------------------------------------
# CDC stream — @td.cdc.* contract
# ---------------------------------------------------------------------------

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
    return str(uuid.uuid4())


def _gen_cdc_stream(batches, batch_size, now, span):
    """Emit I/U/D rows following the @td.cdc.* contract.

    Consecutive rows occasionally share a tx id (runs of 1–3) so the
    consumer sees changes grouped into transactions. Per-key state is
    kept so updates emit a real before-image. Per spec, deletes emit
    the row-state in `new` and NULL in `old`."""
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


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _data_cols(flag_name: str) -> list[Column]:
    """10 payload columns for the non-CDC tables. The first two —
    tenant_id + entity_id — form a composite business key, intentionally
    NOT enforced. The soft-delete table swaps `is_active` for `deleted`."""
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


def build_tables(args):
    if args.batches < 1:
        raise ValueError(f"--batches must be >= 1 (got {args.batches})")
    if args.batch_size < 1:
        raise ValueError(f"--batch-size must be >= 1 (got {args.batch_size})")
    random.seed(args.seed)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    span = timedelta(days=365)
    print(
        f"Generating bronze data: batches={args.batches} "
        f"batch_size={args.batch_size} seed={args.seed}",
        file=sys.stderr,
    )
    return [
        Table(
            "brz_full_snapshot",
            [SIM_COUNTER, *_data_cols("is_active")],
            _gen_full_snapshot(args.batches, args.batch_size, now, span),
        ),
        Table(
            "brz_insup_soft_delete",
            [SIM_COUNTER, *_data_cols("deleted")],
            _gen_insup_soft_delete(args.batches, args.batch_size, now, span),
        ),
        Table(
            "brz_insup_no_delete",
            [SIM_COUNTER, *_data_cols("is_active")],
            _gen_insup_no_delete(args.batches, args.batch_size, now, span),
        ),
        Table(
            "brz_cdc_stream",
            _cdc_cols(),
            _gen_cdc_stream(args.batches, args.batch_size, now, span),
        ),
        Table(
            "brz_append_events",
            [SIM_COUNTER, *_data_cols("is_active")],
            _gen_append_events(args.batches, args.batch_size, now, span),
        ),
    ]


def _quote_char_for_engine(engine: str | None) -> str:
    """MySQL uses backticks; every other supported engine accepts the
    ANSI double quote."""
    return "`" if engine == "mysql" else '"'


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--batches",
        type=int,
        required=True,
        help="number of batches per table — required, so the "
        "caller is explicit about how many arrivals to "
        "simulate (when loaded, the "
        "--size bucket supplies this)",
    )
    p.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        default=100,
        help="rows per batch (default: 100) — large enough that "
        "CDC and INSUP batches typically carry multiple "
        "changes against the same composite key",
    )
    p.add_argument("--format", choices=("sql", "csv", "json", "parquet"), default="sql")
    p.add_argument("--seed", type=int, default=BASE_SEED)
    p.add_argument("--out", required=True)
    p.add_argument("--chunks", type=int, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    tables = build_tables(args)
    engine = os.environ.get("TDX_ENGINE")
    write_tables(
        tables,
        args.out,
        args.format,
        source_dir=HERE,
        chunks=args.chunks,
        sql_quote_char=_quote_char_for_engine(engine),
    )


if __name__ == "__main__":
    main()
