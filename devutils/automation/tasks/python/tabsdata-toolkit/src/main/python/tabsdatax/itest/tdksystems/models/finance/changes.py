#
# Copyright 2026 Tabsdata Inc.
#

"""Change recipe for the `finance` model — see
`tabsdatax.itest.tdksystems.changes` for the contract.

`finance` is SQL/kafka only (no file domains). Inserts and updates go to
`customers` (auto `customer_id`, so inserts omit it and give every NOT NULL
column an explicit value to stay dialect-portable); deletes go to the leaf
`customer_subscription_events` (nothing references it, so a delete never breaks
a foreign key). `customers` itself is referenced by subscriptions / payment
methods, so it is not a delete target."""

from __future__ import annotations

import random
from datetime import datetime

from tabsdatax.itest.tdksystems.changes import ChangeModel, Entity, OpKind
from tabsdatax.itest.tdksystems.emit import Column

_TS = datetime(2020, 1, 1, 0, 0, 0)


class _Customers(Entity):
    table = "customers"
    columns = [
        Column("customer_id", "bigint", nullable=False, auto=True),
        Column("external_id", "varchar(36)", nullable=False),
        Column("name", "varchar(255)", nullable=False),
        Column("email", "varchar(320)"),
        Column("is_active", "smallint", nullable=False),
        Column("created_at", "timestamp", nullable=False),
        Column("updated_at", "timestamp", nullable=False),
    ]
    supports = (OpKind.INSERT, OpKind.UPDATE)

    def new_row(self, rng: random.Random, i: int) -> dict:
        return {
            "external_id": f"chg-cust-{i:010d}",
            "name": f"Change Customer {i}"[:255],
            "email": f"chg{i}@example.com"[:320],
            "is_active": 1,
            "created_at": _TS,
            "updated_at": _TS,
        }

    def target(self, i: int, existing: int) -> dict:
        # BIGSERIAL, loaded in order -> customer_id runs 1..N.
        return {"customer_id": i + 1}

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        return {
            "name": f"Updated Customer {i}"[:255],
            "email": f"upd{i}@example.com"[:320],
        }


class _SubscriptionEvents(Entity):
    table = "customer_subscription_events"
    columns = [Column("event_id", "bigint", nullable=False, auto=True)]
    # Leaf table — safe to delete from; inserts would need an FK + JSON column,
    # so this entity only deletes.
    supports = (OpKind.DELETE,)

    def target(self, i: int, existing: int) -> dict:
        return {"event_id": i + 1}


def recipe() -> ChangeModel:
    return ChangeModel(entities=[_Customers(), _SubscriptionEvents()])
