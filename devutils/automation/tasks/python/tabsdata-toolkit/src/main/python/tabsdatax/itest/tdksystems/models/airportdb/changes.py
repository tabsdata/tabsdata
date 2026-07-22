#
# Copyright 2026 Tabsdata Inc.
#

"""Change recipe for the `airportdb` model — see
`tabsdatax.itest.tdksystems.changes` for the contract.

Inserts / updates target `ap_airport` and `ap_passenger` (auto ids, so inserts
omit the id and mint unique ICAO / passport values that won't clash with the
letter-based seed data). Deletes target the leaf `ap_booking` (nothing
references a booking). `ap_airport` / `ap_passenger` are referenced by flights /
bookings, so they are not delete targets. Because this model also supports file
domains (inserts only), the two insert entities double as the new-file source."""

from __future__ import annotations

import random

from tabsdatax.itest.tdksystems.changes import (
    ChangeModel,
    Entity,
    OpKind,
    base36,
)
from tabsdatax.itest.tdksystems.emit import Column

# 4-char ICAO space for inserts: a leading digit + 3 base-36 chars. The leading
# digit keeps it disjoint from real (letter) ICAO codes even under a
# case-insensitive collation, and gives 10 * 36**3 unique values.
_ICAO_BODY = 36**3


def _insert_icao(i: int) -> str:
    return f"{(i // _ICAO_BODY) % 10}{base36(i % _ICAO_BODY, 3)}"


class _Airport(Entity):
    table = "ap_airport"
    columns = [
        Column("airport_id", "int", nullable=False, auto=True),
        Column("iata", "char(3)"),
        Column("icao", "char(4)", nullable=False),
        Column("name", "varchar(80)", nullable=False),
    ]
    supports = (OpKind.INSERT, OpKind.UPDATE)

    def new_row(self, rng: random.Random, i: int) -> dict:
        return {"icao": _insert_icao(i), "name": f"Change Airport {i}"[:80]}

    def target(self, i: int, existing: int) -> dict:
        return {"airport_id": i + 1}

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        return {"name": f"Renamed Airport {i}"[:80]}


class _Passenger(Entity):
    table = "ap_passenger"
    columns = [
        Column("passenger_id", "int", nullable=False, auto=True),
        Column("passportno", "char(9)", nullable=False),
        Column("firstname", "varchar(100)", nullable=False),
        Column("lastname", "varchar(100)", nullable=False),
    ]
    supports = (OpKind.INSERT, OpKind.UPDATE)

    def new_row(self, rng: random.Random, i: int) -> dict:
        return {
            "passportno": f"C{i:08d}",
            "firstname": f"First{i}"[:100],
            "lastname": f"Last{i}"[:100],
        }

    def target(self, i: int, existing: int) -> dict:
        return {"passenger_id": i + 1}

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        return {"firstname": f"Chg{i}"[:100], "lastname": f"Upd{i}"[:100]}


class _Booking(Entity):
    table = "ap_booking"
    columns = [Column("booking_id", "int", nullable=False, auto=True)]
    # Leaf table (nothing references a booking) — safe to delete from.
    supports = (OpKind.DELETE,)

    def target(self, i: int, existing: int) -> dict:
        return {"booking_id": i + 1}


def recipe() -> ChangeModel:
    return ChangeModel(entities=[_Airport(), _Passenger(), _Booking()])
