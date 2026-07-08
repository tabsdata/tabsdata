#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#

"""
Data generator — Sakila (DVD rental store).

Real reference data extracted from the canonical MySQL "sakila" sample
DB and shipped as data.json. Output is one file per table in --out
(sql, csv, json or parquet).

Tables (15):
    sk_language sk_actor sk_country sk_city sk_address sk_category
    sk_film sk_film_actor sk_film_category sk_store sk_staff
    sk_customer sk_inventory sk_rental sk_payment

Original PKs are remapped to 1..N (insertion order) and FKs are
rewritten to match. The store↔staff cycle is handled by leaving the
sk_store FK on manager_staff_id off in the DDL — the column is plain
INT pointing at the (already-known) new staff id.

Usage:
    python datagen.py [--rentals N] [--format FMT] [--out DIR]
"""

import argparse
import json
import os
import sys
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables  # noqa: E402

BASE_SEED = 42
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")


def remap(rows, pk):
    return {r[pk]: i + 1 for i, r in enumerate(rows)}


# Column specs ---------------------------------------------------------------

LANG_COLS = [
    Column("language_id", "int", nullable=False, auto=True),
    Column("name", "varchar(20)", nullable=False),
]
ACTOR_COLS = [
    Column("actor_id", "int", nullable=False, auto=True),
    Column("first_name", "varchar(45)", nullable=False),
    Column("last_name", "varchar(45)", nullable=False),
]
COUNTRY_COLS = [
    Column("country_id", "int", nullable=False, auto=True),
    Column("country", "varchar(50)", nullable=False),
]
CITY_COLS = [
    Column("city_id", "int", nullable=False, auto=True),
    Column("city", "varchar(50)", nullable=False),
    Column("country_id", "int", nullable=False),
]
ADDRESS_COLS = [
    Column("address_id", "int", nullable=False, auto=True),
    Column("address", "varchar(50)", nullable=False),
    Column("address2", "varchar(50)"),
    Column("district", "varchar(20)", nullable=False),
    Column("city_id", "int", nullable=False),
    Column("postal_code", "varchar(10)"),
    Column("phone", "varchar(20)", nullable=False),
]
CATEGORY_COLS = [
    Column("category_id", "int", nullable=False, auto=True),
    Column("name", "varchar(25)", nullable=False),
]
FILM_COLS = [
    Column("film_id", "int", nullable=False, auto=True),
    Column("title", "varchar(128)", nullable=False),
    Column("description", "text"),
    Column("release_year", "smallint"),
    Column("language_id", "int", nullable=False),
    Column("original_language_id", "int"),
    Column("rental_duration", "smallint", nullable=False),
    Column("rental_rate", "decimal(4,2)", nullable=False),
    Column("length", "smallint"),
    Column("replacement_cost", "decimal(5,2)", nullable=False),
    Column("rating", "varchar(5)"),
    Column("special_features", "varchar(100)"),
]
FILM_ACTOR_COLS = [
    Column("actor_id", "int", nullable=False),
    Column("film_id", "int", nullable=False),
]
FILM_CAT_COLS = [
    Column("film_id", "int", nullable=False),
    Column("category_id", "int", nullable=False),
]
STORE_COLS = [
    Column("store_id", "int", nullable=False, auto=True),
    Column("manager_staff_id", "int"),
    Column("address_id", "int", nullable=False),
]
STAFF_COLS = [
    Column("staff_id", "int", nullable=False, auto=True),
    Column("first_name", "varchar(45)", nullable=False),
    Column("last_name", "varchar(45)", nullable=False),
    Column("address_id", "int", nullable=False),
    Column("email", "varchar(50)"),
    Column("store_id", "int", nullable=False),
    Column("active", "smallint", nullable=False),
    Column("username", "varchar(16)", nullable=False),
    Column("password", "varchar(40)"),
]
CUSTOMER_COLS = [
    Column("customer_id", "int", nullable=False, auto=True),
    Column("store_id", "int", nullable=False),
    Column("first_name", "varchar(45)", nullable=False),
    Column("last_name", "varchar(45)", nullable=False),
    Column("email", "varchar(50)"),
    Column("address_id", "int", nullable=False),
    Column("active", "smallint", nullable=False),
    Column("create_date", "datetime", nullable=False),
]
INVENTORY_COLS = [
    Column("inventory_id", "int", nullable=False, auto=True),
    Column("film_id", "int", nullable=False),
    Column("store_id", "int", nullable=False),
]
RENTAL_COLS = [
    Column("rental_id", "int", nullable=False, auto=True),
    Column("rental_date", "datetime", nullable=False),
    Column("inventory_id", "int", nullable=False),
    Column("customer_id", "int", nullable=False),
    Column("return_date", "datetime"),
    Column("staff_id", "int", nullable=False),
]
PAYMENT_COLS = [
    Column("payment_id", "int", nullable=False, auto=True),
    Column("customer_id", "int", nullable=False),
    Column("staff_id", "int", nullable=False),
    Column("rental_id", "int"),
    Column("amount", "decimal(7,2)", nullable=False),
    Column("payment_date", "datetime", nullable=False),
]


def build_tables(args):
    with open(DATA_PATH) as f:
        data = json.load(f)

    languages = data["language"]
    actors = data["actor"]
    countries = data["country"]
    cities = data["city"]
    addresses = data["address"]
    categories = data["category"]
    films = data["film"]
    film_actors = data["film_actor"]
    film_cats = data["film_category"]
    stores = data["store"]
    staff = data["staff"]
    customers = data["customer"]
    inventory = data["inventory"]
    rentals = data["rental"]
    payments = data["payment"]

    lang_map = remap(languages, "language_id")
    act_map = remap(actors, "actor_id")
    cou_map = remap(countries, "country_id")
    city_map = remap(cities, "city_id")
    addr_map = remap(addresses, "address_id")
    cat_map = remap(categories, "category_id")
    film_map = remap(films, "film_id")
    store_map = remap(stores, "store_id")
    staff_map = remap(staff, "staff_id")
    cust_map = remap(customers, "customer_id")
    inv_map = remap(inventory, "inventory_id")

    cap = max(0, min(args.rentals, len(rentals)))
    kept_rentals = rentals[:cap]
    rent_map = remap(kept_rentals, "rental_id")
    kept_pay_ids = set(rent_map.keys())

    lang_rows = [
        {"language_id": lang_map[r["language_id"]], "name": r["name"]}
        for r in languages
    ]
    actor_rows = [
        {
            "actor_id": act_map[r["actor_id"]],
            "first_name": r["first_name"],
            "last_name": r["last_name"],
        }
        for r in actors
    ]
    country_rows = [
        {"country_id": cou_map[r["country_id"]], "country": r["country"]}
        for r in countries
    ]
    city_rows = [
        {
            "city_id": city_map[r["city_id"]],
            "city": r["city"],
            "country_id": cou_map[r["country_id"]],
        }
        for r in cities
    ]
    addr_rows = [
        {
            "address_id": addr_map[r["address_id"]],
            "address": r["address"],
            "address2": r["address2"],
            "district": r["district"],
            "city_id": city_map[r["city_id"]],
            "postal_code": r["postal_code"],
            "phone": r["phone"],
        }
        for r in addresses
    ]
    cat_rows = [
        {"category_id": cat_map[r["category_id"]], "name": r["name"]}
        for r in categories
    ]
    film_rows = []
    for r in films:
        olang = (
            lang_map[r["original_language_id"]] if r["original_language_id"] else None
        )
        film_rows.append({
            "film_id": film_map[r["film_id"]],
            "title": r["title"],
            "description": r["description"],
            "release_year": r["release_year"],
            "language_id": lang_map[r["language_id"]],
            "original_language_id": olang,
            "rental_duration": r["rental_duration"],
            "rental_rate": r["rental_rate"],
            "length": r["length"],
            "replacement_cost": r["replacement_cost"],
            "rating": r["rating"],
            "special_features": r["special_features"],
        })
    fa_rows = [
        {"actor_id": act_map[r["actor_id"]], "film_id": film_map[r["film_id"]]}
        for r in film_actors
    ]
    fc_rows = [
        {"film_id": film_map[r["film_id"]], "category_id": cat_map[r["category_id"]]}
        for r in film_cats
    ]
    store_rows = [
        {
            "store_id": store_map[r["store_id"]],
            "manager_staff_id": staff_map.get(r["manager_staff_id"]),
            "address_id": addr_map[r["address_id"]],
        }
        for r in stores
    ]
    staff_rows = [
        {
            "staff_id": staff_map[r["staff_id"]],
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "address_id": addr_map[r["address_id"]],
            "email": r["email"],
            "store_id": store_map[r["store_id"]],
            "active": r["active"],
            "username": r["username"],
            "password": r["password"],
        }
        for r in staff
    ]
    cust_rows = [
        {
            "customer_id": cust_map[r["customer_id"]],
            "store_id": store_map[r["store_id"]],
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "email": r["email"],
            "address_id": addr_map[r["address_id"]],
            "active": r["active"],
            "create_date": r["create_date"],
        }
        for r in customers
    ]
    inv_rows = [
        {
            "inventory_id": inv_map[r["inventory_id"]],
            "film_id": film_map[r["film_id"]],
            "store_id": store_map[r["store_id"]],
        }
        for r in inventory
    ]
    rental_rows = [
        {
            "rental_id": rent_map[r["rental_id"]],
            "rental_date": r["rental_date"],
            "inventory_id": inv_map[r["inventory_id"]],
            "customer_id": cust_map[r["customer_id"]],
            "return_date": r["return_date"],
            "staff_id": staff_map[r["staff_id"]],
        }
        for r in kept_rentals
    ]
    payment_rows = []
    for r in payments:
        rid = r.get("rental_id")
        new_rid = rent_map.get(rid) if rid in kept_pay_ids else None
        payment_rows.append({
            "customer_id": cust_map[r["customer_id"]],
            "staff_id": staff_map[r["staff_id"]],
            "rental_id": new_rid,
            "amount": r["amount"],
            "payment_date": r["payment_date"],
        })

    print(
        f"sakila: rentals={len(rental_rows)} payments={len(payment_rows)} "
        f"films={len(film_rows)} customers={len(cust_rows)} "
        f"inventory={len(inv_rows)}",
        file=sys.stderr,
    )

    return [
        Table("sk_language", LANG_COLS, lang_rows),
        Table("sk_actor", ACTOR_COLS, actor_rows),
        Table("sk_country", COUNTRY_COLS, country_rows),
        Table("sk_city", CITY_COLS, city_rows),
        Table("sk_address", ADDRESS_COLS, addr_rows),
        Table("sk_category", CATEGORY_COLS, cat_rows),
        Table("sk_film", FILM_COLS, film_rows),
        Table("sk_film_actor", FILM_ACTOR_COLS, fa_rows),
        Table("sk_film_category", FILM_CAT_COLS, fc_rows),
        Table("sk_store", STORE_COLS, store_rows),
        Table("sk_staff", STAFF_COLS, staff_rows),
        Table("sk_customer", CUSTOMER_COLS, cust_rows),
        Table("sk_inventory", INVENTORY_COLS, inv_rows),
        Table("sk_rental", RENTAL_COLS, rental_rows),
        Table("sk_payment", PAYMENT_COLS, payment_rows),
    ]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--rentals", type=int, default=20000)
    p.add_argument("--format", choices=("sql", "csv", "json", "parquet"), default="sql")
    p.add_argument("--seed", type=int, default=BASE_SEED)
    p.add_argument("--out", required=True)
    p.add_argument("--chunks", type=int, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    tables = build_tables(args)
    write_tables(
        tables,
        args.out,
        args.format,
        source_dir=Path(__file__).parent,
        chunks=args.chunks,
    )


if __name__ == "__main__":
    main()
