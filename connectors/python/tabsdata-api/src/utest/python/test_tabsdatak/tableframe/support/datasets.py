#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import random
from pathlib import Path

import polars as pl
from mimesis import Address, Datetime, Finance, Numeric, Person, Text
from mimesis.locales import Locale

from test_tabsdatak.tableframe.support.frames import (
    DataSet,
    dataset_from_dictionary,
    dataset_from_lazyframe,
)

PENGUINS = Path(__file__).parent / "resources" / "penguins.csv"

LARGE_DATASET_ROWS = 5000
LARGE_DATASET_SEED = 42


def simple_dataset() -> DataSet:
    return dataset_from_dictionary({
        "int_column": [1, 2, 3],
        "string_column": ["a", "b", "c"],
    })


def small_dataset() -> DataSet:
    return dataset_from_lazyframe(pl.scan_csv(PENGUINS))


def large_dataset(rows: int = LARGE_DATASET_ROWS) -> DataSet:
    person = Person(Locale.EN)
    address = Address(Locale.EN)
    moment = Datetime(Locale.EN)
    finance = Finance(Locale.EN)
    text = Text(Locale.EN)
    numeric = Numeric()
    chooser = random.Random(LARGE_DATASET_SEED)
    for provider in (person, address, moment, finance, text, numeric):
        provider.reseed(LARGE_DATASET_SEED)

    data = {
        "first_name": [person.first_name() for _ in range(rows)],
        "last_name": [person.last_name() for _ in range(rows)],
        "full_name": [person.full_name() for _ in range(rows)],
        "email": [person.email() for _ in range(rows)],
        "height": [float(person.height()) for _ in range(rows)],
        "weight": [person.weight() for _ in range(rows)],
        "gender": [person.gender() for _ in range(rows)],
        "occupation": [person.occupation() for _ in range(rows)],
        "phone": [person.phone_number() for _ in range(rows)],
        "nationality": [person.nationality() for _ in range(rows)],
        "identifier": [person.identifier() for _ in range(rows)],
        "city": [address.city() for _ in range(rows)],
        "country": [address.country() for _ in range(rows)],
        "postal_code": [address.postal_code() for _ in range(rows)],
        "latitude": [address.latitude() for _ in range(rows)],
        "longitude": [address.longitude() for _ in range(rows)],
        "street": [address.street_name() for _ in range(rows)],
        "country_code": [address.country_code() for _ in range(rows)],
        "birth_date": [moment.date() for _ in range(rows)],
        "created_at": [moment.datetime() for _ in range(rows)],
        "event_time": [moment.time() for _ in range(rows)],
        "year": [moment.year() for _ in range(rows)],
        "currency": [finance.currency_iso_code() for _ in range(rows)],
        "company": [finance.company() for _ in range(rows)],
        "price": [finance.price() for _ in range(rows)],
        "stock_symbol": [finance.stock_ticker() for _ in range(rows)],
        "word": [text.word() for _ in range(rows)],
        "sentence": [text.sentence() for _ in range(rows)],
        "integer": [numeric.integer_number(-1000, 1000) for _ in range(rows)],
        "floating": [numeric.float_number(0, 1) for _ in range(rows)],
        "active": [chooser.choice([True, False]) for _ in range(rows)],
        "category": [chooser.choice(["A", "B", "C", "D"]) for _ in range(rows)],
    }
    return dataset_from_dictionary(data)
