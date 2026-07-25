#
# Copyright 2026 Tabsdata Inc.
#

"""Data generator for the ``world`` model.

Real reference data (countries / cities / languages) from the canonical MySQL
"world" sample DB, shipped in ``data.json``. The size caps how many cities are
emitted (all countries and languages are always kept). Reference-only: there is
no delta recipe, so ``create-data`` works but ``modify-data`` has nothing to
change.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

from tabsdatax.itest.tdksystems2.datagen import Entity, Model
from tabsdatax.itest.tdksystems2.emit import Column, Table

_DATA = Path(__file__).parent / "data.json"

_COUNTRY_COLS = [
    Column("code", "char(3)", nullable=False),
    Column("name", "varchar(52)", nullable=False),
    Column("continent", "varchar(20)", nullable=False),
    Column("region", "varchar(26)", nullable=False),
    Column("surface_area", "decimal(10,2)", nullable=False),
    Column("indep_year", "smallint"),
    Column("population", "int", nullable=False),
    Column("life_expectancy", "decimal(3,1)"),
    Column("gnp", "decimal(10,2)"),
    Column("gnp_old", "decimal(10,2)"),
    Column("local_name", "varchar(45)", nullable=False),
    Column("government_form", "varchar(45)", nullable=False),
    Column("head_of_state", "varchar(60)"),
    Column("capital", "int"),
    Column("code2", "char(2)", nullable=False),
]

_CITY_COLS = [
    Column("id", "int", nullable=False, auto=True),
    Column("name", "varchar(35)", nullable=False),
    Column("country_code", "char(3)", nullable=False),
    Column("district", "varchar(20)", nullable=False),
    Column("population", "int", nullable=False),
]

_LANG_COLS = [
    Column("country_code", "char(3)", nullable=False),
    Column("language", "varchar(30)", nullable=False),
    Column("is_official", "char(1)", nullable=False),
    Column("percentage", "decimal(4,1)", nullable=False),
]


class _World(Model):
    entities: list[Entity] = []  # reference-only: no deltas

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        data = json.loads(_DATA.read_text(encoding="utf-8"))
        countries = data["countries"]
        cities_sorted = sorted(data["cities"], key=lambda c: c["ID"])
        kept = cities_sorted[: max(0, n)]
        new_ids = {c["ID"]: i + 1 for i, c in enumerate(kept)}

        country_rows = []
        for c in countries:
            cap = c.get("Capital")
            country_rows.append({
                "code": c["Code"],
                "name": c["Name"],
                "continent": c["Continent"],
                "region": c["Region"],
                "surface_area": c["SurfaceArea"],
                "indep_year": c["IndepYear"],
                "population": c["Population"],
                "life_expectancy": c["LifeExpectancy"],
                "gnp": c["GNP"],
                "gnp_old": c["GNPOld"],
                "local_name": c["LocalName"],
                "government_form": c["GovernmentForm"],
                "head_of_state": c["HeadOfState"],
                "capital": new_ids.get(cap) if cap is not None else None,
                "code2": c["Code2"],
            })

        kept_codes = {c["Code"] for c in countries}
        city_rows = [
            {
                "id": new_ids[c["ID"]],
                "name": c["Name"],
                "country_code": c["CountryCode"],
                "district": c["District"],
                "population": c["Population"],
            }
            for c in kept
            if c["CountryCode"] in kept_codes
        ]
        lang_rows = [
            {
                "country_code": lang["CountryCode"],
                "language": lang["Language"],
                "is_official": lang["IsOfficial"],
                "percentage": lang["Percentage"],
            }
            for lang in data["languages"]
            if lang["CountryCode"] in kept_codes
        ]
        return [
            Table("country", _COUNTRY_COLS, country_rows),
            Table("city", _CITY_COLS, city_rows),
            Table("country_language", _LANG_COLS, lang_rows),
        ]


def datagenerator() -> Model:
    return _World()
