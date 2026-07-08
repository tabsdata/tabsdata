#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#

"""
Data generator — World (countries, cities, languages).

Real reference data extracted from the canonical MySQL "world" sample
DB and shipped as data.json next to this script. Output is one file
per table in --out (sql, csv, json or parquet).

Tables: wld_country, wld_city, wld_country_language

Usage:
    python generator.py [--cities N] [--format FMT] [--out DIR]
"""

import argparse
import json
import os
import sys
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables  # noqa: E402

BASE_SEED = 42
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

COUNTRY_COLUMNS = [
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

CITY_COLUMNS = [
    Column("id", "int", nullable=False, auto=True),
    Column("name", "varchar(35)", nullable=False),
    Column("country_code", "char(3)", nullable=False),
    Column("district", "varchar(20)", nullable=False),
    Column("population", "int", nullable=False),
]

LANG_COLUMNS = [
    Column("country_code", "char(3)", nullable=False),
    Column("language", "varchar(30)", nullable=False),
    Column("is_official", "char(1)", nullable=False),
    Column("percentage", "decimal(4,1)", nullable=False),
]


def build_tables(args):
    with open(DATA_PATH) as f:
        data = json.load(f)

    countries = data["countries"]
    cities_sorted = sorted(data["cities"], key=lambda c: c["ID"])
    kept = cities_sorted[: max(0, args.cities)]

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

    print(
        f"world: countries={len(country_rows)} cities={len(city_rows)} "
        f"languages={len(lang_rows)}",
        file=sys.stderr,
    )

    return [
        Table("wld_country", COUNTRY_COLUMNS, country_rows),
        Table("wld_city", CITY_COLUMNS, city_rows),
        Table("wld_country_language", LANG_COLUMNS, lang_rows),
    ]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--cities", type=int, default=10000, help="Cap on cities to emit (default: all)"
    )
    p.add_argument("--format", choices=("sql", "csv", "json", "parquet"), default="sql")
    p.add_argument("--seed", type=int, default=BASE_SEED)
    p.add_argument("--out", required=True, help="Output directory (must not exist)")
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
