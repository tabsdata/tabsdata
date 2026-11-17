#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from types import SimpleNamespace

import polars as pl
import pytest
from test_tabsdatak.conn.awsglue.support import conn

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import _impl as impl
from tabsdatak.conn.awsglue._explorer import AwsGlueConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLConnMetaModel, SQLEntity

pytestmark = pytest.mark.conn_awsglue


def _table(name: str, table_type: str | None = "ICEBERG", columns=()) -> dict:
    table: dict = {"Name": name, "StorageDescriptor": {"Columns": list(columns)}}
    if table_type is not None:
        table["Parameters"] = {"table_type": table_type}
    return table


class FakeGlue:
    def __init__(self, databases: dict[str, list[dict]], page_size: int = 2) -> None:
        self.databases = databases
        self.page_size = page_size
        self.calls: list[tuple] = []
        self.failing: set[str] = set()

    def _pages(self, key: str, items: list) -> list[dict]:
        size = self.page_size
        return [{key: items[i : i + size]} for i in range(0, len(items), size)] or [
            {key: []}
        ]

    def get_paginator(self, operation: str):
        def paginate(**kwargs):
            self.calls.append((operation, kwargs))
            if operation == "get_databases":
                if "*" in self.failing:
                    raise RuntimeError("access denied")
                return self._pages(
                    "DatabaseList", [{"Name": name} for name in self.databases]
                )
            database = kwargs["DatabaseName"]
            if database in self.failing:
                raise RuntimeError("access denied")
            return self._pages("TableList", self.databases[database])

        return SimpleNamespace(paginate=paginate)

    def get_databases(self, **kwargs):
        self.calls.append(("get_databases", kwargs))
        return {"DatabaseList": []}

    def get_database(self, **kwargs):
        self.calls.append(("get_database", kwargs))
        if kwargs["Name"] not in self.databases:
            raise LookupError(kwargs["Name"])
        return {"Database": {"Name": kwargs["Name"]}}


@pytest.fixture
def glue(monkeypatch) -> FakeGlue:
    fake = FakeGlue({
        "sales": [
            _table(
                "orders",
                columns=[
                    {
                        "Name": "id",
                        "Type": "bigint",
                        "Parameters": {"iceberg.field.optional": "false"},
                    },
                    {"Name": "amount", "Type": "decimal(18,2)", "Comment": "net"},
                    {"Name": "tags", "Type": "array<string>"},
                ],
            ),
            _table("legacy_csv", table_type="EXTERNAL_TABLE"),
            _table("untyped", table_type=None),
            _table("customers", table_type="iceberg"),
        ],
        "returns": [_table("orders")],
    })
    monkeypatch.setattr(impl, "glue_catalog", lambda conn: SimpleNamespace(glue=fake))
    return fake


def test_explorer_reports_tables_in_the_sql_model():
    assert isinstance(AwsGlueConnectionExplorer.meta_model, SQLConnMetaModel)
    assert AwsGlueConnectionExplorer.entity_type is SQLEntity


def test_check_connect_asks_for_the_database_the_connection_names(glue):
    assert AwsGlueConnectionExplorer().check_connect(conn()) is None
    assert glue.calls == [("get_database", {"Name": "sales"})]


def test_check_connect_lists_one_database_without_a_default(glue):
    assert AwsGlueConnectionExplorer().check_connect(conn(database=None)) is None
    assert glue.calls == [("get_databases", {"MaxResults": 1})]


def test_check_connect_raises_when_the_database_cannot_be_read(glue):
    with pytest.raises(LookupError):
        AwsGlueConnectionExplorer().check_connect(conn(database="nowhere"))


def test_metadata_reports_the_iceberg_tables_of_the_database(glue):
    entities = AwsGlueConnectionExplorer().metadata(conn())
    assert [(e.database, e.schema, e.table) for e in entities] == [
        ("sales", "", "customers"),
        ("sales", "", "orders"),
    ]
    assert [e.fqn() for e in entities] == ["sales.customers", "sales.orders"]


def test_metadata_reads_every_page_of_tables(glue):
    glue.page_size = 1
    entities = AwsGlueConnectionExplorer().metadata(conn())
    assert [e.table for e in entities] == ["customers", "orders"]


def test_metadata_scans_every_database_without_a_default(glue):
    entities = AwsGlueConnectionExplorer().metadata(conn(database=None))
    assert [e.fqn() for e in entities] == [
        "returns.orders",
        "sales.customers",
        "sales.orders",
    ]


def test_metadata_describes_the_columns(glue):
    orders = AwsGlueConnectionExplorer().metadata(conn())[1]
    assert [(c.name, c.type, c.nullable, c.description) for c in orders.columns] == [
        ("id", "BIGINT", False, ""),
        ("amount", "DECIMAL", True, "net"),
        ("tags", "ARRAY", True, ""),
    ]


def test_metadata_skips_a_database_it_cannot_list(glue):
    glue.failing.add("sales")
    entities = AwsGlueConnectionExplorer().metadata(conn(database=None))
    assert [e.fqn() for e in entities] == ["returns.orders"]


def test_metadata_is_empty_when_the_databases_cannot_be_listed(glue):
    glue.failing.add("*")
    assert AwsGlueConnectionExplorer().metadata(conn(database=None)) == []


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="pyiceberg's local file IO does not take Windows drive-letter paths",
)
def test_data_samples_the_rows_of_a_table(tmp_path, monkeypatch):
    from pyiceberg.catalog.sql import SqlCatalog

    root = tmp_path.resolve()
    catalog = SqlCatalog(
        "local", uri=f"sqlite:///{root.as_posix()}/catalog.db", warehouse=root.as_uri()
    )
    catalog.create_namespace("sales")
    rows = pl.DataFrame({"id": [1, 2, 3]}).to_arrow()
    catalog.create_table("sales.orders", schema=rows.schema).append(rows)
    monkeypatch.setattr(impl, "glue_catalog", lambda conn: catalog)
    entity = SQLEntity(
        connection_type=AwsGlueConnectionExplorer.meta_model,
        database="sales",
        table="orders",
    )
    sampled = AwsGlueConnectionExplorer().data(conn(), entity, limit=2)
    assert len(sampled) == 2
    assert {row["id"] for row in sampled} <= {1, 2, 3}
