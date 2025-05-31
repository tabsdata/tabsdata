#
# Copyright 2025 Tabs Data Inc.
#

import os
import time

import polars as pl
import pytest

from tabsdata.api.tabsdata_server import Collection, DataVersion, Table

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
def test_table_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    collection = Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    assert collection.tables
    assert isinstance(collection.tables, list)
    assert all(isinstance(table, Table) for table in collection.tables)

    table = collection.tables[0]
    assert table.name
    assert table.collection == collection
    assert table.function
    assert table.function.get_table(table.name)
    function = table.function
    assert table in function.tables
    assert table in collection.tables
    assert collection.get_table(table.name)
    assert table.__repr__()
    assert table.__str__()

    lazy_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, table.name
    )
    assert lazy_table.name == table.name
    assert lazy_table.collection == table.collection
    assert lazy_table.function == table.function
    assert lazy_table.__repr__()
    assert lazy_table.__str__()


@pytest.mark.integration
def test_table_class_dataversions(tabsserver_connection, testing_collection_with_table):
    collection = Collection(
        tabsserver_connection.connection, testing_collection_with_table
    )
    assert collection.tables
    assert isinstance(collection.tables, list)
    assert all(isinstance(table, Table) for table in collection.tables)
    table = collection.tables[0]
    assert table.dataversions
    assert isinstance(table.dataversions, list)
    assert all(isinstance(version, DataVersion) for version in table.dataversions)
    for version in table.dataversions:
        assert version.table == table

    lazy_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, table.name
    )
    assert lazy_table.dataversions
    assert isinstance(lazy_table.dataversions, list)
    assert all(isinstance(version, DataVersion) for version in lazy_table.dataversions)
    for version in lazy_table.dataversions:
        assert version.table == lazy_table


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    origin_table.download(
        destination_file=destination_file,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample(tabsserver_connection, testing_collection_with_table):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample()
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema(tabsserver_connection, testing_collection_with_table):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema()
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_at_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    origin_table.download(
        destination_file=destination_file,
        at=epoch_ms,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at(tabsserver_connection, testing_collection_with_table):
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample(at=epoch_ms)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at(
    tabsserver_connection, testing_collection_with_table
):
    epoch_ms = int(time.time() * 1000)  # Current time in milliseconds
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema(at=epoch_ms)
    assert schema
