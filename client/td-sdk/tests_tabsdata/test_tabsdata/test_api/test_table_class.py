#
# Copyright 2025 Tabs Data Inc.
#

import os
import time
from datetime import datetime, timedelta, timezone

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


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at_date(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_at_date_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    origin_table.download(
        destination_file=destination_file,
        at=next_day,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at_date(
    tabsserver_connection, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample(at=next_day)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at_date(
    tabsserver_connection, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema(at=next_day)
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at_utc_time(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_at_utc_time_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    origin_table.download(
        destination_file=destination_file,
        at=next_day,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at_utc_time(
    tabsserver_connection, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample(at=next_day)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at_utc_time(
    tabsserver_connection, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema(at=next_day)
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at_localized_time(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H")
    destination_file = os.path.join(
        tmp_path,
        "test_table_class_download_at_localized_time_collection_output.parquet",
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    origin_table.download(
        destination_file=destination_file,
        at=next_day,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at_localized_time(
    tabsserver_connection, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H")
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample(at=next_day)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at_localized_time(
    tabsserver_connection, testing_collection_with_table
):
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H")
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema(at=next_day)
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at_trx(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_at_trx_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    origin_table.download(
        destination_file=destination_file,
        at_trx=trx,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at_trx(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    table = origin_table.sample(at_trx=trx)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at_trx(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    schema = origin_table.get_schema(at_trx=trx)
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at_trx_id(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_at_trx_id_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    origin_table.download(
        destination_file=destination_file,
        at_trx=trx.id,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at_trx_id(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    table = origin_table.sample(at_trx=trx.id)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at_trx_id(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    schema = origin_table.get_schema(at_trx=trx.id)
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at_dataversion(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_at_dataversion_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    origin_table.download(
        destination_file=destination_file,
        version=version,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at_dataversion(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    table = origin_table.sample(version=version)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at_dataversion(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    schema = origin_table.get_schema(version=version)
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_at_dataversion_id(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path,
        "test_table_class_download_at_dataversion_id_collection_output.parquet",
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    origin_table.download(
        destination_file=destination_file,
        version=version.id,
    )

    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_at_dataversion_id(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    table = origin_table.sample(version=version.id)
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_at_dataversion_id(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    schema = origin_table.get_schema(version=version.id)
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_more_than_one_option_fails(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path,
        "test_table_class_download_more_than_one_option_fails_collection_output.parquet",
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    with pytest.raises(ValueError):
        origin_table.download(
            destination_file=destination_file,
            at=next_day,
            at_trx=trx,
        )
    with pytest.raises(ValueError):
        origin_table.download(
            destination_file=destination_file,
            at=next_day,
            version=version.id,
        )
    with pytest.raises(ValueError):
        origin_table.download(
            destination_file=destination_file,
            at_trx=trx,
            version=version.id,
        )
    with pytest.raises(ValueError):
        origin_table.download(
            destination_file=destination_file,
            at=next_day,
            at_trx=trx,
            version=version.id,
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_more_than_one_option_fails(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    with pytest.raises(ValueError):
        origin_table.sample(
            at=next_day,
            at_trx=trx,
        )
    with pytest.raises(ValueError):
        origin_table.sample(
            at=next_day,
            version=version.id,
        )
    with pytest.raises(ValueError):
        origin_table.sample(
            at_trx=trx,
            version=version.id,
        )
    with pytest.raises(ValueError):
        origin_table.sample(
            at=next_day,
            at_trx=trx,
            version=version.id,
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_more_than_one_option_fails(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    version = origin_table.dataversions[-1]  # Get the latest data version
    trx = tabsserver_connection.list_transactions(order_by="triggered_on+")[-1]
    next_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%HZ")
    with pytest.raises(ValueError):
        origin_table.get_schema(
            at=next_day,
            at_trx=trx,
        )
    with pytest.raises(ValueError):
        origin_table.get_schema(
            at=next_day,
            version=version.id,
        )
    with pytest.raises(ValueError):
        origin_table.get_schema(
            at_trx=trx,
            version=version.id,
        )
    with pytest.raises(ValueError):
        origin_table.get_schema(
            at=next_day,
            at_trx=trx,
            version=version.id,
        )
