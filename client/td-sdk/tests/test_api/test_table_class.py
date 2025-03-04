#
# Copyright 2025 Tabs Data Inc.
#

import datetime
import os

import polars as pl
import pytest

from tabsdata.api.api_server import APIServerError
from tabsdata.api.tabsdata_server import Collection, Table


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
def test_table_class_download_with_version(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_with_version_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    origin_table.download(
        version="HEAD",
        destination_file=destination_file,
    )
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_with_version(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample(
        version="HEAD",
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_with_version(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema(
        version="HEAD",
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_with_commit(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_with_commit_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    commit = tabsserver_connection.commits[0].id
    origin_table.download(
        destination_file=destination_file,
        commit=commit,
    )
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_with_commit(
    tabsserver_connection, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample(
        commit=commit,
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_with_commit(
    tabsserver_connection, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema(
        commit=commit,
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_with_wrong_version(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path,
        "test_table_class_download_with_wrong_version_collection_output.parquet",
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.download(
            version="DOESNTEXIST",
            destination_file=destination_file,
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_with_wrong_version(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.sample(
            version="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_with_wrong_version(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.get_schema(
            version="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_with_wrong_commit(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path,
        "test_table_class_download_with_wrong_commit_collection_output.parquet",
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.download(
            destination_file=destination_file,
            commit="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_with_wrong_commit(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.sample(
            commit="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_with_wrong_commit(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.get_schema(
            commit="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_with_time(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_with_time_collection_output.parquet"
    )
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    origin_table.download(
        destination_file=destination_file,
        time=formatted_time,
    )
    assert os.path.exists(destination_file)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_with_time(
    tabsserver_connection, testing_collection_with_table
):
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    table = origin_table.sample(
        time=formatted_time,
    )
    assert isinstance(table, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_with_time(
    tabsserver_connection, testing_collection_with_table
):
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    schema = origin_table.get_schema(
        time=formatted_time,
    )
    assert schema


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_with_wrong_time(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_with_wrong_time_collection_output.parquet"
    )
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.download(
            destination_file=destination_file,
            time="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_with_wrong_time(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.sample(
            time="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_with_wrong_time(
    tabsserver_connection, testing_collection_with_table
):
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.get_schema(
            time="DOESNTEXIST",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_download_with_all_options_fails(
    tabsserver_connection, tmp_path, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    destination_file = os.path.join(
        tmp_path, "test_table_class_download_with_time_collection_output.parquet"
    )
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.download(
            destination_file=destination_file,
            time=formatted_time,
            commit=commit,
            version="HEAD",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_sample_with_all_options_fails(
    tabsserver_connection, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.sample(
            time=formatted_time,
            commit=commit,
            version="HEAD",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_class_get_schema_with_all_options_fails(
    tabsserver_connection, testing_collection_with_table
):
    commit = tabsserver_connection.commits[0].id
    current_time = datetime.datetime.now(datetime.UTC)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    origin_table = Table(
        tabsserver_connection.connection, testing_collection_with_table, "output"
    )
    with pytest.raises(APIServerError):
        origin_table.get_schema(
            time=formatted_time,
            commit=commit,
            version="HEAD",
        )
