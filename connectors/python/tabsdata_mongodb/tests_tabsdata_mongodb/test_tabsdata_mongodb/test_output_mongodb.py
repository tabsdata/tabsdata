#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
from math import isclose
from urllib.parse import quote_plus

import numpy as np
import polars as pl
import pymongo
import pytest
from tests_tabsdata.bootest import root_folder
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests_tabsdata_mongodb.conftest import (
    DB_PASSWORD,
    DB_USER,
    MONGODB_URI_WITH_CREDENTIALS,
    MONGODB_URI_WITHOUT_CREDENTIALS,
    MONGODB_WITH_REPLICA_SET_URI,
)
from tests_tabsdata_salesforce.conftest import TESTING_RESOURCES_FOLDER
from tests_tabsdata_salesforce.testing_resources.test_input_salesforce.example import (
    input_salesforce,
)
from tests_tabsdata_salesforce.testing_resources.test_input_salesforce_initial_values.example import (
    input_salesforce_initial_values,
)

import tabsdata as td
from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsserver.function_execution.response_utils import RESPONSE_FILE_NAME
from tabsserver.main import EXECUTION_CONTEXT_FILE_NAME
from tabsserver.main import do as tabsserver_main

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = root_folder()
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = os.path.join(
    os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION), "local_dev"
)


@pytest.mark.mongodb
def test_class_initialization_default_options():
    uri = "mongodb://localhost:27017"
    output = td.MongoDBDestination(uri, ("collection.id", None))
    assert output.uri == uri
    assert output.collections_with_ids == [("collection.id", None)]
    assert output.credentials is None
    assert output.if_collection_exists == "append"
    assert output.use_trxs == False
    assert output.update_existing == True
    assert output.maintain_order == False


@pytest.mark.mongodb
def test_class_initialization_all_options():
    uri = "mongodb://localhost:27017"
    output = td.MongoDBDestination(
        uri,
        ("collection.id", "id_column"),
        td.UserPasswordCredentials("hi", "bye"),
        if_collection_exists="replace",
        use_trxs=True,
        update_existing=False,
        maintain_order=True,
    )
    assert output.uri == uri
    assert output.collections_with_ids == [("collection.id", "id_column")]
    assert output.credentials == td.UserPasswordCredentials("hi", "bye")
    assert output.if_collection_exists == "replace"
    assert output.use_trxs == True
    assert output.update_existing == False
    assert output.maintain_order == True


@pytest.mark.mongodb
def test_invalid_class_types():
    uri = "mongodb://localhost:27017"
    with pytest.raises(TypeError):
        td.MongoDBDestination(uri, "collection.id")
    with pytest.raises(TypeError):
        td.MongoDBDestination(uri, ("collection.id", "id_column"), "credentials")
    with pytest.raises(ValueError):
        td.MongoDBDestination(
            uri,
            ("collection.id", "id_column"),
            td.UserPasswordCredentials("hi", "bye"),
            if_collection_exists="hi",
        )


@pytest.mark.mongodb
def test_extract_index():
    from tabsdata_mongodb.connector import _extract_index

    assert _extract_index("example_file_0.jsonl") == 0
    assert _extract_index("example_file_1.jsonl") == 1
    assert _extract_index("example_file_things_and_numbers_4732.jsonl") == 4732


@pytest.mark.mongodb
def test_get_matching_files(tmp_path):
    from tabsdata_mongodb.connector import _get_matching_files

    # Create some files
    files_generated = []
    for index in range(2000):
        file = tmp_path / f"example_file_{index}.jsonl"
        file.write_text("hi")
        files_generated.append(str(file))
    # Create some files that should not be matched
    for index in range(2000):
        file = tmp_path / f"example_file_{index}.csv"
        file.write_text("hi")
    assert (
        _get_matching_files(os.path.join(tmp_path, "example_file_*.jsonl"))
        == files_generated
    )


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.parametrize(
    "maintain_order,update_existing",
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_trigger_output(tmp_path, testing_mongodb, maintain_order, update_existing):
    size = 2500000
    id_column = np.random.choice(range(1, size * 10), size=size, replace=False)
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": np.random.rand(size),
        }
    )
    database_name = f"test_trigger_output_{maintain_order}_{update_existing}_database"
    collection_name = (
        f"test_trigger_output_{maintain_order}_{update_existing}_collection"
    )
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        (f"{database_name}.{collection_name}", "id"),
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="replace",
        maintain_order=maintain_order,
        update_existing=update_existing,
    )
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == size


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.parametrize(
    "maintain_order,update_existing",
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_trigger_output_with_replica_set(
    tmp_path, testing_mongodb_with_replica_set, maintain_order, update_existing
):
    size = 2500000
    id_column = np.random.choice(range(1, size * 10), size=size, replace=False)
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": np.random.rand(size),
        }
    )
    database_name = (
        f"test_trigger_output_with_replica_set_{maintain_order}"
        f"_{update_existing}_database"
    )
    collection_name = f"test_trigger_output_with_replica_set_{maintain_order}_{update_existing}_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_WITH_REPLICA_SET_URI,
        (f"{database_name}.{collection_name}", "id"),
        if_collection_exists="replace",
        use_trxs=True,
        maintain_order=maintain_order,
        update_existing=update_existing,
    )
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_WITH_REPLICA_SET_URI)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == size
