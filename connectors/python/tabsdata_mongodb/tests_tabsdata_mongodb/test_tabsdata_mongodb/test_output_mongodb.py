#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os

import polars as pl
import pymongo
import pytest
from tests_tabsdata.bootest import ROOT_FOLDER, TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    get_lf,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests_tabsdata_mongodb.conftest import (
    DB_PASSWORD,
    DB_USER,
    MONGODB_URI_WITH_CREDENTIALS,
    MONGODB_URI_WITHOUT_CREDENTIALS,
    MONGODB_WITH_REPLICA_SET_URI,
    TESTING_RESOURCES_FOLDER,
)
from tests_tabsdata_mongodb.testing_resources.test_multiple_outputs_mongodb.example import (
    multiple_outputs_mongodb,
)
from tests_tabsdata_mongodb.testing_resources.test_output_mongodb.example import (
    output_mongodb,
)

import tabsdata as td
from tabsdata.tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata.tabsserver.invoker import EXECUTION_CONTEXT_FILE_NAME
from tabsdata.tabsserver.invoker import invoke as tabsserver_main
from tabsdata.utils.bundle_utils import create_bundle_archive

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


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
def test_class_initialization_support_options():
    uri = "mongodb://localhost:27017"
    output = td.MongoDBDestination(
        uri,
        ("collection.id", "id_column"),
        td.UserPasswordCredentials("hi", "bye"),
        if_collection_exists="replace",
        use_trxs=True,
        update_existing=False,
        maintain_order=True,
        support_insert_one={"key": "value"},
    )
    assert output.uri == uri
    assert output.collections_with_ids == [("collection.id", "id_column")]
    assert output.credentials == td.UserPasswordCredentials("hi", "bye")
    assert output.if_collection_exists == "replace"
    assert output.use_trxs == True
    assert output.update_existing == False
    assert output.maintain_order == True
    assert output._support_insert_one == {"key": "value"}


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
@pytest.mark.performance
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.parametrize(
    "maintain_order,update_existing",
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_trigger_output(
    tmp_path, testing_mongodb, maintain_order, update_existing, size
):
    lf = get_lf(size)
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
@pytest.mark.performance
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.parametrize(
    "maintain_order,update_existing",
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_trigger_output_with_replica_set(
    tmp_path, testing_mongodb_with_replica_set, maintain_order, update_existing, size
):
    lf = get_lf(size)
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


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_trigger_output_with_id_none(tmp_path, testing_mongodb):
    size = 25000
    lf = get_lf(size)
    database_name = "test_trigger_output_with_id_none_database"
    collection_name = "test_trigger_output_with_id_none_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        (f"{database_name}.{collection_name}", None),
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="replace",
    )
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == size


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_trigger_output_multiple_lf(tmp_path, testing_mongodb):
    size = 25000
    lf = get_lf(size)
    database_name = "test_trigger_output_multiple_lf_database"
    collection_name = "test_trigger_output_multiple_lf_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [
            (f"{database_name}_1.{collection_name}_1", "id"),
            (f"{database_name}_2.{collection_name}_2", None),
        ],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="replace",
    )
    mongo_destination.trigger_output(str(tmp_path), lf, lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name + "_1"][collection_name + "_1"]
    assert collection.count_documents({}) == size
    collection = client[database_name + "_2"][collection_name + "_2"]
    assert collection.count_documents({}) == size


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_trigger_output_different_len_raises_error(tmp_path, testing_mongodb):
    size = 25000
    lf = get_lf(size)
    database_name = "test_trigger_output_different_len_raises_error_database"
    collection_name = "test_trigger_output_different_len_raises_error_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [
            (f"{database_name}_1.{collection_name}_1", "id"),
            (f"{database_name}_2.{collection_name}_2", None),
        ],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="replace",
    )
    with pytest.raises(ValueError):
        mongo_destination.trigger_output(str(tmp_path), lf)

    with pytest.raises(ValueError):
        mongo_destination.trigger_output(str(tmp_path), lf, lf, lf)


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_single_element_collection_list(tmp_path, testing_mongodb):
    size = 25000
    lf = get_lf(size)
    database_name = "test_single_element_collection_list_database"
    collection_name = "test_single_element_collection_list_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="replace",
    )
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == size


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_if_collection_exists_replace(tmp_path, testing_mongodb):
    size = 25000
    lf = get_lf(size)
    database_name = "test_if_collection_exists_replace_database"
    collection_name = "test_if_collection_exists_replace_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="replace",
    )
    for _ in range(3):
        mongo_destination.trigger_output(str(tmp_path), lf)

        client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
        collection = client[database_name][collection_name]
        assert collection.count_documents({}) == size


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_if_collection_exists_append(tmp_path, testing_mongodb):
    size = 25000
    lf = get_lf(size)
    database_name = "test_if_collection_exists_append_database"
    collection_name = "test_if_collection_exists_append_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", None)],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
    )
    for i in range(3):
        mongo_destination.trigger_output(str(tmp_path), lf)

        client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
        collection = client[database_name][collection_name]
        assert collection.count_documents({}) == (i + 1) * size


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_use_trxs_true(tmp_path, testing_mongodb_with_replica_set):
    size = 2
    id_column = [1 for _ in range(size)]
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": 1,
        }
    )
    database_name = "test_use_trxs_true_database"
    collection_name = "test_use_trxs_true_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_WITH_REPLICA_SET_URI,
        [(f"{database_name}.{collection_name}", "id")],
        if_collection_exists="append",
        update_existing=False,
        use_trxs=True,
    )
    with pytest.raises(Exception):
        mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_WITH_REPLICA_SET_URI)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 0


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_use_trxs_false(tmp_path, testing_mongodb):
    size = 2
    id_column = [1 for _ in range(size)]
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": 1,
        }
    )
    database_name = "test_use_trxs_false_database"
    collection_name = "test_use_trxs_false_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        update_existing=False,
        use_trxs=False,
    )
    with pytest.raises(Exception):
        mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 1


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_fail_on_duplicate_key_false_with_trx(
    tmp_path, testing_mongodb_with_replica_set
):
    size = 2
    id_column = [1 for _ in range(size)]
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": 1,
        }
    )
    database_name = "test_fail_on_duplicate_key_false_with_trx_database"
    collection_name = "test_fail_on_duplicate_key_false_with_trx_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_WITH_REPLICA_SET_URI,
        [(f"{database_name}.{collection_name}", "id")],
        if_collection_exists="append",
        fail_on_duplicate_key=False,
        update_existing=False,
        use_trxs=True,
    )
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_WITH_REPLICA_SET_URI)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 0


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_fail_on_duplicate_key_false(tmp_path, testing_mongodb):
    size = 2
    id_column = [1 for _ in range(size)]
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": 1,
        }
    )
    database_name = "test_fail_on_duplicate_key_false_database"
    collection_name = "test_fail_on_duplicate_key_false_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        use_trxs=False,
        update_existing=False,
        fail_on_duplicate_key=False,
    )
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 1


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_fail_on_duplicate_key_false(tmp_path, testing_mongodb):
    size = 2
    id_column = [1 for _ in range(size)]
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": 1,
        }
    )
    database_name = "test_fail_on_duplicate_key_false_database"
    collection_name = "test_fail_on_duplicate_key_false_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        use_trxs=False,
        fail_on_duplicate_key=False,
    )
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 1


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_update_existing_true(tmp_path, testing_mongodb):
    size = 2
    id_column = [1 for _ in range(size)]
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": 1,
        }
    )
    database_name = "test_update_existing_true_database"
    collection_name = "test_update_existing_true_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        update_existing=True,
    )

    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 1


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_lf_is_none(tmp_path, testing_mongodb):
    size = 200
    lf = get_lf(size)
    database_name = "test_lf_is_none_database"
    collection_name = "test_lf_is_none_collection"

    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="replace",
    )

    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == size

    lf = None
    mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == size


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_update_existing_false(tmp_path, testing_mongodb):
    size = 2
    id_column = [1 for _ in range(size)]
    lf = pl.LazyFrame(
        {
            "id": id_column,
            "name": [f"name_{i}" for i in id_column],
            "value": 1,
        }
    )
    database_name = "test_update_existing_false_database"
    collection_name = "test_update_existing_false_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        update_existing=False,
    )

    with pytest.raises(Exception):
        mongo_destination.trigger_output(str(tmp_path), lf)

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 1


@pytest.mark.mongodb
@pytest.mark.slow
@pytest.mark.requires_internet
def test_docs_per_trx(tmp_path, testing_mongodb):
    size = 200
    lf = get_lf(size)
    database_name = "test_docs_per_trx_database"
    collection_name = "test_docs_per_trx_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [(f"{database_name}.{collection_name}", "id")],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        docs_per_trx=100,
    )

    mongo_destination.trigger_output(str(tmp_path), lf)

    from tabsdata_mongodb.connector import _get_matching_files

    assert len(_get_matching_files(str(tmp_path / "*.jsonl"))) == 2

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 200

    mongo_destination.docs_per_trx = 50
    mongo_destination.trigger_output(str(tmp_path), lf)
    assert len(_get_matching_files(str(tmp_path / "*.jsonl"))) == 4

    assert collection.count_documents({}) == 200


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mongodb
def test_output_mongodb(tmp_path, testing_mongodb):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    database_name = "test_output_mongodb_database"
    collection_name = "test_output_mongodb_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        (f"{database_name}.{collection_name}", None),
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        update_existing=False,
    )
    output_mongodb.output = mongo_destination
    context_archive = create_bundle_archive(
        output_mongodb,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mongodb", "mock_table.parquet"
    )
    write_v1_yaml_file(input_yaml_file, context_archive, [mock_parquet_table])
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mongodb",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert len(expected_output) == collection.count_documents({})


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.mongodb
def test_multiple_outputs_mongodb(tmp_path, testing_mongodb):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    database_name = "test_multiple_outputs_mongodb_database"
    collection_name = "test_multiple_outputs_mongodb_collection"
    mongo_destination = td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        [
            (f"{database_name}_1.{collection_name}_1", None),
            (f"{database_name}_2.{collection_name}_2", None),
        ],
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        if_collection_exists="append",
        update_existing=False,
    )
    multiple_outputs_mongodb.output = mongo_destination
    context_archive = create_bundle_archive(
        multiple_outputs_mongodb,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_multiple_outputs_mongodb", "mock_table.parquet"
    )
    write_v1_yaml_file(input_yaml_file, context_archive, [mock_parquet_table])
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

    # Verify first collection
    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name + "_1"][collection_name + "_1"]
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_mongodb",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert len(expected_output) == collection.count_documents({})

    # Verify second collection
    collection = client[database_name + "_2"][collection_name + "_2"]
    assert len(expected_output) == collection.count_documents({})
