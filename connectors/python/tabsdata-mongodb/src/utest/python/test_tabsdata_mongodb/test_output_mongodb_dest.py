#
# Copyright 2026 Tabs Data Inc.
#

import copy
import json
import logging
import os

# noinspection PyPackageRequirements
import pymongo

# noinspection PyPackageRequirements
import pytest

import tabsdata as td
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
)
from tabsdata.exceptions import ParameterValidationError
from test_tabsdata.conftest import (
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata_mongodb.conftest import (
    DB_PASSWORD,
    DB_USER,
    MONGODB_URI_WITH_CREDENTIALS,
    MONGODB_URI_WITHOUT_CREDENTIALS,
    TESTING_RESOURCES_FOLDER,
)
from test_tabsdata_mongodb.test_output_mongodb_basic.example import (
    output_mongodb,
)
from test_tabsdata_mongodb.test_multiple_outputs_mongodb.example import (
    multiple_outputs_mongodb,
)
from test_tabsdata_mongodb.test_output_mongodb_none.example import (
    output_mongodb_none,
)
from test_tabsdata_mongodb.test_output_mongodb_list_none.example import (
    output_mongodb_list_none,
)
from test_tabsdata_mongodb.test_output_mongodb_named_conn.example import (
    testing_tabsdata_function as output_mongodb_named_conn_generator,
)


pytestmark = [pytest.mark.xdist_group("mongodb-output_mongodb_dest_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

URI = "mongodb://localhost:27017"

MONGODB_NAMED_CONN_NAME = "test_mongodb_named"


def _write_named_conn_repository(tmp_path, name, conn_type, conn_configs, conn_mode):
    """Create a temporary repository with a single named-connection JSON."""
    repo_dir = os.path.join(str(tmp_path), "repository")
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER)
    os.makedirs(connections_dir, exist_ok=True)
    cx_dict = {
        "kind": "connectionDef",
        "apiVersion": "1.0",
        "name": name,
        "conn_mode": conn_mode,
        "conn_type": conn_type,
        "conn_configs": conn_configs,
    }
    connection_file = os.path.join(connections_dir, f"{name.lower()}.json")
    with open(connection_file, "w") as f:
        json.dump(cx_dict, f)
    return repo_dir


# --- MongoDBConn unit tests ---


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_default_options():
    conn = td.MongoDBConn(uri=URI)
    assert conn.uri == URI
    assert conn.credentials is None
    assert conn.connection_options == {}


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_with_credentials_and_options():
    creds = td.UserPasswordCredentials("hi", "bye")
    options = {"serverSelectionTimeoutMS": 1000}
    conn = td.MongoDBConn(uri=URI, credentials=creds, connection_options=options)
    assert conn.uri == URI
    assert conn.credentials == creds
    assert conn.connection_options == options


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_bad_uri_raises():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.MongoDBConn(uri=1)


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_bad_credentials_raises():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.MongoDBConn(uri=URI, credentials="not_a_creds")


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_bad_connection_options_raises():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.MongoDBConn(uri=URI, connection_options=[1, 2, 3])


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_setter_bad_uri_raises():
    conn = td.MongoDBConn(uri=URI)
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        conn.uri = 1


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_setter_bad_credentials_raises():
    conn = td.MongoDBConn(uri=URI)
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        conn.credentials = "not_a_creds"


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_setter_bad_connection_options_raises():
    conn = td.MongoDBConn(uri=URI)
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        conn.connection_options = [1, 2, 3]


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_load_from_dict_plain():
    conn = td.MongoDBConn._load_from_dict({"uri": URI})
    assert conn.uri == URI
    assert conn.credentials is None
    assert conn.connection_options == {}


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbconn_load_from_dict_with_credentials():
    conn = td.MongoDBConn._load_from_dict(
        {
            "uri": URI,
            "credentials": {"user": "hi", "password": "bye"},
            "connection_options": {"serverSelectionTimeoutMS": 1000},
        }
    )
    assert conn.uri == URI
    assert conn.credentials == td.UserPasswordCredentials("hi", "bye")
    assert conn.connection_options == {"serverSelectionTimeoutMS": 1000}


# --- MongoDBDest unit tests ---


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_default_options():
    conn = td.MongoDBConn(uri=URI)
    dest = td.MongoDBDest(conn=conn, collections_with_ids=("collection.id", None))
    assert dest.conn is conn
    assert dest.collections_with_ids == [("collection.id", None)]
    assert dest.if_collection_exists == "append"
    assert dest.use_trxs is False
    assert dest.update_existing is True
    assert dest.maintain_order is False
    # Inner destination is built and reflects the conn's uri/credentials.
    assert dest._mongodb_destination is not None
    assert dest._mongodb_destination.uri == URI
    assert dest._mongodb_destination.credentials is None
    assert dest._mongodb_destination.connection_options == {}


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_with_all_options():
    creds = td.UserPasswordCredentials("hi", "bye")
    conn = td.MongoDBConn(uri=URI, credentials=creds, connection_options={"x": 1})
    dest = td.MongoDBDest(
        conn=conn,
        collections_with_ids=("collection.id", "id_column"),
        if_collection_exists="replace",
        use_trxs=True,
        docs_per_trx=500,
        maintain_order=True,
        update_existing=False,
        fail_on_duplicate_key=False,
        log_intermediate_files=True,
    )
    assert dest.collections_with_ids == [("collection.id", "id_column")]
    assert dest.if_collection_exists == "replace"
    assert dest.use_trxs is True
    assert dest.docs_per_trx == 500
    assert dest.maintain_order is True
    assert dest.update_existing is False
    assert dest.fail_on_duplicate_key is False
    assert dest.log_intermediate_files is True
    # Inner destination reflects conn-level params.
    assert dest._mongodb_destination.uri == URI
    assert dest._mongodb_destination.credentials == creds
    assert dest._mongodb_destination.connection_options == {"x": 1}


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_conn_setter_rebuilds_inner():
    first_conn = td.MongoDBConn(uri=URI)
    dest = td.MongoDBDest(
        conn=first_conn,
        collections_with_ids=("collection.id", None),
    )
    first_inner = dest._mongodb_destination
    assert first_inner is not None

    new_creds = td.UserPasswordCredentials("u", "p")
    new_conn = td.MongoDBConn(uri="mongodb://other:27017", credentials=new_creds)
    dest.conn = new_conn
    assert dest.conn is new_conn
    assert dest._mongodb_destination is not first_inner
    assert dest._mongodb_destination.uri == "mongodb://other:27017"
    assert dest._mongodb_destination.credentials == new_creds


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_conn_setter_rejects_bad_type():
    dest = td.MongoDBDest(
        conn=td.MongoDBConn(uri=URI),
        collections_with_ids=("collection.id", None),
    )
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        dest.conn = "not_a_conn"


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_collections_wrong_type_raises():
    # The wrapper's @_td_validate_call rejects this at the pydantic boundary
    # before the inner MongoDBDestination's TypeError can fire.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.MongoDBDest(
            conn=td.MongoDBConn(uri=URI),
            collections_with_ids="not_a_tuple",
        )


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_invalid_if_collection_exists_raises():
    # IfTableExistStrategySpec is a Literal, so pydantic rejects at the
    # wrapper boundary before the inner ValueError can fire.
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.MongoDBDest(
            conn=td.MongoDBConn(uri=URI),
            collections_with_ids=("collection.id", None),
            if_collection_exists="hi",
        )


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_passthrough_setters_update_inner():
    dest = td.MongoDBDest(
        conn=td.MongoDBConn(uri=URI),
        collections_with_ids=("collection.id", None),
    )
    inner = dest._mongodb_destination
    assert inner is not None

    dest.collections_with_ids = [
        ("collection.a", "id"),
        ("collection.b", None),
    ]
    assert dest.collections_with_ids == [
        ("collection.a", "id"),
        ("collection.b", None),
    ]
    assert inner.collections_with_ids == [
        ("collection.a", "id"),
        ("collection.b", None),
    ]

    dest.if_collection_exists = "replace"
    assert dest.if_collection_exists == "replace"
    assert inner.if_collection_exists == "replace"

    dest.use_trxs = True
    assert dest.use_trxs is True
    assert inner.use_trxs is True

    dest.docs_per_trx = 50
    assert dest.docs_per_trx == 50
    assert inner.docs_per_trx == 50

    dest.maintain_order = True
    assert dest.maintain_order is True
    assert inner.maintain_order is True

    dest.update_existing = False
    assert dest.update_existing is False
    assert inner.update_existing is False

    dest.fail_on_duplicate_key = False
    assert dest.fail_on_duplicate_key is False
    assert inner.fail_on_duplicate_key is False

    dest.log_intermediate_files = True
    assert dest.log_intermediate_files is True
    assert inner.log_intermediate_files is True


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_passthrough_setter_validation_propagates():
    dest = td.MongoDBDest(
        conn=td.MongoDBConn(uri=URI),
        collections_with_ids=("collection.id", None),
    )
    # The passthrough setters have no @_td_validate_call, so they forward to
    # the inner destination's setters and surface the inner errors.
    with pytest.raises(ValueError):
        dest.if_collection_exists = "nope"
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        dest.collections_with_ids = 1


@pytest.mark.mongodb
@pytest.mark.unit
def test_mongodbdest_repr():
    dest = td.MongoDBDest(
        conn=td.MongoDBConn(uri=URI),
        collections_with_ids=("collection.id", None),
    )
    assert "MongoDBDest" in repr(dest)
    assert "collections_with_ids=" in repr(dest)


# --- Full pipeline e2e ---


@pytest.mark.mongodb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_mongodbdest(testing_mongodb):
    database_name = "test_output_mongodbdest_database"
    collection_name = "test_output_mongodbdest_collection"
    fn = copy.deepcopy(output_mongodb)
    fn.output = td.MongoDBDest(
        conn=td.MongoDBConn(
            uri=MONGODB_URI_WITHOUT_CREDENTIALS,
            credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        ),
        collections_with_ids=(f"{database_name}.{collection_name}", None),
        if_collection_exists="append",
        update_existing=False,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mongodb_basic", "mock_table.parquet"
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mongodb_basic",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert len(expected_output) == collection.count_documents({})


# --- NamedConn e2e ---


@pytest.mark.mongodb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_mongodb_with_named_conn(testing_mongodb, tmp_path, monkeypatch):
    repo_dir = _write_named_conn_repository(
        tmp_path,
        MONGODB_NAMED_CONN_NAME,
        "MongoDBConn",
        {
            "uri": MONGODB_URI_WITHOUT_CREDENTIALS,
            "credentials": {"user": DB_USER, "password": DB_PASSWORD},
        },
        "dest",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    database_name = "test_output_mongodb_named_conn_database"
    collection_name = "test_output_mongodb_named_conn_collection"
    testing_function = output_mongodb_named_conn_generator(
        conn_name=MONGODB_NAMED_CONN_NAME,
        database_name=database_name,
        collection_name=collection_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mongodb_basic", "mock_table.parquet"
    )

    fnrun = tdfnrun(
        function=testing_function,
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_mongodb_basic",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert len(expected_output) == collection.count_documents({})


@pytest.mark.mongodb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_multiple_outputs_mongodbdest(testing_mongodb):
    database_name = "test_multiple_outputs_mongodbdest_database"
    collection_name = "test_multiple_outputs_mongodbdest_collection"
    fn = copy.deepcopy(multiple_outputs_mongodb)
    fn.output = td.MongoDBDest(
        conn=td.MongoDBConn(
            uri=MONGODB_URI_WITHOUT_CREDENTIALS,
            credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        ),
        collections_with_ids=[
            (f"{database_name}_1.{collection_name}_1", None),
            (f"{database_name}_2.{collection_name}_2", None),
        ],
        if_collection_exists="append",
        update_existing=False,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_multiple_outputs_mongodb", "mock_table.parquet"
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_multiple_outputs_mongodb",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)

    collection = client[database_name + "_1"][collection_name + "_1"]
    assert len(expected_output) == collection.count_documents({})

    collection = client[database_name + "_2"][collection_name + "_2"]
    assert len(expected_output) == collection.count_documents({})


@pytest.mark.mongodb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_mongodbdest_with_none(testing_mongodb):
    database_name = "test_mongodbdest_none_database"
    collection_name = "test_mongodbdest_none_collection"
    fn = copy.deepcopy(output_mongodb_none)
    fn.output = td.MongoDBDest(
        conn=td.MongoDBConn(
            uri=MONGODB_URI_WITHOUT_CREDENTIALS,
            credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        ),
        collections_with_ids=(f"{database_name}.{collection_name}", None),
        if_collection_exists="replace",
        update_existing=True,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mongodb_none", "mock_table.parquet"
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name]
    assert collection.count_documents({}) == 0


@pytest.mark.mongodb
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_mongodbdest_with_list_none(testing_mongodb):
    database_name = "test_mongodbdest_list_none_database"
    collection_name = "test_mongodbdest_list_none_collection"
    fn = copy.deepcopy(output_mongodb_list_none)
    fn.output = td.MongoDBDest(
        conn=td.MongoDBConn(
            uri=MONGODB_URI_WITHOUT_CREDENTIALS,
            credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        ),
        collections_with_ids=[
            (f"{database_name}.{collection_name}_1", None),
            (f"{database_name}.{collection_name}_2", None),
        ],
        if_collection_exists="replace",
        update_existing=True,
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_mongodb_list_none", "mock_table.parquet"
    )
    fnrun = tdfnrun(fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0

    client = pymongo.MongoClient(MONGODB_URI_WITH_CREDENTIALS)
    collection = client[database_name][collection_name + "_1"]
    assert collection.count_documents({}) == 0

    collection = client[database_name][collection_name + "_2"]
    assert collection.count_documents({}) == 0
