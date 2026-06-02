
import pytest

from tabsdatak.api import Var, Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mongodb import MongoDBDestConn, MongoDBDest
from tabsdatak.conn.mongodb.error import (
    MongoDBValidateException,
    MongodbErrorCode,
)
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.mongodb


def test_mongodb_dest_full_round_trip():
    dest = MongoDBDest(
        collections_with_ids=[("users", "user_id"), ("orders", None)],
        if_collection_exists="replace",
        use_trxs=True,
        docs_per_trx=500,
        maintain_order=True,
        update_existing=False,
        fail_on_duplicate_key=False,
        log_intermediate_files=True,
        dest_cfg={"mongodb.logging_level": 20},
    )
    loaded = MongoDBDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.collections_with_ids == [("users", "user_id"), ("orders", None)]
    assert loaded.if_collection_exists == "replace"
    assert loaded.use_trxs is True
    assert loaded.docs_per_trx == 500
    assert loaded.maintain_order is True
    assert loaded.update_existing is False
    assert loaded.fail_on_duplicate_key is False
    assert loaded.log_intermediate_files is True
    assert loaded.dest_cfg == {"mongodb.logging_level": 20}


def test_mongodb_dest_defaults():
    dest = MongoDBDest(
        collections_with_ids=[("users", None)],
    )
    assert dest.if_collection_exists == "append"
    assert dest.use_trxs is False
    assert dest.docs_per_trx == 1000
    assert dest.maintain_order is False
    assert dest.update_existing is True
    assert dest.fail_on_duplicate_key is True
    assert dest.log_intermediate_files is False
    assert dest.dest_cfg is None


def test_mongodb_conn_accepts_plain_str_uri():
    conn = MongoDBDestConn(
        uri="mongodb+srv://cluster.example.com",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )
    assert conn.uri == "mongodb+srv://cluster.example.com"


def test_mongodb_conn_accepts_secret_uri():
    conn = MongoDBDestConn(
        uri=Secret.lit("mongodb://mongo:27017"),
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )
    assert isinstance(conn.uri, Secret)


def test_mongodb_conn_accepts_env_uri():
    conn = MongoDBDestConn(
        uri=Var._var_key("MONGO_URI"),
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )
    assert isinstance(conn.uri, Var)


def test_mongodb_conn_rejects_bad_uri_scheme():
    # The ``uri`` field is a Union of ``str | Secret | Var``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        MongoDBDestConn(
            uri="mysql://nope",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


def test_mongodb_dest_rejects_bad_collection_name():
    # ``$`` is forbidden in collection names.
    with pytest.raises(ConnInitException):
        MongoDBDest(
            collections_with_ids=[("bad$name", None)],
        )


def test_mongodb_dest_rejects_bad_id_field_name():
    # Dot is forbidden in field names.
    with pytest.raises(ConnInitException):
        MongoDBDest(
            collections_with_ids=[("users", "bad.field")],
        )


def test_mongodb_dest_rejects_non_positive_docs_per_trx():
    with pytest.raises(MongoDBValidateException) as ei:
        MongoDBDest(
            collections_with_ids=[("users", None)],
            docs_per_trx=0,
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_17


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_mongodb_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MongoDBDest(
            collections_with_ids=[("users", None)],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_2


def test_mongodb_dest_accepts_known_dest_cfg_keys():
    dest = MongoDBDest(
        collections_with_ids=[("users", None)],
        dest_cfg={"mongodb.logging_level": 20},
    )
    assert dest.dest_cfg == {"mongodb.logging_level": 20}


def test_mongodb_conn_rejects_missing_uri():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        MongoDBDestConn(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )
    assert ei.value.api_error is MongodbErrorCode.MONGODB_1
