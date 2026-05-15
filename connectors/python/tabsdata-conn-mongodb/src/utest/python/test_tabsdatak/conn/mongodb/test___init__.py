
import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mongodb import MongoDBConn, MongoDBDest


pytestmark = pytest.mark.mongodb


def _conn():
    return MongoDBConn(
        uri="mongodb://mongo.example.com:27017",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"mongodb.timeout": 30},
    )


def test_mongodb_dest_full_round_trip():
    dest = MongoDBDest(
        conn=_conn(),
        collections_with_ids=[("users", "user_id"), ("orders", None)],
        if_collection_exists="replace",
        use_trxs=True,
        docs_per_trx=500,
        maintain_order=True,
        dest_cfg={"chunkSize": 1024},
    )
    loaded = MongoDBDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.conn_cfg == {"mongodb.timeout": 30}
    assert loaded.collections_with_ids == [("users", "user_id"), ("orders", None)]
    assert loaded.if_collection_exists == "replace"
    assert loaded.use_trxs is True
    assert loaded.docs_per_trx == 500
    assert loaded.maintain_order is True
    assert loaded.dest_cfg == {"chunkSize": 1024}


def test_mongodb_dest_round_trip_with_named_conn():
    dest = MongoDBDest(
        conn=NamedConn(name="prod_mongodb"),
        collections_with_ids=[("users", None)],
    )
    loaded = MongoDBDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_mongodb"
