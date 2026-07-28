#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mongodb import MongoDBDomain, mongodb_domain
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.mongodb.backend import MongoStore, dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mongodb import MongoDBDest, _plugin as plugin

pytestmark = pytest.mark.mongodb

# Types the MongoDB write path can't encode today (surfaced by the matrix): BSON has
# no codec for them and the writer registers none, so the insert itself fails.
_XFAIL: dict[str, str] = {}


# The matrix reads back through a table-shaped store; MongoStore speaks collections.
class _CollectionStore:
    def __init__(self, mongo: MongoStore) -> None:
        self._mongo = mongo

    def read_table(self, table: str) -> pl.DataFrame:
        return self._mongo.read_collection(table)

    def drop_table(self, table: str) -> None:
        self._mongo.drop_collection(table)


def _params():
    for case in DATATYPE_CASES:
        marks = (
            [pytest.mark.xfail(reason=_XFAIL[case.id], strict=True)]
            if case.id in _XFAIL
            else []
        )
        # One fixed collection per case, so a run leaves nothing unpredictable behind.
        collection = f"td_dt_{case.id}"
        yield pytest.param(case, collection, id=case.id, marks=marks)


@pytest.mark.parametrize("case, collection", list(_params()))
@mongodb_domain("mongodb")
def test_datatype_create_and_load(
    domain_mongodb: MongoDBDomain,
    case: DataTypeCase,
    collection: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=collection,
        dest_plugin=plugin._MongoDBDestPlugin(),
        make_dest=lambda collections: MongoDBDest(
            collections_with_ids=[(name, None) for name in collections],
            if_collection_exists="replace",
        ),
        dest_conn=dest_conn(domain_mongodb),
        sql=_CollectionStore(store(domain_mongodb)),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)
