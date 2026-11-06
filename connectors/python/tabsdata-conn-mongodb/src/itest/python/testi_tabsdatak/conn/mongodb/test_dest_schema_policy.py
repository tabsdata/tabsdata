#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mongodb import MongoDBDomain, mongodb_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    SCHEMALESS_CASES,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.mongodb.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mongodb import MongoDBDest, _plugin as plugin

pytestmark = pytest.mark.mongodb


def _policy(domain: MongoDBDomain, work: Path) -> Policy:
    db = store(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin._MongoDBDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn(domain),
            MongoDBDest(
                collections_with_ids=[(table, None)], if_collection_exists=mode
            ),
            [frame],
        )

    return Policy(write=write, read=db.read_collection, drop=db.drop_collection)


@pytest.mark.parametrize("case", SCHEMALESS_CASES)
@mongodb_domain("mongodb")
def test_schema_policy(domain_mongodb: MongoDBDomain, tmp_path: Path, case, request):
    run(case, _policy(domain_mongodb, work_dir(tmp_path)), table_for(request))
