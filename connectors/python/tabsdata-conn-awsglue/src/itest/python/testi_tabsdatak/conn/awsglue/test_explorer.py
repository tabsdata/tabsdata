#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.awsglue import AwsGlueDomain, awsglue_domain
from testi_tabsdatak.conn.awsglue.backend import TrxCtx, dest_conn, store
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import AwsGlueDest, _plugin as plugin
from tabsdatak.conn.awsglue._explorer import AwsGlueConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey

pytestmark = pytest.mark.conn_awsglue

ROWS = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]})


@pytest.fixture
def explored(domain_awsglue: AwsGlueDomain, tmp_path: Path):
    """A table the connector wrote, for the explorer to find. Named apart per
    use: the tests taking it run at once on different workers."""
    table = f"td_explored_{uuid.uuid4().hex[:8]}"
    catalog = store(domain_awsglue)
    catalog.drop_table(table)
    plugin.AwsGlueDestPlugin().write_out(
        TrxCtx(work_dir(tmp_path)),
        dest_conn(domain_awsglue),
        AwsGlueDest(tables=[table]),
        [ROWS.lazy()],
    )
    try:
        yield table
    finally:
        catalog.drop_table(table)


def _entity(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


@awsglue_domain("awsglue")
def test_check_connect_succeeds(domain_awsglue: AwsGlueDomain):
    conn = dest_conn(domain_awsglue)
    assert AwsGlueConnectionExplorer().check_connect(conn) is None


@awsglue_domain("awsglue")
def test_check_connect_succeeds_without_a_default_database(
    domain_awsglue: AwsGlueDomain,
):
    conn = dest_conn(domain_awsglue, database=None)
    assert AwsGlueConnectionExplorer().check_connect(conn) is None


@awsglue_domain("awsglue")
def test_check_connect_bad_credentials_raises(domain_awsglue: AwsGlueDomain):
    bad = dest_conn(
        domain_awsglue,
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIAIOSFODNN7EXAMPLE"),
            secret_access_key=Secret("definitely-wrong-secret"),
        ),
    )
    with pytest.raises(Exception):  # noqa: B017 -- whatever botocore raises
        AwsGlueConnectionExplorer().check_connect(bad)


@awsglue_domain("awsglue")
def test_check_connect_unknown_database_raises(domain_awsglue: AwsGlueDomain):
    conn = dest_conn(domain_awsglue, database="td_no_such_database")
    with pytest.raises(Exception):  # noqa: B017 -- whatever botocore raises
        AwsGlueConnectionExplorer().check_connect(conn)


@awsglue_domain("awsglue")
def test_metadata_reports_a_written_table_with_its_columns(
    domain_awsglue: AwsGlueDomain, explored: str
):
    entities = AwsGlueConnectionExplorer().metadata(dest_conn(domain_awsglue))
    entity = _entity(entities, explored)
    assert entity.database == domain_awsglue.glue_database
    assert entity.fqn() == f"{domain_awsglue.glue_database}.{explored}"
    assert [(c.name, c.type) for c in entity.columns] == [
        ("id", "BIGINT"),
        ("name", "STRING"),
    ]


@awsglue_domain("awsglue")
def test_data_samples_the_rows_of_a_written_table(
    domain_awsglue: AwsGlueDomain, explored: str
):
    conn = dest_conn(domain_awsglue)
    explorer = AwsGlueConnectionExplorer()
    entity = _entity(explorer.metadata(conn), explored)
    sampled = explorer.data(conn, entity, limit=2)
    assert len(sampled) == 2
    assert {row["id"] for row in sampled} <= {1, 2, 3}
