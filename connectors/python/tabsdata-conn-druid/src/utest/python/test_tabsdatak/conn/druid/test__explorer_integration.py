#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``DruidConnectionExplorer`` tests against a real Druid
cluster.

Druid datasources are created by ingestion (there is no ``users`` /
``orders`` CREATE-TABLE seed), so the catalog surface of
``DruidConnectionExplorer`` -- ``check_connect`` / ``metadata`` /
``data`` -- is exercised structurally: the probe succeeds, ``metadata``
returns ``SQLEntity`` objects for the ``druid`` schema, and ``data``
samples whatever datasource is present.

The ``fresh_druid`` fixture below mirrors the one in the plugin
integration suite (built from the session-scoped ``druid_config`` and the
shared ``S30__*`` S3 staging bundle); it skips when S3 staging isn't
configured.

Note: the explorer queries the Druid **Broker**'s SQL endpoint, and
``make_url`` uses the port carried by the connection URL (falling back to
the Broker's standard port only when none is given). A ``DruidDestConn``
nominally holds the *Overlord* URL, but this explorer never touches the
Overlord -- so the fixture points ``overlord_url`` at
``druid_config.broker_url``, letting the port-aware ``make_url`` reach the
Broker even though the docker stack maps it to its own host port.
"""

from __future__ import annotations

import os

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.druid import DruidDestConn, S3Location
from tabsdatak.conn.druid.explorer import DruidConnectionExplorer

# Share the serialization domain of the other ``druid_config`` consumers.
pytestmark = [
    pytest.mark.druid,
    pytest.mark.xdist_group("druid-plugin_integration_seq"),
]

_S3_URI = os.environ.get("S30__S3_URI")
_S3_REGION = os.environ.get("S30__S3_REGION")
_S3_ACCESS_KEY = os.environ.get("S30__S3_ACCESS_KEY")
_S3_SECRET_KEY = os.environ.get("S30__S3_SECRET_KEY")


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    body = uri[len("s3://") :] if uri.startswith("s3://") else uri
    bucket, _, base_path = body.partition("/")
    # base_path is a FullPathSpec (leading "/", no trailing "/"); a
    # bucket-only URI yields the bare root "/".
    return bucket, "/" + base_path.strip("/")


@pytest.fixture
def fresh_druid(druid_config) -> DruidDestConn:
    if not all((_S3_URI, _S3_REGION, _S3_ACCESS_KEY, _S3_SECRET_KEY)):
        pytest.skip(
            "Druid staging not configured. Set S30__S3_URI / S30__S3_REGION "
            "/ S30__S3_ACCESS_KEY / S30__S3_SECRET_KEY to run these tests."
        )
    bucket, base_path = _parse_s3_uri(_S3_URI)
    return DruidDestConn(
        # The explorer only reads ``overlord_url`` to build the Broker SQL
        # URL, and ``make_url`` honors the port in it -- so feed the Broker
        # endpoint here to reach Druid's SQL service in the docker stack.
        overlord_url=druid_config.broker_url,
        staging=S3Location(
            bucket=bucket,
            region=_S3_REGION,
            credentials=AwsAccessSecretKey(
                access_key_id=Secret(_S3_ACCESS_KEY),
                secret_access_key=Secret(_S3_SECRET_KEY),
            ),
            base_path=base_path,
        ),
        credentials=UserPassword(user=Secret("admin"), password=Secret("")),
    )


def test_check_connect_succeeds(fresh_druid):
    assert DruidConnectionExplorer().check_connect(fresh_druid) is None


def test_metadata_returns_sql_entities(fresh_druid):
    entities = DruidConnectionExplorer().metadata(fresh_druid)
    assert isinstance(entities, list)
    assert all(isinstance(e, SQLEntity) for e in entities)
    # The scan is pinned to the ``druid`` schema.
    assert all(e.schema == "druid" for e in entities)


def test_data_reads_sample_rows(fresh_druid):
    explorer = DruidConnectionExplorer()
    entities = explorer.metadata(fresh_druid)
    if not entities:
        pytest.skip("no datasources present in the Druid cluster")

    rows = explorer.data(fresh_druid, entities[0], limit=5)
    assert isinstance(rows, list)
    assert len(rows) <= 5
    expected = {c.name for c in entities[0].columns}
    assert all(set(r) == expected for r in rows)
