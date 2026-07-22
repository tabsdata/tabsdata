#
# Copyright 2026 Tabsdata Inc.
#

"""
End-to-end test for NamedConn resolution in ``PostgresCdcTrigger``.

Structurally mirrors ``test_continuous_dml`` in
``test_cdc_postgres_continuous_tables_1.py`` but constructs the trigger with
``td.NamedConn(name=...)`` instead of a concrete ``PostgresCdcConn``.
``TD_URI_REPOSITORY`` is pointed at a temp directory holding a
``PostgresCdcConn`` descriptor matching the test container; resolution happens
automatically inside the trigger-runner code path (mirroring
``execute_stage_trigger`` in production), so the test body never invokes
``resolve_named_conn_if_needed`` itself.

Isolation from ``test_continuous_dml`` and other shared-container tests:

- Distinct table prefix (``td_named_cdc_<suffix>_t{i}`` vs.
  ``td_con_<suffix>_t{i}``) so we never share a table with another test.
- Per-test replication slot via the ``postgres_cdc_slot`` fixture.
- Single-table / single-DML-thread layout — no parametrize sweep — so the
  named-conn cell isn't competing with the matrix in
  ``test_continuous_tables_1.py`` for container resources.

If named-conn resolution regresses on ``PostgresCdcTrigger``, this test fails
at trigger start-up — the ingester would see a ``NamedConn`` and crash before
capturing any DML.
"""

import json
import logging
import os
import random
import string
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

import tabsdata as td
from tabsdata._io.connections.named_connection.kind_registry import kind_for
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
)
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.postgres._connector import PostgresCdcTrigger
from test_tabsdata.conftest import MOCK_PROJECT_ID
from test_tabsdata_cdc_common.common.shared import TRIGGER_END_MINUTES
from test_tabsdata_cdc_common.common.continuous import run_continuous_dml_test
from test_tabsdata_cdc_postgres.common.sql import PostgresCdcSampler

pytestmark = [pytest.mark.xdist_group("postgres_cdc_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

NAMED_CONN_NAME = "test_postgres_cdc_named"


def _write_named_conn_repository(tmp_path, name, conn_type, conn_configs):
    """Write a single-connection NamedConn repository under ``tmp_path``.

    Mirrors the helper in
    ``test_tabsdata.test_tabsserver.test_named_conn_e2e._write_named_conn_repository``.
    """
    repo_dir = os.path.join(str(tmp_path), "repository")
    # Worker artifacts are project-namespaced under the project the tdfnrun
    # request yamls carry.
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER, MOCK_PROJECT_ID)
    os.makedirs(connections_dir, exist_ok=True)

    cx_dict = {
        "kind": kind_for(conn_type, "src"),
        "apiVersion": "1.0",
        "name": name,
        "conn_configs": conn_configs,
    }
    connection_file = os.path.join(connections_dir, f"{name.lower()}.json")
    with open(connection_file, "w") as f:
        json.dump(cx_dict, f)

    return repo_dir


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_continuous_dml_with_named_conn(
    postgres_cdc_config,
    postgres_cdc_slot,
    tmp_path,
    monkeypatch,
):
    config = postgres_cdc_config
    schema = config.cdc_user

    uri = f"postgresql://{config.host}:{config.port}/{config.database}"
    repo_dir = _write_named_conn_repository(
        tmp_path,
        NAMED_CONN_NAME,
        "PostgresCdcConn",
        {
            "uri": uri,
            "credentials": {
                "user": config.cdc_user,
                "password": config.cdc_password,
            },
        },
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    tables = [TableObject(schema=schema, table=f"td_named_cdc_{suffix}_t0")]
    logger.debug(f"Tracking tables: {tables}")
    logger.debug(f"Replication slot: {postgres_cdc_slot}")

    connection = psycopg2.connect(
        host=config.host,
        port=str(config.port),
        dbname=config.database,
        user=config.root_user,
        password=config.root_password,
    )
    connection.autocommit = False

    cursor = connection.cursor()
    sampler = PostgresCdcSampler(config.container, config.port, cursor)

    for table in tables:
        sampler.create_table(table)
    connection.commit()

    trigger = PostgresCdcTrigger(
        conn=td.NamedConn(name=NAMED_CONN_NAME),
        tables=[table.qualified for table in tables],
        start_from="head",
        replication_slot=postgres_cdc_slot,
        slot_behavior="reuse",
        blocking_timeout_seconds=4,
        buffer_max_rows=64,
        buffer_max_secs=4,
        trigger_max_rows=128,
        trigger_max_secs=4,
        end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
    )

    run_continuous_dml_test(
        sampler=sampler,
        connection=connection,
        tables=tables,
        trigger=trigger,
        dml_worker_threads=1,
    )
