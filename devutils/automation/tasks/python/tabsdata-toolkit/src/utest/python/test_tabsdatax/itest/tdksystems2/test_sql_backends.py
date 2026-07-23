#
# Copyright 2026 Tabsdata Inc.
#

"""Dialect-agnostic checks for the SQL container backends (no docker needed).

Every registered SQL system must ship the per-model DDL its shared commands
expect and expose the model/data/change capabilities, so the generic
create-model / create-data / modify-data verbs work for it.
"""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2 import datagen, systems
from tabsdatax.itest.tdksystems2.systems._common.sql import SqlContainerBackend

# SQL *data* systems: a SqlContainerBackend subclass that drives a SQLAlchemy
# dialect and ships model DDL. mongodb (supports_models=False) and redpanda
# (produces JSON to Kafka — no SQL driver / DDL) subclass the base only for its
# container lifecycle, so they're excluded.
_SQL_SLUGS = [
    s
    for s in systems.all_slugs()
    if isinstance(systems.get(s), SqlContainerBackend)
    and systems.get(s).supports_models
    and systems.get(s).driver
]


@pytest.mark.parametrize("slug", _SQL_SLUGS)
def test_sql_backend_capabilities(slug):
    b = systems.get(slug)
    # SQL data-plane capabilities: models + data. (is_container is not asserted —
    # a remote warehouse like snowflake sets it False; supports_changes is not
    # asserted — an OLAP store like clickhouse sets it False.)
    assert b.supports_models and b.supports_data
    assert b.slug and b.label and b.driver


@pytest.mark.parametrize("slug", _SQL_SLUGS)
def test_sql_backend_ships_ddl_for_its_models(slug):
    b = systems.get(slug)
    avail = b.available_models()
    assert avail, f"{slug} ships no model DDL"
    for model in avail:
        # DDL is present and mentions every entity the datagenerator produces.
        ddl = b.ddl_text(model).lower()
        for entity in datagen.entities_of(model):
            assert entity.lower() in ddl, f"{slug}/{model}: '{entity}' missing from DDL"


def test_mysql_and_postgres_have_every_model():
    everything = set(datagen.list_models())
    assert set(systems.get("mysql").available_models()) == everything
    assert set(systems.get("postgres").available_models()) == everything


def test_postgres_dialect_sql():
    pg = systems.get("postgres")
    create = pg.create_domain_sql("d_x", "d_x", "pw")
    assert create[0] == 'CREATE DATABASE "d_x"'
    assert any("OWNER TO" in s for s in create)
    assert pg.delete_domain_sql("d_x")[0].startswith('DROP DATABASE IF EXISTS "d_x"')
    assert pg.admin_autocommit and pg.admin_db == "postgres"
