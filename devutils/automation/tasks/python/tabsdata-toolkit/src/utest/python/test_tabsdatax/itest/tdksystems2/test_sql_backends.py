#
# Copyright 2026 Tabsdata Inc.
#

"""Dialect-agnostic checks for the SQL container backends (no docker needed).

Every registered SQL system must ship the per-model DDL its shared commands
expect and expose the model/data/change capabilities, so the generic
create-model / create-data / modify-data verbs work for it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems2 import datagen, options, systems
from tabsdatax.itest.tdksystems2.errors import TdxError
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


def test_rdspostgres_is_a_remote_provision_only_system():
    rds = systems.get("rdspostgres")
    assert not rds.is_container and rds.docker_images() == []
    assert not (rds.supports_models or rds.supports_data or rds.supports_changes)
    assert rds.supports_purge and "rdsadmin" in rds.system_schemas


def test_rdspostgres_options_come_from_the_environment():
    opts = systems.get("rdspostgres").options()
    assert [o.name for o in opts] == [
        "HOSTNAME", "PORT", "USERNAME", "PASSWORD", "REGION"
    ]
    assert all(o.source == options.ENV for o in opts)
    assert [o.name for o in opts if o.secret] == ["PASSWORD"]


def test_rdspostgres_creates_the_owner_before_the_database():
    # The RDS master user is not a superuser: it must already be a member of the
    # owner role to create a database owned by it, and the owner needs CREATEROLE
    # to add roles of its own inside that database.
    create = systems.get("rdspostgres").create_domain_sql("d_x", "d_x", "pw")
    assert create[0] == 'CREATE USER "d_x" WITH PASSWORD \'pw\' CREATEROLE'
    assert create[1] == 'GRANT "d_x" TO CURRENT_USER'
    assert create[2] == 'CREATE DATABASE "d_x" OWNER "d_x"'


def test_rdspostgres_drops_the_roles_the_owner_created_before_the_owner():
    # The owner is the grantor of the memberships among the roles it created
    # with CREATEROLE, and PostgreSQL 16+ refuses to drop a grantor with grants
    # outstanding. A '%' anywhere would be a driver placeholder.
    delete = systems.get("rdspostgres").delete_domain_sql("d_x")
    assert delete[0] == 'DROP DATABASE IF EXISTS "d_x" WITH (FORCE)'
    assert delete[1].startswith("DO $$") and "m.admin_option" in delete[1]
    assert "o.rolname = 'd_x'" in delete[1] and "DROP ROLE" in delete[1]
    assert delete[2] == 'DROP USER IF EXISTS "d_x"'
    assert not any("%" in st for st in delete)


def test_rdspostgres_lists_owner_roles_that_outlived_their_database():
    sql = systems.get("rdspostgres").list_schemas_sql()
    assert "FROM pg_database" in sql and "UNION" in sql
    assert "pg_auth_members" in sql and "CURRENT_USER" in sql
    assert "%" not in sql


_RDS_SSL = "sslmode=verify-full"
_RDS_CA = "sslrootcert=%2Ftmp%2Fbundle.pem"


@pytest.fixture
def rds(monkeypatch):
    """The RDS backend with the CA-bundle download stubbed, plus its system
    meta."""
    backend = systems.get("rdspostgres")
    monkeypatch.setattr(type(backend), "ca_bundle", lambda self: "/tmp/bundle.pem")
    return backend, backend.plan_system(
        "rds",
        {
            "HOSTNAME": "td.eu-west-1.rds.amazonaws.com",
            "PORT": "5432",
            "USERNAME": "master",
            "PASSWORD": "mpw",
            "REGION": "eu-west-1",
        },
    )


def test_rdspostgres_domain_targets_the_instance(rds):
    backend, sys_meta = rds
    assert sys_meta["LOCATION"] == "postgresql://td.eu-west-1.rds.amazonaws.com:5432"

    meta = backend.domain_meta(sys_meta, "d_u_0_dom", "opw")
    assert meta["SQLALCHEMY_URI"] == (
        "postgresql+psycopg2://d_u_0_dom:opw@td.eu-west-1.rds.amazonaws.com:5432"
        "/d_u_0_dom?sslmode=verify-full"
    )
    assert "@" not in meta["URI"] and "/d_u_0_dom?" in meta["URI"]
    assert meta["SYS_ADMIN_USER"] == "master" and meta["REGION"] == "eu-west-1"
    assert "SYS_CONTAINER" not in meta


def test_rdspostgres_verifies_tls_on_every_connection(rds):
    backend, sys_meta = rds
    # tdx's own connection carries the throwaway CA bundle …
    assert backend._admin_url(backend._admin(sys_meta)).endswith(
        f"/postgres?{_RDS_SSL}&{_RDS_CA}"
    )
    # … the descriptor's URIs verify too, but never name the /tmp bundle.
    urls = [
        v
        for v in backend.domain_meta(sys_meta, "d_u_0_dom", "opw").values()
        if v.startswith("postgresql+")
    ]
    assert urls and all(u.endswith(f"?{_RDS_SSL}") for u in urls)
    assert not any("sslrootcert" in u for u in urls)


def test_rdspostgres_url_survives_a_password_with_a_percent_escape(rds):
    # SQLAlchemy percent-decodes the credentials it parses out of a URL, so an
    # RDS master password containing a literal '%41' must not reach the server
    # as 'A'.
    from sqlalchemy.engine import make_url

    backend, sys_meta = rds
    pw = "p%41ss?w#rd&x=1 y"
    url = backend.sqlalchemy_url("h", "5432", db="postgres", user="ma ster", password=pw)
    parsed = make_url(url)
    assert parsed.password == pw and parsed.username == "ma ster"
    assert dict(parsed.query)["sslmode"] == "verify-full"


def test_rdspostgres_ca_bundle_is_a_fresh_pem_in_tmp(monkeypatch, tmp_path):
    from tabsdatax.itest.tdksystems2.systems.rdspostgres import backend as rdsmod

    fetched = []

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b"-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n"

    def _urlopen(url, timeout=None):
        fetched.append(url)
        return _Resp()

    monkeypatch.setattr(rdsmod.urllib.request, "urlopen", _urlopen)
    monkeypatch.setattr(rdsmod, "CA_BUNDLE_PATH", str(tmp_path / "bundle.pem"))
    backend = systems.get("rdspostgres")

    path = backend.ca_bundle()
    assert fetched == [rdsmod.CA_BUNDLE_URL]
    assert Path(path).read_bytes().startswith(b"-----BEGIN CERTIFICATE-----")
    # fetched once per run, not once per connection
    assert backend.ca_bundle() == path and len(fetched) == 1
    # nothing but the bundle is left behind (the staged pid file is renamed)
    assert [p.name for p in tmp_path.iterdir()] == ["bundle.pem"]


def test_rdspostgres_rejects_a_ca_bundle_that_is_not_a_pem(monkeypatch):
    from tabsdatax.itest.tdksystems2.systems.rdspostgres import backend as rdsmod

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b"<html>404</html>"

    monkeypatch.setattr(
        rdsmod.urllib.request, "urlopen", lambda url, timeout=None: _Resp()
    )
    with pytest.raises(TdxError, match="did not return a PEM bundle"):
        systems.get("rdspostgres").ca_bundle()
