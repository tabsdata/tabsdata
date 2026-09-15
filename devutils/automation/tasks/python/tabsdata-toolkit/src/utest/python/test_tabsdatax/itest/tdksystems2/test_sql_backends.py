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
from tabsdatax.itest.tdksystems2.systems._common.managed_postgres import (
    ManagedPostgres,
)
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


def test_managed_postgres_pair_shares_a_base_and_filters_its_own_provider():
    """rdspostgres and gcppostgres differ only where the provider does. The role
    filter is the load-bearing part: `purge` drops what a listing offers, and a
    managed instance grants its master service roles of its own."""
    rds, gcp = systems.get("rdspostgres"), systems.get("gcppostgres")
    for backend in (rds, gcp):
        assert isinstance(backend, ManagedPostgres)
        assert not backend.is_container and backend.supports_purge
        # every connection option reads the backend's own env prefix
        env_opts = [o for o in backend.options() if o.default_doc.startswith("env $")]
        assert env_opts and all(
            o.default_doc.startswith(f"env ${backend.env_prefix}__") for o in env_opts
        )

    # RDS names an instance by region; Cloud SQL is reached at its host, so
    # there the region is recorded but never holds a create up
    def region(backend):
        return next(o for o in backend.options() if o.name == "REGION")

    assert region(rds).required and not region(gcp).required
    assert "informational" in region(gcp).default_doc

    assert "rdsadmin" in rds.system_schemas and "rds_" in rds.role_prefixes
    assert "cloudsqladmin" in gcp.system_schemas and "cloudsql" in gcp.role_prefixes
    # the master holds cloudsqlsuperuser; without the prefix it lists as a domain
    assert "starts_with(r.rolname, 'cloudsql')" in gcp.list_schemas_sql()
    assert "starts_with(r.rolname, 'rds_')" in rds.list_schemas_sql()
    for backend in (rds, gcp):
        assert "starts_with(r.rolname, 'pg_')" in backend.list_schemas_sql()
    # neither borrows the other's provider names
    assert "cloudsql" not in rds.list_schemas_sql()
    assert "rds_" not in gcp.list_schemas_sql()


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
_RDS_PEM = "-----BEGIN CERTIFICATE-----\nbundle\n-----END CERTIFICATE-----\n"


@pytest.fixture
def rds(monkeypatch):
    """The RDS backend with the CA-bundle download stubbed, plus its system
    meta."""
    backend = systems.get("rdspostgres")
    monkeypatch.setattr(type(backend), "_ca_pem", lambda self, meta: _RDS_PEM)
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
    # the domain descriptor is enough to reach the instance without the system's
    assert meta["SSLMODE"] == "verify-full"


def test_rdspostgres_verifies_tls_on_every_connection(rds):
    backend, sys_meta = rds
    admin = backend._admin(sys_meta)
    # the bundle is the certificate, not a path: one is materialized only for
    # as long as a connection needs it …
    assert admin.ca_pem == _RDS_PEM and not admin.sslrootcert
    assert backend._admin_url(admin).endswith(f"/postgres?{_RDS_SSL}")
    with backend._ca_file(admin) as connectable:
        written = Path(connectable.sslrootcert)
        assert written.read_text() == _RDS_PEM
        assert connectable.sslrootcert in backend._admin_url(connectable).replace(
            "%2F", "/"
        )
    assert not written.exists() and not written.parent.exists()

    # … and the descriptor's URIs verify too, but never name that file.
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
    parsed = make_url(backend._with_ssl(url, backend.ssl_mode))
    assert parsed.password == pw and parsed.username == "ma ster"
    assert dict(parsed.query)["sslmode"] == "verify-full"


def test_rdspostgres_ca_bundle_is_fetched_once_and_never_kept_on_disk(monkeypatch):
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
    backend = systems.get("rdspostgres")

    pem = backend._ca_pem({})
    assert fetched == [rdsmod.CA_BUNDLE_URL]
    assert pem.startswith("-----BEGIN CERTIFICATE-----")
    # fetched once per run, not once per connection
    assert backend._ca_pem({}) == pem and len(fetched) == 1


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
        systems.get("rdspostgres")._ca_pem({})


_GCP_BASE = {
    "HOSTNAME": "10.0.0.1",
    "PORT": "5432",
    "USERNAME": "postgres",
    "PASSWORD": "p",
    "REGION": "europe-west1",
}


def test_gcppostgres_urls_carry_the_mode_and_keep_the_root_cert_to_admin():
    """The root cert rides on tdx's own admin connections only: a descriptor's
    URIs carry the mode alone, since a connector-under-test brings its own root."""
    gcp = systems.get("gcppostgres")
    sys_meta = gcp.plan_system(
        "gcppostgres",
        {
            "HOSTNAME": "10.0.0.1",
            "PORT": "5432",
            "USERNAME": "postgres",
            "PASSWORD": "p@ss %20 word",
            "SSLMODE": "require",
        },
    )
    assert sys_meta["LOCATION"] == "postgresql://10.0.0.1:5432"

    admin_url = gcp._admin_url(gcp._admin(sys_meta))
    assert "sslmode=require" in admin_url
    # the password is percent-encoded, so a literal '%20' survives as itself
    assert "p%40ss%20%2520%20word" in admin_url

    dom = gcp.domain_meta(sys_meta, "d1", "secret")
    assert dom["SSLMODE"] == "require" and dom["REGION"] == ""
    # a domain records which instance its database lives in: not derivable from
    # a host that may be a bare IP
    assert dom["INSTANCE_ID"] == ""
    for key in ("SQLALCHEMY_URI", "URI"):
        assert "sslmode=require" in dom[key] and "sslrootcert" not in dom[key]
    # a container's keys have no meaning on an externally-owned instance
    assert not {"IMAGE", "SYS_CONTAINER", "SYS_CONTAINER_ID"} & set(dom)


def test_gcppostgres_stores_the_ca_itself_and_gives_a_connection_a_file(tmp_path):
    """The descriptor keeps the certificate, not a path into somebody's home
    directory: a path is read at create, the domain inherits the PEM, and a
    connection gets a file materialized from it."""
    pem = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"
    src = tmp_path / "server-ca.pem"
    src.write_text(pem)
    gcp = systems.get("gcppostgres")

    # a path is read at create; the PEM is what is stored
    meta = gcp.plan_system(
        "gcppostgres", {**_GCP_BASE, "SERVER_CA_CERT_PATH": str(src)}
    )
    assert meta["SERVER_CA_CERT"] == pem
    # and so is the PEM handed over directly ($GCPPOSTGRES__SERVER_CA_CERT)
    assert (
        gcp.plan_system("gcppostgres", {**_GCP_BASE, "SERVER_CA_CERT": pem})[
            "SERVER_CA_CERT"
        ]
        == pem
    )

    # the domain inherits the certificate, so it needs no back-ref to the system
    dom = gcp.domain_meta(meta, "d1", "secret")
    assert dom["SERVER_CA_CERT"] == pem
    # ... and never leaks into the URIs a connector-under-test is handed
    assert "sslrootcert" not in dom["SQLALCHEMY_URI"] + dom["URI"]

    # a connection gets a real file -- and only for as long as it is connecting
    admin = gcp._admin(dom)
    assert admin.ca_pem == pem and not admin.sslrootcert
    with gcp._ca_file(admin) as connectable:
        written = Path(connectable.sslrootcert)
        assert written.read_text() == pem
        assert connectable.sslrootcert in gcp._admin_url(connectable).replace(
            "%2F", "/"
        )
    assert not written.exists() and not written.parent.exists()

    for bad in (
        {"SERVER_CA_CERT": "not a pem"},
        {"SERVER_CA_CERT_PATH": str(tmp_path / "x")},
    ):
        with pytest.raises(TdxError):
            gcp.plan_system("gcppostgres", {**_GCP_BASE, **bad})
    with pytest.raises(TdxError, match="not both"):
        gcp.plan_system(
            "gcppostgres",
            {**_GCP_BASE, "SERVER_CA_CERT": pem, "SERVER_CA_CERT_PATH": str(src)},
        )


def test_gcppostgres_refuses_a_verifying_sslmode_without_the_instance_ca(tmp_path):
    """Cloud SQL signs each instance with a CA of its own -- there is no bundle to
    download -- and libpq reports the miss as a bare 'certificate verify failed'
    long after create. So create refuses first, naming the way to get the cert."""
    gcp = systems.get("gcppostgres")
    # the default posture verifies, so it needs the cert
    assert gcp.ssl_mode == "verify-ca"
    with pytest.raises(TdxError) as missing:
        gcp.plan_system("gcppostgres", _GCP_BASE)
    assert "gcloud sql instances describe INSTANCE" in str(missing.value)
    # ... and it names the instance when the system knows which one it is
    with pytest.raises(TdxError) as named:
        gcp.plan_system("gcppostgres", {**_GCP_BASE, "INSTANCE_ID": "k8sdb"})
    assert "gcloud sql instances describe k8sdb" in str(named.value)
    assert "--sslmode require" in str(missing.value)

    with pytest.raises(TdxError, match="is not a file"):
        gcp.plan_system(
            "gcppostgres",
            {**_GCP_BASE, "SERVER_CA_CERT_PATH": str(tmp_path / "nope.pem")},
        )
    with pytest.raises(TdxError, match="unknown sslmode"):
        gcp.plan_system("gcppostgres", {**_GCP_BASE, "SSLMODE": "verify-most"})

    # require encrypts without verifying: no cert to supply
    assert gcp.plan_system("gcppostgres", {**_GCP_BASE, "SSLMODE": "require"})
    # and a real cert file satisfies the verifying default, on the admin URL only
    pem = tmp_path / "server-ca.pem"
    pem.write_text("-----BEGIN CERTIFICATE-----\ncreate-time\n")
    meta = gcp.plan_system(
        "gcppostgres", {**_GCP_BASE, "SERVER_CA_CERT_PATH": str(pem)}
    )
    with gcp._ca_file(gcp._admin(meta)) as connectable:
        admin_url = gcp._admin_url(connectable).replace("%2F", "/")
        assert "sslmode=verify-ca" in admin_url
        written = admin_url.split("sslrootcert=")[1]
        assert Path(written).read_text() == pem.read_text()


def test_a_managed_postgres_descriptor_exports_a_multiline_pem_intact():
    """`info --format export` is eval'd by a shell, so a value is POSIX-quoted:
    repr() would flatten a PEM into one line of literal \n and let the shell
    expand a $ or a backtick in a password."""
    import shlex

    from tabsdatax.itest.tdksystems2 import descriptor

    pem = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"
    info = descriptor.SystemInfo(
        system="gcppostgres",
        name="g1",
        started="2026-01-01T00:00:00Z",
        meta={"SERVER_CA_CERT": pem, "PASSWORD": "a'b$c`d"},
    )
    lines = descriptor.info_render(info, "export", prefix="GCPPOSTGRES")
    env = {}
    for name, _, value in (
        tok.partition("=") for tok in shlex.split(lines) if tok != "export"
    ):
        env[name] = value
    assert env["GCPPOSTGRES__SERVER_CA_CERT"] == pem
    assert env["GCPPOSTGRES__PASSWORD"] == "a'b$c`d"
