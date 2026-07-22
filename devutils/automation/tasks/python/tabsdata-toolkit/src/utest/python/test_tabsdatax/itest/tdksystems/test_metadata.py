#
# Copyright 2026 Tabsdata Inc.
#

"""Tests that each container / cloud system declares the right metadata.

These are class-attribute checks; they don't talk to docker or any cloud
provider, so they're safe to run as unit tests.

The three per-category packages (cloud/container) were unified into a single
`tabsdatax.itest.tdksystems.systems` registry: `systems.get(slug)` instantiates any
backend, and `systems.all_slugs()` lists ALL systems (container + cloud/fs +
saas). Category is now a `System.category` class attribute rather than a
separate registry, so set-equality-against-a-registry assertions become
"these slugs are present AND carry the expected category"."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import systems


# ---- container: every system declares slug/label/etc ----------------------

CONTAINER_SLUGS = (
    "mysql", "postgres", "mssqlserver", "oracle", "db2",
    "clickhouse", "mongodb", "starrocks", "redpanda",
)


def test_container_slugs_present_and_categorised():
    """The unified registry lists every container slug (alongside cloud/saas),
    and each one self-reports category == "container"."""
    known = set(systems.all_slugs())
    assert set(CONTAINER_SLUGS) <= known
    for slug in CONTAINER_SLUGS:
        assert systems.get(slug).category == "container", slug


@pytest.mark.parametrize("slug", CONTAINER_SLUGS)
def test_container_class_metadata(slug):
    obj = systems.get(slug)
    assert obj.slug == slug
    assert obj.category == "container"
    assert obj.label, f"{slug} has no label"
    assert obj.image, f"{slug} has no image"
    assert obj.image_platform in ("linux/amd64", "linux/arm64"), obj.image_platform
    assert obj.internal_port > 0, f"{slug} internal_port not positive"
    # domain_flag should be a single word
    assert " " not in obj.domain_flag


def test_postgres_and_mssqlserver_use_schema_flag():
    """The user-facing terminology was changed from 'database' to 'schema'."""
    assert systems.get("postgres").domain_flag == "schema"
    assert systems.get("mssqlserver").domain_flag == "schema"


def test_redpanda_uses_topic_flag():
    assert systems.get("redpanda").domain_flag == "topic"


def test_entity_container_terminology_per_family():
    """Every system exposes its own word for the entity container (used as the
    redundant descriptor key); salesforce has none (a creds clone)."""
    flags = {slug: systems.get(slug).domain_flag for slug in systems.all_slugs()}
    assert flags["s3"] == flags["gcs"] == flags["azureblob"] == "prefix"
    assert flags["fs"] == "folder"
    assert flags["snowflake"] == flags["databricks"] == "schema"
    assert flags["bigquery"] == "dataset"
    assert flags["mysql"] == "database"
    assert flags["salesforce"] == ""


def test_redpanda_and_mongodb_do_not_support_sql():
    assert systems.get("redpanda").supports_sql is False
    assert systems.get("mongodb").supports_sql is False


@pytest.mark.parametrize("slug", ["mysql", "postgres", "mssqlserver", "oracle",
                                  "db2", "clickhouse", "starrocks"])
def test_sql_systems_support_sql(slug):
    assert systems.get(slug).supports_sql is True


# ---- cloud: every system declares slug/label and options -----------------

CLOUD_SLUGS = ("s3", "gcs", "azureblob")


def test_cloud_slugs_present_and_categorised():
    """The object-store slugs are in the unified registry and self-report
    category == "cloud". fs is a local filesystem — category "native"."""
    known = set(systems.all_slugs())
    assert set(CLOUD_SLUGS) <= known
    for slug in CLOUD_SLUGS:
        assert systems.get(slug).category == "cloud", slug
    assert systems.get("fs").category == "native"


@pytest.mark.parametrize("slug", CLOUD_SLUGS)
def test_cloud_class_metadata(slug):
    obj = systems.get(slug)
    assert obj.slug == slug
    assert obj.category == "cloud"
    assert obj.label, f"{slug} has no label"
    # `options` is now a tuple of `Option(name, cli_flag, is_path, secret)`
    # objects (was a list of (var_name, cli_flag, is_path) triples).
    for opt in obj.options:
        assert opt.name.isupper()
        assert " " not in opt.cli_flag
        assert isinstance(opt.is_path, bool)


def test_s3_options_cover_required_credentials():
    s3 = systems.get("s3")
    flags = {opt.cli_flag for opt in s3.options}
    assert {"bucket", "region", "access-key", "secret-key"} == flags


def test_fs_has_no_connection_options():
    """fs only takes --sys-path (via staging_root); its per-system `options`
    tuple is empty."""
    assert systems.get("fs").options == ()
