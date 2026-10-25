#
# Copyright 2026 Tabsdata Inc.
#

"""The local twins of the cloud object stores (s3local / gcslocal /
azurebloblocal), loaded from descriptors the real tdd create writes: each
carries its endpoint and the polars / obstore storage options, read the same
way as its cloud twin's."""

from __future__ import annotations

import pytest

from tabsdatax.itest.fixture import backend as common
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain
from tabsdatax.itest.fixture.backends.azurebloblocal import (
    AzureBlobLocalDomain,
    AzureBlobLocalSystem,
)
from tabsdatax.itest.fixture.backends.gcs import GCSDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain, GcsLocalSystem
from tabsdatax.itest.fixture.backends.s3 import S3Domain
from tabsdatax.itest.fixture.backends.s3local import (
    S3LocalDomain,
    S3LocalSystem,
    s3local_domain,
    s3local_system,
)

# slug, SystemCls, DomainCls, URI scheme, cloud twin (slug, DomainCls)
LOCAL = [
    (
        "s3local",
        S3LocalSystem,
        S3LocalDomain,
        "s3://",
        ("s3", S3Domain),
    ),
    (
        "gcslocal",
        GcsLocalSystem,
        GcsLocalDomain,
        "gs://",
        ("gcs", GCSDomain),
    ),
    (
        "azurebloblocal",
        AzureBlobLocalSystem,
        AzureBlobLocalDomain,
        "az://",
        ("azureblob", AzureblobDomain),
    ),
]
IDS = [c[0] for c in LOCAL]


@pytest.mark.parametrize("slug,Sys,Dom,scheme,twin", LOCAL, ids=IDS)
def test_local_system(provision, slug, Sys, Dom, scheme, twin):
    provision.system(slug, "s")
    obj = common.load("s", Sys)
    assert isinstance(obj, Sys) and obj.system_type == slug
    assert obj.endpoint.startswith(f"http://{obj.host}:{obj.port}")
    assert obj.location.startswith(scheme)
    assert obj.storage_options and all(
        isinstance(v, str) for v in obj.storage_options.values()
    )


@pytest.mark.parametrize("slug,Sys,Dom,scheme,twin", LOCAL, ids=IDS)
def test_local_domain_reads_like_its_cloud_twin(
    provision, slug, Sys, Dom, scheme, twin
):
    provision.system(slug, "s")
    provision.domain("s", "d")
    obj = common.load("d", Dom)
    assert obj.uri.startswith(scheme) and obj.uri.endswith(obj.prefix)
    assert obj.storage_options == common.load("s", Sys).storage_options
    twin_slug, TwinDom = twin
    provision.system(twin_slug, "c")
    provision.domain("c", "e")
    cloud = common.load("e", TwinDom)
    # The same two fields reach either store: a test written against one runs
    # against the other by switching the fixture.
    assert cloud.uri.startswith(scheme)
    assert isinstance(cloud.storage_options, dict)


@pytest.fixture
def s3local_ready(provision):
    provision.system("s3local", "store")
    provision.domain("store", "inbox")


@s3local_system("store")
@s3local_domain("inbox")
def test_local_injection(
    s3local_ready, system_store: S3LocalSystem, domain_inbox: S3LocalDomain
):
    assert isinstance(system_store, S3LocalSystem)
    assert isinstance(domain_inbox, S3LocalDomain)
    assert domain_inbox.storage_options["aws_endpoint_url"] == system_store.endpoint
