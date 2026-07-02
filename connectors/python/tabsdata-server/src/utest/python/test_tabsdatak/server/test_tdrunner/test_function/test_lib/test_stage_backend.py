#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for ``_lib/stage_backend.py``: ``Location`` -> ``StageBackend``
via ``endpoint.py``, and the ``Endpoint`` -> handler-location bridge."""

from __future__ import annotations

import pytest

from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.common._transporter.invoker import transporter1
from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureConfigs,
    AzureEndpoint,
    EnvRef,
    GcsConfigs,
    GcsEndpoint,
    LocalFileEndpoint,
    S3Configs,
    S3Endpoint,
    UnsupportedSchemeError,
)
from tabsdatak.server._tdrunner.function._lib.request import Location
from tabsdatak.server._tdrunner.function._lib.stage_backend import (
    location_from_endpoint,
    stage_backend_from_location,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

# The bridge builds the provider location through whichever handler the
# caller drives it with; these tests pin the legacy (v1) handler so the
# asserted location types are the ``transporter1.Transporter*`` shapes.
HANDLER = TransporterHandler1()


# --- location_from_endpoint -----------------------------------------------


def test_s3_endpoint_appends_rel_and_maps_env_creds():
    ep = S3Endpoint(
        url="s3://bkt/stage/f",
        configs=S3Configs(
            access_key=EnvRef(name="PFX_AWS_ACCESS_KEY_ID"),
            secret_key=EnvRef(name="PFX_AWS_SECRET_ACCESS_KEY"),
            region=EnvRef(name="PFX_AWS_REGION"),
        ),
    )
    loc = location_from_endpoint(ep, "manifests/stream-manifest-*.json", HANDLER)
    assert isinstance(loc, transporter1.TransporterS3)
    assert loc.url == "s3://bkt/stage/f/manifests/stream-manifest-*.json"
    assert loc.access_key == transporter1.TransporterEnv("PFX_AWS_ACCESS_KEY_ID")
    assert loc.secret_key == transporter1.TransporterEnv("PFX_AWS_SECRET_ACCESS_KEY")
    assert loc.region == transporter1.TransporterEnv("PFX_AWS_REGION")


def test_azure_gcs_local_endpoints_build_their_locations():
    az = location_from_endpoint(
        AzureEndpoint(
            url="az://c/x",
            configs=AzureConfigs(
                account_name=EnvRef(name="N"), account_key=EnvRef(name="K")
            ),
        ),
        "m",
        HANDLER,
    )
    assert isinstance(az, transporter1.TransporterAzure)
    assert az.url == "az://c/x/m"

    gcs = location_from_endpoint(
        GcsEndpoint(
            url="gs://b/x", configs=GcsConfigs(service_account_key=EnvRef(name="J"))
        ),
        "m",
        HANDLER,
    )
    assert isinstance(gcs, transporter1.TransporterGCS)
    assert gcs.url == "gs://b/x/m"

    local = location_from_endpoint(LocalFileEndpoint(url="file:///s"), "m", HANDLER)
    assert isinstance(local, transporter1.TransporterLocalFile)
    assert local.url == "file:///s/m"


# --- stage_backend_from_location ------------------------------------------


def test_stage_backend_from_s3_location():
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage/f", env_prefix="PFX")
    )
    assert backend.is_object_store is True
    # Credentials ride as env-var refs inside the location; the transporter
    # inherits the pod env, so no resolved-values dict is handed over.
    assert backend.credential_env(HANDLER) is None
    loc = backend.build_location("manifests/m.json", HANDLER)
    assert isinstance(loc, transporter1.TransporterS3)
    assert loc.url == "s3://bkt/stage/f/manifests/m.json"
    assert loc.access_key == transporter1.TransporterEnv("PFX_AWS_ACCESS_KEY_ID")


def test_stage_backend_from_file_location_is_not_object_store():
    backend = stage_backend_from_location(Location(uri="file:///stage"))
    assert backend.is_object_store is False
    loc = backend.build_location("files/x.parquet", HANDLER)
    assert isinstance(loc, transporter1.TransporterLocalFile)
    assert loc.url == "file:///stage/files/x.parquet"


def test_stage_backend_from_unknown_scheme_raises():
    with pytest.raises(UnsupportedSchemeError):
        stage_backend_from_location(Location(uri="ftp://nope/x"))


def test_cloud_backend_delete_is_a_typed_seam_until_prereq():
    """Cloud-stage delete needs the transporter delete op (pending); it is a
    typed NotImplementedError seam the sidecar drain logs + swallows."""
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage", env_prefix="PFX")
    )
    with pytest.raises(NotImplementedError):
        backend.delete(["manifests/x.json"])
