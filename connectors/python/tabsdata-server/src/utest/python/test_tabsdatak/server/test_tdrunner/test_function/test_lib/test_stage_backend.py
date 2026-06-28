#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for ``_lib/stage_backend.py``: ``Location`` -> ``StageBackend``
via ``endpoint.py``, and the ``Endpoint`` -> ``TransporterLocation`` bridge."""

from __future__ import annotations

import pytest

from tabsdatak.conn.common.file import _transporter
from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureConfigs,
    AzureEndpoint,
    EnvRef,
    GcsConfigs,
    GcsEndpoint,
    LiteralRef,
    LocalFileEndpoint,
    S3Configs,
    S3Endpoint,
    UnsupportedSchemeError,
)
from tabsdatak.server._tdrunner.function._lib.request import Location
from tabsdatak.server._tdrunner.function._lib.stage_backend import (
    stage_backend_from_location,
    transporter_location_from_endpoint,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- transporter_location_from_endpoint -----------------------------------


def test_s3_endpoint_appends_rel_and_maps_env_creds():
    ep = S3Endpoint(
        url="s3://bkt/stage/f",
        configs=S3Configs(
            access_key=EnvRef(name="PFX_AWS_ACCESS_KEY_ID"),
            secret_key=EnvRef(name="PFX_AWS_SECRET_ACCESS_KEY"),
            region=EnvRef(name="PFX_AWS_REGION"),
        ),
    )
    loc = transporter_location_from_endpoint(ep, "manifests/stream-manifest-*.json")
    assert isinstance(loc, _transporter.TransporterS3)
    assert loc.url == "s3://bkt/stage/f/manifests/stream-manifest-*.json"
    assert loc.access_key == _transporter.TransporterEnv("PFX_AWS_ACCESS_KEY_ID")
    assert loc.secret_key == _transporter.TransporterEnv("PFX_AWS_SECRET_ACCESS_KEY")
    assert loc.region == _transporter.TransporterEnv("PFX_AWS_REGION")


def test_literal_credential_maps_to_transporter_literal():
    ep = S3Endpoint(
        url="s3://b/x",
        configs=S3Configs(
            access_key=LiteralRef(value="AKIAEXAMPLE"),
            secret_key=EnvRef(name="S"),
        ),
    )
    loc = transporter_location_from_endpoint(ep, "a")
    assert loc.access_key == _transporter.TransporterLiteral("AKIAEXAMPLE")


def test_azure_gcs_local_endpoints_build_their_locations():
    az = transporter_location_from_endpoint(
        AzureEndpoint(
            url="az://c/x",
            configs=AzureConfigs(
                account_name=EnvRef(name="N"), account_key=EnvRef(name="K")
            ),
        ),
        "m",
    )
    assert isinstance(az, _transporter.TransporterAzure)
    assert az.url == "az://c/x/m"

    gcs = transporter_location_from_endpoint(
        GcsEndpoint(
            url="gs://b/x", configs=GcsConfigs(service_account_key=EnvRef(name="J"))
        ),
        "m",
    )
    assert isinstance(gcs, _transporter.TransporterGCS)
    assert gcs.url == "gs://b/x/m"

    local = transporter_location_from_endpoint(LocalFileEndpoint(url="file:///s"), "m")
    assert isinstance(local, _transporter.TransporterLocalFile)
    assert local.url == "file:///s/m"


# --- stage_backend_from_location ------------------------------------------


def test_stage_backend_from_s3_location():
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage/f", env_prefix="PFX")
    )
    assert backend.is_object_store is True
    # Credentials ride as env-var refs inside the location; the transporter
    # inherits the pod env, so no resolved-values dict is handed over.
    assert backend.credential_env() is None
    loc = backend.build_location("manifests/m.json")
    assert isinstance(loc, _transporter.TransporterS3)
    assert loc.url == "s3://bkt/stage/f/manifests/m.json"
    assert loc.access_key == _transporter.TransporterEnv("PFX_AWS_ACCESS_KEY_ID")


def test_stage_backend_from_file_location_is_not_object_store():
    backend = stage_backend_from_location(Location(uri="file:///stage"))
    assert backend.is_object_store is False
    loc = backend.build_location("files/x.parquet")
    assert isinstance(loc, _transporter.TransporterLocalFile)
    assert loc.url == "file:///stage/files/x.parquet"


def test_stage_backend_from_unknown_scheme_raises():
    with pytest.raises(UnsupportedSchemeError):
        stage_backend_from_location(Location(uri="ftp://nope/x"))
