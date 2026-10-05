#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for ``_lib/stage_backend.py``: ``Location`` -> ``StageBackend``
via ``endpoint.py``, and the ``Endpoint`` -> handler-location bridge."""

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.common._transporter.handler.handler import TransporterReport
from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.common._transporter.handler.handler2 import TransporterHandler2
from tabsdatak.common._transporter.invoker import transporter1, transporter2
from tabsdatak.server._tdrunner.function._lib import stage_backend
from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureConfigs,
    AzureEndpoint,
    EnvRef,
    GCSConfigs,
    GCSEndpoint,
    LocalFileEndpoint,
    S3Configs,
    S3Endpoint,
    UnsupportedSchemeError,
    endpoint_from_location,
)
from tabsdatak.server._tdrunner.function._lib.errors import UnexpectedError
from tabsdatak.server._tdrunner.function._lib.request import Location
from tabsdatak.server._tdrunner.function._lib.stage_backend import (
    _mount_endpoint,
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
        GCSEndpoint(
            url="gs://b/x", configs=GCSConfigs(service_account_key=EnvRef(name="J"))
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
    # Mode 2 (current): credentials use the mounts scheme, keyed off the
    # location's env_prefix (the storage mount id) and the object_store
    # option keys -- the same env vars the storage secret provides.
    assert loc.access_key == transporter1.TransporterEnv(
        "SECRETS__STORAGE__MOUNTS__PFX__OPTIONS__AWS_ACCESS_KEY_ID"
    )


def test_stage_backend_from_file_location_is_not_object_store():
    backend = stage_backend_from_location(Location(uri="file:///stage"))
    assert backend.is_object_store is False
    loc = backend.build_location("files/x.parquet", HANDLER)
    assert isinstance(loc, transporter1.TransporterLocalFile)
    assert loc.url.startswith("file:///")
    assert loc.url.endswith("/stage/files/x.parquet")


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


# --- credential scheme follows the transporter mode (regression) ----------
#
# The cloud-stage regression (TD-2635): mode 2 must key credentials onto the
# mounts scheme the storage secret provides
# (SECRETS__STORAGE__MOUNTS__<mount>__OPTIONS__<OBJECT_STORE_KEY>), not the
# legacy endpoint scheme (<env_prefix>_*). The object_store option keys also
# rename for azure/gcs vs the endpoint names -- that rename is part of what we
# pin here so a regression to the endpoint scheme fails loudly.

_MOUNT = "ROOT"


def _env(name: str) -> transporter1.TransporterEnv:
    return transporter1.TransporterEnv(name)


def _build_for_mode(uri: str, mode: int, monkeypatch):
    monkeypatch.setattr(stage_backend, "TRANSPORTER_MODE", mode)
    backend = stage_backend_from_location(Location(uri=uri, env_prefix=_MOUNT))
    # HANDLER (v1) pins the asserted location types; build_location only
    # varies the credential env-var *names* by mode, not the handler.
    return backend.build_location("manifests/m.json", HANDLER)


def test_s3_credentials_use_endpoint_scheme_in_mode_1(monkeypatch):
    loc = _build_for_mode("s3://bkt/stage", 1, monkeypatch)
    assert isinstance(loc, transporter1.TransporterS3)
    assert loc.access_key == _env("ROOT_AWS_ACCESS_KEY_ID")
    assert loc.secret_key == _env("ROOT_AWS_SECRET_ACCESS_KEY")
    assert loc.region == _env("ROOT_AWS_REGION")


def test_s3_credentials_use_mounts_scheme_in_mode_2(monkeypatch):
    loc = _build_for_mode("s3://bkt/stage", 2, monkeypatch)
    assert isinstance(loc, transporter1.TransporterS3)
    assert loc.access_key == _env(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID"
    )
    assert loc.secret_key == _env(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_SECRET_ACCESS_KEY"
    )
    assert loc.region == _env("SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_REGION")


def test_azure_credentials_follow_transporter_mode(monkeypatch):
    legacy = _build_for_mode("az://c/x", 1, monkeypatch)
    assert isinstance(legacy, transporter1.TransporterAzure)
    assert legacy.account_name == _env("ROOT_AZURE_ACCOUNT_NAME")
    assert legacy.account_key == _env("ROOT_AZURE_ACCOUNT_KEY")

    mounts = _build_for_mode("az://c/x", 2, monkeypatch)
    assert mounts.account_name == _env(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AZURE_STORAGE_ACCOUNT_NAME"
    )
    assert mounts.account_key == _env(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AZURE_STORAGE_ACCOUNT_KEY"
    )


def test_gcs_credentials_follow_transporter_mode(monkeypatch):
    legacy = _build_for_mode("gs://b/x", 1, monkeypatch)
    assert isinstance(legacy, transporter1.TransporterGCS)
    assert legacy.service_account_key == _env("ROOT_GCP_SERVICE_ACCOUNT_KEY")

    mounts = _build_for_mode("gs://b/x", 2, monkeypatch)
    assert mounts.service_account_key == _env(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__GOOGLE_SERVICE_ACCOUNT_KEY"
    )


def test_build_location_rejects_unknown_transporter_mode(monkeypatch):
    monkeypatch.setattr(stage_backend, "TRANSPORTER_MODE", 99)
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage", env_prefix=_MOUNT)
    )
    with pytest.raises(UnexpectedError) as excinfo:
        backend.build_location("manifests/m.json", HANDLER)
    assert "99" in str(excinfo.value)


def test_mount_endpoint_rekeys_each_provider_to_mounts_scheme():
    s3 = _mount_endpoint(
        endpoint_from_location(Location(uri="s3://b/x", env_prefix=_MOUNT)), _MOUNT
    )
    assert s3.configs.access_key == EnvRef(
        name="SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID"
    )
    assert s3.configs.secret_key == EnvRef(
        name="SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_SECRET_ACCESS_KEY"
    )
    assert s3.configs.region == EnvRef(
        name="SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_REGION"
    )

    az = _mount_endpoint(
        endpoint_from_location(Location(uri="az://c/x", env_prefix=_MOUNT)), _MOUNT
    )
    assert az.configs.account_name == EnvRef(
        name="SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AZURE_STORAGE_ACCOUNT_NAME"
    )
    assert az.configs.account_key == EnvRef(
        name="SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AZURE_STORAGE_ACCOUNT_KEY"
    )

    gcs = _mount_endpoint(
        endpoint_from_location(Location(uri="gs://b/x", env_prefix=_MOUNT)), _MOUNT
    )
    assert gcs.configs.service_account_key == EnvRef(
        name="SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__GOOGLE_SERVICE_ACCOUNT_KEY"
    )

    # LocalFileEndpoint has no credentials to re-key -> returned unchanged.
    local = LocalFileEndpoint(url="file:///s")
    assert _mount_endpoint(local, _MOUNT) is local


# --- credentials reach the transporter request (regression) ---------------
#
# Beyond the env-var *names*: drive the real upload / next_after paths with a
# stubbed transporter execute() and assert the request that would be handed to
# td_transporter2 actually carries the mounts-scheme creds as ``env:`` refs.
# This is the exact failure that bit cloud staging -- credentials not reaching
# the transporter invocation -- caught here without a binary or real cloud.


def _capturing_handler2(monkeypatch) -> dict:
    """Install a mode-2 handler whose execute() records the request instead of
    spawning td_transporter2. Returns the ``captured`` dict, populated on the
    first execute() call with the request and the env handed to it."""
    handler = TransporterHandler2()
    captured: dict = {}

    def fake_execute(request, work_dir, environment=None, extra_args=()):
        captured["request"] = request
        captured["env"] = environment
        return TransporterReport(files=[], lastmod_info=None)

    monkeypatch.setattr(handler, "execute", fake_execute)
    monkeypatch.setattr(stage_backend, "make_handler", lambda *a, **k: handler)
    return captured


def test_upload_request_carries_mounts_scheme_creds(tmp_path, monkeypatch):
    captured = _capturing_handler2(monkeypatch)
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage", env_prefix=_MOUNT)
    )

    backend.upload(tmp_path / "local.parquet", "manifests/m.json")

    # CopyRequest: the cloud stage is the copy target; creds ride as env: refs
    # the binary resolves from the pod env (so no resolved-values dict).
    target = captured["request"].pairs[0].to
    assert target.uri == "s3://bkt/stage/manifests/m.json"
    assert (
        target.configs["access_key_id"]
        == "env:SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID"
    )
    assert (
        target.configs["secret_access_key"]
        == "env:SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_SECRET_ACCESS_KEY"
    )
    assert (
        target.configs["region"]
        == "env:SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_REGION"
    )
    assert captured["env"] is None


def test_next_after_request_carries_mounts_scheme_creds(tmp_path, monkeypatch):
    captured = _capturing_handler2(monkeypatch)
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage", env_prefix=_MOUNT)
    )

    result = backend.next_after("manifests/stream-*.json", None, tmp_path)

    assert result is None  # stubbed execute reports no matching files
    # FindCopyRequest: the cloud glob is the source.
    source = captured["request"].source
    assert source.uri == "s3://bkt/stage/manifests/stream-*.json"
    assert (
        source.configs["access_key_id"]
        == "env:SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__AWS_ACCESS_KEY_ID"
    )
    assert captured["env"] is None


def _finding_handler2(monkeypatch, content: str | None) -> dict:
    """A mode-2 handler whose find-copy reports one file holding ``content``
    (written into the target dir), or nothing when ``content`` is None."""
    handler = TransporterHandler2()
    captured: dict = {}

    def fake_execute(request, work_dir, environment=None, extra_args=()):
        captured["request"] = request
        if content is None:
            return TransporterReport(files=[], lastmod_info=None)
        found = Path(work_dir) / "stream-state.json"
        found.write_text(content, encoding="utf-8")
        return TransporterReport(files=[{"to": str(found)}], lastmod_info="t1")

    monkeypatch.setattr(handler, "execute", fake_execute)
    monkeypatch.setattr(stage_backend, "make_handler", lambda *a, **k: handler)
    return captured


def test_cloud_read_text_returns_none_only_when_the_find_reports_nothing(
    monkeypatch,
):
    captured = _finding_handler2(monkeypatch, None)
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage", env_prefix=_MOUNT)
    )
    assert backend.exists("stream-state.json") is False
    assert backend.read_text("stream-state.json") is None
    assert captured["request"].source.uri == "s3://bkt/stage/stream-state.json"


def test_cloud_read_text_returns_the_found_file(monkeypatch):
    _finding_handler2(monkeypatch, '{"kind": "streamState"}')
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage", env_prefix=_MOUNT)
    )
    assert backend.exists("stream-state.json") is True
    assert backend.read_text("stream-state.json") == '{"kind": "streamState"}'


def test_cloud_read_text_raises_when_the_transporter_fails(monkeypatch):
    handler = TransporterHandler2()

    def failing_execute(request, work_dir, environment=None, extra_args=()):
        raise transporter2.TransporterRunError(1, "", "access denied")

    monkeypatch.setattr(handler, "execute", failing_execute)
    monkeypatch.setattr(stage_backend, "make_handler", lambda *a, **k: handler)
    backend = stage_backend_from_location(
        Location(uri="s3://bkt/stage", env_prefix=_MOUNT)
    )
    with pytest.raises(transporter2.TransporterRunError):
        backend.read_text("stream-state.json")
