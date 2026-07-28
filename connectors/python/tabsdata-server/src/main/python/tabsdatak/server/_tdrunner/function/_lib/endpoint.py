#
# Copyright 2026 Tabsdata Inc.
#

"""Cloud-storage endpoint types and URI→endpoint resolution.

A `Location` (a URI + optional credential env-var prefix, as it appears
in `request.yaml`) is mapped to a typed `Endpoint` carrying explicit
credential references. The transporter binary's YAML on-wire shape is
owned by the common transporter handler; this module is concerned only
with the cloud-storage abstraction and the per-scheme dispatch.

Credentials are referenced via env vars. When `Location.env_prefix` is
set, each credential's env-var name is `<env_prefix>_<CRED>`; otherwise
the canonical name is used:

    AWS:    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    Azure:  AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY
    GCS:    GCP_SERVICE_ACCOUNT_KEY
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Union
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field

from tabsdatak.server._tdrunner.function._lib.request import Location

# ---- credential references ------------------------------------------------


class EnvRef(BaseModel):
    """A credential read from a named environment variable."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str


class LiteralRef(BaseModel):
    """A credential supplied as a literal string."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: str

    def __repr_args__(self):
        # The literal value is a credential; never render it. This covers
        # repr()/str() and pydantic's rendering of any model that nests a
        # LiteralRef.
        return [("value", "***redacted***")]


CredRef = Union[EnvRef, LiteralRef]


# ---- endpoints ------------------------------------------------------------


class S3Configs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    access_key: CredRef | None = None
    secret_key: CredRef | None = None
    region: CredRef | None = None
    extra_configs: dict[str, str] = Field(default_factory=dict)


class S3Endpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    configs: S3Configs = Field(default_factory=S3Configs)


class AzureConfigs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    account_name: CredRef | None = None
    account_key: CredRef | None = None
    extra_configs: dict[str, str] = Field(default_factory=dict)


class AzureEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    configs: AzureConfigs = Field(default_factory=AzureConfigs)


class GCSConfigs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    service_account_key: CredRef | None = None
    extra_configs: dict[str, str] = Field(default_factory=dict)


class GCSEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    configs: GCSConfigs = Field(default_factory=GCSConfigs)


class LocalFileEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str


Endpoint = Union[S3Endpoint, AzureEndpoint, GCSEndpoint, LocalFileEndpoint]


# ---- URI → Endpoint resolution -------------------------------------------


class UnsupportedSchemeError(ValueError):
    """The URI scheme has no registered endpoint builder."""


def _env(prefix: str | None, name: str) -> EnvRef:
    return EnvRef(name=f"{prefix}_{name}" if prefix else name)


def _build_s3(location: Location) -> Endpoint:
    prefix = location.env_prefix
    return S3Endpoint(
        url=location.uri,
        configs=S3Configs(
            access_key=_env(prefix, "AWS_ACCESS_KEY_ID"),
            secret_key=_env(prefix, "AWS_SECRET_ACCESS_KEY"),
            region=_env(prefix, "AWS_REGION"),
        ),
    )


def _build_azure(location: Location) -> Endpoint:
    prefix = location.env_prefix
    return AzureEndpoint(
        url=location.uri,
        configs=AzureConfigs(
            account_name=_env(prefix, "AZURE_ACCOUNT_NAME"),
            account_key=_env(prefix, "AZURE_ACCOUNT_KEY"),
        ),
    )


def _build_gcs(location: Location) -> Endpoint:
    prefix = location.env_prefix
    return GCSEndpoint(
        url=location.uri,
        configs=GCSConfigs(
            service_account_key=_env(prefix, "GCP_SERVICE_ACCOUNT_KEY"),
        ),
    )


def _build_local(location: Location) -> Endpoint:
    return LocalFileEndpoint(url=location.uri)


# Scheme → builder. Add a row to register a new endpoint type.
_BUILDERS: dict[str, Callable[[Location], Endpoint]] = {
    "s3": _build_s3,
    "az": _build_azure,
    "abfs": _build_azure,
    "abfss": _build_azure,
    "gs": _build_gcs,
    "file": _build_local,
}


def endpoint_from_location(location: Location) -> Endpoint:
    """Map a request `Location` to an `Endpoint` by URI scheme.

    Raises:
        UnsupportedSchemeError: if the URI scheme has no registered builder.
    """
    scheme = urlparse(location.uri).scheme
    builder = _BUILDERS.get(scheme)
    if builder is None:
        raise UnsupportedSchemeError(f"unsupported URI scheme: {location.uri!r}")
    return builder(location)
