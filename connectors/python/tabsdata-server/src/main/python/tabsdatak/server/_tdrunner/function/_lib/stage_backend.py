#
# Copyright 2026 Tabs Data Inc.
#

"""Build a :class:`StageBackend` from a request ``Location`` (any scheme).

The stream-stage publisher -- and, later, the streamer + sidecar -- need a
backend for an arbitrary ``function_stage`` ``Location``, including cloud
schemes. :mod:`endpoint` already maps a ``Location`` to a typed
``Endpoint`` with ``env_prefix``-derived credential references; this module
turns that ``Endpoint`` into the ``TransporterLocation`` a
:class:`~tabsdatak.conn.stream.stage._backend.StageBackend` hands to the
``_input`` / ``_output`` transporter helpers.

This is the first interop point between the runner's ``Endpoint`` model
and the connector ecosystem's ``TransporterLocation`` model; collapsing
the two onto one shared model is tracked separately (TD-2585).

Credentials travel as env-var *references* baked into each
``TransporterLocation`` (``!Env <env_prefix>_AWS_ACCESS_KEY_ID`` etc.); the
values come from the pod environment, so :meth:`credential_env` is ``None``
-- the transporter inherits the process env rather than being handed a
resolved-values dict.
"""

from __future__ import annotations

from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.stream.stage._backend import StageBackend
from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureEndpoint,
    CredRef,
    Endpoint,
    EnvRef,
    GcsEndpoint,
    LocalFileEndpoint,
    S3Endpoint,
    endpoint_from_location,
)
from tabsdatak.server._tdrunner.function._lib.request import Location

_TransporterCred = _transporter.TransporterEnv | _transporter.TransporterLiteral


def _cred(ref: CredRef | None) -> _TransporterCred | None:
    """Map an endpoint credential reference to the transporter's form."""
    if ref is None:
        return None
    if isinstance(ref, EnvRef):
        return _transporter.TransporterEnv(ref.name)
    return _transporter.TransporterLiteral(ref.value)  # LiteralRef


def _join(url: str, rel: str) -> str:
    """Append a stage-relative path to an endpoint's root url."""
    return f"{url.rstrip('/')}/{rel}"


def transporter_location_from_endpoint(
    endpoint: Endpoint, rel: str
) -> _transporter.TransporterLocation:
    """Resolve ``endpoint`` + a stage-relative path into a
    ``TransporterLocation`` for the ``_input`` / ``_output`` helpers."""
    url = _join(endpoint.url, rel)
    if isinstance(endpoint, S3Endpoint):
        return _transporter.TransporterS3(
            url=url,
            access_key=_cred(endpoint.configs.access_key),
            secret_key=_cred(endpoint.configs.secret_key),
            region=_cred(endpoint.configs.region),
            extra_configs=dict(endpoint.configs.extra_configs),
        )
    if isinstance(endpoint, AzureEndpoint):
        return _transporter.TransporterAzure(
            url=url,
            account_name=_cred(endpoint.configs.account_name),
            account_key=_cred(endpoint.configs.account_key),
            extra_configs=dict(endpoint.configs.extra_configs),
        )
    if isinstance(endpoint, GcsEndpoint):
        return _transporter.TransporterGCS(
            url=url,
            service_account_key=_cred(endpoint.configs.service_account_key),
            extra_configs=dict(endpoint.configs.extra_configs),
        )
    if isinstance(endpoint, LocalFileEndpoint):
        return _transporter.TransporterLocalFile(url=url)
    raise TypeError(f"unsupported endpoint type: {type(endpoint).__name__}")


class _EndpointStageBackend(StageBackend):
    """:class:`StageBackend` backed by an :mod:`endpoint` ``Endpoint``."""

    def __init__(self, endpoint: Endpoint) -> None:
        self._endpoint = endpoint

    @property
    def is_object_store(self) -> bool:
        return not isinstance(self._endpoint, LocalFileEndpoint)

    def build_location(self, rel: str) -> _transporter.TransporterLocation:
        return transporter_location_from_endpoint(self._endpoint, rel)

    def credential_env(self) -> dict[str, str] | None:
        # Credential env-var refs are baked into each location; their
        # values come from the pod env, so the transporter inherits the
        # process env rather than receiving a resolved-values dict.
        return None


def stage_backend_from_location(location: Location) -> StageBackend:
    """Resolve a request ``Location`` (file / s3 / az / gcs) into a
    :class:`StageBackend`.

    Raises:
        UnsupportedSchemeError: the URI scheme has no endpoint builder.
    """
    return _EndpointStageBackend(endpoint_from_location(location))
