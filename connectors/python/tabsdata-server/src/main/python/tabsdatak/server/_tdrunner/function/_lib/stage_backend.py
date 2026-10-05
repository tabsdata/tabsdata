#
# Copyright 2026 Tabsdata Inc.
#

"""Build a :class:`StageBackend` from a request ``Location`` (any scheme).

The stream-stage publisher -- and, later, the streamer + sidecar -- need a
backend for an arbitrary ``function_stage`` ``Location``, including cloud
schemes. :mod:`endpoint` maps a ``Location`` to a typed ``Endpoint`` with
``env_prefix``-derived credential references; this module turns that
``Endpoint`` into the provider location built by the transporter handler
that a :class:`~tabsdatak.conn.stream.stage._backend.StageBackend` drives
through the ``_input`` / ``_output`` helpers.

Request building and wire serialization are single-sourced in the common
transporter handler, so the runner's ``Endpoint`` model and the connector
ecosystem's location model share one encoder: ``endpoint`` is a thin
``Location`` -> ``Endpoint`` scheme dispatcher, and the handler owns the
provider-location construction for both callers.

Credentials travel as env-var *references* baked into each
``TransporterLocation`` (``!Env <env_prefix>_AWS_ACCESS_KEY_ID`` etc.); the
values come from the pod environment, so :meth:`credential_env` is ``None``
-- the transporter inherits the process env rather than being handed a
resolved-values dict.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

from tabsdatak.common._fileformat.taxonomy import DestFileFormat
from tabsdatak.common._transporter.facade import TRANSPORTER_MODE, make_handler
from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.conn.common.file import _input, _output
from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.conn.stream.stage._backend import StageBackend, _LocalStageBackend
from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureConfigs,
    AzureEndpoint,
    Endpoint,
    EnvRef,
    GCSConfigs,
    GCSEndpoint,
    LocalFileEndpoint,
    S3Configs,
    S3Endpoint,
    endpoint_from_location,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode
from tabsdatak.server._tdrunner.function._lib.mounts import secret_env_name
from tabsdatak.server._tdrunner.function._lib.request import Location

# Baseline handed to the transporter so last-modified tracking activates on the
# first fetch (no cursor yet): epoch means every object is a candidate.
_EPOCH_LASTMOD = "1970-01-01T00:00:00.000000+00:00"


def _local_path(file_uri: str) -> Path:
    """A transporter ``file://`` report target back to a local Path."""
    parsed = urlparse(file_uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    return Path(file_uri)


def _env_name(ref: EnvRef | None) -> str | None:
    """The env-var name of an endpoint credential reference. Endpoints
    built by ``endpoint_from_location`` only ever carry ``EnvRef``."""
    return ref.name if ref is not None else None


def _join(url: str, rel: str) -> str:
    """Append a stage-relative path to an endpoint's root url."""
    return f"{url.rstrip('/')}/{rel}"


def location_from_endpoint(endpoint: Endpoint, rel: str, handler: TransporterHandler):
    """Resolve ``endpoint`` + a stage-relative path into the handler's
    transporter location for the ``_input`` / ``_output`` helpers."""
    url = _join(endpoint.url, rel)
    if isinstance(endpoint, S3Endpoint):
        return handler.s3_location(
            uri=url,
            access_key_env=_env_name(endpoint.configs.access_key),
            secret_key_env=_env_name(endpoint.configs.secret_key),
            region_env=_env_name(endpoint.configs.region),
        )
    if isinstance(endpoint, AzureEndpoint):
        return handler.azure_location(
            uri=url,
            account_name_env=_env_name(endpoint.configs.account_name),
            account_key_env=_env_name(endpoint.configs.account_key),
        )
    if isinstance(endpoint, GCSEndpoint):
        return handler.gcs_location(
            uri=url,
            service_account_key_env=_env_name(endpoint.configs.service_account_key),
        )
    if isinstance(endpoint, LocalFileEndpoint):
        return handler.local_location(url)
    raise TypeError(f"unsupported endpoint type: {type(endpoint).__name__}")


def _mount_endpoint(endpoint: Endpoint, mount: str) -> Endpoint:
    """Re-key an endpoint's credential refs onto the mounts scheme (mode 2).

    The credential env vars become
    ``SECRETS__STORAGE__MOUNTS__<mount>__OPTIONS__<KEY>`` (built from the
    storage mount's ``object_store`` option keys, supplied by the storage
    secret), so the shared :func:`location_from_endpoint` builder emits the
    mounts-scheme location -- the same credential encoding the batch
    commands use, rather than the legacy ``<env_prefix>_*`` names."""

    def ref(option_key: str) -> EnvRef:
        return EnvRef(name=secret_env_name(mount, option_key))

    if isinstance(endpoint, S3Endpoint):
        return S3Endpoint(
            url=endpoint.url,
            configs=S3Configs(
                access_key=ref("aws_access_key_id"),
                secret_key=ref("aws_secret_access_key"),
                region=ref("aws_region"),
            ),
        )
    if isinstance(endpoint, AzureEndpoint):
        return AzureEndpoint(
            url=endpoint.url,
            configs=AzureConfigs(
                account_name=ref("azure_storage_account_name"),
                account_key=ref("azure_storage_account_key"),
            ),
        )
    if isinstance(endpoint, GCSEndpoint):
        return GCSEndpoint(
            url=endpoint.url,
            configs=GCSConfigs(service_account_key=ref("google_service_account_key")),
        )
    return endpoint  # LocalFileEndpoint: no credentials to re-key.


class _EndpointStageBackend(StageBackend):
    """:class:`StageBackend` backed by an :mod:`endpoint` ``Endpoint``.

    Credentials follow the transporter mode, mirroring the batch
    :mod:`transporter` facade: mode 1 uses the endpoint scheme
    (``<env_prefix>_*`` env refs), mode 2 uses the mounts scheme
    (:func:`_mount_endpoint` -> ``SECRETS__STORAGE__MOUNTS__*``)."""

    def __init__(self, endpoint: Endpoint, mount: str | None) -> None:
        self._endpoint = endpoint
        self._mount = mount

    @property
    def is_object_store(self) -> bool:
        return not isinstance(self._endpoint, LocalFileEndpoint)

    def build_location(self, rel: str, handler: TransporterHandler):
        match TRANSPORTER_MODE:
            case 1:
                return location_from_endpoint(self._endpoint, rel, handler)
            case 2:
                endpoint = _mount_endpoint(self._endpoint, self._mount)
                return location_from_endpoint(endpoint, rel, handler)
            case _:
                raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)

    def credential_env(self, handler: TransporterHandler) -> dict[str, str] | None:
        # Credential env-var refs are baked into each location; their
        # values come from the pod env, so the transporter inherits the
        # process env rather than receiving a resolved-values dict.
        return None

    def upload(self, local_path: Path, rel: str) -> None:
        handler = make_handler()
        source = handler.local_path_location(local_path)
        with tempfile.TemporaryDirectory() as work_dir:
            _output.copy_one(
                handler=handler,
                source_target_pairs=[(source, self.build_location(rel, handler))],
                work_dir=Path(work_dir),
                file_format=DestFileFormat.PARQUET,
                paths=[rel],
                env=self.credential_env(handler),
            )

    def exists(self, rel: str) -> bool:
        with tempfile.TemporaryDirectory() as scratch_dir:
            return self.find_file(rel, Path(scratch_dir)) is not None

    def read_text(self, rel: str) -> str | None:
        with tempfile.TemporaryDirectory() as scratch_dir:
            path = self.find_file(rel, Path(scratch_dir))
            return path.read_text(encoding="utf-8") if path is not None else None

    def find_file(self, rel: str, scratch_dir: Path) -> Path | None:
        """Look the object up by its exact name and download it into
        ``scratch_dir``. Three outcomes: the find reports no file, so the
        object does not exist and ``None`` is returned; the find reports it,
        so it was downloaded and its local path is returned; the transporter
        fails (credentials, network, binary), so the error is raised. A plain
        copy cannot tell the first from the third: a missing object and an
        unreachable one fail it the same way."""
        handler = make_handler()
        report = _input.wildcard_copy_one(
            handler=handler,
            source=self.build_location(rel, handler),
            target_dir=scratch_dir,
            initial_lastmod=_EPOCH_LASTMOD,
            lastmod_info=None,
            max_files=1,
            env=self.credential_env(handler),
        )
        targets = [f["to"] for f in report.files if isinstance(f, dict) and f.get("to")]
        return Path(targets[0]) if targets else None

    def next_after(
        self, glob: str, cursor: str | None, scratch_dir: Path
    ) -> tuple[Path, str] | None:
        # The transporter returns the single oldest-by-last-modified object
        # matching the glob whose last-modified is after the cursor, downloaded
        # into scratch_dir (basename preserved). The report's lastmod_info is
        # the watermark to pass back as the next cursor.
        handler = make_handler()
        report = _input.wildcard_copy_one(
            handler=handler,
            source=self.build_location(glob, handler),
            target_dir=scratch_dir,
            initial_lastmod=_EPOCH_LASTMOD,
            lastmod_info=cursor,
            max_files=1,
            env=self.credential_env(handler),
        )
        targets = [f["to"] for f in report.files if isinstance(f, dict) and f.get("to")]
        if not targets:
            return None
        return (_local_path(targets[0]), report.lastmod_info)

    def delete(self, rels: list[str]) -> None:
        # Cloud-stage delete needs the transporter's delete op (not yet
        # available). Typed seam until that prereq lands; the sidecar's drain
        # logs + swallows this so the pod keeps running (manifests accumulate
        # on the stage until cleanup is wired).
        raise NotImplementedError(
            "cloud stage delete requires the transporter delete op (pending)"
        )


def stage_backend_from_location(location: Location) -> StageBackend:
    """Resolve a request ``Location`` into a :class:`StageBackend`.

    ``file://`` returns a local-filesystem backend (``shutil``-based
    upload, direct reads -- no transporter spawn on the streamer's hot
    path); cloud schemes (s3 / az / gs) return an endpoint-backed backend
    that moves data through the transporter.

    Raises:
        UnsupportedSchemeError: the URI scheme has no endpoint builder.
    """
    parsed = urlparse(location.uri)
    if parsed.scheme == "file":
        base_path = Path(url2pathname(parsed.path)).as_posix()
        return _LocalStageBackend(LocalFileSrcConn(base_path=base_path))
    return _EndpointStageBackend(endpoint_from_location(location), location.env_prefix)
