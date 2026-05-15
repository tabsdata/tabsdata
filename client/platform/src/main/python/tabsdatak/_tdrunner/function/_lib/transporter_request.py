#
# Copyright 2026 Tabs Data Inc.
#

"""Transporter request YAML builder.

The publisher's prep-localize-state and prep-upload-state commands
generate a YAML request that the `td_transporter` binary consumes to
copy parquet files between cloud storage and local disk.

The transporter expects YAML with custom tags (`!CopyV1`, `!S3`,
`!Azure`, `!GCS`, `!LocalFile`, `!Env`, `!Literal`) — these are the
binary's contract, not ours, so we mirror them here.

Credentials are referenced via env vars. When `Location.env_prefix` is
set on a request location, each credential's env-var name is
`<env_prefix>_<CRED>`; otherwise the canonical name is used:

    AWS:    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    Azure:  AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY
    GCS:    GCP_SERVICE_ACCOUNT_KEY
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Union

import yaml

from tabsdatak._tdrunner.function._lib.request import Location, Request

# ---- credential references ------------------------------------------------


@dataclass(frozen=True)
class EnvRef:
    """A credential read from a named environment variable."""

    name: str


@dataclass(frozen=True)
class LiteralRef:
    """A credential supplied as a literal string."""

    value: str


CredRef = Union[EnvRef, LiteralRef]


# ---- endpoints ------------------------------------------------------------


@dataclass(frozen=True)
class S3Endpoint:
    url: str
    access_key: CredRef | None = None
    secret_key: CredRef | None = None
    region: CredRef | None = None
    extra_configs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AzureEndpoint:
    url: str
    account_name: CredRef | None = None
    account_key: CredRef | None = None
    extra_configs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class GcsEndpoint:
    url: str
    service_account_key: CredRef | None = None
    extra_configs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class LocalFileEndpoint:
    url: str


Endpoint = Union[S3Endpoint, AzureEndpoint, GcsEndpoint, LocalFileEndpoint]


# ---- copy request ---------------------------------------------------------


@dataclass(frozen=True)
class CopyPair:
    source: Endpoint
    target: Endpoint


@dataclass(frozen=True)
class TransporterCopyRequest:
    pairs: tuple[CopyPair, ...]
    parallelism: int | None = None


class UnsupportedSchemeError(ValueError):
    """The URI scheme is not one the transporter knows how to copy."""


# ---- builders -------------------------------------------------------------


def build_localize_state_request(
    request: Request, in_dir: Path, state_dir: Path
) -> TransporterCopyRequest:
    """Build a copy request that pulls input + state-in tables to local.

    Tables with a `None` location (declared but unbound) are skipped.
    """
    pairs: list[CopyPair] = []
    for table in request.input:
        if table.location is None:
            continue
        pairs.append(
            CopyPair(
                source=_endpoint_from_location(table.location),
                target=LocalFileEndpoint(
                    url=_local_uri(in_dir / f"{table.name}.parquet")
                ),
            )
        )
    for table in request.system_input:
        if table.location is None:
            continue
        pairs.append(
            CopyPair(
                source=_endpoint_from_location(table.location),
                target=LocalFileEndpoint(
                    url=_local_uri(state_dir / f"{table.name}.parquet")
                ),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def build_bundle_fetch_request(
    request: Request, target_path: Path
) -> TransporterCopyRequest:
    """Build a single-pair copy request that pulls the function bundle to
    `target_path`.

    Used by the `localize-fn-bundle` command to download the bundle
    tarball via `td_transporter` before extracting it.
    """
    return TransporterCopyRequest(
        pairs=(
            CopyPair(
                source=_endpoint_from_location(request.function_bundle),
                target=LocalFileEndpoint(url=_local_uri(target_path)),
            ),
        )
    )


def build_upload_state_request(
    request: Request, state_dir: Path
) -> TransporterCopyRequest:
    """Build a copy request that pushes state-out tables from local to cloud.

    Tables with a `None` location are skipped.
    """
    pairs: list[CopyPair] = []
    for table in request.system_output:
        if table.location is None:
            continue
        pairs.append(
            CopyPair(
                source=LocalFileEndpoint(
                    url=_local_uri(state_dir / f"{table.name}.parquet")
                ),
                target=_endpoint_from_location(table.location),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def _endpoint_from_location(location: Location) -> Endpoint:
    """Map a request `Location` to a transporter `Endpoint` by URI scheme."""
    uri = location.uri
    prefix = location.env_prefix
    if uri.startswith("s3://"):
        return S3Endpoint(
            url=uri,
            access_key=_env(prefix, "AWS_ACCESS_KEY_ID"),
            secret_key=_env(prefix, "AWS_SECRET_ACCESS_KEY"),
            region=_env(prefix, "AWS_REGION"),
        )
    if uri.startswith(("az://", "abfs://", "abfss://")):
        return AzureEndpoint(
            url=uri,
            account_name=_env(prefix, "AZURE_ACCOUNT_NAME"),
            account_key=_env(prefix, "AZURE_ACCOUNT_KEY"),
        )
    if uri.startswith("gs://"):
        return GcsEndpoint(
            url=uri,
            service_account_key=_env(prefix, "GCP_SERVICE_ACCOUNT_KEY"),
        )
    if uri.startswith("file://"):
        return LocalFileEndpoint(url=uri)
    raise UnsupportedSchemeError(f"unsupported URI scheme: {uri!r}")


def _env(prefix: str | None, name: str) -> EnvRef:
    if prefix:
        return EnvRef(name=f"{prefix}_{name}")
    return EnvRef(name=name)


def _local_uri(path: Path) -> str:
    return path.absolute().as_uri()


# ---- YAML serialisation ---------------------------------------------------


def write_transporter_request(req: TransporterCopyRequest, path: Path) -> None:
    """Serialise the copy request as YAML at `path`, with the custom tags
    (`!CopyV1`, `!S3`, `!Azure`, `!GCS`, `!LocalFile`, `!Env`, `!Literal`)
    the transporter binary consumes.

    Raises:
        OSError: if `path` is not writable.
    """
    with open(path, "w", encoding="utf-8") as fh:
        yaml.dump(req, fh, Dumper=_make_dumper(), sort_keys=False)


def dump_transporter_request(req: TransporterCopyRequest) -> str:
    """Return the YAML string for `req`. Convenience for tests / preview."""
    return yaml.dump(req, Dumper=_make_dumper(), sort_keys=False)


def _make_dumper() -> type[yaml.SafeDumper]:
    class _Dumper(yaml.SafeDumper):
        pass

    _Dumper.add_representer(EnvRef, _env_repr)
    _Dumper.add_representer(LiteralRef, _literal_repr)
    _Dumper.add_representer(S3Endpoint, _s3_repr)
    _Dumper.add_representer(AzureEndpoint, _azure_repr)
    _Dumper.add_representer(GcsEndpoint, _gcs_repr)
    _Dumper.add_representer(LocalFileEndpoint, _local_repr)
    _Dumper.add_representer(TransporterCopyRequest, _copy_repr)
    return _Dumper


def _env_repr(dumper: yaml.SafeDumper, ref: EnvRef) -> yaml.nodes.ScalarNode:
    return dumper.represent_scalar("!Env", ref.name)


def _literal_repr(dumper: yaml.SafeDumper, ref: LiteralRef) -> yaml.nodes.ScalarNode:
    return dumper.represent_scalar("!Literal", ref.value)


def _s3_repr(dumper: yaml.SafeDumper, ep: S3Endpoint) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!S3",
        {
            "url": ep.url,
            "configs": {
                "access_key": ep.access_key,
                "secret_key": ep.secret_key,
                "region": ep.region,
                "extra_configs": dict(ep.extra_configs),
            },
        },
    )


def _azure_repr(dumper: yaml.SafeDumper, ep: AzureEndpoint) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!Azure",
        {
            "url": ep.url,
            "configs": {
                "account_name": ep.account_name,
                "account_key": ep.account_key,
                "extra_configs": dict(ep.extra_configs),
            },
        },
    )


def _gcs_repr(dumper: yaml.SafeDumper, ep: GcsEndpoint) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!GCS",
        {
            "url": ep.url,
            "configs": {
                "service_account_key": ep.service_account_key,
                "extra_configs": dict(ep.extra_configs),
            },
        },
    )


def _local_repr(
    dumper: yaml.SafeDumper, ep: LocalFileEndpoint
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!LocalFile", {"url": ep.url})


def _copy_repr(
    dumper: yaml.SafeDumper, req: TransporterCopyRequest
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!CopyV1",
        {
            "source_target_pairs": [[p.source, p.target] for p in req.pairs],
            "parallelism": req.parallelism,
        },
    )
