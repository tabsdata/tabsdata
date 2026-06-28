#
# Copyright 2026 Tabs Data Inc.
#

"""Transporter request YAML builder.

The publisher's prep-localize-state, prep-upload-state and
prep-upload-tables commands generate a YAML request that the
`td_transporter` binary consumes to copy parquet files between cloud
storage and local disk.

Endpoint types and URI→endpoint resolution live in `endpoint.py` (a
cloud-storage concern). This module owns the transporter binary's
on-wire YAML format: the `!CopyV1` envelope, the per-endpoint tags
(`!S3`, `!Azure`, `!GCS`, `!LocalFile`), and the credential tags
(`!Env`, `!Literal`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict

from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureEndpoint,
    Endpoint,
    EnvRef,
    GcsEndpoint,
    LiteralRef,
    LocalFileEndpoint,
    S3Endpoint,
    endpoint_from_location,
)
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    AnyRequest,
    InputTable,
    InputTableVersions,
    Request,
    input_tables,
)


class OutputTableNotInManifestError(ValueError):
    """A `request.output` table has no matching entry in the outputs manifest.

    Raised by `build_upload_tables_request` when the function's outputs
    manifest is missing a table that the request declares — typically a
    sign of a mismatch between `info.output_tables` (in the bundle's
    `function_def.json`) and `request.output` (set by the apiserver).
    """


# ---- copy request ---------------------------------------------------------


class CopyPair(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: Endpoint
    target: Endpoint


class TransporterCopyRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    pairs: tuple[CopyPair, ...]
    parallelism: int | None = None


# ---- builders -------------------------------------------------------------


def localized_input_files(
    in_dir: Path, entry: InputTable | InputTableVersions
) -> list[tuple[InputTable, Path]]:
    """Per-member `(member, local path)` pairs an input entry is copied to.

    Members of a multi-version dependency share the table name, so their
    files are disambiguated with the member's `version_pos`.
    """
    if isinstance(entry, InputTableVersions):
        return [
            (member, in_dir / f"{member.name}.{member.version_pos}.parquet")
            for member in input_tables(entry)
        ]
    return [(entry, in_dir / f"{entry.name}.parquet")]


def build_localize_state_request(
    request: Request, in_dir: Path, state_dir: Path
) -> TransporterCopyRequest:
    """Build a copy request that pulls input + state-in tables to local.

    Tables with a `None` location (declared but unbound) are skipped.
    """
    pairs: list[CopyPair] = []
    for entry in request.input:
        for member, path in localized_input_files(in_dir, entry):
            if member.location is None:
                continue
            pairs.append(
                CopyPair(
                    source=endpoint_from_location(member.location),
                    target=LocalFileEndpoint(url=_local_uri(path)),
                )
            )
    for table in request.system_input:
        if table.location is None:
            continue
        pairs.append(
            CopyPair(
                source=endpoint_from_location(table.location),
                target=LocalFileEndpoint(
                    url=_local_uri(state_dir / f"{table.name}.parquet")
                ),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def build_bundle_fetch_request(
    request: Request | AnyRequest, target_path: Path
) -> TransporterCopyRequest:
    """Build a single-pair copy request that pulls the function bundle to
    `target_path`.

    Used by the `localize-fn-bundle` command to download the bundle
    tarball via `td_transporter` before extracting it. Accepts either
    the full :class:`Request` (function-publisher pod, ``Function``
    envelope) or the slim envelope-agnostic :class:`AnyRequest`
    (stream-publisher pod, ``Streamer`` envelope) -- both expose
    ``info.function_bundle`` the same way.
    """
    return TransporterCopyRequest(
        pairs=(
            CopyPair(
                source=endpoint_from_location(request.info.function_bundle),
                target=LocalFileEndpoint(url=_local_uri(target_path)),
            ),
        )
    )


def build_upload_state_request(
    request: Request, state_dir: Path
) -> TransporterCopyRequest:
    """Build a copy request that pushes state-out tables from local to cloud.

    Tables with a `None` location are skipped. Tables whose state file
    is absent from ``state_dir`` are also skipped — the user function
    may not produce state for every declared slot (e.g. a transformer
    or subscriber whose function signature doesn't include
    ``ctx: TrxCtx`` never drains to state), mirroring the ``!NoData``
    convention ``report_completion`` already uses for the same case.
    """
    pairs: list[CopyPair] = []
    for table in request.system_output:
        if table.location is None:
            continue
        source_path = state_dir / f"{table.name}.parquet"
        if not source_path.exists():
            continue
        pairs.append(
            CopyPair(
                source=LocalFileEndpoint(url=_local_uri(source_path)),
                target=endpoint_from_location(table.location),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def build_upload_tables_request(
    request: Request, manifest_entries: list[dict[str, Any]]
) -> TransporterCopyRequest:
    """Build a copy request that pushes the user-function's output
    tables to cloud, using paths from the outputs manifest.

    Iterates `request.output`. For each table with a non-null
    `Location`, looks up the matching entry in `manifest_entries` by
    name (manifest key `name`) and:
      * skips when the manifest's `file` is `None` (the user function
        returned `None` for this slot — "not modified this run");
      * otherwise emits a `CopyPair` whose source is the absolute
        parquet path the manifest carries.

    Tables in `request.output` with `location is None` are silently
    skipped (the apiserver didn't pre-allocate storage for them).

    Raises:
        OutputTableNotInManifestError: a `request.output` table with a
            non-null location has no matching entry in
            `manifest_entries`.
    """
    files_by_name: dict[str, str | None] = {
        e[MANIFEST_NAME_KEY]: e.get(MANIFEST_FILE_KEY) for e in manifest_entries
    }
    pairs: list[CopyPair] = []
    for table in request.output:
        if table.location is None:
            continue
        if table.name not in files_by_name:
            raise OutputTableNotInManifestError(
                f"output table {table.name!r} declared in request "
                f"has no entry in the outputs manifest"
            )
        src = files_by_name[table.name]
        if src is None:
            continue
        pairs.append(
            CopyPair(
                source=LocalFileEndpoint(url=_local_uri(Path(src))),
                target=endpoint_from_location(table.location),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def _local_uri(path: Path) -> str:
    return path.absolute().as_uri()


# ---- YAML serialisation ---------------------------------------------------


def write_transporter_request(req: TransporterCopyRequest, path: Path) -> None:
    """Serialise the copy request as YAML at `path`, with the custom tags
    (`!CopyV1`, `!S3`, `!Azure`, `!GCS`, `!LocalFile`, `!Env`, `!Literal`)
    the transporter binary consumes.

    Parent directories of `path` are created if missing — under the
    publisher init-container chain, prep-* commands write to subpaths
    of the `shared/shuttle/` emptyDir which start out empty (the
    image-template-provided `shared/shuttle/` subdirs are overlaid by
    the emptyDir mount).

    Raises:
        OSError: if `path` is not writable.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
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
                "access_key": ep.configs.access_key,
                "secret_key": ep.configs.secret_key,
                "region": ep.configs.region,
                "extra_configs": dict(ep.configs.extra_configs),
            },
        },
    )


def _azure_repr(dumper: yaml.SafeDumper, ep: AzureEndpoint) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!Azure",
        {
            "url": ep.url,
            "configs": {
                "account_name": ep.configs.account_name,
                "account_key": ep.configs.account_key,
                "extra_configs": dict(ep.configs.extra_configs),
            },
        },
    )


def _gcs_repr(dumper: yaml.SafeDumper, ep: GcsEndpoint) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!GCS",
        {
            "url": ep.url,
            "configs": {
                "service_account_key": ep.configs.service_account_key,
                "extra_configs": dict(ep.configs.extra_configs),
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
