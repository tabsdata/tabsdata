#
# Copyright 2026 Tabsdata Inc.
#

"""Data operations shared by the tdx commands.

A library, not a command: the `data create` / `data load` pipeline that
`tdx-domain create --model M [--data SIZE]` runs after writing the domain
descriptor. It generates a model's rows and hands them to the domain's
`System` backend (`install_schema` / `load_data`); the per-system load
mechanics (SQL run-order, cloud upload, kafka produce, saas INSERTs,
salesforce cci) live on the backends under `systems/`.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

from . import data_gen, descriptor, env_check, kafka_producer, systems
from .errors import DatagenError
from .systems.base import DomainContext, LoadRequest

# Credentials are never echoed to the terminal — only that they're set.
_SECRET_MASK = "(set)"


def _mask_secret(value: str | None) -> str:
    return _SECRET_MASK if value else ""


def _mask_uri(uri: str | None) -> str:
    """Mask the password in a connection URI: scheme://user:PASS@host -> …:(set)@…"""
    return re.sub(r"(://[^:/@]+:)[^@]+(@)", rf"\1{_SECRET_MASK}\2", uri or "")


def _resolve_staging_root(sys_obj, args) -> tuple[str | None, str | None]:
    """Resolve a file-staging system's staging root from the namespace into
    (literal_value, env_var_name) — exactly one is non-None (or both None when
    nothing was given). Mirrors the cloud --sys-path / --env:sys-path /
    --env-prefix resolution."""
    if not getattr(sys_obj, "uses_file_staging", False):
        return None, None
    lit = getattr(args, "lit_SYS_PATH", None)
    envv = getattr(args, "env_SYS_PATH", None)
    prefix = getattr(args, "env_prefix", None) or None
    if lit and envv:
        raise DatagenError("specify either --sys-path or --env:sys-path, not both")
    if not lit and not envv and prefix is not None:
        if not re.fullmatch(r"[A-Za-z0-9_]+", prefix) or set(prefix) == {"_"}:
            raise DatagenError(
                f"--env-prefix must be non-empty alphanumeric/underscore "
                f"(got: {prefix!r})"
            )
        envv = f"{prefix}__SYS_PATH"
    from .systems.base import _validate_path

    if envv is not None:
        if not re.fullmatch(r"[A-Za-z0-9_]+", envv):
            raise DatagenError(f"--env:sys-path expects an env-var name (got: {envv})")
        if not os.environ.get(envv):
            raise DatagenError(f"env var '{envv}' is unset / empty")
        return None, envv
    if lit:
        _validate_path(lit, "sys-path")
        return lit, None
    return None, None


def _check_kind_compat(model: str, meta: dict[str, str], action: str) -> None:
    """Reject incompatible model/system pairs early — a model can only target
    a system whose `kind` (SYSTEM_KIND) is in the model's sizing.json `kinds`."""
    sys_kind = meta.get("SYSTEM_KIND", "")
    if not sys_kind:
        return  # older descriptor without SYSTEM_KIND — be permissive
    model_compat = data_gen.model_kinds(model)
    if sys_kind not in model_compat:
        raise DatagenError(
            f"model '{model}' is not compatible with this domain — "
            f"system kind is {sys_kind!r}, but the model supports "
            f"{model_compat!r}. Use a model whose `kinds` includes "
            f"{sys_kind!r}, or pick a different system."
        )


def _domain(domain_path: str):
    """Load a domain descriptor -> (System, DomainContext, meta). meta carries
    KIND + the resolved connection settings."""
    p = Path(domain_path)
    if not p.is_file():
        raise DatagenError(f"--domain descriptor does not exist: {domain_path}")
    doc = descriptor.load(p)
    slug = doc["system"]
    meta = {"KIND": doc["kind"], **descriptor.resolve(doc)}
    sysobj = systems.get(slug)
    ctx = DomainContext(slug, doc.get("name", ""), meta)
    return sysobj, ctx, meta


def _run_data_create(args: argparse.Namespace) -> int:
    """Schema-only: install the model's DDL into the domain (SQL systems).
    Cloud/fs have no schema; saas non-SQL (salesforce / others) is manual."""
    env_check.check_requirements()
    sysobj, ctx, meta = _domain(args.domain)
    _check_kind_compat(args.model, meta, "data create")
    kind = meta.get("KIND", "")
    if kind.startswith("cloud"):
        print()
        print(
            f"data create: no schema needed for {kind} domains "
            f"(skipping for {meta.get('STORAGE_TYPE')}://"
            f"{meta.get('BASE_PATH', '')})"
        )
        print()
        return 0
    if meta.get("SYSTEM_KIND") == "kafka":
        # Kafka topics have no schema to install — the records are produced by
        # `data load` (which builds them from the json + schema sidecar).
        print()
        print("data create: no schema needed for kafka domains (topics only)")
        print()
        return 0
    if meta.get("SYSTEM_KIND") == "druid":
        # Druid datasources have no schema to install — they materialise when
        # `data load` stages the parquet and ingests it.
        print()
        print("data create: no schema needed for druid domains (datasources only)")
        print()
        return 0
    if not sysobj.supports_sql:
        if kind.startswith("saas") and ctx.slug == "salesforce":
            print()
            print(
                ">>> salesforce has no schema to create — objects already "
                "exist in the org."
            )
            print(
                ">>> use `tdx-domain create --model salesforce --data …` to "
                "wipe + upload the data."
            )
            print()
            return 0
        if kind.startswith("saas"):
            return _print_saas_model_instructions(
                args.model, meta, action="data create"
            )
        raise DatagenError(
            f"'data create' is not supported for {ctx.slug} (no SQL backend)"
        )
    print()
    sysobj.install_schema(ctx, args.model, force=args.force)
    print()
    return 0


def _run_data_load(args: argparse.Namespace) -> int:
    """Data-only: generate the model's data and load it into the domain via the
    system backend (SQL INSERTs / cloud upload / kafka produce / saas INSERTs /
    salesforce cci)."""
    env_check.check_requirements()
    sysobj, ctx, meta = _domain(args.domain)
    _check_kind_compat(args.model, meta, "data load")
    kind = meta.get("KIND", "")
    is_kafka = meta.get("SYSTEM_KIND") == "kafka"

    # saas non-SQL: salesforce loads via its own loader; anything else manual.
    if kind.startswith("saas") and not sysobj.supports_sql:
        if ctx.slug == "salesforce":
            return sysobj.load_data(
                ctx, LoadRequest(model=args.model, data_dir=Path("."), size=None)
            )
        return _print_saas_model_instructions(args.model, meta, action="data load")

    if is_kafka:
        if not args.schemas:
            raise DatagenError(
                "--schemas DIR is required when loading into a kafka domain"
            )
        if not args.ser:
            raise DatagenError(
                f"--ser is required for kafka domains "
                f"(one of {', '.join(kafka_producer.SER_FORMATS)})"
            )

    size = _resolve_size(args.model, args.size)

    with tempfile.TemporaryDirectory(prefix="tdx-load-") as tmp:
        tmp_out = Path(tmp) / "data"
        print()
        if kind.startswith("cloud"):
            # file/az/gcs/s3 domains: stage EVERY supported file format, each in
            # its own subdir (<base>/csv, <base>/ndjson, <base>/parquet — JSON is
            # staged as NDJSON), so the object store carries a copy of the model in
            # each format. The entity counts are keyed by each staged file's path.
            table_counts = _generate_file_formats(
                args.model, size, tmp_out, args.chunks
            )
            entities = _file_path_entities(ctx.meta.get("BASE_PATH", ""), table_counts)
        else:
            # kafka + saas-SQL build messages / INSERTs from the json + schema
            # sidecar; SQL containers keep their native sql format.
            if is_kafka or kind.startswith("saas"):
                fmt, sidecar, gen_chunks = "json", True, None
            else:
                fmt, sidecar, gen_chunks = (
                    args.format or _default_format_for(kind, meta),
                    False,
                    args.chunks,
                )
            size_msg = f"size={size}, " if size else ""
            chunks_msg = f", chunks={gen_chunks}" if gen_chunks else ""
            print(
                f">>> generating model='{args.model}' "
                f"({size_msg}format={fmt}{chunks_msg}) ..."
            )
            entities = data_gen.generate(
                data_gen.model_dir(args.model),
                size,
                tmp_out,
                fmt,
                chunks=gen_chunks,
                engine=(
                    ctx.slug if kind.startswith("container") and not is_kafka else None
                ),
                schema_sidecar=sidecar,
            )
            if meta.get("SYSTEM_KIND") == "druid":
                # Druid fans each table out to its own datasource; record the
                # counts under the datasource names the cluster will expose.
                from .systems.druid.system import _table_datasource

                base = ctx.meta.get("DATASOURCE", "")
                entities = {
                    _table_datasource(base, table): rows
                    for table, rows in entities.items()
                }
        print()
        sysobj.load_data(
            ctx,
            LoadRequest(
                model=args.model,
                data_dir=tmp_out,
                size=size,
                force=args.force,
                ser=args.ser,
                schemas_dir=args.schemas,
                use_schema_registry=args.use_schema_registry,
            ),
        )
        print()
    # Record each entity's row count on the descriptor now the data is in place.
    if entities:
        descriptor.record_entities(args.domain, entities)
    return 0


def _generate_file_formats(
    model: str, size: str | None, out: Path, chunks: int | None
) -> dict[str, int]:
    """Stage `model` under ``out/<format>/`` for every supported file format,
    returning the per-entity record counts (identical across formats, so the
    first non-empty result is kept). ``run-order.txt`` — a SQL-load manifest the
    object-store / fs data plane doesn't use — is dropped from each format dir so
    only the data files are staged."""
    entities: dict[str, int] = {}
    size_msg = f"size={size}, " if size else ""
    for fmt in data_gen.FILE_FORMATS:
        print(f">>> generating model='{model}' ({size_msg}format={fmt}) ...")
        counts = data_gen.generate(
            data_gen.model_dir(model), size, out / fmt, fmt, chunks=chunks
        )
        (out / fmt / "run-order.txt").unlink(missing_ok=True)
        entities = entities or counts
    return entities


def _file_path_entities(base_path: str, table_counts: dict[str, int]) -> dict[str, int]:
    """Key a file domain's record counts by each staged file's full path
    (``<base>/<format>/<table>.<ext>``) rather than the bare table name — one
    entry per (table, format), since a file domain stages every format. Each file
    format's extension is the format name (``csv``/``ndjson``/``parquet``)."""
    base = base_path.rstrip("/")
    entities: dict[str, int] = {}
    for table, count in table_counts.items():
        for fmt in data_gen.FILE_FORMATS:
            rel = f"{fmt}/{table}.{fmt}"
            entities[f"{base}/{rel}" if base else rel] = count
    return entities


def _print_saas_model_instructions(
    model: str,
    meta: dict[str, str],
    *,
    action: str,
) -> int:
    """Saas domains with no automatic create/load — print the model's README
    (e.g. SALESFORCE.md) so the user knows the manual steps."""
    src = data_gen.model_dir(model)
    saas_type = meta.get("SAAS_TYPE", "")
    readme = next(
        (
            p
            for p in (src / f"{saas_type.upper()}.md", src / "README.md")
            if p.is_file()
        ),
        None,
    )
    print()
    print(f">>> '{action}' on a {saas_type} (saas) domain is a manual workflow")
    if readme is not None:
        print(f">>> see: {readme}")
        print()
        body = readme.read_text()
        sys.stdout.write(body)
        if not body.endswith("\n"):
            sys.stdout.write("\n")
    else:
        print(f">>> model '{model}' has no README documenting the procedure")
    print()
    return 0


def _resolve_size(model: str, size: str | None) -> str | None:
    """Validate --size against the model type. Sizeable models default to 'xs';
    fixed models reject --size."""
    if data_gen.is_sizeable(model):
        return size or "xs"
    if size is not None:
        raise DatagenError(f"model '{model}' is fixed-data; --data size does not apply")
    return None


def _default_format_for(kind: str, meta: dict[str, str]) -> str:
    if meta.get("SYSTEM_KIND") == "druid":
        # Druid ingests its input from staged parquet (see DruidSystem.load_data).
        return "parquet"
    if kind.startswith("container"):
        if meta.get("SUPPORTS_SQL") != "1":
            raise DatagenError(
                f"loading into {meta.get('SYSTEM_TYPE')} requires non-SQL format; "
                f"please pass --format explicitly"
            )
        return "sql"
    return "csv"


def _read_domain_meta(domain_path) -> dict[str, str]:
    """{'KIND': ..., <resolved meta vars>} — used by tests / callers that want a
    domain's connection details."""
    return descriptor.read_meta(domain_path)
