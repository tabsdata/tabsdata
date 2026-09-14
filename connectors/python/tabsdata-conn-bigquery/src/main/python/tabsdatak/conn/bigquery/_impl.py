#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import (
    BigQueryDest,
    BigQueryDestConn,
    IfTableExistsSpec,
    MergeCfg,
    SchemaEvolutionSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery._iceberg import quote_column

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery._validation import _BIGQUERY_FQN_DEF

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import fqn
from tabsdatak.conn.common.sql.cdc.constants import (
    OP_DELETE,
    OP_INSERT,
    OP_UPDATE,
)
from tabsdatak.conn.common.types import CfgSpec, GCPJson, _convert

# --- Constants -------------------------------------------------------

MERGE_TARGET_ALIAS = "target"
MERGE_SOURCE_ALIAS = "source"

# ``CdcOperation`` mixes in ``str``, so an f-string interpolates the
# member (``CdcOperation.INSERT``) rather than its value. The markers are
# taken as plain strings once, here, so no emitted SQL can carry a
# predicate no row could ever match.
_OP_INSERT = OP_INSERT.value
_OP_UPDATE = OP_UPDATE.value
_OP_DELETE = OP_DELETE.value

# --- Client builders -------------------------------------------------
#
# All ``google-cloud-*`` imports are made lazily inside the body of
# the helpers below so that ``import tabsdatak.conn.bigquery._impl``
# stays cheap when no flow actually exercises the plugin. This is the
# same convention used by the other connectors -- the registry only
# pays for ``SrcDef`` / ``DestDef`` metadata + the abstract classes
# when scanning for plugins.


def _gcp_json_to_dict(credentials: GCPJson) -> dict[str, Any]:
    """Resolve a ``GCPJson`` credentials field to a plain dict.

    Mirrors the legacy behaviour: the service-account-key JSON travels
    through the connector as a ``Secret``-backed string, and
    the underlying SDK takes a ``dict``.
    """
    raw = _convert(credentials.json, str)
    return json.loads(raw)


def build_bigquery_client(
    credentials: GCPJson,
    project: str | None,
    connect_args: CfgSpec | None,
) -> Any:
    """Build a ``google.cloud.bigquery.Client``.

    ``project`` is forwarded to the SDK so queries / load jobs default
    to it when a table FQN omits the project component. ``connect_args``
    is the free-form ``conn_cfg``-style cfg dict and is forwarded
    verbatim as keyword arguments to ``Client.from_service_account_info``
    (i.e. it must be a flat ``str``-keyed mapping the SDK accepts).
    """
    from google.cloud import bigquery

    info = _gcp_json_to_dict(credentials)
    kwargs: dict[str, Any] = dict(connect_args or {})
    if project is not None:
        kwargs.setdefault("project", project)
    return bigquery.Client.from_service_account_info(info, **kwargs)


def build_gcs_client(
    credentials: GCPJson,
    connect_args: CfgSpec | None,
) -> Any:
    """Build a ``google.cloud.storage.Client``."""
    from google.cloud import storage

    info = _gcp_json_to_dict(credentials)
    kwargs: dict[str, Any] = dict(connect_args or {})
    return storage.Client.from_service_account_info(info, **kwargs)


# --- Table FQN helpers -----------------------------------------------


def unquote_table_fqn(table: str) -> str:
    """Strip outer backticks from a BigQuery table FQN.

    The :data:`tabsdatak.conn.bigquery._validation.BIGQUERY_TABLE_FQN_VALIDATION`
    requires the user-supplied value to be wrapped in outer-only
    backticks (``\\`project.dataset.table\\```). The BigQuery SDK accepts
    a bare ``project.dataset.table`` string in ``load_table_from_uri``
    / ``client.query``, so we drop the wrapping quotes (and re-resolve
    any backslash-escaped inner backticks) before forwarding.
    """
    # noinspection PyProtectedMember
    from tabsdatak.conn.common._validation import unfqn

    parts = unfqn(table, _BIGQUERY_FQN_DEF)
    # Validator guarantees up to 3 parts, all non-None.
    return ".".join(p for p in parts if p is not None)


def temp_table_fqn(table_fqn: str) -> str:
    """Name a staging table beside ``table_fqn``.

    The suffix lands on the table part, so the staging table keeps the
    target's project and dataset -- a query cannot join tables across
    regions, and a merge reads both sides at once. The random suffix is
    what keeps two concurrent runs against one target apart.
    """
    return f"{table_fqn}_{uuid.uuid4().hex[:8]}"


def qualify_table_fqn(table: str, project: str | None, dataset: str | None) -> str:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common._validation import complete_levels

    raw = table.split(".")
    parts: list[str | None] = [None] * (3 - len(raw)) + list(raw)
    filled = complete_levels(parts, [project, dataset, None])
    return ".".join(p for p in filled if p is not None)


# --- Dest cfg --------------------------------------------------------


@dataclass(kw_only=True)
class BigQueryWriteCfg:
    """Resolved configuration for ``_BigQueryDestPlugin.write_out``.

    Mirrors :class:`MySQLWriteCfg`'s purpose in the SQL connectors -- a
    plain dataclass holding the already-resolved view of the
    ``BigQueryDest`` (and its ``BigQueryDestConn``) plus a couple of derived
    fields the plugin needs to drive the staging-upload-then-load-job
    sequence.
    """

    conn: BigQueryDestConn
    target_tables: list[str]
    if_table_exists: IfTableExistsSpec
    schema_evolution: SchemaEvolutionSpec
    # One entry per target table when merging, already normalized by
    # ``BigQueryDest._merge_cfgs()`` (a single MergeCfg broadcast across every
    # table); ``None`` in every other disposition.
    merge_cfgs: list[MergeCfg] | None = None
    # The target column each row is stamped with the trx id in, already in the
    # form the target names it by; ``None`` when the write names no watermark.
    # A merge reads its own off ``merge_cfgs``.
    watermark_column: str | None = None
    dest_cfg: CfgSpec | None = None

    @classmethod
    def from_dest(
        cls, conn: BigQueryDestConn, dest: BigQueryDest
    ) -> "BigQueryWriteCfg":
        # Validator-enforced invariant: when ``tables is None`` the
        # plugin can't proceed -- the legacy plugin took table names
        # from the upstream input plugin's table list, which the new
        # SPI does not yet surface. The defensive check in
        # ``_plugin.write_out`` raises BIGQUERY_4 in that case.
        project = conn._project() if conn.project is not None else None
        dataset = conn._dataset() if conn.dataset is not None else None
        target_tables: list[str] = []
        if dest.tables is not None:
            for t in dest.tables:
                target_tables.append(
                    qualify_table_fqn(unquote_table_fqn(t), project, dataset)
                )
        return cls(
            conn=conn,
            target_tables=target_tables,
            if_table_exists=dest.if_table_exists,
            schema_evolution=dest.schema_evolution,
            merge_cfgs=dest._merge_cfgs(),
            watermark_column=dest._watermark_column(),
            dest_cfg=dest.dest_cfg,
        )


# --- Load-job config -------------------------------------------------


def build_load_job_config(
    if_table_exists: IfTableExistsSpec,
    schema_evolution: SchemaEvolutionSpec,
    schema_evolution_engine: str = "native",
) -> Any:
    """Translate the dest's strategy fields into a BigQuery
    ``LoadJobConfig`` for a parquet -> table load.

    Mirrors legacy ``BigQueryDest._upload_single_uri_to_table``:

    - ``if_table_exists='replace'`` -> ``WRITE_TRUNCATE``; schema is
      replaced wholesale, so no ``schema_update_options`` are passed.
    - ``if_table_exists='append'`` + ``schema_evolution='strict'`` ->
      ``WRITE_APPEND`` with no schema-update options (incoming parquet
      must match the table's current schema).
    - ``if_table_exists='append'`` + ``schema_evolution='update'`` ->
      ``WRITE_APPEND`` with ``ALLOW_FIELD_ADDITION`` and
      ``ALLOW_FIELD_RELAXATION``.

    ``schema_evolution_engine='iceberg'`` takes the options off whatever
    the other two say: the connector has already ALTERed the target into
    shape, and leaving BigQuery's own evolution on would be a second
    engine on one write -- one the write did not ask for, and one that
    would quietly re-add a column the Iceberg pass had refused.

    ``CREATE_IF_NEEDED`` is set unconditionally so the first write to a
    new table creates it.

    ``decimal_target_types`` widens the candidate list for a parquet
    decimal column. BigQuery otherwise picks ``NUMERIC``, whose scale
    caps at 9, and a source decimal with a larger scale fails the load
    outright ("out of valid NUMERIC range: Value will lose precision
    after scaling down to NUMERIC type"). Listing ``BIGNUMERIC`` after
    ``NUMERIC`` keeps ``NUMERIC`` for everything that fits and falls
    through to ``BIGNUMERIC`` only when it does not.
    """
    from google.cloud import bigquery

    natively = schema_evolution_engine == "native"
    if if_table_exists == "replace":
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        schema_update_options: list[Any] = []
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        if schema_evolution == "strict" or not natively:
            schema_update_options = []
        else:
            schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        schema_update_options=schema_update_options,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        decimal_target_types=["NUMERIC", "BIGNUMERIC"],
    )


def build_merge_staging_load_job_config() -> Any:
    """Translate a merge's staging load into a ``LoadJobConfig``.

    A merge lands its batch in a staging table of its own before the
    ``MERGE`` reads it, so the load answers to the staging table, not to
    the dest's strategy: ``WRITE_TRUNCATE`` (the batch is the table's
    whole content) and ``CREATE_IF_NEEDED`` (the first load creates it).
    No ``schema_update_options`` -- the staging table takes the incoming
    parquet's schema outright, and it is the ``MERGE`` that answers for
    evolving the target.

    ``decimal_target_types`` is the same as in
    :func:`build_load_job_config`, and for the same reason -- see its
    docstring.
    """
    from google.cloud import bigquery

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        decimal_target_types=["NUMERIC", "BIGNUMERIC"],
    )


# --- Merge -----------------------------------------------------------


def quote_table(table_fqn: str) -> str:
    """Reference a table in emitted SQL.

    BigQuery wraps the whole path in one backtick pair --
    ``\\`project.dataset.table\\``` -- which is what lets a project id
    carry hyphens without the parser reading them as an operator.
    """
    return fqn(table_fqn.split("."), _BIGQUERY_FQN_DEF)


def _standard_kind(field: Any) -> str:
    """The GoogleSQL name of what one ``SchemaField`` holds.

    ``SchemaField.field_type`` reports the legacy name -- ``INTEGER`` for an
    ``INT64`` column, ``RECORD`` for a ``STRUCT`` -- so the name is read off
    ``to_standard_sql()``, which knows the mapping. That folds
    ``mode="REPEATED"`` into an ``ARRAY``, so the element is unwrapped again
    here: repetition is the field's, not the type's.
    """
    type_ = field.to_standard_sql().type
    if type_.type_kind.value == "ARRAY":
        type_ = type_.array_element_type
    return type_.type_kind.value


def _type_parameters(field: Any) -> str:
    """The parameters a ``SchemaField``'s type carries, as DDL spells them.

    ``STRING`` and ``BYTES`` take a length, ``NUMERIC`` and ``BIGNUMERIC`` a
    precision and optionally a scale. A scale without a precision cannot be
    spelled, so it is dropped rather than guessed at.
    """
    if field.max_length is not None:
        return f"({field.max_length})"
    if field.precision is None:
        return ""
    if field.scale is not None:
        return f"({field.precision}, {field.scale})"
    return f"({field.precision})"


def column_type(field: Any) -> str:
    """The GoogleSQL type of a ``SchemaField``, as DDL spells it.

    Walked over the ``SchemaField`` itself rather than over what
    ``to_standard_sql()`` returns, because that drops the type's parameters: a
    column defined ``NUMERIC(20, 4)`` reads back there as plain ``NUMERIC``, and
    a target created from it would take the type's whole range instead of the
    staging table's. Only the type *name* is read from the standard-SQL view,
    which is the part the legacy spelling gets wrong.

    Parameters are kept wherever they sit: on the element of a repeated column,
    and on each field of a struct.
    """
    element = _element_type(field)
    return f"ARRAY<{element}>" if field.mode == "REPEATED" else element


def _element_type(field: Any) -> str:
    """``column_type`` for the field's element, with its repetition set aside."""
    kind = _standard_kind(field)
    if kind == "STRUCT":
        fields = ", ".join(
            f"{quote_column(sub.name)} {column_type(sub)}" for sub in field.fields
        )
        return f"STRUCT<{fields}>"
    if kind == "RANGE":
        return f"RANGE<{field.range_element_type.element_type}>"
    return f"{kind}{_type_parameters(field)}"


def table_column_types(bq_client: Any, table_fqn: str) -> list[tuple[str, str]]:
    """The ``(name, type)`` pairs of ``table_fqn``, in the table's own order."""
    table_schema = bq_client.get_table(table_fqn).schema
    return [(field.name, column_type(field)) for field in table_schema]
