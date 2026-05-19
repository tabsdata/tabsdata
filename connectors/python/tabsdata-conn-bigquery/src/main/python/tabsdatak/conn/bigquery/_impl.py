from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import (
    BigQueryConn,
    BigQueryDest,
    IfTableExistsSpec,
    SchemaEvolutionSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery._validation import _BIGQUERY_FQN_DEF
from tabsdatak.conn.common.types import CfgSpec, GCPJson

# --- Client builders -------------------------------------------------
#
# All ``google-cloud-*`` imports are made lazily inside the body of
# the helpers below so that ``import tabsdatak.conn.bigquery._impl``
# stays cheap when no flow actually exercises the plugin. This is the
# same convention used by the other connectors -- the registry only
# pays for ``SrcDef`` / ``DestDef`` metadata + the abstract classes
# when scanning for plugins.


def _gcp_json_to_dict(credentials: GCPJson) -> dict[str, Any]:
    raw = credentials.json.value()
    return json.loads(raw)


def build_bigquery_client(
    credentials: GCPJson,
    project: str | None,
    connect_args: CfgSpec | None,
) -> Any:
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
    from google.cloud import storage

    info = _gcp_json_to_dict(credentials)
    kwargs: dict[str, Any] = dict(connect_args or {})
    return storage.Client.from_service_account_info(info, **kwargs)


# --- Table FQN helpers -----------------------------------------------


def unquote_table_fqn(table: str) -> str:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common._validation import unfqn

    parts = unfqn(table, _BIGQUERY_FQN_DEF)
    # Validator guarantees up to 3 parts, all non-None.
    return ".".join(p for p in parts if p is not None)


# --- Dest cfg --------------------------------------------------------


@dataclass(kw_only=True)
class BigQueryWriteCfg:

    conn: BigQueryConn
    target_tables: list[str]
    if_table_exists: IfTableExistsSpec
    schema_evolution: SchemaEvolutionSpec
    dest_cfg: CfgSpec | None = None

    @classmethod
    def from_dest(cls, dest: BigQueryDest) -> "BigQueryWriteCfg":
        # Validator-enforced invariant: when ``tables is None`` the
        # plugin can't proceed -- the legacy plugin took table names
        # from the upstream input plugin's table list, which the new
        # SPI does not yet surface. The defensive check in
        # ``_plugin.write_out`` raises BIGQUERY_4 in that case.
        target_tables: list[str] = []
        if dest.tables is not None:
            for t in dest.tables:
                target_tables.append(unquote_table_fqn(t))
        return cls(
            conn=dest.conn,  # type: ignore[arg-type]
            target_tables=target_tables,
            if_table_exists=dest.if_table_exists,
            schema_evolution=dest.schema_evolution,
            dest_cfg=dest.dest_cfg,
        )


# --- Load-job config -------------------------------------------------


def build_load_job_config(
    if_table_exists: IfTableExistsSpec,
    schema_evolution: SchemaEvolutionSpec,
) -> Any:
    from google.cloud import bigquery

    if if_table_exists == "replace":
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        schema_update_options: list[Any] = []
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        if schema_evolution == "strict":
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
    )


