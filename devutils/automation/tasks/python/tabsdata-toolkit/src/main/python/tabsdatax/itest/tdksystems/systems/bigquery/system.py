#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery system — a managed warehouse (datasets) staged through a GCS bucket.

Hybrid, so it subclasses `System` directly rather than a category mixin:
  * system: a GCP PROJECT + service-account CREDENTIALS + a GCS BUCKET and
    SYS_PATH staging prefix (the GCS side reuses `GcsBackend`);
  * domain: named like every other system — the domain NAME is both the
    BigQuery DATASET and the GCS staging prefix segment (BASE_PATH =
    SYS_PATH/<name>); destroying the domain drops the dataset and clears the
    prefix.
No models/data (`supports_models = False`): it stages datasets/prefixes only.
"""

from __future__ import annotations

from ...errors import DatagenError
from ..base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    Option,
    Setting,
    System,
    SystemContext,
)
from ..gcs.backend import GcsBackend
from . import client


class BigQuerySystem(System):
    slug = "bigquery"
    label = "BigQuery"
    category = "saas"  # managed account, externally owned (no container)
    kind = "file"  # GCS-staged; no SQL data-load path
    staging_root = True  # -> base adds the SYS_PATH (--sys-path) option
    supports_models = False  # datasets + prefixes only; no schema/data load
    domain_flag = "dataset"  # a domain is a BigQuery dataset (DATASET is baked)
    max_entity_name_length = 1024  # BigQuery dataset id limit
    owns_entities = True  # datasets carry an OWNER access entry (see list_owned...)

    options = (
        Option("PROJECT", "project"),
        Option("SERVICE_ACCOUNT_KEY", "service-account-key", secret=True),
        Option("BUCKET", "bucket"),
    )
    # No extra domain options: like every other system, the domain NAME is the
    # resource name (here the dataset + the GCS prefix segment).

    # ---- fakeable seams (mirror gcs._backend / snowflake._cursor) ----

    def _gcs(self, values: dict[str, str]) -> GcsBackend:
        return GcsBackend(values)

    def _bq(self, values: dict[str, str]) -> "client.BigQueryClient":
        return client.BigQueryClient(values)

    def _base_path(self, values: dict[str, str], domain_path: str) -> str:
        root = values.get("SYS_PATH", "")
        return f"{root}/{domain_path}" if root else domain_path

    # ---- system lifecycle ----

    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        return dict(spec.settings)

    def provision_system(
        self, spec: CreateSystemSpec, settings: dict[str, Setting]
    ) -> None:
        values = self.resolve_values(settings)
        try:
            self._bq(values).ping()
        except Exception as exc:  # noqa: BLE001
            raise DatagenError(
                f"could not reach BigQuery project '{values.get('PROJECT', '')}': {exc}"
            )
        gcs = self._gcs(values)
        try:
            gcs.status()
        except Exception as exc:  # noqa: BLE001
            raise DatagenError(
                f"could not reach GCS bucket '{values.get('BUCKET', '')}': {exc}"
            )
        root = values.get("SYS_PATH", "")
        if gcs.exists(root):
            if spec.force:
                gcs.clear(root)
                print(
                    f">>> --force: cleared existing root prefix "
                    f"gs://{values.get('BUCKET', '')}/{root}"
                )
            else:
                raise DatagenError(
                    f"root prefix has objects: gs://{values.get('BUCKET', '')}/"
                    f"{root} (use --force to clear)"
                )

    def destroy_system(self, ctx: SystemContext) -> None:
        return None  # project/bucket external

    def system_status(self, ctx: SystemContext) -> Health:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        try:
            self._bq(values).ping()
            self._gcs(values).status()
            return Health(
                True,
                f"bigquery project '{values.get('PROJECT', '')}' + "
                f"gs://{values.get('BUCKET', '')} reachable",
            )
        except Exception as exc:  # noqa: BLE001
            return Health(False, f"cannot reach bigquery: {exc}")

    # ---- domain lifecycle ----

    def plan_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        settings = self.settings_from_meta(ctx.meta)
        values = self.resolve_values(settings)
        # The (prefixed) entity name is the dataset AND the staging prefix
        # segment, so `tdx-domain create --sys wh --domain <name>` is all it
        # takes — same as every other system (it's a validated identifier).
        dataset = spec.entity_name
        settings["DATASET"] = Setting("DATASET", value=dataset)
        settings["BASE_PATH"] = Setting(
            "BASE_PATH", value=self._base_path(values, dataset), is_path=True
        )
        return settings

    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        values = self.resolve_values(settings)
        dataset = settings["DATASET"].value or ""
        full = settings["BASE_PATH"].value or ""
        self._bq(values).create_dataset(dataset, force=spec.force)
        # If the GCS prefix is dirty and we're not forcing, roll the dataset back
        # so a failed domain create leaves nothing behind.
        gcs = self._gcs(values)
        if gcs.exists(full):
            if spec.force:
                gcs.clear(full)
            else:
                self._bq(values).delete_dataset(dataset)
                raise DatagenError(
                    f"domain prefix has objects: gs://{values.get('BUCKET', '')}/"
                    f"{full} (use --force to clear)"
                )

    def destroy_domain(self, ctx: DomainContext) -> None:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        dataset = ctx.meta.get("DATASET")
        if dataset:
            self._bq(values).delete_dataset(dataset)
        base = ctx.meta.get("BASE_PATH")
        if base:
            self._gcs(values).clear(base)

    def list_domain_entities(self, ctx: SystemContext) -> list[str]:
        # A domain is a BigQuery dataset; enumerate the project's datasets.
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        return self._bq(values).list_datasets()

    def list_owned_domain_entities(self, ctx: SystemContext) -> list[str]:
        # Datasets our service account created carry it as an OWNER access entry.
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        return self._bq(values).list_owned_datasets()

    def purge_domain_entity(self, ctx: SystemContext, name: str) -> None:
        # Drop the dataset AND clear its GCS staging prefix (SYS_PATH/<name>),
        # mirroring destroy_domain.
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        self._bq(values).delete_dataset(name)
        self._gcs(values).clear(self._base_path(values, name))

    def domain_status(self, ctx: DomainContext) -> Health:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        dataset = ctx.meta.get("DATASET", "")
        try:
            ok = self._bq(values).dataset_exists(dataset)
            self._gcs(values).status()
            return Health(
                ok, f"bigquery dataset '{dataset}' {'reachable' if ok else 'missing'}"
            )
        except Exception as exc:  # noqa: BLE001
            return Health(False, f"cannot reach bigquery domain: {exc}")
