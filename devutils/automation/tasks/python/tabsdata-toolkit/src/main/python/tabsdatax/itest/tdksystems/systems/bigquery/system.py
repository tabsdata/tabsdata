"""BigQuery system — a managed warehouse (datasets) staged through a GCS bucket.

Hybrid, so it subclasses `System` directly rather than a category mixin:
  * system: a GCP PROJECT + service-account CREDENTIALS + a GCS BUCKET and
    SYS_PATH staging prefix (the GCS side reuses `GcsBackend`);
  * domain: a BigQuery DATASET *and* a GCS prefix (DOMAIN_PATH, relative to
    SYS_PATH) -> BASE_PATH; destroying the domain drops the dataset and clears
    the prefix.
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

    options = (
        Option("PROJECT", "project"),
        Option("SERVICE_ACCOUNT_KEY", "service-account-key", secret=True),
        Option("BUCKET", "bucket"),
    )
    domain_options = (
        Option("DATASET", "dataset"),
        Option("DOMAIN_PATH", "domain-path", is_path=True),
    )

    # ---- fakeable seams (mirror gcs._backend / snowflake._cursor) ----

    def _gcs(self, values: dict[str, str]) -> GcsBackend:
        return GcsBackend(values)

    def _bq(self, values: dict[str, str]) -> "client.BigQueryClient":
        return client.BigQueryClient(values)

    def _base_path(self, values: dict[str, str], domain_path: str) -> str:
        root = values.get("SYS_PATH", "")
        return f"{root}/{domain_path}" if root else domain_path

    # ---- system lifecycle ----

    def create_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        values = self.resolve_values(spec.settings)
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
        return dict(spec.settings)

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

    def create_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        settings = self.settings_from_meta(ctx.meta)
        values = self.resolve_values(settings)
        dataset = spec.options.get("DATASET", "")
        domain_path = spec.options.get("DOMAIN_PATH", "")
        if not dataset:
            raise DatagenError("domain --dataset is empty")
        if not domain_path:
            raise DatagenError("domain --domain-path is empty")
        full = self._base_path(values, domain_path)

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

        settings["DATASET"] = Setting("DATASET", value=dataset)
        settings["DOMAIN_PATH"] = Setting(
            "DOMAIN_PATH", value=domain_path, is_path=True
        )
        settings["BASE_PATH"] = Setting("BASE_PATH", value=full, is_path=True)
        return settings

    def destroy_domain(self, ctx: DomainContext) -> None:
        values = self.resolve_values(self.settings_from_meta(ctx.meta))
        dataset = ctx.meta.get("DATASET")
        if dataset:
            self._bq(values).delete_dataset(dataset)
        base = ctx.meta.get("BASE_PATH")
        if base:
            self._gcs(values).clear(base)

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
