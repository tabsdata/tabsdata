#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery backend — a GCP-project + GCS-bucket hybrid, stage-only.

A domain is a BigQuery **dataset** plus a GCS staging **prefix** (both named from
the domain). BigQuery has no tdx data plane (``supports_models=False``): it only
provisions the empty dataset + prefix for a connector-under-test to load into, so
only create / destroy / status / purge are exposed. Config comes from the
``BIGQUERY__`` environment (``PROJECT`` / ``SERVICE_ACCOUNT_KEY`` [inline JSON or
a path] / ``BUCKET`` / ``SYS_PATH``); the same service account drives both
BigQuery and GCS.
"""

from __future__ import annotations

import json
from functools import cached_property

from tabsdatax.itest.tdksystems2 import _venv, options
from tabsdatax.itest.tdksystems2.errors import TdxError


class BigQuery:
    is_container: bool = False
    supports_purge: bool = True
    supports_models: bool = False
    supports_data: bool = False
    supports_changes: bool = False

    # Both planes go through google-cloud-bigquery / -storage.
    sys_env = _venv.ENV_A
    dom_env = _venv.ENV_A

    slug = "bigquery"
    label = "BigQuery"
    kind = "file"
    weight = 1
    domain_word = "dataset"
    max_entity_name_length = 1024
    env_prefix = "BIGQUERY"
    _config_keys = ("PROJECT", "BUCKET", "SERVICE_ACCOUNT_KEY")

    _bound: dict[str, str] | None = None

    def docker_images(self) -> list[tuple[str, str]]:
        return []

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option("PROJECT", "project", options.ENV, "env $BIGQUERY__PROJECT"),
            options.Option("BUCKET", "bucket", options.ENV, "env $BIGQUERY__BUCKET"),
            options.Option(
                "SERVICE_ACCOUNT_KEY",
                "service-account-key",
                options.ENV,
                "env $BIGQUERY__SERVICE_ACCOUNT_KEY",
                secret=True,
            ),
            options.Option(
                "SYS_PATH", "sys-path", options.ENV, "env $BIGQUERY__SYS_PATH"
            ),
        )

    # ---- config / clients ----------------------------------------------

    def configure(self, meta: dict[str, str]) -> None:
        self._bound = dict(meta)

    def _cfg(self, key: str) -> str:
        """A config value from the bound descriptor (create binds it before
        provisioning; every later op binds it from the loaded descriptor)."""
        return (self._bound or {}).get(key, "")

    @cached_property
    def _sa_info(self) -> dict | None:
        sa = self._cfg("SERVICE_ACCOUNT_KEY")
        return json.loads(sa) if sa.lstrip().startswith("{") else None

    @cached_property
    def _bq(self):
        from google.cloud import bigquery  # type: ignore[import-untyped]

        project = self._cfg("PROJECT")
        if self._sa_info is not None:
            return bigquery.Client.from_service_account_info(
                self._sa_info, project=project
            )
        return bigquery.Client.from_service_account_json(
            self._cfg("SERVICE_ACCOUNT_KEY"), project=project
        )

    @cached_property
    def _gcs(self):
        from google.cloud import storage  # type: ignore[import-untyped]

        if self._sa_info is not None:
            return storage.Client.from_service_account_info(self._sa_info)
        return storage.Client.from_service_account_json(
            self._cfg("SERVICE_ACCOUNT_KEY")
        )

    # ---- dataset + GCS-prefix primitives -------------------------------

    def _dataset_id(self, name: str) -> str:
        return f"{self._cfg('PROJECT')}.{name}"

    def _list_datasets(self) -> list[str]:
        return [ds.dataset_id for ds in self._bq.list_datasets()]

    def _create_dataset(self, name: str, *, drop_first: bool) -> None:
        from google.cloud import bigquery  # type: ignore[import-untyped]

        if drop_first:
            self._bq.delete_dataset(
                self._dataset_id(name), delete_contents=True, not_found_ok=True
            )
        self._bq.create_dataset(bigquery.Dataset(self._dataset_id(name)))

    def _delete_dataset(self, name: str) -> None:
        self._bq.delete_dataset(
            self._dataset_id(name), delete_contents=True, not_found_ok=True
        )

    @staticmethod
    def _prefix(base: str) -> str:
        p = base.strip("/")
        return f"{p}/" if p else ""

    def _gcs_status(self) -> None:
        next(iter(self._gcs.list_blobs(self._cfg("BUCKET"), max_results=1)), None)

    def _gcs_exists(self, base: str) -> bool:
        return any(
            self._gcs.list_blobs(
                self._cfg("BUCKET"), prefix=self._prefix(base), max_results=1
            )
        )

    def _gcs_clear(self, base: str) -> None:
        for blob in list(
            self._gcs.list_blobs(self._cfg("BUCKET"), prefix=self._prefix(base))
        ):
            blob.delete()

    # ---- system lifecycle ----------------------------------------------

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        o = overrides or {}
        root = (o.get("SYS_PATH") or "").strip("/")
        meta = {key: o.get(key, "") for key in self._config_keys}
        meta["SYS_PATH"] = root
        meta["LOCATION"] = (
            f"bigquery://{meta['PROJECT']} (staging gs://{meta['BUCKET']}/{root})"
        )
        self.configure(meta)
        return meta

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        list(self._bq.list_datasets(max_results=1))  # BigQuery reachable
        self._gcs_status()  # GCS reachable
        return {}

    def destroy_system(self, meta: dict[str, str]) -> None:
        return None  # project/bucket are externally owned

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        try:
            list(self._bq.list_datasets(max_results=1))
            self._gcs_status()
        except Exception as exc:  # noqa: BLE001
            return False, f"cannot reach BigQuery: {exc}"
        return True, f"BigQuery project {meta.get('PROJECT', '')}"

    # ---- domain lifecycle (a dataset + a GCS prefix) -------------------

    def new_domain_password(self) -> str:
        return ""

    def _base_path(self, root: str, entity: str) -> str:
        return f"{root}/{entity}" if root else entity

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        base = self._base_path(sys_meta["SYS_PATH"], entity_name)
        meta = {key: sys_meta.get(key, "") for key in self._config_keys}
        meta.update({
            "SYS_PATH": sys_meta["SYS_PATH"],
            "DATASET": entity_name,
            "BASE_PATH": base,
            "URI": (
                f"bigquery://{sys_meta['PROJECT']}/{entity_name}"
                f" + gs://{sys_meta['BUCKET']}/{base}"
            ),
        })
        return meta

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        base = domain_meta["BASE_PATH"]
        occupied = entity_name in self._list_datasets() or self._gcs_exists(base)
        if occupied and not force:
            raise TdxError(
                f"bigquery domain '{entity_name}' exists (dataset or staging prefix) "
                f"— use --force"
            )
        self._create_dataset(entity_name, drop_first=occupied)
        if force:
            self._gcs_clear(base)

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        self._delete_dataset(domain_meta["DATASET"])
        self._gcs_clear(domain_meta["BASE_PATH"])

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        try:
            self._bq.get_dataset(self._dataset_id(domain_meta["DATASET"]))
        except Exception as exc:  # noqa: BLE001
            return False, f"BigQuery dataset unavailable: {exc}"
        return True, f"BigQuery dataset {domain_meta.get('DATASET', '')}"

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        return self._list_datasets()

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        self._delete_dataset(name)
        self._gcs_clear(self._base_path(sys_meta["SYS_PATH"], name))
