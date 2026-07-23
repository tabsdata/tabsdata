#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery dataset ops (google-cloud-bigquery). Only what the System contract
needs: ping / dataset_exists / create_dataset / delete_dataset. Built from a
plain `{OPTION: value}` dict of resolved connection values, like GcsBackend, so
create-time and run-time callers construct it the same way. The client library
is imported lazily so it is only required when BigQuery is actually used."""

from __future__ import annotations


def _is_inline_json(value: str) -> bool:
    """The service-account key may be a file path or the key JSON itself
    (e.g. resolved from a 1Password field). Inline JSON starts with '{'."""
    return value.lstrip().startswith("{")


class BigQueryClient:
    def __init__(self, values: dict[str, str]) -> None:
        import json

        from google.cloud import bigquery

        self.project = values.get("PROJECT") or ""
        sa_key = values.get("SERVICE_ACCOUNT_KEY") or ""
        if _is_inline_json(sa_key):
            info = json.loads(sa_key)
            self.sa_email = info.get("client_email", "")
            self.client = bigquery.Client.from_service_account_info(
                info, project=self.project
            )
        else:
            with open(sa_key) as f:
                self.sa_email = json.load(f).get("client_email", "")
            self.client = bigquery.Client.from_service_account_json(
                sa_key, project=self.project
            )

    def _ref(self, dataset: str) -> str:
        return f"{self.project}.{dataset}"

    def ping(self) -> None:
        # Cheap reachability probe: a single page of the project's datasets.
        next(iter(self.client.list_datasets(max_results=1)), None)

    def list_owned_datasets(self) -> list[str]:
        """Dataset ids where our service account holds an OWNER access entry —
        the datasets it created. Strict: no SA email (nothing to match) -> none."""
        me = (self.sa_email or "").lower()
        if not me:
            return []
        owned: list[str] = []
        for ds in self.client.list_datasets():
            full = self.client.get_dataset(ds.reference)
            for entry in full.access_entries:
                role = (entry.role or "").upper()
                ent = (getattr(entry, "entity_id", "") or "").lower()
                if role == "OWNER" and ent == me:
                    owned.append(ds.dataset_id)
                    break
        return owned

    def list_datasets(self) -> list[str]:
        return [ds.dataset_id for ds in self.client.list_datasets()]

    def dataset_exists(self, dataset: str) -> bool:
        from google.cloud.exceptions import NotFound

        try:
            self.client.get_dataset(self._ref(dataset))
            return True
        except NotFound:
            return False

    def create_dataset(self, dataset: str, *, force: bool = False) -> None:
        from google.cloud import bigquery

        ref = self._ref(dataset)
        if force:
            self.client.delete_dataset(ref, delete_contents=True, not_found_ok=True)
        self.client.create_dataset(bigquery.Dataset(ref))

    def delete_dataset(self, dataset: str) -> None:
        self.client.delete_dataset(
            self._ref(dataset), delete_contents=True, not_found_ok=True
        )
