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
            self.client = bigquery.Client.from_service_account_info(
                json.loads(sa_key), project=self.project)
        else:
            self.client = bigquery.Client.from_service_account_json(
                sa_key, project=self.project)

    def _ref(self, dataset: str) -> str:
        return f"{self.project}.{dataset}"

    def ping(self) -> None:
        # Cheap reachability probe: a single page of the project's datasets.
        next(iter(self.client.list_datasets(max_results=1)), None)

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
            self.client.delete_dataset(
                ref, delete_contents=True, not_found_ok=True)
        self.client.create_dataset(bigquery.Dataset(ref))

    def delete_dataset(self, dataset: str) -> None:
        self.client.delete_dataset(
            self._ref(dataset), delete_contents=True, not_found_ok=True)
