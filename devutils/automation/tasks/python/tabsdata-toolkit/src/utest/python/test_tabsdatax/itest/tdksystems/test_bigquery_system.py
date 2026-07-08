"""bigquery: a managed warehouse (datasets) staged through a GCS bucket. The
system takes project / service-account-key / bucket / sys-path; a domain takes a
dataset and a domain-path (relative to sys-path) -> BASE_PATH; destroying the
domain drops the dataset AND clears the prefix. No models/data. The BigQuery and
GCS clients are faked here; the real ones are exercised by the itest."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import descriptor, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.systems.bigquery.system import BigQuerySystem

_SYS_CREATE = ["--project", "proj", "--service-account-key", "{}",
               "--bucket", "bkt", "--sys-path", "staging", "-f"]


@pytest.fixture
def bq(monkeypatch):
    """Fake the two backends and share their recorded state via a dict:
    `datasets` (created BQ datasets) and `cleared` (cleared GCS prefixes)."""
    state: dict = {"datasets": set(), "cleared": []}

    class _FakeGcs:
        def __init__(self, values): pass
        def status(self): pass
        def exists(self, prefix): return False
        def clear(self, prefix): state["cleared"].append(prefix)
        def upload(self, prefix, paths): return 0

    class _FakeBq:
        def __init__(self, values): pass
        def ping(self): pass
        def dataset_exists(self, name): return name in state["datasets"]
        def create_dataset(self, name, *, force=False): state["datasets"].add(name)
        def delete_dataset(self, name): state["datasets"].discard(name)

    monkeypatch.setattr(BigQuerySystem, "_gcs", lambda self, v: _FakeGcs(v))
    monkeypatch.setattr(BigQuerySystem, "_bq", lambda self, v: _FakeBq(v))
    return state


def test_bigquery_lifecycle_through_tdx(tmp_path, bq):
    sysf, domf = str(tmp_path / "bq.json"), str(tmp_path / "bq_dom.json")

    # system: project / creds / bucket / sys-path
    assert tdx._sys_create(["--type", "bigquery", "--sys", sysf, "--name", "wh",
                            *_SYS_CREATE]) == 0
    doc = descriptor.load(sysf)
    assert (doc["category"], doc["system"], doc["role"]) == ("saas", "bigquery", "sys")
    assert tdx._sys_status(["--sys", sysf]) == 0

    # domain: dataset + domain-path (relative to sys-path)
    assert tdx._domain_create([
        "--sys", sysf, "--domain", domf, "--name", "inbox",
        "--dataset", "sales", "--domain-path", "d1", "-f"]) == 0
    m = descriptor.read_meta(domf)
    assert m["DATASET"] == "sales"
    assert m["DOMAIN_PATH"] == "d1"
    assert m["BASE_PATH"] == "staging/d1"        # domain-path under sys-path
    assert "sales" in bq["datasets"]

    # destroy: the dataset is dropped AND the prefix is cleared
    assert tdx._domain_destroy(["--domain", domf]) == 0
    assert "sales" not in bq["datasets"]
    assert "staging/d1" in bq["cleared"]
    assert tdx._sys_destroy(["--sys", sysf]) == 0


def test_bigquery_domain_requires_dataset_and_domain_path(tmp_path, bq):
    sysf = str(tmp_path / "bq.json")
    tdx._sys_create(["--type", "bigquery", "--sys", sysf, "--name", "wh",
                     *_SYS_CREATE])
    # add_domain_options makes both required -> argparse exits (SystemExit).
    with pytest.raises(SystemExit):
        tdx._domain_create(["--sys", sysf, "--domain", str(tmp_path / "d.json"),
                            "--name", "inbox", "-f"])


def test_bigquery_rejects_models_and_data(tmp_path, bq):
    sysf = str(tmp_path / "bq.json")
    tdx._sys_create(["--type", "bigquery", "--sys", sysf, "--name", "wh",
                     *_SYS_CREATE])
    with pytest.raises(DatagenError, match="does not support --model"):
        tdx._domain_create([
            "--sys", sysf, "--domain", str(tmp_path / "d.json"), "--name", "inbox",
            "--dataset", "sales", "--domain-path", "d1", "--model", "sakila", "-f"])
