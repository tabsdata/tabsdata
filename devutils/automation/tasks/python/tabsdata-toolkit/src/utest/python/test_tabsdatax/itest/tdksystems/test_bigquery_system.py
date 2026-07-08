"""bigquery: a managed warehouse (datasets) staged through a GCS bucket. The
system takes project / service-account-key / bucket / sys-path; a domain is
named like every other system — the domain NAME is both the dataset and the GCS
prefix segment (BASE_PATH = SYS_PATH/<name>); destroying the domain drops the
dataset AND clears the prefix. No models/data. The BigQuery and GCS clients are
faked here; the real ones are exercised by the itest."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import descriptor, store, tdx
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


def test_bigquery_lifecycle_through_tdx(stores, bq):
    # system: project / creds / bucket / sys-path
    assert tdx._sys_create(["--type", "bigquery", "--sys", "wh",
                            *_SYS_CREATE]) == 0
    doc = descriptor.load(store.system_path("wh"))
    assert (doc["category"], doc["system"], doc["role"]) == ("saas", "bigquery", "sys")
    assert tdx._sys_status(["--sys", "wh"]) == 0

    # domain: named like any other system — the domain NAME is the dataset and
    # the GCS prefix segment (BASE_PATH = SYS_PATH/<name>). No extra options.
    # Pin the prefix off so the dataset/prefix are exactly the domain name.
    assert tdx._domain_create(["--sys", "wh", "--domain", "inbox",
                               "--domain-prefix", "", "-f"]) == 0
    m = descriptor.read_meta(store.domain_path("inbox"))
    assert m["DATASET"] == "inbox"
    assert m["BASE_PATH"] == "staging/inbox"      # <name> under sys-path
    assert "DOMAIN_PATH" not in m                 # no longer a separate option
    assert "inbox" in bq["datasets"]

    # destroy: the dataset is dropped AND the prefix is cleared
    assert tdx._domain_destroy(["--domain", "inbox"]) == 0
    assert "inbox" not in bq["datasets"]
    assert "staging/inbox" in bq["cleared"]
    assert tdx._sys_destroy(["--sys", "wh"]) == 0


def test_bigquery_domain_needs_no_extra_options(stores, bq):
    # No --dataset / --domain-path: the domain name supplies both, consistent
    # with every other system.
    tdx._sys_create(["--type", "bigquery", "--sys", "wh", *_SYS_CREATE])
    assert tdx._domain_create(["--sys", "wh", "--domain", "sales",
                               "--domain-prefix", "", "-f"]) == 0
    m = descriptor.read_meta(store.domain_path("sales"))
    assert m["DATASET"] == "sales" and m["BASE_PATH"] == "staging/sales"


def test_bigquery_dataset_is_prefixed(stores, bq):
    """The DOMAIN_PREFIX carries into the dataset name and the GCS staging
    prefix; the logical domain name stays bare and the prefixed name is the
    recorded EXTERNAL_NAME."""
    tdx._sys_create(["--type", "bigquery", "--sys", "wh", *_SYS_CREATE])
    assert tdx._domain_create(["--sys", "wh", "--domain", "sales",
                               "--domain-prefix", "ci_1_", "-f"]) == 0
    doc = descriptor.load(store.domain_path("sales"))
    assert doc["name"] == "sales" and doc["external_name"] == "ci_1_sales"
    m = descriptor.read_meta(store.domain_path("sales"))
    assert m["DATASET"] == "ci_1_sales"
    assert m["BASE_PATH"] == "staging/ci_1_sales"
    assert "ci_1_sales" in bq["datasets"]


def test_bigquery_rejects_models_and_data(stores, bq):
    tdx._sys_create(["--type", "bigquery", "--sys", "wh", *_SYS_CREATE])
    with pytest.raises(DatagenError, match="does not support --model"):
        tdx._domain_create(
            ["--sys", "wh", "--domain", "inbox", "--model", "sakila", "-f"])
