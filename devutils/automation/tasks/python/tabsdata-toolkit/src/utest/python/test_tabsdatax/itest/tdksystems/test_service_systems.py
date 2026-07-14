#
# Copyright 2026 Tabsdata Inc.
#

"""Credential-only service systems (anthropic / openai): externally-owned keyed
API accounts with nothing to provision. Exercised through the real tdx verbs —
no network. `create` bakes the API key resolved from `$<SLUG>__API_KEY`; a
domain is a credentials clone (no entity container); neither takes models."""
from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems import descriptor, store, systems, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.systems._service import ServiceBase
from tabsdatax.itest.tdksystems.systems.base import SystemContext

SERVICE_SLUGS = ("anthropic", "openai")

_FAKE_KEY = {"anthropic": "sk-ant-fake", "openai": "sk-oai-fake"}


@pytest.fixture
def service_creds(monkeypatch):
    for slug, key in _FAKE_KEY.items():
        monkeypatch.setenv(f"{slug.upper()}__API_KEY", key)


# ---- class metadata -------------------------------------------------------

@pytest.mark.parametrize("slug", SERVICE_SLUGS)
def test_service_class_metadata(slug):
    obj = systems.get(slug)
    assert isinstance(obj, ServiceBase)
    assert obj.slug == slug
    assert obj.category == "service"
    assert obj.supports_models is False
    assert obj.supports_sql is False
    assert obj.domain_is_named is False
    assert obj.domain_flag == ""
    # One secret credential, resolved from $<SLUG>__API_KEY.
    assert [o.name for o in obj.options] == ["API_KEY"]
    assert obj.options[0].secret is True
    assert obj.default_env_prefix == slug.upper()


# ---- system lifecycle -----------------------------------------------------

@pytest.mark.parametrize("slug", SERVICE_SLUGS)
def test_create_bakes_api_key_and_info_exposes_it(stores, service_creds, slug):
    assert tdx._sys_create(["--type", slug, "--sys", slug, "-f"]) == 0
    doc = descriptor.load(store.system_path(slug))
    assert doc["category"] == "service"
    assert doc["kind"] == "service-system"
    assert doc["role"] == "sys"
    obj = json.loads(descriptor.info(doc, "json"))
    assert obj["NAME"] == slug
    assert obj["TYPE"] == slug
    assert obj["SERVICE_TYPE"] == slug
    assert obj["API_KEY"] == _FAKE_KEY[slug]


def test_create_fails_when_api_key_env_unset(stores, monkeypatch):
    monkeypatch.delenv("ANTHROPIC__API_KEY", raising=False)
    with pytest.raises(DatagenError, match="ANTHROPIC__API_KEY"):
        tdx._sys_create(["--type", "anthropic", "--sys", "anthropic", "-f"])


def test_status_reports_credentials_present(stores, service_creds):
    tdx._sys_create(["--type", "openai", "--sys", "openai", "-f"])
    assert tdx._sys_status(["--sys", "openai"]) == 0


def test_status_flags_missing_credentials():
    """A descriptor whose baked API key is empty reports unhealthy — offline."""
    obj = systems.get("anthropic")
    health = obj.system_status(
        SystemContext("anthropic", "anthropic", {"API_KEY": ""})
    )
    assert health.ok is False
    assert "API_KEY" in health.detail


# ---- domain lifecycle (credentials clone, no entity, no models) -----------

def test_domain_create_is_a_credentials_clone(stores, service_creds):
    tdx._sys_create(["--type", "anthropic", "--sys", "anthropic", "-f"])
    assert tdx._domain_create(["--sys", "anthropic", "-f"]) == 0
    doc = descriptor.load(store.domain_path("anthropic"))
    assert doc["category"] == "service"
    assert doc["kind"] == "service-domain"
    assert doc["role"] == "env"
    info = json.loads(descriptor.info(doc, "json"))
    assert info["API_KEY"] == _FAKE_KEY["anthropic"]
    assert "SCHEMA" not in info
    assert tdx._domain_status(["--domain", "anthropic"]) == 0


def test_domain_rejects_model_and_data(stores, service_creds):
    tdx._sys_create(["--type", "openai", "--sys", "openai", "-f"])
    with pytest.raises(DatagenError, match="does not support --model"):
        tdx._domain_create(["--sys", "openai", "--model", "world", "-f"])


def test_destroy_is_a_noop_teardown(stores, service_creds):
    tdx._sys_create(["--type", "openai", "--sys", "openai", "-f"])
    tdx._domain_create(["--sys", "openai", "-f"])
    tdx._domain_destroy(["--domain", "openai"])
    tdx._sys_destroy(["--sys", "openai"])
    assert not store.domain_path("openai").exists()
    assert not store.system_path("openai").exists()


# ---- manifest: the API key stays a reference, never resolved --------------

def test_manifest_dump_keeps_api_key_as_env_reference(stores, service_creds, capsys):
    tdx._sys_create(["--type", "anthropic", "--sys", "anthropic", "-f"])
    capsys.readouterr()
    assert tdx._sys_dump(["--sys-manifest", "-"]) == 0
    import yaml
    entries = yaml.safe_load(capsys.readouterr().out)
    entry = next(e for e in entries if e["name"] == "anthropic")
    assert entry["type"] == "anthropic"
    assert entry["api-key"] == {"env": "ANTHROPIC__API_KEY"}
