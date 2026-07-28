#
# Copyright 2026 Tabsdata Inc.
#

"""The salesforce data plane: model discovery, env routing, and the pure parts
of the loader.

Nothing here talks to an org or runs snowfakery — the loader's org-facing calls
live behind ``simple_salesforce``, which only exists in the tdx-sys-b venv.
"""

from __future__ import annotations

import datetime

import pytest

from tabsdatax.itest.tdksystems2 import _venv, datagen, salesforce_loader, systems, tdx
from tabsdatax.itest.tdksystems2 import constants
from tabsdatax.itest.tdksystems2.errors import TdxError

DOM = constants.DOM_TOOL


@pytest.fixture
def sf():
    return systems.get("salesforce")


def test_the_org_offers_its_own_models(sf):
    assert sf.available_models() == ["salesforce"]
    assert sf.has_model("salesforce")
    # a shared-pipeline model has no recipe here, so the org cannot load it
    assert not sf.has_model("employees")


def test_data_plane_capabilities(sf):
    assert sf.supports_models and sf.supports_data
    # a load is a full wipe + replace, so there is no incremental delta
    assert not sf.supports_changes


def test_model_is_outside_the_shared_pipeline():
    # entities.json identifies the model, so entity names resolve ...
    assert datagen.entities_of("salesforce") == ["Account", "Opportunity"]
    # ... but it ships no datagenerator, so it must not appear as a model any
    # other system could be asked to generate
    assert "salesforce" not in datagen.list_models()
    with pytest.raises(TdxError, match="no datagenerator.py"):
        datagen.load_model("salesforce")


def test_model_declares_the_salesforce_env():
    assert _venv.env_for_model("salesforce") == _venv.ENV_B


@pytest.mark.parametrize(
    "verb,expected",
    [
        (constants.V_MODELS, "systems.salesforce.domain.models"),
        (constants.V_CREATE_MODEL, "systems.salesforce.domain.create_model"),
        (constants.V_CREATE_DATA, "systems.salesforce.domain.create_data"),
    ],
)
def test_the_org_overrides_the_shared_data_plane_verbs(verb, expected):
    assert tdx._command_module(DOM, "salesforce", verb).__name__.endswith(expected)


def test_other_systems_keep_the_shared_verbs_and_ignore_the_model():
    shared = tdx._command_module(DOM, "mysql", constants.V_CREATE_DATA)
    assert shared.__name__.endswith("_common.commands.dom_create_data")
    # redpanda accepts any pipeline model; the org's must not be one of them
    assert not systems.get("redpanda").has_model("salesforce")


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://login.salesforce.com", "login"),
        ("https://test.salesforce.com", "test"),
        ("https://acme.my.salesforce.com", "acme.my"),
        ("https://acme.my.salesforce.com/", "acme.my"),
        ("", "login"),
        ("acme.my.salesforce.com", "acme.my"),
    ],
)
def test_login_url_maps_to_a_simple_salesforce_domain(url, expected):
    assert salesforce_loader._domain_from_url(url) == expected


def test_mapping_parses_into_ordered_steps(sf):
    mapping = salesforce_loader._find_one(
        sf.model_assets("salesforce"), ".mapping.yml"
    )
    steps = salesforce_loader._load_mapping(mapping)
    # Account before Opportunity: the wipe walks this list in reverse, and the
    # upload resolves Opportunity.AccountId against the Accounts loaded first.
    assert [s["object"] for s in steps] == ["Account", "Opportunity"]
    assert steps[1]["lookups"] == {"AccountId": "Account"}
    assert "Name" in steps[0]["fields"]


def test_recipe_and_mapping_ship_with_the_model(sf):
    assets = sf.model_assets("salesforce")
    assert salesforce_loader._find_one(assets, ".recipe.yml").is_file()
    assert (assets / "cumulusci.yml").is_file()
    with pytest.raises(TdxError, match="no '\\*.nope'"):
        salesforce_loader._find_one(assets, ".nope")


def test_dates_are_coerced_for_the_bulk_api():
    assert salesforce_loader._coerce(datetime.date(2026, 7, 28)) == "2026-07-28"
    assert salesforce_loader._coerce("already a string") == "already a string"
    assert salesforce_loader._coerce(7) == 7


def test_a_partial_load_is_an_error():
    ok = [{"success": True}, {"success": True}]
    salesforce_loader._check(ok, "Account")  # does not raise
    with pytest.raises(TdxError, match="1/2 record"):
        salesforce_loader._check([{"success": True}, {"success": False}], "Account")


@pytest.fixture
def org(stores, monkeypatch):
    """A salesforce system + domain in the store, with the org-facing load stubbed
    at the loader boundary (so the real verb path runs, uploading nothing)."""
    for key in ("URL", "USERNAME", "PASSWORD", "TOKEN"):
        monkeypatch.setenv(f"SALESFORCE__{key}", f"the-{key.lower()}")
    loaded: list[tuple] = []

    def _load(model_dir, meta, *, log=print):
        loaded.append((model_dir.name, meta.get("USERNAME")))
        return {"Account": 75, "Opportunity": 180}

    monkeypatch.setattr(salesforce_loader, "load", _load)
    assert tdx._dispatch(constants.SYS_TOOL, ["create", "--type", "salesforce",
                                              "--sys", "org1"]) == 0
    assert tdx._dispatch(DOM, ["create", "--sys", "org1", "--domain", "d1"]) == 0
    return loaded


def _dom(argv):
    return tdx._dispatch(DOM, argv)


def test_create_model_records_without_installing(org):
    from tabsdatax.itest.tdksystems2 import descriptor, store

    assert _dom(["create-model", "--domain", "d1", "--model", "salesforce"]) == 0
    info = descriptor.load(store.domain_path("d1"))
    assert info.model == "salesforce"
    assert list(info.entities) == ["Account", "Opportunity"]
    assert org == []  # recording a model uploads nothing


def test_create_data_loads_the_org_and_records_the_counts(org):
    from tabsdatax.itest.tdksystems2 import descriptor, store

    assert _dom(["create-model", "--domain", "d1", "--model", "salesforce"]) == 0
    assert _dom(["create-data", "--domain", "d1"]) == 0
    info = descriptor.load(store.domain_path("d1"))
    assert info.data == "1"
    assert info.entities == {"Account": 75, "Opportunity": 180}
    # the loader got the org's own assets and the domain's credentials
    assert org == [("salesforce", "the-username")]


def test_create_data_needs_a_model_first(org):
    with pytest.raises(TdxError, match="no model"):
        _dom(["create-data", "--domain", "d1"])


def test_reloading_an_org_needs_force(org):
    assert _dom(["create-model", "--domain", "d1", "--model", "salesforce"]) == 0
    assert _dom(["create-data", "--domain", "d1"]) == 0
    with pytest.raises(TdxError, match="already has data"):
        _dom(["create-data", "--domain", "d1"])
    assert _dom(["create-data", "--domain", "d1", "-f"]) == 0
    assert len(org) == 2


def test_size_is_accepted_but_fixed_by_the_recipe(org):
    assert _dom(["create-model", "--domain", "d1", "--model", "salesforce"]) == 0
    # a manifest replay always passes --size, so 1 must work ...
    assert _dom(["create-data", "--domain", "d1", "--size", "1"]) == 0
    # ... and a multiplier is refused rather than silently ignored
    with pytest.raises(TdxError, match="fixed by model"):
        _dom(["create-data", "--domain", "d1", "-f", "--size", "5"])


def test_an_unknown_model_names_what_the_org_has(org):
    with pytest.raises(TdxError, match="available: salesforce"):
        _dom(["create-model", "--domain", "d1", "--model", "employees"])
