"""Tests for the automated salesforce loader's pure logic — URL→domain
mapping, value coercion, and parsing the cci load mapping. The org-facing
parts (snowfakery generation, simple-salesforce login/bulk) need the
salesforce venv + a live org and are exercised via integration, not here."""
from __future__ import annotations

import datetime

import pytest

from tabsdatax.itest.tdksystems import data_gen, salesforce_loader as sfl


def test_domain_from_url():
    assert sfl._domain_from_url("https://login.salesforce.com") == "login"
    assert sfl._domain_from_url("https://test.salesforce.com/") == "test"
    assert sfl._domain_from_url("https://acme.my.salesforce.com") == "acme.my"
    assert sfl._domain_from_url("login.salesforce.com") == "login"


def test_coerce():
    assert sfl._coerce(datetime.date(2025, 1, 2)) == "2025-01-02"
    assert sfl._coerce("text") == "text"
    assert sfl._coerce(42) == 42


def test_load_mapping_matches_recipe():
    pytest.importorskip("yaml")
    mapping = data_gen.model_dir("salesforce") / "accounts_opps.mapping.yml"
    steps = sfl._load_mapping(mapping)
    # Account is inserted before Opportunity, which looks up AccountId->Account.
    assert [s["object"] for s in steps] == ["Account", "Opportunity"]
    assert steps[1]["lookups"] == {"AccountId": "Account"}
    assert "Name" in steps[0]["fields"]
    assert steps[0]["table"] == "Account"
