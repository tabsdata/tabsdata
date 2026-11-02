#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import urllib.parse

import pytest

from tabsdatak.server._tdrunner.probe._handlers.connection.secrets import MAX_ERROR_BYTES, scrub

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_scrub_replaces_every_secret_value_and_caps_the_text():
    environ = {
        "SECRET_CONN__USER": "bobby",
        "SECRET_CONN__TOKEN": "bobbytoken",
        "HOME": "x",
    }
    assert scrub("bobby used bobbytoken", environ) == "*** used ***"
    assert scrub("x" * (MAX_ERROR_BYTES * 2), environ) == "x" * MAX_ERROR_BYTES


def test_scrub_catches_url_encoded_and_json_escaped_secrets():
    secret = 'p@ss w/"q"&=\\'
    environ = {"SECRET_CONN__PASSWORD": secret}
    for form in [
        urllib.parse.quote(secret, safe=""),
        urllib.parse.quote_plus(secret),
        json.dumps(secret)[1:-1],
    ]:
        assert form != secret
        scrubbed = scrub(f"failed with {form} at host", environ)
        assert scrubbed == "failed with *** at host", form


def test_scrub_leaves_values_too_short_to_be_secrets():
    environ = {"SECRET_CONN__PORT": "543", "SECRET_CONN__DB": "a"}
    assert scrub("a table on port 543", environ) == "a table on port 543"


def test_scrub_never_splits_a_multibyte_character():
    capped = scrub("é" * MAX_ERROR_BYTES, {})
    assert len(capped.encode("utf-8")) <= MAX_ERROR_BYTES
    assert set(capped) == {"é"}
