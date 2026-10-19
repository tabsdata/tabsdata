#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.sdk._project_secret import from_read, to_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_secrets_become_the_api_payload():
    assert to_payload({"aws": {"key1": "mykey1", "key2": None, "key3": ""}}) == [
        {
            "name": "aws",
            "entries": [
                {"key": "key1", "value": "mykey1"},
                {"key": "key2", "value": None},
                {"key": "key3", "value": ""},
            ],
        }
    ]


def test_no_secrets_become_an_empty_payload():
    assert to_payload({}) == []


def test_the_api_read_becomes_names_with_their_keys():
    read = {"secrets": [{"name": "aws", "keys": ["key1", "key2"]}]}
    assert from_read(read) == {"aws": ["key1", "key2"]}
    assert from_read({"secrets": []}) == {}
