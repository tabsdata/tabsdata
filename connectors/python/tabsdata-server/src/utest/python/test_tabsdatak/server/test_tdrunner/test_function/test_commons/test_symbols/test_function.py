#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_SUBSCRIBER_DEF,
    PublisherInfo,
    SubscriberInfo,
)
from tabsdatak.common._archive.bundle import (
    BUNDLE_MANIFEST_FILENAME,
    BUNDLE_USER_CODE_DIRNAME,
)
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    FunctionBundle,
    FunctionDefinitionError,
    InvalidDefinitionError,
    UnexpectedKindError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

SUBSCRIBERS = {KIND_SUBSCRIBER_DEF: SubscriberInfo}


def write_definition(bundle_folder: Path, text: str) -> Path:
    bundle_folder.mkdir(parents=True, exist_ok=True)
    manifest = bundle_folder / BUNDLE_MANIFEST_FILENAME
    manifest.write_text(text)
    return manifest


def test_a_missing_definition_is_a_definition_error(tmp_path):
    with pytest.raises(FunctionDefinitionError) as raised:
        FunctionBundle(tmp_path).load(SUBSCRIBERS)
    assert type(raised.value) is FunctionDefinitionError
    assert raised.value.manifest == tmp_path / BUNDLE_MANIFEST_FILENAME


def test_a_malformed_definition_is_a_definition_error(tmp_path):
    write_definition(tmp_path, "{ not json")
    with pytest.raises(FunctionDefinitionError) as raised:
        FunctionBundle(tmp_path).load(SUBSCRIBERS)
    assert type(raised.value) is FunctionDefinitionError


def test_a_kind_outside_the_given_ones_is_unexpected(tmp_path):
    manifest = write_definition(tmp_path, json.dumps({"kind": KIND_PUBLISHER_DEF}))
    with pytest.raises(UnexpectedKindError) as raised:
        FunctionBundle(tmp_path).load(SUBSCRIBERS)
    assert raised.value.kind == KIND_PUBLISHER_DEF
    assert raised.value.allowed == [KIND_SUBSCRIBER_DEF]
    assert raised.value.manifest == manifest


def test_a_definition_without_kind_is_unexpected(tmp_path):
    write_definition(tmp_path, json.dumps([]))
    with pytest.raises(UnexpectedKindError) as raised:
        FunctionBundle(tmp_path).load(SUBSCRIBERS)
    assert raised.value.kind is None


def test_invalid_content_of_a_known_kind_is_an_invalid_definition(tmp_path):
    write_definition(
        tmp_path,
        json.dumps({"kind": KIND_PUBLISHER_DEF, "spec": {"func": 42}}),
    )
    with pytest.raises(InvalidDefinitionError):
        FunctionBundle(tmp_path).load({KIND_PUBLISHER_DEF: PublisherInfo})


def test_the_user_code_is_importable_before_the_definition_is_read(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        sys,
        "path",
        list(sys.path),
    )
    write_definition(tmp_path, json.dumps({"kind": KIND_PUBLISHER_DEF}))
    with pytest.raises(UnexpectedKindError):
        FunctionBundle(tmp_path).load(SUBSCRIBERS)
    assert str((tmp_path / BUNDLE_USER_CODE_DIRNAME).resolve()) in sys.path
