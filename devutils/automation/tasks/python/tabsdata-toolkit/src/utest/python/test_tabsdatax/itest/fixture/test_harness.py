#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tabsdatax.itest.fixture.functions import harness


class FakeCollection:
    def __init__(self, connection, name, group, connection_document, secrets):
        self.name = name

    def create(self) -> None:
        pass


def test_project_collections_provisions_nothing_unused(monkeypatch):
    monkeypatch.setattr(harness, "Collection", FakeCollection)
    server = MagicMock()
    with harness.project_collections(server):
        pass
    server.create_project.assert_not_called()
    server.delete_collection.assert_not_called()
    server.delete_project.assert_not_called()


def test_project_collections_deletes_what_it_created_when_the_body_fails(
    monkeypatch,
):
    monkeypatch.setattr(harness, "Collection", FakeCollection)
    server = MagicMock()
    with pytest.raises(RuntimeError):
        with harness.project_collections(server) as make:
            make("default", name="first")
            make("default", name="second")
            raise RuntimeError("the body failed")
    deleted = [call.args[0] for call in server.delete_collection.call_args_list]
    assert deleted == ["second", "first"]
    project = server.create_project.call_args.args[0]
    server.delete_project.assert_called_once_with(project, raise_for_status=False)
