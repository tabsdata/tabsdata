#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
import yaml

from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.server._tdrunner.function._commons.document.connection import (
    ConnectionDocument,
    InvalidConnectionError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_a_connection_document_loads_its_connection(tmp_path):
    path = tmp_path / "connection.yaml"
    path.write_text(yaml.safe_dump(LocalFileSrcConn(base_path=str(tmp_path)).to_dict()))
    connection = ConnectionDocument(path).load()
    assert isinstance(connection, LocalFileSrcConn)
    assert str(connection.base_path) == str(tmp_path)


@pytest.mark.parametrize(
    "text",
    [None, "{ not: yaml", "kind: nothing-registered\n"],
    ids=["missing", "not yaml", "not a connection"],
)
def test_a_connection_that_cannot_be_loaded_is_refused(tmp_path, text):
    path = tmp_path / "connection.yaml"
    if text is not None:
        path.write_text(text)
    with pytest.raises(InvalidConnectionError) as raised:
        ConnectionDocument(path).load()
    assert raised.value.path == path
