#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSSrcConn
from tabsdatak.conn.gcs import _explorer

pytestmark = pytest.mark.gcs


def test_metadata_reads_object_schemas_through_the_blob_reader(tmp_path, monkeypatch):
    (tmp_path / "e.ndjson").write_text('{"id": 1, "kind": "open"}\n')
    blobs = [SimpleNamespace(name="e.ndjson", size=24, updated=datetime(2026, 9, 1, tzinfo=timezone.utc))]
    opened = []

    def blob(key):
        def open_(mode, chunk_size=None):
            opened.append(key)
            return open(tmp_path / key, "rb")

        return SimpleNamespace(open=open_)

    client = SimpleNamespace(
        list_blobs=lambda bucket, prefix=None: blobs,
        bucket=lambda name: SimpleNamespace(blob=blob),
    )
    monkeypatch.setattr(_explorer, "_storage_client", lambda conn: client)
    conn = GCSSrcConn(bucket="my-bucket", credentials=GCPJson(json=Secret('{"type":"service_account"}')))

    [entity] = _explorer.GCSConnectionExplorer().metadata(conn)

    assert opened == ["e.ndjson"]
    assert [(c.name, c.type) for c in entity.columns] == [("id", "int"), ("kind", "string")]
