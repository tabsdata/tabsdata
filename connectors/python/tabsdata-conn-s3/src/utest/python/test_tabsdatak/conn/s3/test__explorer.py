#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3SrcConn
from tabsdatak.conn.s3 import _explorer

pytestmark = pytest.mark.s3


def test_metadata_reads_object_schemas_through_the_filesystem(tmp_path, monkeypatch):
    (tmp_path / "landing").mkdir()
    (tmp_path / "landing" / "orders_1.csv").write_text("id,total\n1,2.5\n")
    (tmp_path / "landing" / "orders_2.csv").write_text("id,total\n2,3.5\n")
    (tmp_path / "landing" / "orders_3.csv").write_text("id,total\n3,4.5\n")
    listed = [
        {"Key": f"landing/orders_{day}.csv", "Size": 16, "LastModified": datetime(2026, 9, day, tzinfo=timezone.utc)}
        for day in (1, 2, 3)
    ]
    opened = []
    paginator = SimpleNamespace(paginate=lambda **kw: [{"Contents": listed}])
    monkeypatch.setattr(_explorer, "_s3_client", lambda conn: SimpleNamespace(get_paginator=lambda name: paginator))

    def open_input_file(path):
        opened.append(path)
        return open(tmp_path / path.split("/", 1)[1], "rb")

    monkeypatch.setattr(_explorer, "_filesystem", lambda conn: SimpleNamespace(open_input_file=open_input_file))
    conn = S3SrcConn(
        bucket="my-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIA0000000000000000"), secret_access_key=Secret("secret-value")
        ),
    )

    entities = {e.file.pattern: e for e in _explorer.S3ConnectionExplorer().metadata(conn)}

    assert sorted(opened) == ["my-bucket/landing/orders_2.csv", "my-bucket/landing/orders_3.csv"]
    assert all(e.field_names() == ["id", "total"] for e in entities.values())
    assert entities["orders_1.csv"].schema_from == entities["orders_3.csv"].fqn()
