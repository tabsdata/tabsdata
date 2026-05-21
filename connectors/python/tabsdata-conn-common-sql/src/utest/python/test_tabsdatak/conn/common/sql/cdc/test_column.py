from __future__ import annotations

import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn


def test_tracked_column_minimum_init():
    col = TrackedColumn(name="id", satype=sa.types.Integer())
    assert col.name == "id"
    assert isinstance(col.satype, sa.types.Integer)
    # patype / oname / sa_name / sa_oname default to None until the
    # ingester fills them in.
    assert col.patype is None
    assert col.oname is None
    assert col.sa_name is None
    assert col.sa_oname is None


def test_tracked_column_full_init():
    col = TrackedColumn(
        name="USER_NAME",
        satype=sa.types.String(),
        patype="utf8-marker",
        oname="OLD_USER_NAME",
        sa_name="user_name",
        sa_oname="old_user_name",
    )
    assert col.oname == "OLD_USER_NAME"
    assert col.sa_oname == "old_user_name"
