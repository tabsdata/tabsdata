"""``SchemaRegistry`` -- registration from change-stream log events
vs from the catalog snapshot at startup."""

from __future__ import annotations

import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.common.sql.cdc._schema import INITIAL_SCHEMA_VERSION, SchemaRegistry
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


def _registry() -> SchemaRegistry:
    # Trivial mapper: pretend any raw type already is a SQLAlchemy
    # type. The mapper just decorates with sa.types.String() when
    # the raw type is the string "VARCHAR", else sa.types.Integer().
    def mapper(raw):
        if raw == "VARCHAR":
            return sa.types.String()
        return sa.types.Integer()

    return SchemaRegistry(type_mapper=mapper, cdc_format=CdcFormat())


# --- register_from_log ---------------------------------------------


def test_log_registration_increments_version():
    reg = _registry()
    raw_v0 = (("id", "INT", "INT"),)
    raw_v1 = (("id", "INT", "INT"), ("name", "VARCHAR", "VARCHAR"))

    v0 = reg.register_from_log("s.t", raw_v0)
    v1 = reg.register_from_log("s.t", raw_v1)
    assert v0.version == INITIAL_SCHEMA_VERSION
    assert v1.version == 1
    assert v0.hash != v1.hash


def test_log_registration_dedupes_identical_raw_schemas():
    reg = _registry()
    raw = (("id", "INT", "INT"),)
    a = reg.register_from_log("s.t", raw)
    b = reg.register_from_log("s.t", raw)
    assert a is b


def test_log_registration_separates_tables():
    reg = _registry()
    raw = (("id", "INT", "INT"),)
    a = reg.register_from_log("s.a", raw)
    b = reg.register_from_log("s.b", raw)
    assert a is not b
    # Each table starts at version 0.
    assert a.version == b.version == 0


# --- register_from_catalog -----------------------------------------


def test_catalog_registration_uses_existing_tracked_columns():
    reg = _registry()
    columns = [
        TrackedColumn(name="id", satype=sa.types.Integer(), patype="<pa-int>"),
        TrackedColumn(name="name", satype=sa.types.String(), patype="<pa-str>"),
    ]
    tt = TrackedTable(
        data_table=TableObject(schema="s", table="t"),
        cdc_table=TableObject(schema="s", table="t"),
        columns=columns,
    )
    sv = reg.register_from_catalog("s.t", tt)
    assert sv.version == INITIAL_SCHEMA_VERSION
    assert len(sv.columns) == 2
    # The registered columns are *copied*, not the same identities.
    assert sv.columns[0].name == "id"
    assert sv.columns[1].name == "name"


def test_catalog_registration_resets_history_for_table():
    reg = _registry()
    # Seed a log-side version, then a catalog registration replaces
    # the table's history with version 0.
    reg.register_from_log("s.t", (("id", "INT", "INT"),))
    columns = [TrackedColumn(name="id", satype=sa.types.Integer())]
    tt = TrackedTable(
        data_table=TableObject(schema="s", table="t"),
        cdc_table=TableObject(schema="s", table="t"),
        columns=columns,
    )
    sv = reg.register_from_catalog("s.t", tt)
    assert sv.version == 0
