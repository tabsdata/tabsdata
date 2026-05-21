"""SQLAlchemy → PyArrow type-map happy paths. We don't aim to
exhaustively cover every SQLAlchemy type -- just the buckets the
mapper distinguishes, plus the utf8 fallback."""

from __future__ import annotations

import pyarrow as pa
import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow


def test_integer_buckets():
    assert sqlalchemy_to_arrow(sa.types.SmallInteger()) == pa.int16()
    assert sqlalchemy_to_arrow(sa.types.Integer()) == pa.int32()
    assert sqlalchemy_to_arrow(sa.types.BigInteger()) == pa.int64()


def test_float_precision_picks_float32_or_float64():
    assert sqlalchemy_to_arrow(sa.types.Float(precision=12)) == pa.float32()
    assert sqlalchemy_to_arrow(sa.types.Float(precision=53)) == pa.float64()
    # No precision -> float64.
    assert sqlalchemy_to_arrow(sa.types.Float()) == pa.float64()


def test_double_maps_to_float64():
    assert sqlalchemy_to_arrow(sa.types.Double()) == pa.float64()


def test_numeric_with_precision_and_scale_is_decimal128():
    assert sqlalchemy_to_arrow(sa.types.Numeric(precision=10, scale=2)) == pa.decimal128(10, 2)


def test_numeric_without_precision_falls_back_to_utf8():
    assert sqlalchemy_to_arrow(sa.types.Numeric()) == pa.utf8()


def test_boolean_and_string_and_date_time():
    assert sqlalchemy_to_arrow(sa.types.Boolean()) == pa.bool_()
    assert sqlalchemy_to_arrow(sa.types.String()) == pa.utf8()
    assert sqlalchemy_to_arrow(sa.types.Date()) == pa.date32()
    assert sqlalchemy_to_arrow(sa.types.Time()) == pa.time64("us")


def test_datetime_tz_awareness():
    naive = sqlalchemy_to_arrow(sa.types.DateTime())
    aware = sqlalchemy_to_arrow(sa.types.DateTime(timezone=True))
    assert naive == pa.timestamp("us")
    assert aware == pa.timestamp("us", tz="UTC")


def test_binary_and_largebinary():
    assert sqlalchemy_to_arrow(sa.types.LargeBinary()) == pa.binary()


def test_json_and_uuid_fallback_to_utf8():
    assert sqlalchemy_to_arrow(sa.types.JSON()) == pa.utf8()
    assert sqlalchemy_to_arrow(sa.types.Uuid()) == pa.utf8()


def test_unknown_type_falls_back_to_utf8():
    class WeirdType:
        pass
    assert sqlalchemy_to_arrow(WeirdType()) == pa.utf8()
