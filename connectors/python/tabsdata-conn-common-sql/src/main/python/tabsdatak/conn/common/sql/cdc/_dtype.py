#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Any

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# Types whose meaning differs from the generic type they reflect as. A MySQL
# TIME is an interval of up to 838 hours either way, read as a timedelta by
# both its driver and its binlog reader, so it is a duration as batch reads
# publish it, not a time of day. A MySQL FLOAT is single precision, but
# neither its binlog description (a byte length) nor its catalog one (a
# precision only when declared) tells that apart reliably, so it is a double,
# as batch reads publish it.
DIALECT_TYPES: dict[type, pa.DataType] = {
    mysql.TIME: pa.duration("us"),
    mysql.FLOAT: pa.float64(),
}


def sqlalchemy_to_arrow(sa_type: Any) -> pa.DataType:
    dialect_type = DIALECT_TYPES.get(type(sa_type))
    if dialect_type is not None:
        return dialect_type
    try:
        gt = sa_type.as_generic()
    except (NotImplementedError, AttributeError):
        gt = sa_type

    # An unsigned integer (MySQL) holds twice the signed range, so it takes
    # the next wider type, or the unsigned one past 64 bits.
    unsigned = bool(getattr(sa_type, "unsigned", False))
    if isinstance(gt, sa.types.SmallInteger):
        return pa.int32() if unsigned else pa.int16()
    if isinstance(gt, sa.types.BigInteger):
        return pa.uint64() if unsigned else pa.int64()
    if isinstance(gt, sa.types.Integer):
        return pa.int64() if unsigned else pa.int32()

    if isinstance(gt, sa.types.Double):
        return pa.float64()
    if isinstance(gt, sa.types.Float):
        p = getattr(sa_type, "precision", None)
        if p is not None and p <= 24:
            return pa.float32()
        return pa.float64()

    if getattr(sa_type, "__visit_name__", None) == "DOUBLE":
        return pa.float64()

    if isinstance(gt, sa.types.Numeric):
        p = getattr(sa_type, "precision", None)
        s = getattr(sa_type, "scale", None)
        if p is not None and s is not None:
            return pa.decimal128(p, s)
        return pa.utf8()

    if isinstance(gt, sa.types.Boolean):
        return pa.bool_()

    if isinstance(gt, sa.types.DateTime):
        tz = getattr(sa_type, "timezone", False)
        return pa.timestamp("us", tz="UTC") if tz else pa.timestamp("us")

    if isinstance(gt, sa.types.Date):
        return pa.date32()

    if isinstance(gt, sa.types.Time):
        return pa.time64("us")

    if isinstance(gt, sa.types.LargeBinary):
        return pa.binary()

    _binary = getattr(sa.types, "_Binary", None)
    if _binary is not None and isinstance(gt, _binary):
        return pa.binary()

    if isinstance(gt, sa.types.JSON):
        return pa.utf8()

    if isinstance(gt, sa.types.Uuid):
        return pa.utf8()

    if isinstance(gt, sa.types.String):
        return pa.utf8()

    return pa.utf8()
