#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Any

import pyarrow as pa
import sqlalchemy as sa


def sqlalchemy_to_arrow(sa_type: Any) -> pa.DataType:
    try:
        gt = sa_type.as_generic()
    except (NotImplementedError, AttributeError):
        gt = sa_type

    if isinstance(gt, sa.types.SmallInteger):
        return pa.int16()
    if isinstance(gt, sa.types.BigInteger):
        return pa.int64()
    if isinstance(gt, sa.types.Integer):
        return pa.int32()

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
