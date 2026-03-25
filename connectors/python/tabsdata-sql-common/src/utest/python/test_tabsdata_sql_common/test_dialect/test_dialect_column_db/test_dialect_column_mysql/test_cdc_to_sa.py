#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import pymysqlreplication.constants.FIELD_TYPE as FIELD_TYPE
import sqlalchemy.types as sa_types
from sqlalchemy.dialects.mysql import DATETIME, ENUM, SET, TIME, TIMESTAMP

from tabsdata.connector.sql.common._database.mysql.dialect import MySQLDialect

dialect = MySQLDialect()


class MockColumn:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class TestBinlogIntegerTypes:
    def test_tiny(self):
        col = MockColumn(type=FIELD_TYPE.TINY)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.TypeEngine)

    def test_short(self):
        col = MockColumn(type=FIELD_TYPE.SHORT)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.SmallInteger)

    def test_long(self):
        col = MockColumn(type=FIELD_TYPE.LONG)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.Integer)

    def test_longlong(self):
        col = MockColumn(type=FIELD_TYPE.LONGLONG)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.BigInteger)

    def test_int24(self):
        col = MockColumn(type=FIELD_TYPE.INT24)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.TypeEngine)


class TestBinlogNumericTypes:
    def test_newdecimal(self):
        col = MockColumn(type=FIELD_TYPE.NEWDECIMAL, precision=10, decimals=2)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.Numeric)
        assert result.precision == 10
        assert result.scale == 2

    def test_newdecimal_no_params(self):
        col = MockColumn(type=FIELD_TYPE.NEWDECIMAL)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.Numeric)

    def test_float(self):
        col = MockColumn(type=FIELD_TYPE.FLOAT)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.Float)

    def test_float_with_size(self):
        col = MockColumn(type=FIELD_TYPE.FLOAT, size=8)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.Float)
        assert result.precision == 8

    def test_double(self):
        col = MockColumn(type=FIELD_TYPE.DOUBLE)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.TypeEngine)


class TestBinlogStringTypes:
    def test_varchar(self):
        col = MockColumn(type=FIELD_TYPE.VARCHAR, max_length=255)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.String)
        assert result.length == 255

    def test_varchar_no_length(self):
        col = MockColumn(type=FIELD_TYPE.VARCHAR)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.String)

    def test_var_string(self):
        col = MockColumn(type=FIELD_TYPE.VAR_STRING, max_length=100)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.String)
        assert result.length == 100

    def test_string(self):
        col = MockColumn(type=FIELD_TYPE.STRING, max_length=50)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, sa_types.String)
        assert result.length == 50


class TestBinlogTemporalTypes:
    def test_datetime(self):
        col = MockColumn(type=FIELD_TYPE.DATETIME)
        assert isinstance(dialect.cdc_to_sa("binlog", col), DATETIME)

    def test_datetime_with_fsp(self):
        col = MockColumn(type=FIELD_TYPE.DATETIME, fsp=3)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, DATETIME)
        assert result.fsp == 3

    def test_timestamp(self):
        col = MockColumn(type=FIELD_TYPE.TIMESTAMP)
        assert isinstance(dialect.cdc_to_sa("binlog", col), TIMESTAMP)

    def test_timestamp_with_fsp(self):
        col = MockColumn(type=FIELD_TYPE.TIMESTAMP, fsp=6)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, TIMESTAMP)
        assert result.fsp == 6

    def test_time(self):
        col = MockColumn(type=FIELD_TYPE.TIME)
        assert isinstance(dialect.cdc_to_sa("binlog", col), TIME)

    def test_time_with_fsp(self):
        col = MockColumn(type=FIELD_TYPE.TIME, fsp=2)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, TIME)
        assert result.fsp == 2

    def test_date(self):
        col = MockColumn(type=FIELD_TYPE.DATE)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.Date)

    def test_year(self):
        col = MockColumn(type=FIELD_TYPE.YEAR)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.TypeEngine)


class TestBinlogBinaryTypes:
    def test_blob(self):
        col = MockColumn(type=FIELD_TYPE.BLOB)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.LargeBinary)

    def test_tiny_blob(self):
        col = MockColumn(type=FIELD_TYPE.TINY_BLOB)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.TypeEngine)

    def test_medium_blob(self):
        col = MockColumn(type=FIELD_TYPE.MEDIUM_BLOB)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.TypeEngine)

    def test_long_blob(self):
        col = MockColumn(type=FIELD_TYPE.LONG_BLOB)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.TypeEngine)

    def test_geometry(self):
        col = MockColumn(type=FIELD_TYPE.GEOMETRY)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.LargeBinary)


class TestBinlogOtherTypes:
    def test_json(self):
        col = MockColumn(type=FIELD_TYPE.JSON)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.JSON)

    def test_enum(self):
        col = MockColumn(type=FIELD_TYPE.ENUM, enum_values=["a", "b", "c"])
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, ENUM)

    def test_enum_no_values(self):
        col = MockColumn(type=FIELD_TYPE.ENUM)
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, ENUM)

    def test_set(self):
        col = MockColumn(type=FIELD_TYPE.SET, set_values=["x", "y"])
        result = dialect.cdc_to_sa("binlog", col)
        assert isinstance(result, SET)

    def test_bit(self):
        col = MockColumn(type=FIELD_TYPE.BIT)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.TypeEngine)

    def test_null_type(self):
        col = MockColumn(type=FIELD_TYPE.NULL)
        assert isinstance(dialect.cdc_to_sa("binlog", col), sa_types.NullType)


class TestBinlogUnknownType:
    def test_unknown_field_type_raises(self):
        col = MockColumn(type=9999)
        with pytest.raises(ValueError, match="Unsupported"):
            dialect.cdc_to_sa("binlog", col)


class TestUnsupportedIngester:
    def test_wal2json_raises(self):
        with pytest.raises(NotImplementedError, match="wal2json"):
            dialect.cdc_to_sa("wal2json", "integer")

    def test_sqlreplica_raises(self):
        with pytest.raises(NotImplementedError, match="sqlreplica"):
            dialect.cdc_to_sa("sqlreplica", "INTEGER")
