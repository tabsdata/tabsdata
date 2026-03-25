#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import sqlalchemy.types as sa_types

from tabsdata.connector.sql.common._database.db2.dialect import Db2Dialect

dialect = Db2Dialect()


class TestIntegerTypes:
    def test_integer(self):
        assert isinstance(dialect.ddl_to_sa("INTEGER"), sa_types.Integer)

    def test_bigint(self):
        assert isinstance(dialect.ddl_to_sa("BIGINT"), sa_types.BigInteger)

    def test_smallint(self):
        assert isinstance(dialect.ddl_to_sa("SMALLINT"), sa_types.SmallInteger)


class TestNumericTypes:
    def test_decimal_with_precision(self):
        result = dialect.ddl_to_sa("DECIMAL(10,2)")
        assert isinstance(result, sa_types.Numeric)
        assert result.precision == 10
        assert result.scale == 2

    def test_numeric_with_precision(self):
        result = dialect.ddl_to_sa("NUMERIC(8,3)")
        assert isinstance(result, sa_types.Numeric)
        assert result.precision == 8
        assert result.scale == 3

    def test_double(self):
        assert isinstance(dialect.ddl_to_sa("DOUBLE"), sa_types.Float)

    def test_real(self):
        assert isinstance(dialect.ddl_to_sa("REAL"), sa_types.Float)

    def test_float(self):
        assert isinstance(dialect.ddl_to_sa("FLOAT"), sa_types.Float)


class TestStringTypes:
    def test_varchar_with_length(self):
        result = dialect.ddl_to_sa("VARCHAR(255)")
        assert isinstance(result, sa_types.String)
        assert result.length == 255

    def test_char_with_length(self):
        result = dialect.ddl_to_sa("CHAR(10)")
        assert isinstance(result, sa_types.String)
        assert result.length == 10

    def test_clob(self):
        assert isinstance(dialect.ddl_to_sa("CLOB"), sa_types.Text)


class TestTemporalTypes:
    def test_date(self):
        assert isinstance(dialect.ddl_to_sa("DATE"), sa_types.Date)

    def test_timestamp(self):
        assert isinstance(dialect.ddl_to_sa("TIMESTAMP"), sa_types.DateTime)

    def test_time(self):
        assert isinstance(dialect.ddl_to_sa("TIME"), sa_types.Time)


class TestBinaryTypes:
    def test_blob(self):
        assert isinstance(dialect.ddl_to_sa("BLOB"), sa_types.LargeBinary)


class TestOtherTypes:
    def test_boolean(self):
        assert isinstance(dialect.ddl_to_sa("BOOLEAN"), sa_types.Boolean)


class TestCdcToSaUnsupported:
    def test_binlog_raises(self):
        with pytest.raises(NotImplementedError, match="binlog"):
            dialect.cdc_to_sa("binlog", None)

    def test_wal2json_raises(self):
        with pytest.raises(NotImplementedError, match="wal2json"):
            dialect.cdc_to_sa("wal2json", "integer")
