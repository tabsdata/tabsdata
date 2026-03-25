#
# Copyright 2026 Tabs Data Inc.
#

import sqlalchemy.types as sa_types

from tabsdata.connector.sql.common._database.mysql.dialect import MySQLDialect

dialect = MySQLDialect()


class TestIntegerTypes:
    def test_int(self):
        assert isinstance(dialect.ddl_to_sa("int"), sa_types.Integer)

    def test_bigint(self):
        assert isinstance(dialect.ddl_to_sa("bigint"), sa_types.BigInteger)

    def test_smallint(self):
        assert isinstance(dialect.ddl_to_sa("smallint"), sa_types.SmallInteger)

    def test_tinyint(self):
        assert isinstance(dialect.ddl_to_sa("tinyint"), sa_types.SmallInteger)

    def test_mediumint(self):
        assert isinstance(dialect.ddl_to_sa("mediumint"), sa_types.Integer)

    def test_int_unsigned(self):
        assert isinstance(dialect.ddl_to_sa("int unsigned"), sa_types.Integer)


class TestNumericTypes:
    def test_decimal_with_precision(self):
        result = dialect.ddl_to_sa("decimal(10,2)")
        assert isinstance(result, sa_types.Numeric)
        assert result.precision == 10
        assert result.scale == 2

    def test_float(self):
        assert isinstance(dialect.ddl_to_sa("float"), sa_types.Float)

    def test_double(self):
        assert isinstance(dialect.ddl_to_sa("double"), sa_types.Float)


class TestStringTypes:
    def test_varchar_with_length(self):
        result = dialect.ddl_to_sa("varchar(255)")
        assert isinstance(result, sa_types.String)
        assert result.length == 255

    def test_char_with_length(self):
        result = dialect.ddl_to_sa("char(10)")
        assert isinstance(result, sa_types.String)
        assert result.length == 10

    def test_text(self):
        assert isinstance(dialect.ddl_to_sa("text"), sa_types.Text)


class TestTemporalTypes:
    def test_date(self):
        assert isinstance(dialect.ddl_to_sa("date"), sa_types.Date)

    def test_datetime(self):
        assert isinstance(dialect.ddl_to_sa("datetime"), sa_types.DateTime)

    def test_datetime_with_fsp(self):
        assert isinstance(dialect.ddl_to_sa("datetime(3)"), sa_types.DateTime)

    def test_timestamp(self):
        result = dialect.ddl_to_sa("timestamp")
        assert isinstance(result, sa_types.DateTime)

    def test_time(self):
        assert isinstance(dialect.ddl_to_sa("time"), sa_types.Time)

    def test_year(self):
        assert isinstance(dialect.ddl_to_sa("year"), sa_types.Integer)


class TestBinaryTypes:
    def test_blob(self):
        assert isinstance(dialect.ddl_to_sa("blob"), sa_types.LargeBinary)

    def test_binary(self):
        assert isinstance(dialect.ddl_to_sa("binary(16)"), sa_types.LargeBinary)

    def test_varbinary(self):
        assert isinstance(dialect.ddl_to_sa("varbinary(256)"), sa_types.LargeBinary)


class TestOtherTypes:
    def test_json(self):
        assert isinstance(dialect.ddl_to_sa("json"), sa_types.JSON)

    def test_boolean(self):
        assert isinstance(dialect.ddl_to_sa("boolean"), sa_types.Boolean)

    def test_bit(self):
        assert isinstance(dialect.ddl_to_sa("bit(8)"), sa_types.Boolean)

    def test_enum(self):
        result = dialect.ddl_to_sa("enum('a','b','c')")
        assert isinstance(result, sa_types.String)
