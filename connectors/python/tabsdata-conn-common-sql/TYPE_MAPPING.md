<!-- Copyright 2026 Tabsdata Inc. -->

# SQL connector write type mapping

Scope: the shared SQLAlchemy **write** path (`_engine/sqlalchemy_writer.py`,
`PolarsWriteEngine`) used by the **db2, mssqlserver, mysql, oracle, postgres**
connectors. Covers **create-on-write** — the column types chosen when a `Dest`
writes to a table that does not yet exist (`if_table_exists` = `replace`, or
`append` to a missing table). Writing into a table that already exists does not
use this mapping (the existing column governs).

## How the column type is chosen

The write goes `polars → pandas.to_sql → CREATE TABLE`. For most types pandas
infers the SQLAlchemy type from the (polars→pandas) column and the dialect
renders it. pandas sees only the column, never the target, so where its choice is
wrong for a dialect we override it (`dtype=` on `to_sql`) and, where the driver
also refuses the Python value, transform the value too:

- **Decimal** — pandas has no decimal dtype (an Arrow `decimal128` arrives as a
  Python-`Decimal` `object` column and mis-maps, e.g. to `CLOB` on DB2). We force
  every decimal column to `Numeric(precision, scale)`, **clamped to the engine's
  maximum** (see below). Implemented in `_decimal_dtype_overrides`.
- **Duration / interval** — polars writes it as an integer (nanoseconds); it
  lands as a 64-bit integer column. Accepted as-is.
- **tz-aware `Datetime` on SQL Server** — pandas picks a generic `TIMESTAMP`,
  which T-SQL reads as `ROWVERSION`: an auto-generated row counter that rejects
  every insert. Forced to `DATETIMEOFFSET`, the tz-aware column SQL Server has —
  so SQL Server is the one target that preserves the offset.
- **`Float32` / `Float64` on Oracle** — pandas' `Float` carries decimal
  precision, which the Oracle dialect refuses to compile at all ("FLOAT types use
  binary precision"), so float columns never reached the database. Forced to
  Oracle's native IEEE types, `BINARY_FLOAT` and `BINARY_DOUBLE`.
- **`Time` on Oracle** — Oracle has no time-of-day type, and `oracledb` rejects a
  Python `time` value whatever the target column is (DPY-3002). Stored as a
  `TIMESTAMP` on the sentinel date **1970-01-01**, so `09:30:15` reads back as
  `1970-01-01 09:30:15`; a reader recovers the original time by dropping the date.

The last three live in `_dialect_type_fixes`, which returns both the type to force
and any value transform that type needs.

## Verified mapping (live, 2026-07-14)

TableFrame/polars type → created column type (as reflected by SQLAlchemy). Every
cell below was produced by writing a one-column table through the connector to a
live backend.

| TableFrame type | DB2 | PostgreSQL | MySQL | Oracle | SQL Server |
|---|---|---|---|---|---|
| `Boolean` | BOOLEAN | BOOLEAN | TINYINT | INTEGER | BIT |
| `Int8/16/32/64` | BIGINT | BIGINT | BIGINT | NUMBER | BIGINT |
| `UInt8/16/32/64` | BIGINT | BIGINT | BIGINT | NUMBER | BIGINT |
| `Float32/64` | DOUBLE | DOUBLE PRECISION | DOUBLE | ❌ **fails** | FLOAT |
| `Decimal(p,s)` | DECIMAL (clamped) | NUMERIC(p,s) | DECIMAL (clamped) | NUMBER (clamped) | NUMERIC (clamped) |
| `String` | CLOB | TEXT | TEXT | CLOB | VARCHAR |
| `Date` | DATE | DATE | DATE | DATE | DATE |
| `Datetime` (naive) | TIMESTAMP | TIMESTAMP | DATETIME | DATE | DATETIME |
| `Datetime` (tz) | TIMESTAMP ⚠️ | TIMESTAMP ⚠️ | TIMESTAMP ⚠️ | TIMESTAMP ⚠️ | ❌ **fails** |
| `Time` | TIME | TIME | TIME | ❌ **fails** | TIME |
| `Duration` | BIGINT | BIGINT | BIGINT | NUMBER | BIGINT |
| `Binary` | CLOB ⚠️ | TEXT ⚠️ | TEXT ⚠️ | CLOB ⚠️ | VARCHAR ⚠️ |

Legend: ❌ create/load fails today (see gaps); ⚠️ succeeds but loses fidelity.

`Int128` and `Float16` are out of scope (Arrow/parquet edge cases, not exercised).

## Decimal clamping

Source decimals are Arrow `decimal128` (precision ≤ 38, scale ≤ precision). We
emit `Numeric(min(p, MAX_P), min(s, MAX_S, min(p, MAX_P)))` per engine:

| Engine | max precision | max scale | notes |
|---|---|---|---|
| PostgreSQL | 1000 | 1000 | always faithful for `decimal128` |
| MySQL | 65 | **30** | scale > 30 is truncated |
| Oracle | 38 | 38 | faithful for `decimal128` |
| SQL Server | 38 | 38 | faithful for `decimal128` |
| **DB2** | **31** | 31 | precision 32–38 is clamped, e.g. `decimal(38,10)` → `DECIMAL(31,10)` |

Clamping is a **stopgap so writes succeed** where the source precision exceeds
the engine limit. It reduces the integer-digit capacity, so a value that needs
more than the clamped precision will overflow at INSERT time (DB2 only, for
precision > 31). A fuller Arrow→SQLAlchemy type map would make this configurable.

## Known gaps & caveats (surfaced by the datatype matrix)

- **Oracle `Float32`/`Float64` fail** on write (ORACLE_9) — the float column is
  not created/bound. Needs connector-side handling.
- **Oracle `Time` fails** — Oracle has no `TIME` type (only `DATE`/`TIMESTAMP`).
- **SQL Server `Datetime` with tz fails** (MSSQL_9) — no tz-aware column is
  chosen (`DATETIMEOFFSET` would be required).
- **tz is dropped** for `Datetime` with a timezone on DB2/PostgreSQL/MySQL/Oracle
  — stored as a naive timestamp.
- **`Binary` lands in a character LOB/VARCHAR**, not a binary type (BLOB/
  VARBINARY) — values round-trip in these tests but this is not a faithful binary
  column.
- **All integer widths collapse to a 64-bit integer** (`BIGINT`/`NUMBER`); a
  `UInt64` value above 2^63 would overflow the signed 64-bit target.
- **`Duration` becomes a 64-bit integer** of nanoseconds (no interval type).

The reusable coverage matrix lives in
`testi_tabsdatak/conn/common/sql/datatype_matrix.py`; each connector's
`test_dest_datatypes.py` runs it against that engine, one fixed table per case.
