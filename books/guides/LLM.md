<!--
Copyright 2026 Tabsdata Inc.
-->

# LLM authoring guide — `tabsdatak`

This doc is written for **an LLM generating user code with
`tabsdatak`** — wiring `@publisher` / `@transformer` /
`@subscriber` / `@auto_publisher` against the shipped connectors.

It is **not** a guide to implementing new connectors. The connector
implementer surface (`tabsdatak.spi`, the `<connector>/_validation.py`
validators, `<Name>SrcDef` / `<Name>DestDef`, the `ErrorCode`
machinery) is intentionally invisible to user code and irrelevant to
this doc.

If you find yourself reading anything from `tabsdatak._api` /
`tabsdatak._info` / `tabsdatak.spi` / `tabsdatak.conn.<name>._*`,
you are probably solving the wrong problem.

---

## Cheat sheet — the public surface

Three modules. Everything an end user ever types comes from one of them.

```python
# 1. Core API: decorators + abstract bases + value refs + type aliases.
from tabsdatak.api import (
    publisher, stream_publisher, transformer, subscriber,  # entry points
    CronTrigger,  # firing rule
    Secret,  # secret value ref
    # function-signature slot
)
from tabsdatak.tableframe import TableFrame

# 2. Cross-cutting connector types: credentials, cloud locations, conn_cfg.
from tabsdatak.conn.common.types import (
    UserPassword, GCPJson, AwsAccessSecretKey,
    AzureAccountKey, HMAC,
    S3Location, AzBlobLocation, GCSLocation,
)

# 3. The vendor connector you need (one of ~17). Example:
from tabsdatak.conn.mysql import MySQLSrc, MySQLDest
# (The `<Vendor>Conn` class is NOT imported in flow code: the
#  connection is registered on the function's collection and the
#  runtime resolves it and hands it to the connector at run time.)

# 4. (Optional) Errors — only if you need to react to them.
from tabsdatak.error import ApiException
from tabsdatak.conn.common.error import ConnException

# 5. (Optional) Discover what's installed at runtime.
from tabsdatak._connectors import avail_srcs, avail_stream_srcs, avail_dests
# Each returns a fresh dict keyed by type_ (unique across all
# plugins: "mysql-sql-in", "s3-file-in", "salesforce-report-in",
# ...). Values are SrcDef /
# AutoSrcDef / DestDef instances exposing the concrete config
# class (.src / .dest), .table_mode (for sources), .cardinality,
# .plugin, and .explorer.

# 6. (Optional) Vendor-level metadata without importing connectors.
from tabsdatak.conn.common._connectors import (
    CONNECTORS, list_connectors, import_connector,
)
# CONNECTORS is a dict[str, ConnectorInfo] keyed by vendor key
# ("mysql", "s3", ...). Each ConnectorInfo carries class names
# as strings (src_classes / dest_classes / conn_classes), kind /
# kind_tags, and staging description.
```

**That's the entire user-facing surface.** Anything else is
framework internals.

### Connector index

The "src field" / "dest field" columns name the field on
`<Vendor>Src` / `<Vendor>Dest` that lists what the connector
reads from / writes to. The names are vendor-shaped on purpose
(SQL connectors run `queries`; file connectors enumerate
`path` entries; Salesforce has its own SOQL / Report shapes;
MongoDB carries an id-column per collection) -- see "Common
LLM authoring traps #12" below for the full asymmetry rundown.

| Vendor | Module | Roles | Src field | Dest field |
|---|---|---|---|---|
| MySQL / MariaDB | `tabsdatak.conn.mysql` | src + dest | `queries: list[str]` | `tables: list[str]` |
| PostgreSQL | `tabsdatak.conn.postgres` | src + dest | `queries: list[str]` | `tables: list[str]` |
| Oracle | `tabsdatak.conn.oracle` | src + dest | `queries: list[str]` | `tables: list[str]` |
| MSSQL | `tabsdatak.conn.mssql` | src + dest | `queries: list[str]` | `tables: list[str]` |
| IBM Db2 | `tabsdatak.conn.db2` | src only | `queries: list[str]` | — |
| BigQuery | `tabsdatak.conn.bigquery` | dest only | — | `tables: list[str]` |
| Snowflake | `tabsdatak.conn.snowflake` | dest only | — | `tables: list[str]` |
| Databricks | `tabsdatak.conn.databricks` | src + dest | `queries: list[str]` | `tables: list[str]` |
| ClickHouse | `tabsdatak.conn.clickhouse` | dest only | — | `tables: list[str]` |
| Druid | `tabsdatak.conn.druid` | dest only | — | `tables: list[str]` |
| StarRocks | `tabsdatak.conn.starrocks` | dest only | — | `tables: list[str]` |
| MongoDB | `tabsdatak.conn.mongodb` | dest only | — | `collections_with_ids: list[tuple[str, str \| None]]` |
| Salesforce | `tabsdatak.conn.salesforce` | src only (SOQL + Reports) | `SalesforceSrc.queries: list[str]` (max 1) / `SalesforceReportSrc.reports: list[str]` (max 1) | — |
| Local file | `tabsdatak.conn.localfile` | src + dest | `path: list[str]` | `path: list[str]` |
| AWS S3 | `tabsdatak.conn.s3` | src + dest | `path: list[str]` | `path: list[str]` |
| GCS | `tabsdatak.conn.gcs` | src + dest | `path: list[str]` | `path: list[str]` |
| Azure Blob | `tabsdatak.conn.azureblob` | src + dest | `path: list[str]` | `path: list[str]` |

Class naming is uniform: `<Vendor>Src` / `<Vendor>Dest`.
Source-only or dest-only connectors omit the missing `Src`/`Dest`.

**Flow code never declares a connection.** A `Src` / `Dest` carries
only its own config (`queries` / `path` / `tables` / ...) — there is
no `conn` field and no `conn=` argument anywhere in user flow code.
The connection (the `<Vendor>Conn`) is registered on the function's
*collection*; the runtime resolves it and hands it to the
connector's plugin at run time.

### Canonical user code

```python
from tabsdatak.api import publisher, CronTrigger
from tabsdatak.tableframe import TableFrame
from tabsdatak.conn.mysql import MySQLSrc

src = MySQLSrc(
  queries=["SELECT id, email FROM users"],
)


@publisher(
  source=src,
  output_tables=["users_out"],
  trigger_by=CronTrigger(mask="@hourly"),
)
def my_pub(users: TableFrame) -> TableFrame:
  return users
```

Three pieces, in order: build a `Src` (or `Dest`) from its own
config only (no connection — that lives on the collection); decorate
a function whose first `N` positional parameters match the source's
slot count -- the connector's `SrcDef.cardinality(src)` -- (each
typed `TableFrame` for SINGLE-mode sources or `list[TableFrame]`
for ARRAY-mode sources) and whose return is a fixed-length
`tuple[TableFrame, ...]` of length `len(output_tables)` (a
single-output function may return the lone `TableFrame` bare,
without the enclosing 1-tuple). The MySQL
connection itself is registered on the function's collection and
resolved by the runtime; it is never named or built in flow code.

---

## What's friendly for LLM authoring

### 1. Decorator surface is small and parallel

Four decorators, all with the same skeleton (`source` or
`destination`, `input_tables` / `output_tables`, optional
`trigger_by`, optional `on_tables`):

```python
@publisher(source=..., output_tables=[...], trigger_by=..., on_tables=...)
@transformer(input_tables=[...], output_tables=[...], trigger_by=..., on_tables=...)
@subscriber(destination=..., input_tables=[...], trigger_by=...)
@auto_publisher(source=..., use_existing_data=..., fallback_rollover_mins=...,
                output_tables=[...], on_tables=...)
```

Once you've seen one, you've seen all four.

### 2. Connector class names are predictable

`<Vendor>Src`, `<Vendor>Dest`. Generation by template works. One
piece to remember per vendor: how to build the source / destination
from its own config (`<V>Src(queries=[...])`,
`<V>Dest(tables=[...])`). There is no connection to wire — it is
registered on the collection and resolved by the runtime.

### 3. Type aliases are descriptive

`TableNameSpec`, `TableNamesSpec`, `CronMaskSpec`, `VarNameSpec`,
`UriSpec`, `IdentifierSpec`. The shape *is* the name. When a field
is typed `UriSpec`, you know it's a string + a URI validator.

### 4. `Var` / `Secret` work everywhere a string does

Anywhere a field accepts `ValOrRefSpec` (the `str | ValRef` union),
you can pass either:

```python
password="literal-pw"                        # literal string
password=Var(name="DB_PASSWORD")             # env var lookup at runtime
password=Secret(name="db-creds#PASSWORD")    # K8s secret lookup
```

No special syntax per call site. You don't have to know which
connectors support `Var` / `Secret`; they all do, because the field
type is the union.

### 5. Errors are named, not numbered

If a generated descriptor fails validation, the exception's
`api_error` is a named enum value (`ApiErrorCode.API_4`,
`MySQLErrorCode.MYSQL_3`, ...). The names tell you what failed. The
exception message is one line:

```
ConnParamTypeException: ORACLE_6: Oracle URI is invalid: Oracle URI must start with oracle:// (...): 'postgres://x'
```

Greppable, no documentation-URL noise.

### 6. JSON envelope round-trips

Every descriptor has `to_json()` / `from_json()`. The on-wire form
is self-describing:

```json
{"kind": "...", "apiVersion": "1.0", "type": "<module>:<class>", "spec": {...}}
```

You can dump a `MySQLSrc(...)` to JSON and reload it later without
plumbing.

### 7. The connection is not your problem

You never declare, name, or build a connection in flow code. The
`<Vendor>Conn` is registered on the function's collection and the
runtime resolves it and hands it to the connector at run time. A
`Src` / `Dest` carries only its own config, so generated flow code
has nothing to guess about credentials or URIs:

```python
src = MySQLSrc(queries=["SELECT 1"])   # no conn anywhere
```

---

## Common LLM authoring traps

### 1. The function-signature shape is mandatory and easy to get wrong

Every publisher / auto_publisher / transformer / subscriber function
has a **flat** signature:

```python
# Schematic -- the actual return type must be a *fixed-length*
# tuple of TableFrame elements, one per entry in `output_tables`
# (or per dest cardinality, for @subscriber). A variadic
# `tuple[TableFrame, ...]` is rejected.
def f(
    in_1: TableFrame,           # one positional param per input slot.
    in_2: TableFrame,           # input_count = source's cardinality
                                #   for publisher / auto_publisher;
                                #   = len(input_tables) for transformer / subscriber.
    # extra params (config, logger, ...) are allowed past in_N
    # and are not type-checked.
) -> tuple[TableFrame, TableFrame, TableFrame]:   # one element per output slot
    ...
```

Specifically:

* The first `N` positional parameters are the input slots:
  - For `@publisher` / `@auto_publisher`, each is `TableFrame` if
    `source.table_mode is SINGLE` or `list[TableFrame]` if
    `source.table_mode is ARRAY`. SQL-shaped sources
    (`MySQLSrc`, `PostgresSrc`, `OracleSrc`, `MSSQLSrc`, `Db2Src`,
    `DatabricksSrc`, `SalesforceSrc`) are SINGLE-mode -- one
    `TableFrame` per query. File / object-storage sources
    (`LocalFileSrc`, `S3Src`, `GCSSrc`, `AzureBlobSrc`) are
    ARRAY-mode -- each path entry may glob to multiple files, so
    the slot binds to a `list[TableFrame]`.
  - For `@transformer` / `@subscriber`, each is either
    `TableFrame` or `list[TableFrame]` (the framework does not yet
    enforce a mode here).
  - `TableFrame` is shorthand for `TableFrame | None`
    (auto-nullable). For `list[TableFrame]` the list is required
    but its elements may be `None`.
* The return must be a fixed-length tuple of `TableFrame` of arity:
  - `len(output_tables)` for `@publisher`, `@auto_publisher`,
    `@transformer`.
  - `DestDef.cardinality(destination)` for `@subscriber` (the
    framework looks the binding up via the
    ``tabsdatak.connectors`` registry).
  A single-output function may return the lone `TableFrame`
  directly, without the enclosing 1-tuple.

Typical LLM mistakes:
- Wrapping inputs in a single `tables` tuple parameter (the old
  shape). The new contract is one positional parameter per slot.
- Variable-length return tuple `tuple[TableFrame, ...]` (Ellipsis
  means variable arity, framework rejects it).
- Wrong arity (`output_tables=["a", "b"]` but return tuple has one
  element).

### 2. `*Tables` lists must be non-empty and follow the regex

`output_tables` / `input_tables` / `tables` (on Dest) and the
similar parameters all have:

* `min_length=1` — empty list rejected.
* Each name matches `^[A-Za-z][A-Za-z0-9_]{0,63}$` (or, for
  `CollTableNamesSpec`, optionally with one `/`).
* No trailing whitespace, no leading digit, no hyphens.

Generated code that uses `output_tables=["my-table"]` or
`output_tables=[]` fails immediately.

### 3. Datetimes must be timezone-aware

Anywhere a field is typed `datetime` (e.g. `CronTrigger.start`,
`SalesforceSrc.initial_last_modified`):

```python
# rejected:
datetime(2025, 1, 1)

# accepted:
from datetime import datetime, timezone
datetime(2025, 1, 1, tzinfo=timezone.utc)
```

LLM training data is full of naive datetimes; the framework rejects
them with a clear message.

### 4. Decorate functions defined at module top level

`@publisher` (and friends) capture the function as
`"<module>:<qualname>"`. A function defined inside another function
has `<locals>` in its qualname and **fails to serialize** when the
descriptor is dumped.

```python
# fine — module-level:
@publisher(source=src, output_tables=["t"])
def my_pub(tables): ...

# rejected at to_json() time:
def make_pub():
    @publisher(source=src, output_tables=["t"])
    def my_pub(tables): ...
    return my_pub
```

### 5. There is no `conn` argument on a `Src` / `Dest`

Flow code never wires a connection. A `Src` / `Dest` has no `conn`
field; passing `conn=...` (a string, a `<V>Conn`, anything) is a
`TypeError`. The connection is registered on the function's
collection and resolved by the runtime:

```python
# rejected -- no such field:
src = MySQLSrc(conn="prod_mysql", queries=["SELECT 1"])
src = MySQLSrc(conn=MySQLConn(...), queries=["SELECT 1"])

# correct -- config only:
src = MySQLSrc(queries=["SELECT 1"])
```

### 6. Cloud destinations need an explicit staging location

ClickHouse, Druid, StarRocks — and any other destination that
bulk-loads via object storage — require a `staging: CloudLocation`
on their **connection**. The connection (with its staging block) is
registered on the collection, not built in flow code, so this is a
connection-configuration concern rather than something the `Dest`
carries. The connection looks like:

```python
from tabsdatak.conn.common.types import (
  AwsAccessSecretKey, S3Location, UserPassword,
)
from tabsdatak.conn.clickhouse import ClickHouseConn

dc = ClickHouseConn(
  host="ch.local", port=9000, database="analytics",
  credentials=UserPassword(user="u", password="p"),
  staging=S3Location(
    bucket="my-staging", region="us-east-1", base_path="ch/",
    credentials=AwsAccessSecretKey(
      access_key_id="...", secret_access_key="...",
    ),
  ),
)
```

The `S3Location` itself needs `bucket`, `region`, `base_path` (must
end with `/`), and `credentials`. Omitting any of those throws the
load failure at run time.

### 7. Source-only / destination-only connectors

These connectors **do not have** a `<V>Src`:

* `bigquery`, `snowflake`, `clickhouse`, `druid`, `starrocks`,
  `mongodb` — dest-only.

These do not have a `<V>Dest`:

* `db2`, `salesforce` — src-only.

LLMs frequently invent `BigQuerySrc` or `SalesforceDest` because
they look symmetric. They don't exist.

### 8. SQL `queries` is a list, not a string

```python
# rejected:
src = MySQLSrc(queries="SELECT 1")

# accepted:
src = MySQLSrc(queries=["SELECT 1"])
```

One element per output slot. SalesforceSrc currently restricts
`queries` to exactly one element.

### 9. `tables` on a destination is also a list

```python
# rejected:
dest = MySQLDest(tables="my_table")

# accepted:
dest = MySQLDest(tables=["my_table"])
```

The single-string-or-list union was deliberately removed for
consistency — destinations always receive a list.

### 10. Salesforce report filter operators are constrained

The `filter` field on `SalesforceReportSrc` is a list of
`(column, operator, value)` tuples. The middle element is a
Salesforce Analytics REST API operator — a `Literal` of:

```
equals, notEqual, lessThan, greaterThan,
lessOrEqual, greaterOrEqual, contains, notContain,
startsWith, includes, excludes, within, between
```

`"="`, `"!="`, `">"` are rejected. So is anything else not in that
list. (Pydantic's `Literal` enforcement will say which.)

### 11. `format_cfg` keys depend on the side

For S3 / GCS / AzureBlob / LocalFile sources and destinations, the
`format_cfg` field accepts CSV options, but the *allowed key set*
depends on whether you're on the src or dest side:

```python
# CSV common (both sides): separator, quote_char, eol_char
# CSV src-only: encoding, null_values, has_header, skip_rows, ...
# CSV dest-only: include_header, datetime_format, quote_style, ...
```

Mixing them up (e.g. `include_header` on a `Src`) raises
`ConnValidateException(<NAME>_5)`.

### 12. Source / destination "what to read from" / "what to write to" fields are vendor-shaped

The "list of read sources" and "list of write targets" fields on
each connector use names that match the vendor's vocabulary, so
the same shorthand does **not** work everywhere:

| Connector kind | Src field | Dest field |
|---|---|---|
| SQL / warehouse (MySQL, Postgres, Oracle, MSSQL, Db2, Databricks, ClickHouse, Druid, StarRocks, BigQuery, Snowflake) | `queries: list[str]` (SQL strings, src side) | `tables: list[str]` (target table names) |
| File / object storage (LocalFile, S3, GCS, AzureBlob) | `path: list[str]` (file / object / glob paths) | `path: list[str]` |
| MongoDB | — (dest-only) | `collections_with_ids: list[tuple[str, str \| None]]` (collection name + id-column name; `None` to auto-generate) |
| Salesforce | `SalesforceSrc.queries: list[str]` (SOQL, exactly 1 entry today) and `SalesforceReportSrc.reports: list[str]` (exactly 1 entry today) | — (src-only) |

```python
# fine -- SQL dest:
MySQLDest(tables=["users_out", "orders_out"])

# fine -- file dest:
S3Dest(path=["out/users/", "out/orders/"])

# rejected -- MongoDB doesn't take 'tables':
MongoDBDest(tables=["users", "orders"])
#   -> use collections_with_ids=[("users", "_id"), ("orders", None)]

# rejected -- file dest doesn't take 'tables':
S3Dest(tables=["out/users.csv"])
#   -> use path=["out/users.csv"]
```

The full per-connector mapping is in the **Connector index**
table near the top of this document. The asymmetry is
deliberate -- each name reflects what the underlying engine
actually accepts (a SQL connector runs query strings, an
object-storage connector navigates a path namespace, MongoDB
needs a per-collection id column) -- so renaming to a unified
``targets`` field would lose information that the validation
layer relies on.

---

## Design history: what was painful for LLMs and how it was addressed

> **Status note.** This section is a record of friction points
> that were diagnosed during the API design plus a tracking list
> of enhancements that didn't ship. Each diagnosis is tagged:
>
> * **[Implemented]** -- a mitigation is in the code today; the
>   "Mitigation in place" prose describes what was shipped. An
>   LLM authoring against the API does not need to work around
>   these.
> * **[Open]** -- not yet shipped. May be a deliberate non-goal,
>   a low-priority enhancement, or a structural item awaiting
>   first implementation. The prose describes the gap and the
>   smallest reasonable fix.
>
> Current tally: **D1, D2, D3, D4, D5, D6, D7, D8, D10, D11,
> D14** are Implemented; **D12** is Partially implemented (the
> structural cleanup landed; a registry surface follow-up is
> still open); **D9, D13** are Open. The API as it stands today
> is described in the sections *above* this one -- none of the
> Open items block first-shot LLM authoring.

### Diagnoses

#### Diagnosis 1: No top-level connector index module  **[Implemented]**

**Problem.** An LLM has to know `tabsdatak.conn.mysql` exists.
Without a discovery surface, a generated
`from tabsdatak.conn.snowflake import SnowflakeSrc` just fails
(snowflake is dest-only) with no recourse.

**Mitigation in place.** Two complementary discovery surfaces:

* :mod:`tabsdatak.connectors` -- the *runtime* binding registry.
  ``avail_srcs() / avail_auto_srcs() / avail_dests()`` each return
  a fresh ``dict[str, <Def>]`` keyed by ``type_``
  (unique across all plugins), populated lazily on first call from the
  ``tabsdatak.connectors`` entry-point group. Each ``SrcDef`` /
  ``AutoSrcDef`` / ``DestDef`` exposes the concrete config class
  (``.src`` / ``.dest``), ``.table_mode`` (sources), ``.cardinality``,
  ``.plugin``, and ``.explorer``.
* :mod:`tabsdatak.conn.connectors` -- the *vendor-level* index.
  ``CONNECTORS: dict[str, ConnectorInfo]`` is hand-curated; each
  ``ConnectorInfo`` carries class **names as strings**
  (``src_classes`` / ``dest_classes`` / ``conn_classes``) plus
  ``kind`` (e.g. ``"SQL OLTP"``, ``"OLAP SQL"``), ``kind_tags``
  (``"sql"`` / ``"oltp"`` / ``"warehouse"`` / ``"file"`` / ...),
  and a ``staging`` description. ``import_connector(name)`` lazily
  imports the public module so an LLM can resolve the strings to
  classes only when needed.

```python
from tabsdatak._connectors import avail_srcs, avail_dests
from tabsdatak.conn.common._connectors import (
  CONNECTORS, list_connectors, import_connector,
)

# What's installed?
sorted(CONNECTORS)
# ['azureblob', 'bigquery', 'clickhouse', 'databricks', 'db2', ...]

# Pick a destination class (resolve the string name to the class):
sf_mod = import_connector("snowflake")
DestCls = getattr(sf_mod, CONNECTORS["snowflake"].dest_classes[0])

# Find every ARRAY-mode source (the runtime binding registry
# carries table_mode):
[d.src for d in avail_srcs().values()
 if d.table_mode.name == "ARRAY"]
# [LocalFileSrc, S3Src, GCSSrc, AzureBlobSrc]

# All vendors with a source role, filtered by tag:
list_connectors(role="src", tag="object-storage")
# ['azureblob', 'gcs', 's3']
```

The mechanism is open: third-party connector packages that declare
the same entry-point group and expose ``SrcDef`` / ``AutoSrcDef``
/ ``DestDef`` constants get picked up automatically with no change
to
:mod:`tabsdatak`.

#### Diagnosis 2: The flat-signature shape is the #1 mistake source  **[Implemented]**

**Problem.** Every publisher / auto_publisher / transformer /
subscriber function has to spell out the right number of slot-typed
parameters and a matching `tuple[TableFrame, ...]` return. LLMs
routinely:
- pack inputs into one `tables` tuple parameter (the old shape),
- use a variable-length return tuple `tuple[TableFrame, ...]`,
- swap `TableFrame` for `list[TableFrame]` (or vice versa) against
  the source's `table_mode`.

**Mitigation in place.** When the framework's signature check fails,
the error message appends a copy-pasteable correct signature
computed from the binding's `cardinality(src)`, `table_mode`, and
the decorator's `output_tables` -- the LLM iterating against the
error sees the exact shape it should have written.

```
Expected signature (2 input slot(s), 1 output slot(s); slot type = TableFrame  (or TableFrame | None)):
    def my_pub(
        in_1: TableFrame,
        in_2: TableFrame,
    ) -> tuple[TableFrame]:
        ...
    # a single output may also be returned bare, without the 1-tuple:
    def my_pub(...) -> TableFrame:
        ...
```

A previous experiment with per-arity aliases (`SingleInputSpec` /
`DualInputSpec` / etc.) was reverted: 10 type aliases for a pattern
already inferable from the error message added more vocabulary
than they removed.

#### Diagnosis 3: Many `*Spec` aliases that look similar  **[Implemented]**

**Problem.** `TableNameSpec`, `TableNamesSpec`, `CollTableNameSpec`,
`CollTableNamesSpec`, `OperatorSpec`, `ApiNameSpec`,
`ApiVersionSpec`, etc.
Some are validated `str`; some are
`Annotated[list[…], Field(min_length=1)]`; some are `Union[…]`. From
the alias name alone an LLM couldn't always tell which.

**Mitigation in place.** The framework now codifies a naming
convention for ``*Spec`` aliases (documented in the
``tabsdatak.api`` module docstring under "Type-alias naming
convention"):

* ``<Thing>NameSpec``  -- single validated string.
* ``<Thing>NamesSpec`` -- non-empty list of those.
* ``<Thing>RefSpec``   -- single validated string whose shape is a
  textual reference (e.g. ``SecretRefSpec`` is ``<secret>#<key>``).
* ``<Thing>Spec`` (no ``Name`` / ``Names`` / ``Ref`` qualifier)
  -- a composed shape: ``Literal``, ``Union``,
  ``Annotated[int, ...]`` bound, etc.

Connector packages follow the same convention (``UriSpec``,
``CredentialsSpec``, ``QueriesSpec``, ``MySQLTableNameSpec`` /
``MySQLTableNamesSpec``, ...).

#### Diagnosis 4: Connections are resolved from the collection, not declared in flow code  **[Implemented]**

**Problem.** LLM training data is full of `conn="prod"` /
`conn=<V>Conn(...)` shorthand wired directly onto a source or
destination. Earlier versions of the framework did take a `conn`
field (and a `NamedConn` wrapper for by-name references).

**Mitigation in place.** The `conn` field — and the `NamedConn`
by-name wrapper, and the bare-string auto-promotion — were removed
entirely. A `Src` / `Dest` carries only its own config; the
connection is registered on the function's *collection* and the
runtime resolves it and hands it to the connector at run time. Flow
code that passes `conn=...` fails with an "unexpected keyword"
`TypeError`, which steers the LLM to drop the argument.

#### Diagnosis 5: Cloud-staging boilerplate is heavy for an LLM  **[Implemented]**

**Problem.** A `ClickHouseConn` with an `S3Location` staging is
~10 lines of nested constructors. LLMs frequently miss `region`,
`base_path` (which must end in `/`), or one of the
`AwsAccessSecretKey` fields.

**Mitigation in place.** Each cloud `Location` ships a `from_uri`
classmethod that fills the structural fields from a single URI:

```python
S3Location.from_uri(
    "s3://my-staging/ch/?region=us-east-1",
    credentials=AwsAccessSecretKey(...),
)
```

`bucket`, `base_path`, and `region` come from the URI; the LLM only
supplies `credentials`. Same for `GCSLocation.from_uri` /
`AzBlobLocation.from_uri`.

#### Diagnosis 6: `format_cfg` allowed-keys are documented in code, not in errors  **[Implemented]**

**Problem.** A wrong key on `format_cfg` (e.g. `include_header` on a
`Src`) used to raise `FILECOMMON_3` whose primary message was
just "format_cfg validation failed"; the allowed-key list only
appeared as a chained `ValueError` cause. An LLM iterating
against the error had to drill into the cause chain to learn the
recovery path.

**Mitigation in place.** Two changes:

1. The two failure modes are now distinct codes: ``FILECOMMON_3``
   for unknown-key errors, ``FILECOMMON_4`` for non-dict cfg
   values. Each has a dedicated message template.
2. ``FILECOMMON_3``'s template inlines the format, side, the
   unknown keys, and the full allowed-key set via
   ``str.format_map``:

   ```
   FILECOMMON_3: format_cfg[CSV] has unknown keys ['include_header'];
                 allowed for CSV (src): ['comment_prefix',
                 'decimal_comma', 'encoding', 'eol_char',
                 'has_header', ...]
   ```

   Same data is also available structured on ``e.extra``
   (``format``, ``side``, ``unknown``, ``allowed``) for callers
   that prefer programmatic access over string parsing.

The "src vs dest CSV keys" reference also lives in the
:data:`SrcFormatCfgSpec` / :data:`DestFormatCfgSpec` docstrings
in :mod:`tabsdatak.conn.file_common_typing`, so an LLM reading
the type annotations sees the same set without having to trigger
a failure first.

#### Diagnosis 7: Source-only / destination-only is invisible from the public surface  **[Implemented]**

**Problem.** The asymmetry between `mysql` (both) and `bigquery`
(dest-only) is documented only in the README. Module docstrings on
each connector don't say at the top, "this is a destination-only
connector."

**Fix.** Standard module-docstring header per connector:

```python
"""BigQuery destination connector for tabsdatak.

ROLES: dest-only (no `BigQuerySrc`; the conn is `BigQueryConn`).
KIND: SQL.
STAGING: requires `staging: GCSLocation` on the dest connection.
...
"""
```

Three structured fields the LLM can grep for: ROLES, KIND, STAGING.
Apply uniformly to all 17 connector docstrings.

#### Diagnosis 8: Validation errors don't link to the field that failed in the user code  **[Implemented]**

**Problem.** A pydantic ValidationError's `loc` is rendered concisely
(e.g. `bucket: Field required; credentials: Input should be ...`) but
points at pydantic field names — not the LLM-generated literal. If
the LLM wrote `bucket="my-bucket"`, the error doesn't quote the call
site or the line number.

**Mitigation in place.** ``TabsdataException.__init__`` walks
the stack, finds the first frame outside any
``tabsdatak`` / ``pydantic`` package, and stores
``"<file>:<line>: <code>"`` on the exception's ``source``
attribute. The same string is appended to ``str(exc)`` as
``" at <file>:<line>: <code>"`` so it shows up in a single line
of output:

```
ApiException: API_2: CronTrigger validation failed: mask: Field required at /path/to/my_pipeline.py:42: t = CronTrigger()
```

The capture is **on by default**: the framework's exceptions only
fire at descriptor-build / decoration time (declarative code,
not a hot path), so the per-exception stack-walk cost is
negligible compared to the LLM-iteration payoff. The capture is
defensive -- any failure to walk the stack returns ``None``
instead of breaking exception construction -- and skips synthetic
frames (``<frozen ...>``, ``<string>``, eval'd code) and any
frame inside ``tabsdatak`` / ``pydantic``.

To disable in environments that don't want the capture, set
``TABSDATAK_DEBUG_FRAMES`` to a falsy value (``"0"``,
``"false"``, ``"no"``, ``"off"``, or empty). Programmatic check:
``tabsdatak.api_error.DEBUG_FRAMES_ENV_VAR``.

#### Diagnosis 9: No high-level "build this connector with reasonable defaults" helper  **[Open]**

**Problem.** An LLM asked to write a Postgres publisher needs to
remember every required field on `PostgresConn`. A generated
example often omits `credentials` or invents a field like
`database_url`.

**Fix.** A `quickstart` module with builder functions per connector:

```python
from tabsdatak.conn.mysql.quickstart import mysql_src

src = mysql_src(uri="mysql://...", user="etl", password=Var(...),
                queries=["SELECT 1"])
```

The function does the wiring; the LLM only sets actual values. Same
shape across all connectors.

#### Diagnosis 10: The decorator factories produce `*Info` instances, not callable functions  **[Implemented]**

**Problem.** After `@publisher`, the decorated name is a
`PublisherInfo` instance. An LLM that writes `my_pub(some_args)`
expecting to call the decorated function gets a confusing error.
The decorator pattern looks like a wrapper but is actually a
descriptor builder.

**Mitigation in place.** Each `*Info` class overrides
`__call__` to raise a friendly `TypeError`: *"this is a
flow-descriptor, not a callable. The decorated function lives on
`.func`; the framework invokes it at runtime. Calling the
decorated name in user code is almost certainly a bug."* The
underlying function stays reachable via `.func` (e.g.
`my_pub.func(...)`). The descriptor-builder pattern itself is
structural and doesn't change.

#### Diagnosis 11: Trigger types are sparse  **[Implemented]**

**Problem.** Earlier the framework shipped only `CronTrigger`.
Other patterns (table-arrival trigger, wildcard) were expressed
as bare lists or the magic string ``"*"`` in the `trigger_by`
field. An LLM couldn't easily tell what was allowed.

**First mitigation (additive).** Two typed wrappers were added
alongside `CronTrigger` in :mod:`tabsdatak.api`:

* :class:`TablesTrigger` -- ``trigger_by=TablesTrigger(tables=[...])``,
  the typed equivalent of ``trigger_by=["users", "orders"]``.
  Accepted on every flow primitive (publisher / transformer /
  subscriber). Names use the looser
  :data:`CollTableNamesSpec` shape so the same trigger fits all
  call sites.
* :class:`AllTablesTrigger` -- ``trigger_by=AllTablesTrigger()``,
  the typed equivalent of ``trigger_by="*"``. Accepted on
  transformers and subscribers (the only sites that take the
  wildcard).

The bare-list and bare-``"*"`` shortcuts kept working in that
revision, so the typed wrappers were sugar an LLM *could* prefer.

**Second mitigation (typed-only, current state).** The bare-list
and bare-``"*"`` shortcuts have been **removed**. ``trigger_by``
now accepts only `Trigger` subclasses
(`CronTrigger` / `TablesTrigger` / `AllTablesTrigger`). The
``AllSpec = Literal["*"]`` alias and the bare-``TableNamesSpec``
/ ``CollTableNamesSpec`` branches were dropped from
``PublisherTriggerBySpec`` / ``TransformerTriggerBySpec`` /
``SubscriberTriggerBySpec``.

Why typed-only is the right shape for LLM authoring:

* **Symbol discovery beats magic strings.** An LLM enumerating
  ``tabsdatak.api.__all__`` finds the closed set
  ``{CronTrigger, TablesTrigger, AllTablesTrigger}`` with full
  docstrings. A bare ``"*"`` is undiscoverable from the symbol
  table and easy to confuse with glob / regex / SQL ``*`` --
  the LLM is more likely to invent ``"all"`` / ``"any"`` /
  ``["*"]`` and be wrong with confidence.
* **One canonical form keeps the model consistent.** When two
  forms are valid, an LLM drifts between them across a long
  generation, producing files where adjacent decorators read
  differently for no reason. Removing the shortcut collapses the
  call site to one shape.
* **Type errors are more recoverable.** A wrong typed value
  yields ``"expected Trigger subclass, got str"`` -- one
  round-trip and the LLM corrects. A wrong shortcut yielded
  ``"expected list[str] | Literal['*'] | Trigger"`` -- three
  branches the LLM has to disambiguate from the message.
* **Slot overloading is the real foot-gun.** A bare list at
  ``trigger_by`` looked structurally identical to the lists at
  ``input_tables`` / ``output_tables`` -- same shape, different
  semantics. LLMs slip the wrong list into the wrong slot when
  the call site is long. Removing the list-branch from
  ``trigger_by`` eliminates the ambiguity entirely.

Trade-off: human authors at a REPL lose the most concise spelling
(``trigger_by="*"`` was nicer to type than
``trigger_by=AllTablesTrigger()``). The framework's primary
authoring channel is generation tooling, so the LLM-friendly
shape wins.

#### Diagnosis 12: Staging configuration shape varies by vendor  **[Partially implemented]**

**Problem.** Several destinations need extra "staging" info on
the connection before they can ingest, but the vendor-side
mechanism differs:

* ``ClickHouseConn``, ``DruidConn``, ``StarRocksConn`` --
  ``staging: CloudLocation`` (any of :class:`S3Location` /
  :class:`GCSLocation` / :class:`AzBlobLocation`).
* ``BigQueryConn`` -- ``staging: GCSLocation`` (BigQuery only
  loads from GCS).
* ``DatabricksConn`` -- ``volume: VolumeSpec`` (a Databricks
  Unity Catalog Volume reference).
* ``SnowflakeConn`` -- optional ``stage: IdentifierSpec`` (a
  pre-created Snowflake stage object that wraps the underlying
  S3 / GCS / Azure location, managed Snowflake-side).
* SQL transactional conns (``MySQLConn`` / ``PostgresConn`` /
  ``OracleConn`` / ``MSSQLConn``) and the file/object-storage
  conns need no staging.

The earlier draft (uniform ``staging: <something>`` field
everywhere) over-simplified this: Snowflake's ``stage`` and
Databricks's ``volume`` are vendor-specific names for
vendor-specific concepts, not interchangeable cloud locations.

**Mitigation in place.** Two structural fixes:

1. **All staging info now lives on the connection**, never on
   the dest descriptor. Snowflake's ``stage`` was moved from
   ``SnowflakeDest`` to ``SnowflakeConn`` so an LLM wiring the
   connection has all the info to know whether a ``stage`` is
   even relevant. (Databricks's ``volume`` was already on the
   conn; ClickHouse / Druid / StarRocks already had ``staging:
   CloudLocation`` there.)
2. **BigQuery now uses :class:`GCSLocation` for staging**
   instead of a bare ``bucket: GsUriSpec``. The shared
   ``GCSLocation`` carries bucket + base_path + credentials with
   uniform validation, so the BigQuery staging block looks
   structurally like ClickHouse / Druid / StarRocks staging
   (just narrower: only ``GCSLocation`` is accepted).

The vendor-specific *names* (``staging`` / ``stage`` /
``volume``) are deliberately kept -- they reflect real
differences in how each backend handles staging.

**Open follow-up.** The registry could still surface a hint
like ``info.staging_field`` (``"staging"`` / ``"stage"`` /
``"volume"`` / ``None``) so an LLM walking
``CONNECTORS`` can ask *"does this dest's connection take a
staging-shaped field, and what's it called?"* without parsing
each connector's docstring. Additive, non-breaking, one-line
per connector. Today the workaround is the ``staging`` string
field on :class:`ConnectorInfo` (free-form description).

#### Diagnosis 13: No concrete `AutoSrc` or `OnTablesAction` ships  **[Open]**

**Problem.** The framework defines :class:`AutoSrc` (the source
abstraction backing ``@auto_publisher``) and
:class:`OnTablesAction` (the hook abstraction for
``on_tables=[...]``) plus the matching SPI machinery
(:class:`AutoSrcDef`, :class:`AutoSrcPlugin`,
:class:`AutoSrcContext`). None of the 17 shipped ``tabsdatak``
connectors declares an ``AutoSrc`` subclass, and no
``OnTablesAction`` subclasses ship anywhere.
An LLM asked to write code using these features has nothing
concrete to instantiate.

**Status.** Structural -- these are framework hooks waiting for
their first implementations, not really an "API friendliness"
miss. The shape works (the test suite covers
``AutoPublisherInfo`` end-to-end with a test-only ``AutoSrc``);
the gap is purely "first concrete subclass per connector". Pick
a connector with a polling / file-watch source first
(``LocalFileSrc`` is the obvious candidate) and ship a
``LocalFileAutoSrc`` to seed the pattern.

#### Diagnosis 14: Field-name asymmetry across source / destination classes  **[Implemented]**

**Problem.** The "list of read sources" and "list of write
targets" fields have different names per connector kind:

* SQL / warehouse: ``queries: list[str]`` on src,
  ``tables: list[str]`` on dest;
* file / object-storage: ``path: list[str]`` on both sides;
* MongoDB: ``collections_with_ids: list[tuple[str, str|None]]``
  on dest (different *shape* too -- name + id-column);
* Salesforce: ``SalesforceSrc.queries`` (SOQL) and
  ``SalesforceReportSrc.reports`` (Salesforce Report names),
  each capped at one entry today.

LLMs targeting "MongoDB destination with these collections"
sometimes write ``tables=[...]``; the validation error is clear
("unexpected keyword 'tables'") but it costs an iteration.

**Mitigation in place.** Documentation-only fix:

1. The **Connector index** table near the top of this document
   now carries a ``Src field`` and ``Dest field`` column for
   every connector. An LLM that has read the table knows what
   to write at the constructor up front.
2. A new "Common LLM authoring traps #12" entry spells out the
   asymmetry with concrete *fine* / *rejected* examples and
   explains the rationale (the names reflect what each
   underlying engine actually accepts -- query strings vs. path
   navigation vs. per-collection id columns -- so renaming to a
   unified ``targets`` field would lose information the
   validation layer relies on).

A breaking rename to a single field name (``tables``
everywhere, say) is deliberately *not* in scope -- it would
either drop validation precision or just shuffle the asymmetry
into the type, with no payoff for first-shot LLM authoring once
the index table is consulted.

---

## What to do when something goes wrong

When code generated against `tabsdatak` raises an exception, the
exception will be a subclass of `TabsdataException` with four
useful attributes:

```python
e.api_error  # the named ErrorCode enum value
e.cause  # underlying exception (typically pydantic ValidationError)
e.extra  # dict of any kwargs passed at the raise site
e.source  # "<file>:<line>: <code>" of the user call site
#   (None only if disabled via TABSDATAK_DEBUG_FRAMES=0
#   or no user frame can be located)
str(e)  # one-line summary: "<CODE>: <message>: <cause-summary>"
#   (with " at <file>:<line>: <code>" appended when source is set)
```

`source` is populated by default — the framework's exceptions only
fire at pipeline-build time, so pinning each error to its call
site is worth the small stack-walk cost. To turn it off in
environments that don't want the capture, set
`TABSDATAK_DEBUG_FRAMES` to a falsy value:

```sh
TABSDATAK_DEBUG_FRAMES=0 python my_pipeline.py
```

Decision tree:

1. **`ApiException` subclass** — the error is in your end-user
   code (publisher/transformer/subscriber wiring, function-shape
   mismatch, bad TableNameSpec, etc.). The code (`API_4` etc.) tells
   you which decorator failed. Fix in your generated code.
2. **`ConnException` subclass** — the error is in connector
   configuration (bad URI, missing credential, schema/database
   mismatch, etc.). The code (`MYSQL_3`, `S3_5` etc.) tells you
   which connector + site. Fix in the `<V>Conn` / `<V>Src` /
   `<V>Dest` constructor.
3. **Other exception** — it's not a tabsdatak validation error;
   you've hit a runtime issue (network, file, etc.). Read the
   exception type.

The shortest debugging loop is to print `str(e)` — it's a one-liner
listing every offending field with its short reason. No
documentation URLs, no per-error stack frames.
