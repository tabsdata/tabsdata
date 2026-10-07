<!--
Copyright 2026 Tabsdata Inc.
-->

# Agent guide — tabsdatak connectors

Read this before authoring code that uses the `tabsdatak.api` surface.
The codebase is small but has a few load-bearing conventions; missing
them produces validation errors at runtime that look like pydantic
noise but trace back to deliberate design decisions.

## What this codebase is

A set of ~21 packages under `connectors/python/tabsdata-*` in
tabsdata-os that share the `tabsdatak` namespace and expose four
user-facing decorators:

- `@publisher(source, output_tables, ...)` — reads from a `Src`,
  writes named tables. Triggered by upstream events / cron / table
  arrivals.
- `@stream_publisher(source, output_tables, batch_min_secs=..,
  batch_max_secs=.., batch_idle_secs=.., batch_max_rows=..,
  batch_max_bytes=.., ...)` — reads from a `StreamSrc` (CDC, Kafka).
  Runs a long-lived stream loop and stages batches by its thresholds;
  one of the three `batch_max_*` is required.
- `@transformer(input_tables, output_tables, ...)` — reads tables,
  writes tables.
- `@subscriber(destination, input_tables, ...)` — reads tables,
  writes to a `Dest`.

`Src` / `StreamSrc` / `Dest` are abstract bases in `tabsdatak.api`;
`Conn` is the abstract connection base in `tabsdatak.spi`. Connector
packages (`tabsdata-conn-mysql`, etc.) ship concrete subclasses —
a `<Vendor>Conn` per connector, plus `<Vendor>Src` / `<Vendor>Dest`.
A `Src` / `Dest` carries only its own config and has **no `conn`
field**: the connection is registered on the function's collection
and the runtime resolves it and hands the `<Vendor>Conn` instance to
the connector's plugin at run time.

## Public symbols — where to import each from

Every name you should ever type in user code is in one of four modules.
Anything in `tabsdatak._api` or `tabsdatak._info` is framework-internal
and must not be imported.

### `from tabsdatak.api import …`  (end-user surface)

| Kind        | Symbols |
| ----------- | ------- |
| Decorators  | `publisher`, `stream_publisher`, `transformer`, `subscriber` |
| Triggers    | `Trigger` (abstract), `CronTrigger`, `TablesTrigger`, `AllTablesTrigger` |
| Value refs  | `Secret` |
| Bases       | `Src` (abstract), `StreamSrc` (abstract), `Dest` (abstract), `OnTablesAction` (abstract), `TableFrame` |
| Specs (validated string types) | `TableNameSpec`, `TableNamesSpec`, `CollTableNameSpec`, `CollTableNamesSpec`, `CronMaskSpec`, `SecretRefSpec` |
| Specs (composed) | `PublisherTriggerBySpec`, `TransformerTriggerBySpec`, `SubscriberTriggerBySpec`, `OnTablesActionSpec`, `FallbackRolloverMinsSpec` (`Annotated[int, ge=1]`) |

`ErrorCategory`, `ErrorDef`, `ErrorCode`, `ApiErrorCode`,
`TabsdataException`, `ApiException`, `ApiParamTypeException`,
`ApiValidateException` are *not* re-exported from `tabsdatak.api`
— import them from `tabsdatak.api_error`. The same convention
applies to connectors: each `tabsdata-conn-<x>` package ships a
public `tabsdatak.conn.<x>_error` module with its own
`<X>ErrorCode`.

### `from tabsdatak.connectors import …`  (runtime kind-binding registry)

Opt-in registry of every installed `SrcDef` / `StreamSrcDef` /
`DestDef` binding. Populated lazily from the
``tabsdatak.connectors`` entry-point group on the first call to
one of the discovery functions; importing :mod:`tabsdatak.api`
does **not** trigger the scan. A connector that fails to load
warns and is skipped rather than breaking the registry.

| Kind        | Symbols |
| ----------- | ------- |
| Discovery   | `avail_srcs() -> dict[str, SrcDef]` |
|             | `avail_stream_srcs() -> dict[str, StreamSrcDef]` |
|             | `avail_dests() -> dict[str, DestDef]` |

Each dict is keyed by ``type_``, unique across all plugins (e.g.
``"mysql-sql-in"``, ``"salesforce-report-in"``). Each binding exposes the
concrete config class (``.src`` / ``.dest``), ``cardinality``
helper, ``plugin``, ``explorer``, and ``table_mode`` (for
``SrcDef`` / ``StreamSrcDef``).

### `from tabsdatak.conn.connectors import …`  (vendor-level index)

Hand-curated, lazy index of every vendor that ships under
`connectors/python/tabsdata-*`. Lives in
:mod:`tabsdatak.conn.connectors` (the ``tabsdata-conn-common``
package). Used by tooling that needs a package-level view without
loading every connector module.

| Kind        | Symbols |
| ----------- | ------- |
| Type        | `ConnectorInfo` (frozen dataclass) |
| Registry    | `CONNECTORS: dict[str, ConnectorInfo]` |
| Helpers     | `list_connectors(role=None, tag=None)`, `import_connector(name)` |

`ConnectorInfo` fields:

* `name`           — vendor key (`"mysql"`, `"snowflake"`, …).
* `module`         — dotted import path of the public connector module.
* `src_classes`    — tuple of `Src` subclass *names* (strings).
* `dest_classes`   — tuple of `Dest` subclass *names*.
* `conn_classes`   — tuple of `Conn` subclass *names*.
* `kind`           — human label (`"SQL OLTP"`, `"OLAP SQL"`, …).
* `kind_tags`      — tuple of structured tags for filtering
  (`"sql"`, `"oltp"`, `"warehouse"`, `"olap"`, `"file"`,
  `"object-storage"`, `"saas"`, `"nosql"`, `"local"`,
  `"lakehouse"`).
* `staging`        — human description of the dest-side staging
  requirement (`None` / `"self"` / `"GCSLocation (required) on
  the dest connection"` / …).
* `roles`          — convenience property: `("src",)`, `("dest",)`,
  or `("src", "dest")`.

Class names are stored as strings so building the index does not
import the connector modules. Resolve to actual classes via
`import_connector` and `getattr`.

### `from tabsdatak.spi import …`  (connector implementer surface)

`spi` re-exports only the symbols a connector author needs. If you
also need `Src` / `StreamSrc` / `Dest`, import them from
`tabsdatak.api`; if you need `ApiErrorCode`, import it from
`tabsdatak.api_error`.

| Kind          | Symbols |
| ------------- | ------- |
| Connection    | `Conn` (abstract; subclass as `<Name>Conn`). Connections are not declared in user flow code — they are registered on the function's collection and resolved by the runtime; the descriptor (`Src` / `Dest`) carries no `conn` field. |
| Kind bindings | `SrcDef[C, S]`, `StreamSrcDef[C, AS]`, `DestDef[C, D]` (frozen pydantic dataclasses; instantiate one per concrete `Src` / `StreamSrc` / `Dest` subclass and expose it via the `tabsdatak.connectors` entry-point group; `conn=<Name>Conn` on each binds it to its connection type) |
| Plugins       | `SrcPlugin[C, S]`, `StreamSrcPlugin[C, AS]`, `DestPlugin[C, D]` (abstract; subclass and implement `read_in` / `write_out`) |
| Contexts      | `SrcContext`, `StreamSrcPluginCtx`, `DestContext` (per-call handles passed to plugins; expose `work_dir()` etc.) |
| Helper        | `Explorer[C]` (abstract — schema / metadata extractor) |
| Enum          | `TableMode` (`SINGLE` / `ARRAY`) |
| File aliases  | `ParquetFile` (`pathlib.Path` subtype), `TableFileSpec`, `ManifestFileSpec` |

### `from tabsdatak.conn.<vendor> import …`  (connector packages)

Vendor-specific concrete classes. Example for MySQL:

| Symbols (from `tabsdatak.conn.mysql`) |
| --- |
| `MySQLConn`, `MySQLSrc`, `MySQLDest`, `QueriesSpec`, `MySQLTableNamesSpec` |

Cross-cutting types live in `tabsdatak.conn.common_typing`
(credentials, cloud locations, `ConnCfgSpec`),
`tabsdatak.conn.sql_common_typing` (SQL-shaped helpers like
`SchemaOverrideSpec`), and `tabsdatak.conn.file_common_typing`
(file-shaped enums and path / format-cfg specs for the localfile /
s3 / gcs / azureblob connectors).

## How to author code (end user)

```python
from tabsdatak.api import publisher, CronTrigger
from tabsdatak.tableframe import TableFrame
from tabsdatak.conn.mysql import MySQLSrc

# No connection here: the MySQL connection is registered on the
# function's collection and resolved by the runtime. The source
# carries only its own config.
src = MySQLSrc(
    queries=["SELECT * FROM users"],
)


@publisher(
    source=src,
    output_tables=["users_out"],
    trigger_by=CronTrigger(mask="@hourly"),
)
def my_pub(users: TableFrame) -> TableFrame:
    return users
```

Notes the framework enforces:

- The first `N` positional parameters are the source slots, where
  `N` is `cardinality(source)` from the `SrcDef` registered for
  `type(source)` (the framework looks this up internally via the
  ``tabsdatak.connectors`` entry-point registry). Each slot is
  `TableFrame` for SINGLE-mode sources or `list[TableFrame]` for
  ARRAY-mode sources. `TableFrame` is shorthand for
  `TableFrame | None` (auto-nullable); for `list[TableFrame]` the
  list itself is required, but its elements may be `None`.
  Parameters past the first `N` are not type-checked.
- The return must be `tuple[TableFrame, ...]` of exactly
  `len(output_tables)` elements (each `TableFrame` is
  auto-nullable, so a slot may be returned as `None`). A
  single-output function may return the lone `TableFrame` bare,
  without the enclosing 1-tuple.
- Shape mismatches and argument-type mismatches both raise
  `ApiParamTypeException(api_error=API_4)` at decoration time
  (when `PublisherInfo` is constructed). The same applies for
  `@stream_publisher` (API_18), `@transformer` (API_5), and
  `@subscriber` (API_6). There is no separate factory-level
  validation layer; the `*Info` dataclass is the single
  validation surface.

## How to author a connector (implementer)

A connector is one wheel under
`connectors/python/tabsdata-conn-<name>/` in tabsdata-os that
contributes modules to the `tabsdatak.conn` namespace and registers
its bindings via the `tabsdatak.connectors` entry-point group. The
full reference is in `CONN_DEV.md`; this is the shape at a glance.

Layout (per-connector, tabsdata-os connector shape):

```
tabsdata-conn-<name>/
├── pyproject.toml          # setuptools, entry-points, [tool.tabsdata], ruff extends root
├── setup.py                # one-liner: tabsdatax.setup.builder.connector.build
├── setup.cfg               # egg_info
├── requirements.txt        # aggregator
├── requirements-dev.txt    # aggregator
├── requirements/{first,third}-party.txt + dev-{first,third}-party.txt
└── src/main/python/tabsdatak/conn/<name>/
    ├── __init__.py        # public: descriptors + type aliases
    ├── _validation.py     # private: ValueConstraints / Rule constants
    ├── _plugin.py         # private: plugins, explorer, binding constants
    ├── error.py           # public: ErrorCode enum
    └── assets/manifest/{VERSION, README-PyPi.md, RELEASE-NOTES, THIRD-PARTY}
```

Tests under `src/utest/python/test_tabsdatak/conn/<name>/` with
per-connector `bootest.py` / `conftest.py` / `pytest.ini` /
`.coveragerc`, and `pkgutil.extend_path` `__init__.py` at every
level of the test tree.

`__init__.py` — the public descriptor module. **Use pydantic** (`BaseModel`
or `@pydantic.dataclasses.dataclass`); stdlib `@dataclass` works for
JSON serde but bypasses the framework's type coercion and the
timezone-aware datetime validator. First-party connectors use the
internal wrapper `@_api.dataclass(<ErrorCode>)` which tags
`ValidationError` with a stable `ErrorCode`; third-party authors
can use plain pydantic.

```python
from typing import Annotated, TypeAlias, Union

from pydantic.dataclasses import dataclass

from tabsdatak.api import Dest, Src, StrOrSecretSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.my._validation import URI_VALIDATION  # ValueConstraints
from tabsdatak.spi import Conn

# One `ValueConstraints` gates both arms of a str-or-secret field --
# never write the rule twice.
UriSpec: TypeAlias = Annotated[StrOrSecretSpec, URI_VALIDATION]


@dataclass(kw_only=True)
class MyConn(Conn):
    uri: UriSpec
    credentials: UserPassword
    conn_cfg: CfgSpec = None


@dataclass(kw_only=True)
class MySrc(Src):
    queries: list[str]


@dataclass(kw_only=True)
class MyDest(Dest):
    tables: list[str]
```

`MySrc` / `MyDest` have **no `conn` field**. The connection is
registered on the function's collection; the runtime resolves it and
passes the `MyConn` instance to the plugin. The source/destination is
bound to its connection *type* in the `SrcDef(conn=MyConn, ...)` /
`DestDef(conn=MyConn, ...)` binding below.

`_plugin.py` — runtime wiring. Subclass `SrcPlugin` /
`DestPlugin` / `Explorer` and instantiate one `SrcDef` / `DestDef`
per concrete config class. The `cardinality` callable returns the
slot count for a given source / destination instance.

```python
from tabsdatak.conn.my import MyConn, MyDest, MySrc
from tabsdatak.spi import (
    DestDef, DestPlugin, Explorer,
    SrcPluginCtx, SrcDef, SrcPlugin, TableFileSpec, TableMode,
)


# The runtime resolves the connection from the collection and passes
# the `MyConn` instance to each plugin method as `conn`.
class _MySrcPlugin(SrcPlugin[MyConn, MySrc]):
    def read_in(self, ctx: SrcPluginCtx, conn: MyConn, src: MySrc) -> list[list[TableFileSpec]]: ...


class _MyDestPlugin(DestPlugin[MyConn, MyDest]):
    def write_out(self, ctx, conn: MyConn, dest: MyDest, tables: list[TableFileSpec]) -> None: ...


class _MyExplorer(Explorer[MyConn]):
    def metadata(self, conn: MyConn): ...

    def data(self, conn: MyConn, query: str): ...


# `conn=MyConn` binds the source/destination to its connection type.
MY_SRC = SrcDef[MyConn, MySrc](
    type_="my-sql-in", src_version="v1",
    conn=MyConn, src=MySrc, table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_MySrcPlugin, explorer=_MyExplorer,
)

MY_DEST = DestDef[MyConn, MyDest](
    type_="my-sql-out", dest_version="v1",
    conn=MyConn, dest=MyDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_MyDestPlugin, explorer=_MyExplorer,
)
```

`pyproject.toml` — one entry per binding (not per package), under
the `tabsdatak.connectors` group:

```toml
[project.entry-points."tabsdatak.connectors"]
src_my  = "tabsdatak.conn.my._plugin:MY_SRC"
dest_my = "tabsdatak.conn.my._plugin:MY_DEST"
```

The vendor itself is registered separately, in
`tabsdata-conn-common`'s hand-curated
`CONNECTORS: dict[str, ConnectorInfo]` in
`tabsdatak/conn/connectors.py`. Add an entry there listing
`src_classes` / `dest_classes` / `conn_classes` (all *strings*),
`kind`, `kind_tags`, and any `staging` description.

Key decisions:

- **Keep descriptor subclasses pure.** `Conn` / `Src` / `StreamSrc` /
  `Dest` / `Trigger` subclasses are data classes — typed fields,
  optional read-only `@property` for derived values, and (ideally)
  no other methods. Runtime behaviour belongs on the
  `SrcPlugin` / `StreamSrcPlugin` / `DestPlugin` classes referenced
  by the connector's `SrcDef` / `StreamSrcDef` / `DestDef`, not on
  the descriptor itself. The only exception is `validate(self)`,
  which the framework calls automatically post-construction so
  you can enforce cross-field invariants:

  ```python
  @dataclass(kw_only=True)
  class MyConn(Conn):
      host: str | None = None
      port: int | None = None
      tls: bool = False

      def validate(self) -> None:
          if self.tls and self.port is None:
              raise MyErrorCode.MY_5.exception(
                  port=self.port, tls=self.tls,
              )
  ```

  Cross-field `validate(self)` failures raise `ConnValidateException`
  (from `tabsdatak.conn.common_error`) tagged with the connector's
  `VALIDATION` code. Per-field `ValueConstraints` failures raise
  `ConnParamTypeException` tagged with the connector's `PARAM_TYPE`
  code. The single `code.exception(...)` factory dispatches via
  `<Name>ErrorCode.exception_classes()`.
- Secret-bearing fields use `StrOrSecretSpec` and are gated by a single
  `ValueConstraints` covering both arms (e.g.
  `Annotated[StrOrSecretSpec, URI_VALIDATION]`). There is no `ValRef`
  base or `ValOrRefSpec` alias any more, and the rule is never spelled
  once per arm.
- Polymorphic field types (`source: Src`, `destination: Dest`) "just
  work" — no `SerializeAsAny` annotation needed; the JsonSerde schema
  dispatches by actual class. `Conn` subclasses round-trip the same
  way (`Conn.from_json(...)` returns the right subclass), even though
  a connection is no longer carried as a field on a `Src` / `Dest`.

## JSON serde

Every public class round-trips through JSON automatically. The
`@publisher` / `@stream_publisher` / `@transformer` / `@subscriber`
decorators each return a `PublisherInfo` / `StreamPublisherInfo` /
`TransformerInfo` / `SubscriberInfo` instance (framework-internal
class; reachable via `from tabsdatak._info import PublisherInfo`
when you need the type for `from_json`).

```python
from tabsdatak.api import publisher
from tabsdatak.tableframe import TableFrame


@publisher(source=src, output_tables=["out_a", "out_b"])
def my_pub(users: TableFrame) -> tuple[TableFrame, TableFrame]:
  return (users, None)


s = my_pub.to_json()  # envelope-wrapped string

from tabsdatak._info import PublisherInfo

my_pub2 = PublisherInfo.from_json(s)  # round-trip
```

The envelope:

```json
{"kind": "...", "apiVersion": "1.0", "type": "module:Class", "spec": {"...": "..."}}
```

Polymorphic load works through any abstract base:

```python
from tabsdatak.spi import Conn
back = Conn.from_json(some_envelope_str)   # returns the right subclass
```

### What can break round-tripping

- **Closures.** A function with `<locals>` in its `__qualname__` can't
  round-trip. Pipeline functions must be defined at module top level.
- **Local classes.** Same constraint as closures — defining a
  `Conn` subclass inside a test function will fail at dump time.
- **Module renames.** The wire format embeds `module:qualname`.
  Renaming the module a class lives in invalidates persisted JSON.

## Validation contract

All API-boundary failures raise an `ApiException` subclass —
`ApiParamTypeException` (single-attribute / pydantic init /
`ValueConstraints`) or `ApiValidateException` (cross-field
`validate(self)` hook). Both carry:

- `.api_error: ErrorCode` — enum tag identifying *which* boundary
  failed.
- `.cause: Exception | None` — the underlying error (typically a
  pydantic `ValidationError`); also wired to `__cause__` so
  traceback chaining works without `raise … from e`.
- `.extra: dict[str, Any]` — the kwargs passed at the raise site;
  same kwargs feed the `ErrorDef.message_template`.

Tag → site mapping is documented in the `ApiErrorCode` enum (in
`tabsdatak.api_error`). Per-connector codes live in
`tabsdatak.conn.<x>_error`. When writing tests, prefer asserting
the tag (and catching the broadest applicable parent so either
leaf type matches):

```python
from tabsdatak.api import CronTrigger
from tabsdatak.error import ApiErrorCode, ApiException

with pytest.raises(ApiException) as excinfo:
  CronTrigger(mask="not a cron mask")
assert excinfo.value.api_error is ApiErrorCode.API_2
```

### Field-value rules on descriptor subclasses

Every **pydantic-managed** `Src` / `StreamSrc` / `Dest` / `Conn`
(and `Trigger`) subclass inherits a shared
`@model_validator(mode="after")` that enforces:

- **Lax type coercion.** Pydantic auto-coerces inbound values into
  declared field types: `str` ↔ numbers / bools, ISO 8601 strings →
  `datetime` / `date` / `time`, numeric Unix timestamps → `datetime`,
  JSON lists / dicts → typed collections, enum values → enum members,
  bytes ↔ base64, envelope dicts → the right `JsonSerde` subclass.
- **Timezone-aware datetimes.** Any direct datetime field with
  `tzinfo is None` raises. ISO strings without an offset
  (`"2024-01-15T10:00:00"`) are also rejected — they'd coerce to a
  naive datetime. Use `...+00:00` / `...Z`, or pass a constructed
  `datetime(..., tzinfo=timezone.utc)`.

If you need stricter behavior on a specific field, annotate it with
pydantic's `StrictStr` / `Field(strict=True)` etc.

> **Tabsdata does not provide automatic type coercion or validation
> for stdlib `@dataclass` or plain Python classes.** Pydantic's
> schema generation only fires for `BaseModel` and
> `@pydantic.dataclasses.dataclass`; everything else bypasses both
> coercion and the tz check. JSON serde still works — inputs are just
> accepted verbatim. If you need validation, use pydantic.

## Tests

From the tabsdata-os repo root:

```bash
pytest                                                                   # all collected tests
pytest connectors/python/tabsdata-conn-mysql                            # one connector
pytest -k "round_trips"                                                  # filter
```

Or via the project task runner: `cargo make test` (preferred, matches
CI). Each connector ships its own `conftest.py` / `bootest.py` /
`pytest.ini` / `.coveragerc` under
`src/utest/python/test_tabsdatak/conn/<name>/`, and pytest's namespace
package support stitches the `test_tabsdatak.*` tree together across
installed sibling packages via `pkgutil.extend_path`.

## Things to NOT do

- Don't import from `tabsdatak._info` or `tabsdatak._api` in user
  code. They're framework-internal and the interface is not stable.
  The public bases (`Src` / `StreamSrc` / `Dest` / `Trigger` /
  `OnTablesAction`, plus `Conn` on the connector-implementer `spi`
  surface) provide everything you need, including JSON serde via
  inheritance.
- Don't add `SerializeAsAny` annotations. The `JsonSerde` schema handles
  polymorphic dispatch directly; the annotation is unnecessary and
  actively breaks stdlib-dataclass nesting.
- Don't add `__init__.py` files under `tabsdatak/` or any shared
  intermediate directory. The shared namespace works because those
  packages are *implicit* (PEP 420). Adding `__init__.py` turns them
  back into regular packages and breaks cross-distribution imports.
- Don't use `@_api.dataclass(...)` from outside framework code. It is
  framework-internal and not part of the public contract. Use plain
  pydantic / stdlib dataclasses for your own classes — they round-trip
  through serde identically.

## Where to look first

| Question                                  | Read |
| ----------------------------------------- | ---- |
| What types should I import?               | `tabsdatak/api.py` (module docstring) |
| How does serde work?                      | `tabsdatak/_api.py` (`JsonSerde` class + module-level comments) |
| What `ApiError` tags exist and where?     | `tabsdatak/_api.py` (`ApiError` enum) |
| How do I add a new connector kind?        | `tabsdatak/spi.py` (module docstring) |
| Workspace setup, test commands, layout    | `connectors/python/guides/README.md` |
| API reference for the core package        | `connectors/python/tabsdata-api/README.md` |
