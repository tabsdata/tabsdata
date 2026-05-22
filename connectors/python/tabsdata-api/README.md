# tabsdata-api

> **Backward-compatibility policy.** This package is **shared** across
> every connector package and depended on by external consumers. Every
> change to its public surface MUST be backward-compatible:
>
> - Do **not** rename, remove, or change the type of an exported
>   symbol, decorator, type alias, dataclass field, error code or
>   `__kind__` tag.
> - Do **not** tighten an existing validator (anything previously
>   accepted must still be accepted).
> - Do **not** change the on-wire JSON envelope (`kind` /
>   `apiVersion` / `type` / `spec`) or the meaning of an existing
>   `apiVersion` of a class.
> - Adding new symbols, optional dataclass fields (with defaults),
>   new error codes, or a higher `apiVersion` accompanied by a
>   migration in `__migrations__` is the supported way to evolve.
>
> If you cannot avoid a breaking change, bump the package's major
> version and coordinate the rollout with every dependent connector.

Core API module for the tabsdatak package. Defines the public decorators
end users reach for (`@publisher`, `@transformer`, `@subscriber`), the
implementer-facing Service Provider Interface for connector authors, and
the private framework machinery that validates both.

## Errors live in their own module — `<module>_error.py`

Every error code and exception type tabsdatak raises lives in
`tabsdatak.api_error` (`ErrorCategory`, `ErrorDef`, `ErrorCode`,
`ApiErrorCode`, `TabsdataException`, `ApiException`,
`ApiParamTypeException`, `ApiValidateException`). They are **not**
re-exported from `tabsdatak.api`.

Rationale: code that only constructs publishers / transformers /
descriptors does not need any error symbol — it imports
`tabsdatak.api` and lets exceptions propagate. Code that *cares*
about reacting to errors (catching a particular exception, branching
on a specific `ErrorCode` member) imports from `tabsdatak.api_error`
explicitly. The dependency makes the intent visible at the import
site rather than burying it in a wildcard re-export.

The same convention applies to every connector package. Each
connector ships:

- `tabsdatak.conn.<connector>` — the public descriptor / source /
  destination classes.
- `tabsdatak.conn.<connector>_error` — the connector's
  `<Connector>ErrorCode` enum.

The shared `tabsdatak.conn.common_error` carries the cross-cutting
`ConnErrorCode`, `ConnException` (base), `ConnParamTypeException`
(raised by per-field validators / pydantic init) and
`ConnValidateException` (raised from `validate(self)` hooks). The
shared `tabsdatak.conn.common_typing` carries the credential
descriptors and cloud-location dataclasses.

## Module layout

The package ships four public + two private modules. Lower rows never
import from higher ones.

| Module                  | Audience                                                     | Exports                                                                                                                                                                                                                                                                                                                       |
| ----------------------- | ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tabsdatak.api`        | **End users** — publisher / transformer / subscriber authors | `@publisher`, `@auto_publisher`, `@transformer`, `@subscriber`; `Src`, `AutoSrc`, `Dest`, `OnTablesAction` abstract bases; `Conn`, `NamedConn`; `ValRef`, `Var`, `Secret`; `Trigger` / `CronTrigger` / `TablesTrigger` / `AllTablesTrigger`; `TableFrame`; type aliases (see "Type aliases" below). **No** error / exception re-exports. |
| `tabsdatak.api_error`  | **Anyone reacting to errors**                                | `ErrorCategory`, `ErrorDef`, `ErrorCode`, `ApiErrorCode`, `TabsdataException`, `ApiException`, `ApiParamTypeException`, `ApiValidateException`. Imported directly when callers need to catch / branch on errors.                                                                                                              |
| `tabsdatak.spi`        | **Connector implementers**                                   | `TableMode`; `SrcDef` / `AutoSrcDef` / `DestDef` (pydantic dataclasses generic over `(<Conn>, <Config>)`); `SrcPlugin` / `AutoSrcPlugin` / `DestPlugin` (generic ABCs with abstract `read_in` / `write_out`); `Explorer`; `SrcContext` / `AutoSrcContext` / `DestContext`; `ParquetFile` / `TableFileSpec` / `ManifestFileSpec`. |
| `tabsdatak.connectors` | **Framework + tooling**                                      | `avail_srcs()`, `avail_auto_srcs()`, `avail_dests()` — entry-point-driven discovery functions, each returning a fresh `dict[str, <Def>]` keyed by `*_type`. Internal helpers `_get_src` / `_get_auto_src` / `_get_dest` resolve a concrete config class to its def. |
| `tabsdatak._info`      | **Framework internals**                                      | `PublisherInfo`, `AutoPublisherInfo`, `TransformerInfo`, `SubscriberInfo` — the pydantic dataclasses that back the public decorators, plus their `@model_validator` signature checks and `func` field serde.                                                                                                                |
| `tabsdatak._api`       | **Framework internals**                                      | `dataclass` wrapper around pydantic (attaches `ApiErrorCode` tags, builds a `ConnParamTypeException` / `ApiParamTypeException` on pydantic `ValidationError`); `JsonSerde` mixin (envelope serde); `DescriptorBase` (`validate` hook + tz check); `check_tz_aware_datetimes`; `callable_to_str` / `callable_from_str`; field-level validator constants (`TABLE_NAME_VALIDATOR`, `COLL_TABLE_NAME_VALIDATOR`, `CRON_MASK_VALIDATOR`, `NAMED_CONNECTION_VALIDATOR`, `ENV_VAR_NAME_VALIDATOR`, `SECRET_REF_VALIDATOR`). |

## Type aliases (public, in `tabsdatak.api`)

| Alias                       | Definition                                                  | What it means at a field site |
| --------------------------- | ----------------------------------------------------------- | ---- |
| `TableNameSpec`             | `Annotated[str, TABLE_NAME_VALIDATOR]`                      | A bare table name (1-64 chars, `[A-Za-z][A-Za-z0-9_]*`). |
| `TableNamesSpec`                | `Annotated[list[TableNameSpec], min_length=1]`              | A non-empty list of table names. |
| `CollTableNameSpec`         | `Annotated[str, COLL_TABLE_NAME_VALIDATOR]`                 | `<name>` or `<collection>/<name>`. |
| `CollTableNamesSpec`            | `Annotated[list[CollTableNameSpec], min_length=1]`          | A non-empty list of collection-qualified names. |
| `CronMaskSpec`              | `Annotated[str, CRON_MASK_VALIDATOR]`                       | A K8s CronJob schedule. |
| `VarNameSpec`            | `Annotated[str, ENV_VAR_NAME_VALIDATOR]`                    | A POSIX var name (`[A-Za-z_][A-Za-z0-9_]*`). |
| `SecretRefSpec`             | `Annotated[str, SECRET_REF_VALIDATOR]`                      | A K8s secret reference: `<secret>#<key>`. |
| `ValOrRefSpec`       | `Union[str, ValRef]`                                        | A value that may be inline or sourced from any `ValRef`. |
| `InlineConnectionNameSpec`  | `Annotated[str, NAMED_CONNECTION_VALIDATOR]`                | A bare connection identifier (1-64 chars, `[A-Za-z][A-Za-z0-9_]*`). The `NamedConn.name` field is typed `InlineConnectionNameSpec \| Var` -- the union is spelled out at the use site rather than aliased, since a `*NameSpec` alias would falsely promise a single-string shape. |
| `*TriggerBySpec`            | Unions of `Trigger` subclasses (`CronTrigger` / `TablesTrigger` / `AllTablesTrigger`); publishers do not accept `AllTablesTrigger`. | Per-decorator firing conditions. |
| `OnTablesActionSpec`        | `Annotated[list[OnTablesAction], min_length=1]`             | Optional hooks attached to a publisher / transformer. |

## Dependency graph

Each arrow's **pointy end is on the side of the module doing the
import** -- i.e. the arrowhead sits at the importer, the tail at the
imported. So `A ◀── B` reads "A is imported into B" / "B imports A".

```
   _api    (leaf — no intra-package imports)
    ▲
    ├──◀── api          (api imports _api)
    ├──◀── _info        (_info imports _api)
    └──◀── spi          (spi imports _api)

   api
    ▲
    ├──◀── _info        (_info imports api)
    ├──◀── spi          (spi imports api)
    └──◀── connectors   (connectors imports api)

   spi
    ▲
    └──◀── connectors   (connectors imports spi)

   _info
    ▲
    └──◀── api          (api late-imports _info; resolves the
                         api ↔ _info cycle)

   connectors
    ▲
    └──◀── _info        (function-local import inside the
                         signature validators; resolves the
                         _info ↔ connectors cycle)
```

Strictly acyclic at runtime (the two late / function-local imports
break the otherwise-circular relationships):

- `_api.py` — leaf. No intra-package imports. Pure infrastructure:
  pydantic wiring, validators, signature-validation utility, the
  `JsonSerde` mixin and `DescriptorBase` mixin.
- `api.py` — imports `_api`. Declares public types, then does a late
  import of `_info` (after `Src` / `Dest` / triggers are bound in its
  namespace so the partial circle resolves), then defines the
  `publisher` / `auto_publisher` / `transformer` / `subscriber`
  factories.
- `spi.py` — imports `_api` and `api` (for `Src`, `AutoSrc`, `Dest`,
  `Conn`, `ApiErrorCode`).
- `_info.py` — imports `_api` and `api` (for `Src`, `AutoSrc`,
  `Dest`, triggers, type aliases). It looks up the binding for a
  source / destination at validation time via
  `tabsdatak.connectors._get_src` / `_get_auto_src` / `_get_dest`,
  imported function-locally inside each `@model_validator` to break
  the import cycle with `connectors` (which depends on `api`).
- `connectors.py` — imports `_api` and `spi`. Builds the
  entry-point-driven registry. Opt-in: importing `tabsdatak.api`
  alone does not load it.

## How the layers collaborate

```
    End user                 Connector author            Framework
    ──────────               ───────────────             ──────────
    api.py                   spi.py + connectors.py      _info.py, _api.py

    @publisher(              # _<name>_plugin.py         PublisherInfo (dataclass)
        source=s,             MYSQL_SRC = SrcDef[          @model_validator
        output_tables=          MySQLConn, MySQLSrc](        _get_src(type(source))
          [...])                src_type="mysql",            calls
    def my_pub(...):            cardinality=lambda s:        check_callable_signature
        ...                       len(s.queries),
                                plugin=_MySQLSrcPlugin,
                                ...)
                              # pyproject.toml
                              # entry-points."tabsdatak.connectors"
                              # src_mysql = "...:MYSQL_SRC"
```

1. An implementer ships a connector package that declares its
   `<Name>Conn`, `<Name>Src`, `<Name>Dest` descriptors in the
   public module and a binding constant per kind in
   `_<name>_plugin.py`:
   ```python
   MYSQL_SRC = SrcDef[MySQLConn, MySQLSrc](
       src_type="mysql", src_version="v1",
       src=MySQLSrc, table_mode=TableMode.SINGLE,
       cardinality=lambda src: len(src.queries),
       plugin=_MySQLSrcPlugin, explorer=_MySQLExplorer,
   )
   ```
   The package's `pyproject.toml` exposes the constant under the
   `tabsdatak.connectors` entry-point group
   (`src_mysql = "tabsdatak.conn.mysql._plugin:MYSQL_SRC"`).
2. An end user imports `MySQLSrc`, constructs it, and passes it to
   `@publisher(source=my_src, output_tables=[...])`.
3. The `publisher` factory (in `api.py`) constructs a
   `PublisherInfo` (in `_info.py`). Its
   `@model_validator(mode="after")` calls
   `tabsdatak.connectors._get_src(type(self.source))` to resolve
   the registered binding (lazy entry-point scan on first access),
   then calls `_api.check_callable_signature(...)` with:
   - `input_count  = definition.cardinality(self.source)` — per-instance
   - `input_mode`  — `"single"` (each input slot is `TableFrame` /
     `TableFrame | None`) when `definition.table_mode is SINGLE`;
     `"array"` (each input slot is `list[TableFrame]` /
     `list[TableFrame | None]`) when `definition.table_mode is ARRAY`
   - `output_count = len(self.output_tables)`
4. Any shape mismatch raises `ValueError`, pydantic wraps it as
   `ValidationError`, and the `_api.dataclass` wrapper tags it as
   `ApiParamTypeException(API_4, cause)`.

Transformers (`API_5`) and subscribers (`API_6`) follow the same
pattern: `input_count = len(self.input_tables)` (with
`input_mode="any"` — the framework does not yet enforce a slot
mode there); `output_count = len(self.output_tables)` for
transformers and `_get_dest(type(self.destination)).cardinality(self.destination)`
for subscribers. Auto-publishers (`API_10`) mirror the publisher
shape but call `_get_auto_src` instead of `_get_src`.

## JSON serde

Every public class transparently round-trips through JSON. The on-wire
envelope is:

```json
{
  "kind":       "<resource kind>",
  "apiVersion": "1.0",
  "type":       "<module>:<qualname>",
  "spec":       { ... fields ... }
}
```

`kind` tags are stable per hierarchy:

| Class                | `__kind__`         |
| -------------------- | ------------------ |
| `Trigger`            | `trigger`          |
| `Conn`               | `connectionDef`    |
| `NamedConn`          | `connectionRef`    |
| `Src`                | `sourceDef`        |
| `AutoSrc`            | `autoSourceDef`    |
| `Dest`               | `destinationDef`   |
| `OnTablesAction`     | `onTablesAction`   |
| `ValRef`             | `valueRef`         |
| `Var`                | `varRef`           |
| `Secret`             | `secretRef`        |
| `PublisherInfo`      | `publisherDef`     |
| `AutoPublisherInfo`  | `autoPublisherDef` |
| `TransformerInfo`    | `transformerDef`   |
| `SubscriberInfo`     | `subscriberDef`    |

Methods are inherited (zero per-class boilerplate):

```python
obj.to_dict()   obj.to_json()
Cls.from_dict(d)   Cls.from_json(s)
```

`Cls.from_dict(envelope)` resolves the `type` string via
`importlib.import_module` plus attribute walk, enforces
`issubclass(resolved, Cls)` (rejecting unrelated types from untrusted
input), and dispatches to the concrete class. Polymorphic loads
work — call `Conn.from_json(s)` and you get the right subclass back.

Schema evolution is supported via `__migrations__` on each class:

```python
__migrations__: ClassVar[dict[str, tuple[str, Callable[[dict], dict]]]] = {
    "1.0": ("1.1", _migrate_1_0_to_1_1),
    "1.1": ("1.2", _migrate_1_1_to_1_2),
}
```

The chain is walked on load until the envelope's `apiVersion` matches
the class's current `__api_version__`.

### Caveats

- **Closures don't round-trip.** A function with `<locals>` in its
  qualname can't be re-resolved via importlib. Pipeline functions and
  classes intended for serde must be defined at module top level.
- **Module path is the contract.** Renaming the module a class lives in
  is a breaking change for any persisted JSON.
- **Importing the module runs its top-level code.** Resolution is
  implicit `import` — keep top-level side effects modest.

## Authoring guidance for descriptor subclasses

`Conn` / `Src` / `AutoSrc` / `Dest` / `Trigger` subclasses are
intended as **pure data classes**:

- Declare typed fields and nothing else. No business logic, no I/O,
  no plugin-style methods. Runtime behavior belongs on the
  `SrcPlugin` / `AutoSrcPlugin` / `DestPlugin` / `Explorer`
  classes that the connector wires into its `SrcDef` /
  `AutoSrcDef` / `DestDef` binding constant in
  `_<name>_plugin.py`.
- Derive computed values via read-only `@property`, not methods.
- Enforce cross-field invariants by overriding the inherited
  `validate(self)` hook (default no-op, called automatically after
  pydantic's per-field validation). Raise the descriptor's own
  `VALIDATION`-category code via `code.exception(...)` -- it
  resolves to a `ConnValidateException` / `ApiValidateException`
  for connector / api descriptors respectively.

Ideally a descriptor subclass has zero methods other than `validate`.

```python
@dataclass
class MyConn(Conn):
    host: str | None = None
    port: int | None = None
    tls: bool = False

    def validate(self):
        if self.tls and self.port is None:
            raise ValueError("tls=True requires an explicit port")
```

## Field-value contract on descriptor subclasses

`Conn` / `Src` / `AutoSrc` / `Dest` (and `Trigger`) install a shared
`@model_validator(mode="after")` that enforces, after pydantic's normal
field validation:

- Pydantic's lax type coercion applies — `str` <-> `int` / `float` /
  `bool`, ISO 8601 strings to `datetime` / `date` / `time`, numeric
  Unix timestamps to `datetime`, JSON-serialised lists / dicts to typed
  collections, `Enum` from underlying value, bytes <-> base64,
  `JsonSerde` envelope dicts to the matching subclass.
- **Every direct datetime field must be timezone-aware.** Naive values
  (`tzinfo is None`) are rejected — including ISO strings without an
  offset (`"2024-01-15T10:00:00"`) which pydantic would otherwise
  coerce to a naive `datetime`. Pass `...+00:00` / `...Z`, or
  `datetime(..., tzinfo=timezone.utc)`.

To opt into stricter behavior on a particular field, use pydantic's
`Annotated[..., StrictStr]` / `Field(strict=True)` mechanisms.

> **Both rules apply only to pydantic-managed subclasses** (`BaseModel`
> or `@pydantic.dataclasses.dataclass`). Stdlib `@dataclass` and plain
> Python classes bypass pydantic's schema generation, so tabsdata
> performs **no automatic type coercion and no validation** on them at
> construction. JSON serde via `to_json` / `from_json` still works,
> but inputs are accepted verbatim. If you need coercion / validation
> in a non-pydantic class, implement it yourself — the recommended
> approach is simply to use pydantic.

## Validators (in `tabsdatak._api`)

Each validator constant is exposed for use in `Annotated[str, ...]`
type aliases:

| Constant                       | What it validates                                                       |
| ------------------------------ | ----------------------------------------------------------------------- |
| `TABLE_NAME_VALIDATOR`         | 1-64 char identifier (`[A-Za-z][A-Za-z0-9_]*`).                         |
| `COLL_TABLE_NAME_VALIDATOR`    | `<name>` or `<collection>/<name>`.                                      |
| `CRON_MASK_VALIDATOR`          | K8s CronJob schedule (5-field cron, or `@hourly` / `@daily` / etc.).    |
| `NAMED_CONNECTION_VALIDATOR`   | Same shape as a table identifier.                                       |
| `ENV_VAR_NAME_VALIDATOR`       | POSIX env var: `[A-Za-z_][A-Za-z0-9_]*`.                                |
| `SECRET_REF_VALIDATOR`         | `<secret>#<key>` — secret as RFC 1123 DNS subdomain (≤253 chars), key as `[-._a-zA-Z0-9]+`. |

## Running the tests

From the tabsdata-os repo root:

```bash
pytest connectors/python/tabsdata-api
```

Or `cargo make test` (matches CI). See `connectors/python/guides/README.md`
for the full setup and the conda-env install option.
