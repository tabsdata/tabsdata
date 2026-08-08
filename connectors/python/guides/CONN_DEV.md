<!--
Copyright 2026 Tabsdata Inc.
-->

# Connector cheat sheet

One-page reference for authoring a `tabsdata-conn-<name>` package.
Pair this with `AGENTS.md` (overall framework conventions) and the
shipped MySQL connector
(`connectors/python/tabsdata-conn-mysql/`) as the canonical example.

## Package layout

Each connector follows the tabsdata-os connector shape (Maven-style
src tree + setuptools + split requirements):

```
tabsdata-conn-<name>/
├── pyproject.toml          # setuptools build, [project.entry-points."tabsdatak.connectors"],
│                            # [tool.tabsdata], [tool.ruff] extends root, etc.
├── setup.py                # one-liner: tabsdatax.setup.builder.connector.build
├── setup.cfg               # egg_info location
├── requirements.txt        # aggregator: -r requirements/requirements-{first,third}-party.txt
├── requirements-dev.txt    # aggregator: -r requirements/requirements-dev-{first,third}-party.txt
├── requirements/
│   ├── requirements-first-party.txt        # pinned sibling tabsdata-* deps
│   ├── requirements-third-party.txt        # external runtime deps
│   ├── requirements-dev-first-party.txt    # internal test deps (often empty)
│   └── requirements-dev-third-party.txt    # external test deps (docker, dbapi drivers, …)
└── src/
    ├── itest/{python,resources,scripts}/.gitkeep
    ├── main/
    │   ├── python/
    │   │   └── tabsdatak/conn/<name>/
    │   │       ├── __init__.py             # public: descriptors + type aliases
    │   │       ├── _validation.py          # private: ValueConstraints / Rule constants
    │   │       ├── _plugin.py              # private: plugins, explorer, binding constants
    │   │       ├── error.py                # public: ErrorCode enum
    │   │       └── assets/manifest/
    │   │           ├── VERSION
    │   │           ├── README-PyPi.md
    │   │           ├── RELEASE-NOTES
    │   │           └── THIRD-PARTY
    │   ├── resources/.gitkeep
    │   └── scripts/.gitkeep
    └── utest/
        ├── python/test_tabsdatak/conn/<name>/
        │   ├── __init__.py                 # pkgutil.extend_path at every level
        │   ├── .coveragerc                 # source = tabsdatak.conn.<name>
        │   ├── pytest.ini                  # full markers list (synced by `cargo make lock`)
        │   ├── bootest.py                  # TESTING_RESOURCES_PATH + sys.path bootstrap
        │   ├── conftest.py                 # wires test_tabsdata.bootest / .conftest
        │   └── test_*.py
        ├── resources/test_tabsdatak/conn/<name>/__init__.py    # pkgutil
        └── scripts/.gitkeep
```

`tabsdatak/`, `tabsdatak/conn/` are PEP-420 namespace packages — no
`__init__.py` at those levels in connector packages. The platform owns
`tabsdatak/__init__.py` once (with `pkgutil.extend_path`). Each
connector ships its own `<name>/__init__.py` that re-exports the
public surface. Tests live under `src/utest/python/test_tabsdatak/conn/<name>/`
with `pkgutil.extend_path` `__init__.py` at every level of the test
tree (`test_tabsdatak/`, `test_tabsdatak/conn/`, `test_tabsdatak/conn/<name>/`)
to ease the future `tabsdatak` → `tabsdata` namespace merge.

## `<name>/__init__.py` — descriptor-only (no SPI imports)

```python
from __future__ import annotations
from typing import Annotated, TypeAlias, Union

import tabsdatak._api as _api
from tabsdatak.api import Dest, Src, StrOrSecretSpec
from tabsdatak.conn. < name >._validation
import URI_VALIDATION  # noqa
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn. < name >.error
import < Name > ErrorCode
from tabsdatak.spi import Conn

__all__ = ["<Name>Conn", "<Name>Src", "<Name>Dest",
           "CredentialsSpec", "UriSpec", ...]

CredentialsSpec: TypeAlias = UserPassword
# A field that accepts a plain string or a `Secret`. One
# `ValueConstraints` gates both arms -- never write the rule twice.
# The rules live in `_validation.py` next to their pattern; each carries
# the error code reported when it fails, so a rejection names the broken
# rule instead of dumping the regex at the user.
UriSpec: TypeAlias = Annotated[StrOrSecretSpec, URI_VALIDATION]


@_api.dataclass( < Name > ErrorCode. < NAME > _1

, kw_only = True)

class <Name > Conn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec


@_api.dataclass( < Name > ErrorCode. < NAME > _2

, kw_only = True)

class <Name > Src(Src):
    queries: list[str]


@_api.dataclass( < Name > ErrorCode. < NAME > _3

, kw_only = True)

class <Name > Dest(Dest):
    tables: list[str]
```

`<Name>Src` / `<Name>Dest` carry **only their own config** — no
`conn` field. The connection is registered on the function's
collection; the runtime resolves it and passes the `<Name>Conn`
instance to the plugin's `read_in` / `write_out` (see `_plugin.py`).
The source/destination is *bound* to its connection type in the
`SrcDef(conn=<Name>Conn, ...)` / `DestDef(conn=<Name>Conn, ...)`
binding, not via a field on the descriptor.

Dest-only / src-only connectors omit the side they don't ship.

## `<name>/_plugin.py` — runtime wiring

```python
from __future__ import annotations
from tabsdatak.conn.<name> import <Name>Conn, <Name>Dest, <Name>Src
from tabsdatak.spi import (
    DestDef, DestPlugin, Explorer, SrcDef, SrcPlugin, TableMode,
)


# The runtime resolves the connection from the function's collection
# and passes the `<Name>Conn` instance to every plugin method.
class _<Name>SrcPlugin(SrcPlugin[<Name>Conn, <Name>Src]):
    def read_in(self, ctx, conn, src): ...

class _<Name>DestPlugin(DestPlugin[<Name>Conn, <Name>Dest]):
    def write_out(self, ctx, conn, dest, tables): ...

class _<Name>Explorer(Explorer[<Name>Conn]):
    def metadata(self, conn): ...
    def data(self, conn, query): ...


# `conn=<Name>Conn` binds this source to its connection *type*; the
# registry/runtime uses it to resolve and inject the connection.
<NAME>_SRC = SrcDef[<Name>Conn, <Name>Src](
    type_="<name>-sql-in", src_version="v1",
    conn=<Name>Conn, src=<Name>Src, table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_<Name>SrcPlugin, explorer=_<Name>Explorer,
)

<NAME>_DEST = DestDef[<Name>Conn, <Name>Dest](
    type_="<name>-sql-out", dest_version="v1",
    conn=<Name>Conn, dest=<Name>Dest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_<Name>DestPlugin, explorer=_<Name>Explorer,
)
```

## `<name>/error.py` — error enum

```python
from tabsdatak.error import _ErrorCategory, _ErrorCode, _ErrorDef
from tabsdatak.conn.common.error import (
    ConnException, ConnInitException, ConnValidateException,
)


class <Name > ErrorCode(ErrorCode):
    < NAME > _1 = _ErrorDef(_ErrorCategory.PARAM_TYPE, "<Name>Conn validation failed")
    < NAME > _2 = _ErrorDef(_ErrorCategory.PARAM_TYPE, "<Name>Src validation failed")
    < NAME > _3 = _ErrorDef(_ErrorCategory.PARAM_TYPE, "<Name>Dest validation failed")
    < NAME > _4 = _ErrorDef(_ErrorCategory.PARAM_TYPE, "bad <thing>: {value}")  # validator
    < NAME > _5 = _ErrorDef(_ErrorCategory.VALIDATION, "...")  # validate(self)

    @classmethod
    def exception_classes(cls):
        return {
            _ErrorCategory.PARAM_TYPE: ConnInitException,
            _ErrorCategory.VALIDATION: ConnValidateException,
        }
```

Numbering is a wire contract: never renumber existing members.

## `pyproject.toml` entry points

```toml
[project.entry-points."tabsdatak.connectors"]
src_<name>  = "tabsdatak.conn.<name>._plugin:<NAME>_SRC"
dest_<name> = "tabsdatak.conn.<name>._plugin:<NAME>_DEST"
```

One entry per binding. Salesforce ships
`src_salesforce` + `src_salesforce_report`. Dest-only connectors
ship just `dest_<name>`.

## Names at a glance

| Concept                | Name                                              | Where               |
| ---------------------- | ------------------------------------------------- | ------------------- |
| Connection             | `<Name>Conn(Conn)` (`Conn` from `tabsdatak.spi`)  | `<name>/__init__.py`   |
| Source                 | `<Name>Src(Src)` (no `conn` field)                | `<name>/__init__.py`   |
| Destination            | `<Name>Dest(Dest)` (no `conn` field)              | `<name>/__init__.py`   |
| Conn ↔ Src/Dest binding | `SrcDef(conn=<Name>Conn, ...)` / `DestDef(conn=<Name>Conn, ...)` | `<name>/_plugin.py` |
| Src plugin             | `_<Name>SrcPlugin(SrcPlugin[<Name>Conn, <Name>Src])`   | `<name>/_plugin.py` |
| Dest plugin            | `_<Name>DestPlugin(DestPlugin[<Name>Conn, <Name>Dest])` | `<name>/_plugin.py` |
| Explorer               | `_<Name>Explorer(Explorer[<Name>Conn])`           | `<name>/_plugin.py`    |
| Src binding            | `<NAME>_SRC = SrcDef[<Name>Conn, <Name>Src](...)` | `<name>/_plugin.py`    |
| Dest binding           | `<NAME>_DEST = DestDef[<Name>Conn, <Name>Dest](...)` | `<name>/_plugin.py` |
| Error enum             | `<Name>ErrorCode(ErrorCode)`                      | `<name>/error.py`      |

## Validation surface

| Tier             | Where it fires                                          | Exception                  |
| ---------------- | ------------------------------------------------------- | -------------------------- |
| `PARAM_TYPE`     | pydantic init / `ValueConstraints` in `<name>/_validation.py` | `ConnParamTypeException`   |
| `VALIDATION`     | descriptor's `validate(self)` hook in `<name>/__init__.py`  | `ConnValidateException`    |
| runtime          | plugin code paths                                       | `ErrorDef.category` (the class itself) |

Single factory at every raise site:
`<Name>ErrorCode.<CODE>.exception(cause=…, **kwargs)`. Both
`ConnParamTypeException` and `ConnValidateException` inherit from
`ConnException`.

## Smoke-check after install

Editable-install the connector and verify it shows up in the registry.
From the tabsdata-os repo root:

```sh
pip install -e connectors/python/tabsdata-conn-<name>
```

```python
from tabsdatak._connectors import avail_dests, avail_srcs

assert "<name>" in avail_srcs()  # or avail_dests()
```
