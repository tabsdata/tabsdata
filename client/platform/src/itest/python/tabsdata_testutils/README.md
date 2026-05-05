# tabsdata_testutils

Shared helpers for the tabsdata-ag integration-test suite. The current
headline piece is the `external`-system framework: a tiny decorator +
fixture combo that replaces hand-rolled `resolver("FOO__BAR")` string
concatenation with a typed `ExternalSystem` subclass per target.

## Quick start

```python
from tabsdata_testutils.external import external
from tabsdata_testutils.mysql import MYSQL

@external(system=MYSQL)
def test_mysql_check_connect(envs):
    # envs["HOST"], envs["PORT"], envs["DATABASE"], envs["USERNAME"],
    # envs["PASSWORD"] — dict values are str | None.
    MySQLConnectionExtractor().check_connect({
        "uri": f"mysql://{envs['HOST']}:{envs['PORT']}/{envs['DATABASE']}",
        "credentials": {
            "user": envs.get("USERNAME") or "",
            "password": envs.get("PASSWORD") or "",
        },
    })
```

No `@pytest.mark.live_extractor`, no hand-rolled env-var plumbing
— `@external` absorbs all of it.

## The three moving pieces

### `ExternalSystem` (ABC)

One subclass per live system. Two abstract class methods:

| method | what it returns |
| --- | --- |
| `system_env_name()` | the env var whose value gates the test AND the default prefix applied to each suffix in `env_names()` |
| `env_names()` | the env-var suffixes the system expects — without any prefix |

Optional:

| method | default | why override |
| --- | --- | --- |
| `env_var(prefix, name)` | `f"{prefix}__{name}"` | Systems whose native env-var naming deviates from the `<PREFIX>__<NAME>` form. No shipped class overrides this today — every system (including OpenAI) follows the default. |

**Class-naming convention.** Every subclass is named exactly as
`system_env_name()` returns, even when that collides with PEP-8
style (`MYSQL`, `CDCMYSQL`, `OPENAI`). That trade-off is
deliberate — it keeps the call site readable:

```python
@external(system=MYSQL)     # → gated by $MYSQL
@external(system=CDCMYSQL)  # → gated by $CDCMYSQL
@external(system=OPENAI)    # → gated by $OPENAI
```

**Gate-var naming rule: no underscores.** The gate env var (== the
class name == the prefix applied to every suffix) must not contain
underscores. The `<PREFIX>__<SUFFIX>` pattern uses `__` as the
separator, and an underscore inside the prefix would be visually
ambiguous with it (e.g. `CDC_MYSQL__HOST` — is that a CDC gate with
`MYSQL__HOST` as a suffix, or a `CDC_MYSQL` gate with `HOST` as a
suffix?). Keep prefixes alphanumeric: `CDCMYSQL`, not `CDC_MYSQL`.
Suffixes may and often do carry underscores (`ACCESS_KEY`,
`SERVICE_NAME`, `ENDPOINT_URL`, …) — that's fine because they sit
after the `__` separator.

All systems — including the LLM ones — use the same
`<PREFIX>__<SUFFIX>` shell convention. For OpenAI that means:

```bash
export OPENAI=openai          # gate (any non-"skip" value runs)
export OPENAI__API_KEY=sk-... # key
export OPENAI__MODEL=gpt-4o-mini
```

### `@external(system=..., prefix=None)`

The decorator a test wears. What it does:

* Applies `@pytest.mark.live_extractor` so the test participates in
  the live-extractor run.
* Attaches an `external_system` pytest marker carrying the system
  class + optional prefix override — read at run time by the `envs`
  fixture.
* When `prefix` is non-`None`, it overrides `system_env_name()` as
  both the gate var and the prefix applied to each suffix — useful
  for targeting a second instance of the same system
  (`@external(system=MYSQL, prefix="MYSQL2")`).

### `envs` fixture

Injected into any test decorated with `@external`. Returns
`dict[str, str | None]` keyed by the bare suffixes from
`env_names()`. Gate handling:

| `$<gate>` value | behavior |
| --- | --- |
| unset | **test runs**, dict values are all `None` (or whatever sub-vars *are* set) |
| `"skip"` (case-insensitive, whitespace tolerated) | **skip** the test |
| anything else | test runs, dict values are the env-var strings (or `None` for unset sub-vars) |

Registration: each consuming `conftest.py` imports the fixture so
pytest discovers it in that subtree:

```python
# conftest.py
from tabsdata_testutils.external import envs, pytest_runtest_makereport  # noqa: F401
```

The second import (the hook) is what powers the "export MYSQL=skip
to skip this test" hint on failure.

## Skipping a live suite you don't have locally

Set the gate env var to `skip`:

```bash
export MYSQL=skip
pytest -m live_extractor
```

The setup shell scripts can also auto-emit this when prerequisites
aren't met (e.g. Docker isn't running) so the same config works in
CI and on a laptop.

## Adding a new system

1. Create `tabsdata_testutils/<system>.py` with a class named after the
   gate env var:

   ```python
   #
   # Copyright 2026 Tabs Data Inc.
   #
   """Redshift external system for live integration tests."""
   from __future__ import annotations

   from tabsdata_testutils.external import ExternalSystem


   class REDSHIFT(ExternalSystem):
       @classmethod
       def system_env_name(cls) -> str:
           return "REDSHIFT"

       @classmethod
       def env_names(cls) -> list[str]:
           return ["HOST", "PORT", "DATABASE", "USERNAME", "PASSWORD"]
   ```

2. In the test suite's `conftest.py`, the `envs` fixture is already
   registered — no changes needed there.

3. Have the setup shell script (`redshift.sh`) export `REDSHIFT=…`
   (anything non-`skip`) plus `REDSHIFT__HOST` / `REDSHIFT__PORT` /
   etc.

4. Write the test:

   ```python
   from tabsdata_testutils.external import external
   from tabsdata_testutils.redshift import REDSHIFT

   @external(system=REDSHIFT)
   def test_redshift_check_connect(envs):
       ...
   ```

## The "export GATE=skip" hint on failure

When an `@external`-decorated test fails, the final report grows a
section like:

```
----------------------------- external-system hint -----------------------------
export MYSQL=skip   # to skip this test
```

Powered by a `pytest_runtest_makereport` hookwrapper in
`external.py`. Hooks are discovered by name from `conftest.py`, so
the consuming conftest has to re-import it:

```python
from tabsdata_testutils.external import (
    envs,
    pytest_runtest_makereport,  # noqa: F401
)
```

## Targeting a second instance of the same system

Pass `prefix=`:

```python
@external(system=MYSQL, prefix="MYSQL2")
def test_replica(envs):
    # reads $MYSQL2 / $MYSQL2__HOST / $MYSQL2__PORT / …
    # envs dict keys are still HOST / PORT / … (prefix stripped)
    ...
```

No changes needed to the `MYSQL` class or the test body — the
prefix just shifts which env vars the framework reads.
