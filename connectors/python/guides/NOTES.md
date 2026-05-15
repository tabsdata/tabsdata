# Resolving ConnRefs (named connections)

Traverse the Json with a `connectionRef` resolver replacing the entry with a resolved connection (loading similar
how it is done in tabsdata 1.x).

Then you need to resolve ValRefs (see next).

# Resolving ValRefs (secrets,envs)

On ConnRefs are resolved, traverse the Json with a secret/env resolver and go replacing SecretRef and EnvRef with the 
actual values.

Then you need to do revalidation (see next) to ensure the resolved values are OK.

# Revalidation after resolving NameRefs

For a deeply nested pydantic-dataclass-based structure, the cleanest way to clone-with-revalidation is to **round-trip
through dict** and re-construct via a `TypeAdapter`. That forces validation at every level, unlike
`dataclasses.replace` (which skips validation entirely) or the dataclass constructor on a pre-built instance (which only
validates the top level).

## The recommended pattern

```python
from pydantic import TypeAdapter
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class Inner:
    x: int


@dataclass(frozen=True)
class Outer:
    inner: Inner
    name: str


original = Outer(inner=Inner(x=1), name="foo")

adapter = TypeAdapter(Outer)
clone = adapter.validate_python(adapter.dump_python(original))
```

`dump_python` recursively converts the whole tree to plain dicts/primitives; `validate_python` re-constructs it, running
every validator at every level. The clone is a fully independent deep copy and is guaranteed to be valid by the current
schema — useful if validators have changed, if you mutated something through a backdoor, or if you just want defensive
certainty.