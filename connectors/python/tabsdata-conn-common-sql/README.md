# tabsdata-conn-common-sql

> **Backward-compatibility policy.** This package is **shared** by
> every SQL-flavored connector (`tabsdata-conn-mysql`,
> `tabsdata-conn-postgres`, `tabsdata-conn-mssql`,
> `tabsdata-conn-db2`, `tabsdata-conn-snowflake`,
> `tabsdata-conn-bigquery`). Every change to its public surface MUST
> be backward-compatible:
>
> - Do **not** rename, remove, or change the type of an exported
>   symbol, dataclass field, `__kind__` tag, or error-code member.
> - Do **not** tighten validation rules (previously-valid inputs
>   must continue to validate).
> - Do **not** change the on-wire JSON envelope or the semantics of
>   an existing `apiVersion`.
> - Adding new helpers, new error-code members, or new optional
>   fields with defaults is the supported way to evolve.
>
> If a breaking change is unavoidable, bump the major version of
> this package and coordinate the rollout with every SQL connector
> that depends on it.

Shared building blocks for SQL connectors. Connector-specific
identifiers, URI shapes, validators, and error codes belong in the
matching connector package, not here. Add a new symbol here only
when two or more SQL connectors will reuse it as-is.
