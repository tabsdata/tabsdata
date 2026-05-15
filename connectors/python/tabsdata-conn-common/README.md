# tabsdata-conn-common

> **Backward-compatibility policy.** This package is **shared** by
> every connector package (`tabsdata-conn-mysql`,
> `tabsdata-conn-postgres`, ..., plus `tabsdata-conn-common-sql`).
> Every change to its public surface MUST be backward-compatible:
>
> - Do **not** rename, remove, or change the type of an exported
>   symbol, dataclass field, `__kind__` tag, or error-code member.
> - Do **not** tighten validation rules (previously-valid inputs
>   must continue to validate).
> - Do **not** change the on-wire JSON envelope or the semantics of
>   an existing `apiVersion`.
> - Adding new credential classes, new error-code members, or new
>   optional fields with defaults is the supported way to evolve.
>
> If a breaking change is unavoidable, bump the major version of
> this package and coordinate the rollout with every connector that
> depends on it.

Shared connector building blocks:

* `tabsdatak.conn.common_typing` — reusable credential descriptors
  (`Credentials` ABC plus `UserPassword`, `GCPJson`,
  `AwsAccessSecretKey`, `AzureAccountKey`, `HMAC`) and cloud-location
  dataclasses (`CloudLocation` ABC plus `S3Location`,
  `AzBlobLocation`, `GCSLocation`).
* `tabsdatak.conn.common_error` — `ConnErrorCode` enum (used by the
  shared validators and cloud-location dataclasses) and the connector
  exception hierarchy (`ConnException` base; `ConnParamTypeException`
  for pydantic init / `AfterValidator` failures;
  `ConnValidateException` for cross-field `validate(self)` failures).

Connector-specific types, validators, and error codes belong in the
matching connector package, not here. Add a new symbol here only when
two or more connectors will reuse it as-is.
