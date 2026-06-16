# Connection-aware Collections (Source / Destination / DataFactory)

> Status: design proposal. Scope below deliberately **excludes** the k8s
> bridge API (Dimas' service): we describe the apiserver REST surface and the
> CLI/Python client work, and treat the bridge as an opaque backend the
> apiserver forwards the three JSON objects to.

## 1. Motivation

Today a `Collection` is pure metadata: `name`, `description`, a `group_id`, and
audit columns. It carries no notion of *what kind* of collection it is, and no
notion of an external **connection** (a database/object-store the collection
ingests from or publishes to).

We want three flavours of collection:

| Flavour       | Inferred from group                | Carries a connection? | Connection role |
|---------------|------------------------------------|-----------------------|-----------------|
| `DataFactory` | **any group except** source/dest   | no                    | —               |
| `Source`      | `source`                           | **yes**               | a `Src` / `StreamSrc` connection (ingestion) |
| `Destination` | `destination`                      | **yes**               | a `Dest` connection (publishing) |

`DataFactory` is the default flavour: it is *not* tied to a single group. Any
group other than `source` / `destination` (including `default` and any
user-created group) yields a `DataFactory` collection.

The grouping already exists. `td-schema` seeds exactly these three groups
(`server/libraries/td-schema/src/schema/current/groups.rs:96-101`):

```rust
("00000000000000000000000208", "default",     "Default group — assigned to collections that do not specify a group."),
("0000000000000000000000020C", "source",      "Source group — assigned to ingestion collections."),
("0000000000000000000000020G", "destination", "Destination group — assigned to publisher collections."),
```

So we don't add a parallel concept; we **derive a collection type from the
group** at creation time and persist it, and we attach connection info to
`source` / `destination` collections.

This splits into two independent pieces of work:

- **Part A — Collection type** (a new enum column on the `collections` DAO,
  inferred from the group, immutable for source/destination).
- **Part B — Connection info** (the connector template → 3-JSON →
  apiserver → bridge pipeline, CRU done from Python/CLI only for now).

---

## 2. Part A — Collection type

### 2.1 The enum

Add a `CollectionType` persisted enum using the repo's standard
`#[ta_objects::typed_enum]` pattern (same shape as `ExecutionStatus` in
`server/libraries/td-objects/src/types/basic/typed_enum.rs`), with a
single-character storage code per variant:

```rust
#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
pub enum CollectionType {
    #[typed_enum(rename = "F")]
    DataFactory,
    #[typed_enum(rename = "S")]
    Source,
    #[typed_enum(rename = "D")]
    Destination,
}
```

### 2.2 Inference at creation time

The collection type is **never accepted from the client**. It is inferred from
the group the collection is created in. The create service already resolves the
group (`server/libraries/td-services/src/collection/service/create.rs:36-41`):

```rust
from_fn(With::<CollectionCreate>::extract::<GroupName>),
from_fn(By::<GroupName>::fetch_one::<GroupDB>),
from_fn(With::<GroupDB>::extract::<GroupId>),
...
from_fn(With::<GroupId>::set::<CollectionCreateDBBuilder>),
```

We insert one more step that maps the resolved group → `CollectionType` and
sets it on the builder. **The match is on the well-known seeded group ids**, not
on the group name:

- `GroupId` == `0000000000000000000000020C` (`source`)      → `CollectionType::Source`
- `GroupId` == `0000000000000000000000020G` (`destination`) → `CollectionType::Destination`
- any other group id                                        → `CollectionType::DataFactory`

Keying on the ids (the immutable seed ids in
`td-schema/src/schema/current/groups.rs:96-101`) is deliberate: it is robust
against group renames and keeps the source/destination identity pinned to the
two reserved seeds, while every other group — `default` or any user-created
group — falls through to `DataFactory`.

### 2.3 DAO / DTO / schema changes

`server/libraries/td-objects/src/dxo/collection.rs`:

- Add `pub collection_type: CollectionType` to `CollectionCreateDB` (set via
  `#[dxo(setter)]`, exactly like `group_id` at line 38-39 — it's derived by the
  service, not part of `CollectionCreate`).
- Add `pub collection_type: CollectionType` to `CollectionDB` and surface it on
  `CollectionRead` so the type is visible in get/list responses.
- **Do not** add it to `CollectionCreate` (the request DTO, lines 91-100) — the
  type is server-inferred.
- **Do not** add it to `CollectionUpdate` (lines 102-106) — the type is
  immutable.

`server/libraries/td-schema/src/schema/current/collections.rs`: add a
`collection_type` column to the `collections` table, plus a schema migration.
The schema is currently at `VERSION = 1` with no deltas, so this is the first
`v1_to_v2` delta (see the recipe in `server/libraries/td-schema/src/schema/mod.rs`):
add the column, bump `VERSION`, refresh `FINGERPRINTS`, and add the
`v1_to_v2` module + match arms in `deltas/mod.rs`.

Existing rows backfill to `DataFactory`, which is correct for every collection
that exists today (none are in the `source`/`destination` groups yet).

### 2.4 Immutability rules

Two invariants, enforced in the services:

1. **Group is immutable for source/destination collections.** Group is already
   effectively immutable for *all* collections — `CollectionUpdate` has no
   `group` field and the update service never sets `group_id`
   (`server/libraries/td-services/src/collection/service/update.rs`). So this
   rule is satisfied for free *as long as no "move collection to group" feature
   is added later*. We make it explicit: if a future change introduces group
   reassignment, it must reject the change when `collection_type` is `Source`
   or `Destination` (changing the group would change the inferred type, which is
   not allowed).
2. **Connection type is immutable.** The *connection info* of a source/dest
   collection can be updated (its values, secrets, vars), but the **connector
   type** (`mysql`, `s3`, …) behind it cannot change. Enforced on the
   connection-update path (Part B), not on the collection row.

### 2.5 Connection required for source/destination

If a collection is created in the `source` or `destination` group, a valid
connection info **must** be provided. This is a Part B concern (the connection
is created alongside the collection), but the *requirement* is a Part A rule:

- `Source` / `Destination` collection → connection info is mandatory.
- `DataFactory` collection → connection info must be absent.

Because CRU of connections is **Python/CLI-only for now** (see Part B), this
mandatory-connection rule is initially enforced **on the CLI side** before it
calls the apiserver. (The apiserver can still create a typed collection row, but
the user-facing flow always pairs creation with a connection for source/dest.)

---

## 3. Part B — Connection info

### 3.1 Where connection shapes come from

`tabsdata-api`'s `tabsdatak._connectors` module
(`connectors/python/tabsdata-api/src/main/python/tabsdatak/_connectors.py`)
exposes the registry of installed connectors:

```python
def avail_srcs() -> dict[str, SrcDef]:          # lines 144-149
def avail_stream_srcs() -> dict[str, StreamSrcDef]:  # lines 152-157
def avail_dests() -> dict[str, DestDef]:        # lines 160-165
```

Each is keyed by the wire-stable type string (`"mysql"`, `"postgres"`, `"s3"`,
`"gcs"`, `"azureBlob"`, `"localfile"`, `"mssql"`, `"oracle"`, `"db2"`,
`"mysql-cdc"`, …) and built lazily from the `tabsdatak.connectors` entry-point
group.

The definition dataclasses (`tabsdatak/spi.py`) each carry a
`conn: Type[C]` attribute where `C: Conn`:

```python
@_api.dataclass(...)
class SrcDef[C: Conn, S: Src]:
    src_type: str
    src_version: str
    conn: Type[C]      # <-- the connection class
    src: Type[S]
    ...

class StreamSrcDef[C: Conn, S: StreamSrc]: ...   # same conn: Type[C]
class DestDef[C: Conn, D: Dest]:     ...          # same conn: Type[C]
```

`Conn` is the abstract base
(`connectors/python/tabsdata-api/src/main/python/tabsdatak/api.py:301-308`,
`__kind__ = "connectionDef"`). A concrete `Conn` is a pydantic dataclass whose
fields *are* the connection parameters — e.g. `MySQLSrcConn`
(`connectors/python/tabsdata-conn-mysql/.../conn/mysql/__init__.py`):

```python
@_api.dataclass(MysqlErrorCode.MYSQL_1, kw_only=True)
class MySQLSrcConn(Conn):
    __kind__ = "mysqlSrcConn"
    uri: UriSpec                         # str | Secret | Var (pattern-validated)
    credentials: CredentialsSpec         # UserPassword(user: Secret|Var, password: Secret|Var)
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None
```

Every value field can be a literal, a `Secret`, or a `Var`. Serialization is the
envelope form (`kind`/`apiVersion`/`type`/`spec`) via the `JsonSerde` mixin in
`_api.py`. **The resulting JSON is large and verbose** (every secret/var is a
nested envelope). The note from the brief stands: *we do not try to slim this
down yet* — get the end-to-end flow working first, then revisit ergonomics.

### 3.2 CLI: list connectors and print a template

Two new CLI surfaces (alongside the existing collection CLI in
`client/platform/src/main/python/tabsdata/_cli/td/collection_group.py`):

1. **List available connectors** — print the keys (and type/version) from
   `avail_srcs()`, `avail_stream_srcs()`, `avail_dests()`.
2. **Print a connection JSON template** — given one of those names, build a
   template JSON for that connector's `conn: Type[Conn]` and print it.

The template is the connector's serialized envelope with every field present and
secret/var fields shown as inline literal placeholders the user fills in. The
user edits the template (sets URIs, credentials, etc.) and passes it back to the
`create collection` command.

> A template generator can be derived from the pydantic model of the `conn`
> type (defaults + a placeholder literal `Secret`/`Var` for each secret/var
> field). Exact rendering is an implementation detail; the contract is "valid
> skeleton in, filled-in JSON out".

### 3.3 Validation: pydantic deser against the conn type

When the user supplies the filled template at create time, the CLI validates it
by **deserializing into the connector's `conn` type**
(`Conn.from_dict` / `from_json` on the concrete class resolved from the chosen
connector name). Pydantic validation rejects malformed/missing/extra fields and
enforces the field patterns (e.g. URI regex). All JSON validation lives on the
**CLI side for now** (the apiserver does not validate the connection JSON yet).

### 3.4 The three JSON objects

A connection is represented to the apiserver as **three JSON objects**, derived
from the single filled-in, validated connection JSON using the existing
secret/var pipeline in
`connectors/python/tabsdata-common/src/main/python/tabsdatak/common/_json_serde.py`:

1. **Dehydrated connection** — the connection JSON with all inline secret/var
   *literals* rewritten as *references*:
   - `dehydrate_secrets(conn_json)` (line 224) rewrites every `secretLiteral`
     envelope to a `secretRef` (drops the value, keeps `_value_pattern`).
   - `dehydrate_vars(conn_json)` (line 321) does the same for
     `varLiteral` → `varRef`.
   - Both are deep-copy, recursive (`find_by_kind` descends into nested
     envelopes), and non-mutating.
2. **Extracted secrets** — `extract_secrets(conn_json)` (line 197) returns a
   `list[KeyValue]` (one per `secretRef`/`secretLiteral`), where `key` is the
   env-var-style name derived from the envelope's JSONPath (`_to_key`, e.g.
   `$.spec.credentials.password` → `CREDENTIALS_PASSWORD`) and `value` is the
   literal (or `None` for an already-ref).
3. **Extracted vars/envs** — `extract_vars(conn_json)` (line 213), same shape.
   ("envs" in the brief == "vars" here.)

So from one hydrated, validated connection JSON the CLI produces:

```
conn_json  ──dehydrate_secrets+dehydrate_vars──▶  dehydrated connection   (refs only, no values)
conn_json  ──extract_secrets──────────────────▶  secrets: [{key, value}]  (the secret values)
conn_json  ──extract_vars─────────────────────▶  vars:    [{key, value}]  (the var values)
```

These three travel to the apiserver, which forwards them to the bridge keyed by
the **collection id** (the retrieval key for all later reads/updates). The
bridge stores the dehydrated connection plus the secret and var values
separately. *(Bridge internals are out of scope here.)*

### 3.5 New function needed in `_json_serde.py`

The brief asks for a new function: **given a dehydrated JSON, return the list of
secret and var refs it contains.** This is the inverse-of-extract for the
no-values case, used for *alignment validation* (§3.7) without needing the
secret/var values.

It does not exist yet. It is a thin wrapper over `find_by_kind` +
`_to_key` (the same key derivation `extract_*` already uses), e.g.:

```python
def list_refs(dehydrated_data: JsonSerdeData) -> tuple[list[str], list[str]]:
    """Return (secret_ref_keys, var_ref_keys) for a dehydrated JSON.

    Keys are the env-var-style names derived from each ref's JSONPath,
    matching the keys produced by extract_secrets()/extract_vars().
    """
    secret_keys = [_to_key(p) for p, _ in find_by_kind(dehydrated_data, _SECRET_REF_KIND)]
    var_keys    = [_to_key(p) for p, _ in find_by_kind(dehydrated_data, _VAR_REF_KIND)]
    return secret_keys, var_keys
```

(Exact return shape — two lists, or `list[KeyValue]` with `value=None` to match
`extract_*` — is an implementation choice. A dehydrated JSON has *only* refs, so
`extract_secrets`/`extract_vars` on it already yields `value=None` entries; the
new function is the convenience that says "I only want the ref names, from a
known-dehydrated input".)

### 3.6 apiserver REST surface

New endpoints under the collection routes
(`server/libraries/td-apiserver/src/router/collections.rs`,
mirrored in the Python client `client/.../tabsdata/api/apiserver.py` and CLI).
All operate on the collection id and just shuttle the 3 JSON objects to/from the
bridge:

- **Create** — POST the connection: all **3 JSON at once** (dehydrated
  connection + secrets + vars). Used when a `source`/`destination` collection is
  created.
- **Update** — POST an update: either **all 3 at once**, **just secrets**, or
  **just vars**. (The connection *type* cannot change — §2.4.)
- **Read** — GET returns:
  - the dehydrated connection JSON,
  - the list of stored **secret names** (not values),
  - the list of stored **var names** (not values).

The apiserver create flow today has no outbound calls
(`collection/service/create.rs` only touches the DB). The connection
create/update handlers are where the apiserver gains an outbound call to the
bridge — keyed by collection id. *(That bridge call is out of scope here.)*

### 3.7 Alignment validation (CLI side, for now)

The three objects must be **fully aligned**: the set of secret refs in the
dehydrated connection must exactly equal the set of keys in the secrets object,
and likewise for vars. This is the same key-set invariant `hydrate_secrets`
already enforces internally (it raises if the key set of the supplied values
doesn't exactly match the `secretRef` keys in the dehydrated tree). The new
`list_refs` function (§3.5) lets the CLI check alignment **without** having all
the values:

- **On create** — the CLI has the full hydrated JSON, so all three objects are
  derived from one source and are aligned by construction; still validate:
  `list_refs(dehydrated)` secret keys == `extract_secrets` keys, and var keys ==
  `extract_vars` keys.
- **On update** — the submitter may send only a subset (e.g. just secrets). The
  CLI **fetches the not-submitted objects from the backend** (via Read, §3.6)
  and validates the full triple is aligned:
  - update conn only → fetch stored secret names + var names; the new conn's
    refs (`list_refs`) must match both stored name sets.
  - update secrets only → fetch the stored dehydrated conn; its secret refs
    (`list_refs`) must match the submitted secret keys.
  - update vars only → symmetric to secrets.
  - update all three → validate among the three submitted objects.

All of this validation lives on the CLI for now (the apiserver trusts the
aligned triple). Moving validation server-side is future work.

---

## 4. CRU is Python/CLI-only for now

Connection create/read/update is exposed **only through the Python client and
CLI** in this iteration. The UI must **not** surface connection CRU yet (the
brief: "the UI must remove this"). There is no delete in this iteration beyond
whatever collection deletion implies.

---

## 5. Files likely to change

| Area | File | Change |
|------|------|--------|
| Enum | `server/.../td-objects/src/types/basic/typed_enum.rs` | add `CollectionType` |
| DAO/DTO | `server/.../td-objects/src/dxo/collection.rs` | `collection_type` on `*DB` + `CollectionRead` |
| Schema | `server/.../td-schema/src/schema/current/collections.rs` | new column |
| Migration | `server/.../td-schema/src/schema/deltas/` | first `v1_to_v2` delta |
| Create svc | `server/.../td-services/src/collection/service/create.rs` | infer type from group; set it |
| Update svc | `server/.../td-services/src/collection/service/update.rs` | guard against type/group change |
| REST | `server/.../td-apiserver/src/router/collections.rs` | connection create/update/read routes |
| json_serde | `connectors/python/tabsdata-common/.../tabsdatak/common/_json_serde.py` | new `list_refs` (ref-listing) function |
| Client | `client/platform/.../tabsdata/api/apiserver.py` (+ `tabsdata_server.py`) | connection create/update/read methods |
| CLI | `client/platform/.../tabsdata/_cli/td/collection_group.py` | list connectors, print template, create-with-conn, update-conn, alignment checks |

---

## 6. Out of scope / deferred

- **k8s bridge API** (Dimas' service) — treated as an opaque backend behind the
  apiserver; its CRUD on the 3 objects is not designed here.
- **Slimming the connection JSON** — the secret/var-heavy template is verbose;
  ergonomics improvements come *after* the end-to-end flow works.
- **Server-side JSON validation** — all connection JSON validation and alignment
  checks live on the CLI for now; promoting them to the apiserver is future work.
- **UI** — connection CRU must be removed from the UI for this iteration.
