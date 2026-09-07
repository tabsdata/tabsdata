<!--
Copyright 2026 Tabsdata Inc.
-->

# td-queries

Type-safe, composable query abstractions for the Tabsdata database layer.

`td-queries` sits between application logic and `td-database`. Queries are built
with **sea-query** and executed via **sqlx** against PostgreSQL. There is no
backend abstraction: the SQL, the schema snapshots and the tests are all
Postgres.

## Table of Contents

- [The Sea-Query / SQLx Bridge](#the-sea-query--sqlx-bridge)
- [The Executor](#the-executor)
- [The DAO System](#the-dao-system)
- [Query Surfaces](#query-surfaces)
- [Connections and Transactions](#connections-and-transactions)
- [Usage Examples](#usage-examples)

## The Sea-Query / SQLx Bridge

The bridge converts a sea-query AST into an sqlx-executable statement in three layers.

### Layer 1 — Building SQL + Values

The `SeaQuerySqlxBuild` trait (defined in `td-database`) converts any sea-query
statement into a `(String, SqlxValues)` tuple:

```rust
pub trait SeaQuerySqlxBuild {
    fn build_sea_query(self) -> (String, SqlxValues);
}
```

It is implemented for `&mut T where T: QueryStatementWriter` — every sea-query
builder (`SelectStatement`, `InsertStatement`, `UpdateStatement`,
`DeleteStatement`, …) — which renders through `PostgresQueryBuilder`, and for an
already-rendered `(String, SqlxValues)` pair.

`SqlxValues` (from the `sea-query-sqlx` crate) wraps the parameter vector and
implements sqlx's `Arguments` trait.

### Layer 2 — Fluent Builder API

The `SeaQueryExt` trait adds fluent methods to any `SeaQuerySqlxBuild` type:

```rust
pub trait SeaQueryExt: SeaQuerySqlxBuild + Sized {
    fn build_query(self)           -> SeaQueryBuilder<Self>;          // raw PgRow
    fn build_query_as<O>(self)     -> SeaQueryAsBuilder<Self, O>;     // typed O: FromRow
    fn build_query_scalar<O>(self) -> SeaQueryScalarBuilder<Self, O>; // scalar O: Decode
}
```

Each builder takes a `SeaExecutor` and returns a `BoxFuture`.

| Builder                  | Output type            | Methods                                            |
|--------------------------|------------------------|----------------------------------------------------|
| `SeaQueryBuilder`        | `PgRow`                | `execute`, `fetch_one`, `fetch_all`, `fetch_optional` |
| `SeaQueryAsBuilder<_,O>` | `O: FromRow<PgRow>`    | `fetch_one`, `fetch_all`, `fetch_optional`          |
| `SeaQueryScalarBuilder`  | `O: Decode + Type`     | `fetch_one`, `fetch_all`, `fetch_optional`          |

### Layer 3 — Execution

`SeaExecutor` (in `td-database`) has exactly one required method; everything
else is provided on top of it:

```rust
pub type RunOp<T> =
    for<'a> fn(&'a str, SqlxValues, &'a mut PgConnection) -> BoxFuture<'a, Result<T, Error>>;

pub trait SeaExecutor: Domained + Sized {
    type Reused<'a>: SeaExecutor where Self: 'a;

    fn reuse(&mut self) -> Self::Reused<'_>;

    fn run<'e, T: Send + 'e>(self, query: impl SeaQuerySqlxBuild, op: RunOp<T>)
        -> BoxFuture<'e, Result<T, Error>> where Self: 'e;
}
```

An implementor supplies only `run`: obtain a `PgConnection`, apply the `RunOp`.
`RunOp` is a `fn` pointer rather than a closure so the `for<'a>` binding is
explicit — closure inference will not produce it for a future borrowing its
connection argument.

The provided methods are all `run_`-prefixed — `run_execute`, `run_fetch_all`,
`run_fetch_one_as`, `run_fetch_optional_scalar`, and so on. That is deliberate:
this is the raw seam, and the unprefixed names (`fetch_one`, `fetch_all`, …)
belong to the typed [`Queries`](#the-executor) surface, which is in scope on the
very same executors. Callers reach the raw seam through the builders above, not
by naming these directly.

`reuse` reborrows an executor for one statement in a loop — the same database in
the same RLS scope, so a reused executor reaches exactly what its parent does.
It is what lets the [chunked grain](#query-surfaces) issue one statement per
chunk.

| Implementor                     | Unit of work                          |
|---------------------------------|---------------------------------------|
| `&PgPool`                       | one connection per statement          |
| `&TdPool`                       | delegates to its `PgPool`             |
| `&mut PgConnection`             | the caller's connection               |
| `&mut DomainConnection<D, S>`   | one domain, one RLS scope — what services hold |
| `&Scoped<&TdPool, P>`           | test-only: each statement in its own scoped transaction |

## The Executor

Every executor type gains every surface from one blanket-implemented trait:

```rust
pub trait Queries: QueryExecutor { … }
impl<E: QueryExecutor> Queries for E {}
```

So `use td_queries::Queries;` is the only import a caller needs. The
unversioned default grain is on the trait itself — `insert(&dao)`,
`fetch_one::<D>(cond)`, `fetch_all::<D>(cond)`, `update`, `delete`, `list`
— with no entry word, because there is nothing else the caller could have
picked. A word appears only where there is a choice: a point in time
(`latest()`, `as_of(&t)`, `history()`,
`data_version(&on, &partition)`), row locks (`for_update()`), batching
(`chunked()`), a traversal (`recursive::<D, R>(opts)`), or a per-DAO
handle (`logical_runs()`, `physical_runs()`, `table_data_versions()`,
`executions()`, `dependencies()`, `sessions()`, `metrics()`).

A grain word is a modifier and names no DAO; the terminal names the DAO and
the shape, exactly as the wordless grain does — `db.latest().fetch_one::<D>(c)`,
not `db.current::<D>().fetch_one(c)`. Where the terminal takes the DAO as an
argument (`insert`, `update`) or derives it from a list DTO (`list`), the
turbofish is unnecessary.

`SeedEntry` (test-utils only) stays a separate trait, so a production
import cannot pull the seed surface in.

The executor/DAO pairing is one trait:

```rust
pub trait Serves<D: Dao>: QueryExecutor + Domained<Domain = D::Db> {}
```

`E: Serves<D>` means the executor opens on `D`'s database domain *and* its
RLS scope can see `D`'s rows — see [DAO Trait Hierarchy](#dao-trait-hierarchy)
on `#[dao(project_scoped)]`.

### Which pool a query runs on

Nothing is inferred per statement. The pool is picked once, when the connection
is acquired, by the **access mode** the service asked for: `RO` and `RW` share
the component's regular pool, `XW` draws from the exclusive one and serializes on
the project it is scoped to. See
[Connections and Transactions](#connections-and-transactions).

## The DAO System

### The `#[Dao]` Macro

The `#[ta_objects::Dao]` attribute macro (defined in `tm-objects`) generates:

1. An **`Iden` enum** (e.g. `CollectionDBIden`) with a variant per field plus a
   `Table` variant. Field names go `snake_case` → `UpperCamelCase`
   (`created_on` becomes `CreatedOn`).
2. A **`sqlx::FromRow`** derive, except on an update payload, whose
   `Option` fields mean "leave the column alone" rather than "nullable".
3. The **structure** every DAO has — **`Dao`**, **`HasColumn<T>`**, plus
   whichever of **`PkDao`**, **`TimelineDao`**, **`Scd2Dao`**,
   **`RecursiveDao`**, **`UnversionedDao`**, **`TableDao`**,
   **`ProjectScoped`**, **`ReachableFrom`** and **`InsertableFrom`** the
   attribute and the field marks call for — and the **capabilities**
   `ops(...)` declares: **`SelectDao`**, **`InsertDao`**, **`UpdateDao`**,
   **`UpsertDao`**, **`DeleteDao`**.
4. **`HasColumn<T>`** per field — the compile-time column resolution behind
   [conditions](#conditions) — and the addressing impls, in every argument
   order: **`AddressesOneRow`** for the pk and, on an unversioned DAO, every
   `unique(..)` set; on a versioned DAO the sets feed **`AddressesOneVersion`**
   instead, together with the partition column.

There is no builder: DAOs are constructed with struct literals, and a DAO
becomes a DTO through a hand-written `FromDao`.

### Conditions

A query's `WHERE` clause is a value, not a string. A leaf is a `SeaValue`
newtype and resolves to `column = value` through the DAO's `HasColumn<T>`
impl, so a condition mentioning a type the queried DAO has no column of is a
compile error rather than a runtime one:

```rust
let user: UserDB = conn.fetch_one(&user_id).await?;                       // one leaf
let f: FunctionDB = conn.latest().fetch_one(and!(&function_id, &enabled)).await?;
```

`and!` / `or!` compose in value position, `And!` / `Or!` in type position, and
a DAO names its own filters as inherent fns returning `impl SqlCondition<Self>`
(`FunctionDB::active()`, `TableDBWithNames::frozen()`). `()` is the
match-all condition — `update`, `delete` and `close` refuse it rather than run
without a `WHERE`.

### DAO Trait Hierarchy

Structure is what the relation is; capability is what the DAO may be the
subject of. The two are separate trees, so an insert shape need not be a read
surface:

```
Dao                — Iden, Db, RELATION, TABLE, columns(), order_by(), display_name()
  ├── HasColumn<T> — COL: the column of domain type T
  ├── PkDao        — Pk, PK: the row's addressing column
  │    └── TimelineDao — Partition/PARTITION, VersionOrder/VERSION_ORDER
  │         └── Scd2Dao    — VALID_TO: the [valid_from, valid_to) chain
  ├── UnversionedDao — marker: one row per entity, no version log
  ├── RecursiveDao   — Key, UP, DOWN: the traversal edge
  └── TableDao       — marker: a base table, so writes are legal

SelectDao  (ops(select))         — marker: decodes a row; requires FromRow
InsertDao  (ops(insert))         — insert_values(), over a TableDao
UpdateDao  (ops(update(RowDao))) — FilterBy, update_values(), over a TableDao
DeleteDao  (ops(delete))         — marker, over an unversioned TableDao
UpsertDao  (ops(upsert(A, B)))   — key_columns(), value_columns(); an unversioned InsertDao
```

Every query surface also bounds the executor's RLS scope: `ReachableFrom`
lets a `#[dao(project_scoped)]` DAO — one whose relation the
`project_isolation` policy governs — be reached only from `InProject` (RLS
draws the tenant boundary) or `AcrossProjects` (the query does), never from
`Global`. `DomainConnection<D, S>` carries the scope from the pool to the
query, so bypassing RLS is a declared `AcrossProjects` rather than the
default it used to be. `daos::project_scoping` checks every mark against
`pg_policies` and the view dependency graph, both directions.

A single-row read needs an addressing condition, and which one depends on
the grain:

- On the default grain and `for_update()`, `fetch_one` / `fetch_optional`
  take an `AddressesOneRow<D, S>`: the `#[dao(pk)]` column, or a whole
  `unique(..)` set, with the key columns leading and any narrowing
  condition trailing via `and!` (`and!(&user_id, &enabled)`). On a
  versioned relation only the pk qualifies — a unique set there holds per
  instant, not per row, and the partition column matches every version.
- On `latest()` and `as_of(&t)`, they take an `AddressesOneVersion<D>`:
  the partition column qualifies, since intervals within a partition are
  disjoint and one is visible at any instant, and so does every declared
  `unique(..)` or `unique_at(..)` set. `unique_at(..)` is unique among the
  versions visible at one instant only — one `(collection, name)` at a time,
  an invariant the write path keeps rather than an index — so it addresses
  one version but not one row of the history
  (`project_scoped(unique_at(CollectionIdName, FunctionIdName))`), and the
  name-addressed read is `and!(&collection, &name)`.

Both are the compile-time half of the claim. The runtime half is that a
second matching row is an error (`single_row`), never a silent pick.

The open reads (`fetch_all`, `fetch_first`, `fetch_last`, `list`,
`fetch_latest_before`, `assert_exists` / `assert_not_exists`) bind on
`UnversionedDao`, which only an unversioned relation implements. On a versioned one
they do not exist: reach for `latest()` / `as_of(&t)` for the
version visible at a time, or `history()` to read history on purpose.

A `view = "..."` DAO gets no `TableDao`, so `insert`, `update` and `delete`
do not typecheck for it. `SELECT ... FOR UPDATE` stays available: locking
through a view locks the rows of every table it draws from.

A `#[dao(table = "...", ops(update))]` DAO is an update payload: `update` is
the only op it may declare, and it gets no `FromRow` — its `Option` fields
mean "leave the column alone", which a readable DAO would read as "nullable".
Its condition is typed over `U::FilterBy`, the payload DAO itself. A
partial-column payload names the relation's row DAO instead —
`#[dao(table = "collections", ops(update(CollectionDB)), project_scoped)]` —
so it can be filtered on a column it does not carry. The named DAO must
address the same relation, asserted at compile time against
`Dao::RELATION`: a view's columns can be computed
(`collections_active.name` is a `COALESCE`), and must not supply the filter
vocabulary for an UPDATE on the base table. The named DAO may be an SCD2
row DAO: a payload still targets rows of the base table.

### `#[dao(...)]` Attribute Options

An argument naming a **type** is written in the call form, `name(Type)`; only
a literal relation name uses `name = "..."`. Everything else is a bare flag.

| Attribute | Purpose |
|-----------|---------|
| `table = "..."` | The base table this DAO maps to; writes allowed |
| `view = "..."` | The view this DAO reads; read-only |
| `db(...)` | The `DbDomain` the relation lives in (default `Apiserver`) |
| `ops(select, insert, delete)` | What the DAO may be the subject of. Required on a `table`, forbidden on a `view` |
| `ops(update)` / `ops(update(RowDao))` | Update payload: the only op it may declare, its condition typed over the named DAO (default `Self`) |
| `ops(upsert(A, B))` | An `insert` whose `ON CONFLICT` target is the named unique set; needs `insert` too |
| `scd2` | `Scd2Dao` + `TimelineDao`; `delete` and `upsert` are rejected (a version is closed or superseded, never removed) |
| `timeline` | `TimelineDao` only — a positional history, no interval chain; same write bans |
| `recursive` | `RecursiveDao`, for CTE traversal |
| `project_scoped` | The relation is governed by `project_isolation`; needs a project-seeing scope |
| `unique(A)` / `unique(A, B)` | A unique set, named by column type, repeatable |
| `project_scoped(unique(A))` | A unique set that holds only within the project partition |

The dividing line: an **argument** states a fact about the relation, or names a
*set* of columns by type; a **field mark** states the role of *one* column. So
every unique set is an argument: a set spans columns, and a flattened DAO's may
live in its base, neither of which a mark can name. Set columns resolve through
`HasColumn`, so naming a type that is not a column of the DAO is a compile
error. A DAO cannot be generic: it maps to one relation.

Database-owned columns never ride on a write, and the ban is written in the
same currency as the rest of the DAO layer — the column's **type**, not its
field name. A `project_scoped` DAO declaring `insert` may not carry a
`ProjectId` column (the `project_isolation` policy defaults it from the
connection), and no DAO declaring `insert` may carry a `ValidTo` one (the
close-out trigger owns the interval end). Both look through `Option<..>` and
wrapper newtypes, both run over the flattened field list too, and both are
compile errors naming the reason. That is what splits a written relation into
an insert DAO and a row DAO that flattens it and projects what the database
fills.

Which single column plays which role is a **field mark**:

| Mark | Role |
|------|------|
| `#[dao(flatten)]` | Embed a base DAO's columns (base first) |
| `#[dao(immutable)]` | Set-once column in generated UPDATEs |
| `#[dao(order_by)]` / `#[dao(order_by(asc|desc))]` | Default result ordering |
| `#[dao(pk)]` | The primary key, the row's addressing column |
| `#[dao(partition_by)]` | The timeline's partition |
| `#[dao(valid_from)]` | The SCD2 interval start, which is also the version order |
| `#[dao(valid_to)]` | Names the projected column `Scd2Dao::VALID_TO` points at. Unmarked, the macro synthesizes the `ValidTo` `Iden` variant for a column the DAO does not project |
| `#[dao(version_by)]` | Timeline version order (non-SCD2) |
| `#[dao(recurse_up)]`, `#[dao(recurse_down)]` | Traversal edge |

One attribute may carry several: `#[dao(pk, partition_by)]`.

A `#[dao(flatten)]` DAO inherits only its base's `#[dao(pk)]`: row identity
passes through a row-preserving view untouched, while a *value* can be
redefined by it (`projects_resolved.name` is a `COALESCE`), so uniqueness over
that column is a fresh claim and must be restated when it survives.

A DAO that flattens a base and leaves a role unmarked **inherits the base's
column** — the view projects the base table's columns, so a `…WithNames`
view over an SCD2 table declares only `scd2` and the roles resolve through
the base.

### Example DAO Definition

```rust
#[ta_objects::Dao]
#[dao(view = "collections_active", project_scoped(unique(CollectionName)))]
pub struct ActiveCollectionDB {
    #[dao(pk)]
    pub id: CollectionId,
    pub project_id: ProjectId,
    pub name: CollectionName,
    pub description: Description,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub group_id: GroupId,
    pub base_path: Option<BasePath>,
}
```

This generates `ActiveCollectionDBIden` with variants `Table`, `Id`,
`ProjectId`, `Name`, … and implements `Dao` with `columns()` / `order_by()`,
`PkDao`, `ProjectScoped`, `AddressesOneRow` for `&CollectionId` and for
`&CollectionName` (unique within the project), plus `SelectDao` — a view is
readable by construction and declares no `ops`.

### SCD2 DAO Example

```rust
#[ta_objects::Dao]
#[dao(table = "dependencies", ops(insert), scd2, recursive, project_scoped)]
pub struct DependencyInsertDB {
    #[dao(pk)]
    pub id: DependencyVersionId,
    #[dao(partition_by)]
    pub dependency_id: DependencyId,
    #[dao(recurse_down)]
    pub function_id: FunctionId,
    #[dao(recurse_up)]
    pub table_function_id: OfTable<FunctionId>,
    #[dao(order_by)]
    pub dep_pos: DependencyPos,
    #[dao(valid_from)]
    pub defined_on: AtTime,
    ...
}
```

This implements `TableDao` / `InsertDao` (it is a table and declares
`insert`), `Scd2Dao` and `TimelineDao`, and `RecursiveDao`. Being `scd2`, it
does **not** implement `UnversionedDao`.

The row DAO is its flatten plus the trigger-owned interval end, and it is the
one that is read:

```rust
#[ta_objects::Dao]
#[dao(table = "dependencies", ops(select), scd2, recursive, project_scoped)]
pub struct DependencyDB {
    #[dao(flatten)]
    pub base: DependencyInsertDB,
    #[dao(valid_to)]
    pub valid_to: ValidTo,
}
```

`SelectDao` is what enables `latest()` / `as_of()` / `history()` /
`data_version()` / `recursive()` — the kind traits are structure, and say
only how the relation is versioned.

## Query Surfaces

### The default grain — `db.<op>(..)`

Unversioned reads and every write, straight on the executor.

| Method | Description |
|--------|-------------|
| `insert(dao)` | INSERT a row |
| `upsert(dao)` / `insert_or_ignore(dao)` | INSERT … ON CONFLICT, for an `UpsertDao` |
| `fetch_one(condition)` | One row, error if absent; needs `AddressesOneRow` |
| `fetch_optional(condition)` | Same, `None` if absent |
| `fetch_first(condition)` | The first row in the DAO's order, of however many match |
| `fetch_last(condition)` | The last row in that order |
| `fetch_all(condition)` | Every matching row |
| `assert_exists` / `assert_not_exists` | Existence checks with a clean error |
| `update(dao, condition)` | UPDATE; the condition is typed on `U::FilterBy` |
| `delete(condition)` | DELETE; needs a `DeleteDao` |
| `list(condition, params)` | Paginated/filtered SELECT, returns DTOs |
| `fetch_latest_before(condition, offset)` | Nth most recent row |

Writes and the addressing reads take any DAO kind (on a versioned one the
address is the pk); the open reads take a `UnversionedDao`, so on a versioned
relation they are not offered. `list`
takes only the domain — no scope bound — because row visibility is the
connection's business: RLS scopes project rows, and a cross-project caller
narrows by its viewable projects in the condition.

Two grains hang off the same level:

- `for_update()` — the same reads holding row locks: `fetch_one`,
  `fetch_optional`, `fetch_all`, plus `claim_all` (`SKIP LOCKED`).
- `chunked()` — `insert(&[D])`, `fetch_all(values, extra)`,
  `update(dao, values)` over a slice too large for one statement's bind
  parameters: each chunk is one statement, the slice's conditions OR-joined
  and `extra` AND-ed onto every chunk.

### `Latest<E>` — `db.latest()`

At-now reads over the open SCD2 intervals (`valid_to = 'infinity'`, served
by the partial indexes): `fetch_one`, `fetch_optional` (`AddressesOneVersion`),
`fetch_first`, `fetch_all`, `assert_not_exists`, `list`,
`recursive::<D, R>(opts)`, and `chunked().fetch_all(values, extra)`.

### `AsOf<'a, E, V>` — `db.as_of(&t)`

Latest-at-time by condition, across partitions — a flat interval predicate
(`version_order <= t AND valid_to > t`), no ranking window, so cost follows
the condition's selectivity. The same methods as `latest()`.

### `Versions<E>` — `db.history()`

The whole history of a `TimelineDao`, no time predicate: `fetch_all`,
`fetch_first`, `fetch_last`, `list`, `list_until(&t, cond, params)` (the
audit trail up to an instant), and `chunked().fetch_all(values, extra)`. The
named slow path — the open reads on the default surface refuse a versioned
relation precisely so that a history read says so at the call site.

### `DataVersion<'a, E, V, P>` — `db.data_version(&on, &partition)`

Ordinal timeline resolution: a `Versions` selector (`Single`, `List` or
`Range` of `Head~k` / `Initial+k` / `Fixed` positions) over one partition at
`on` — `fetch_one`, `fetch_optional`, `fetch_all`, `fetch_relative_offset`. Positions resolve as top-k scans and
key probes on the `(partition, version_order, pk)` index, no window
functions.

### `RecursiveQuery<'a, D, R, E>` — `db.recursive::<D, R>(opts)`

Graph traversal with `WITH RECURSIVE`, for upstream/downstream dependency
resolution. `D` is the `RecursiveDao` walked, `R` the reference DAO its
`Key` points into; `direct_reference(&v)` / `direct_references(vec)` anchor
the walk, `fetch_all` runs it. `db.recursive::<D, R>(..)` traverses an
unversioned relation; `latest().recursive::<D, R>(..)` and
`as_of(&t).recursive::<D, R>(..)` traverse at a time and add `filter`,
`filter_references` and `filter_start` conditions.

### Per-DAO Surfaces

Each per-DAO handle is opened by a `Queries` method; its inherent methods
drop the DAO/verb prefixes — the handle is the namespace.

```rust
db.sessions().purge().await?;
db.dependencies().active_for_function_at(&fid, &at).await?;
db.executions().list_by_table_data_version(&tdv, &params).await?;

db.logical_runs().previous_for_function_at(&fid, &before).await?;
db.logical_runs().to_commit(&execution_ids).await?;
db.physical_runs().to_dispatch(limit).await?;
db.table_data_versions()
    .latest_bound_tdv_with_data_for_table(&table_id, &triggered_on, true)
    .await?;
```

Adding a surface is one method on `Queries` plus the inherent `impl` for
the handle.

## Connections and Transactions

A service body never holds a raw connection. It holds a `DbPool<S, D>` handle —
or a `GatedConnection` already acquired for it — and both carry the RLS scope
`S` and the database domain `D` in their own type, so which database a query
opens on and what rows it can see are settled where the handle is declared, not
at each call.

```rust
// Acquire in the mode the work needs. RO and RW share the regular pool; XW
// draws from the exclusive one and serializes on the project it is scoped to.
let mut conn = db.read().await?;              // GatedConnection<RO, S, D>
let mut conn = db.begin().await?;             // GatedConnection<RW, S, D>
let mut conn = db.begin_exclusive().await?;   // GatedConnection<XW, InProject, D>
```

Every acquire applies its scope: `InProject` sets `app.project_id` and lets RLS
draw the tenant boundary; `Global` stays on the tenant role with no project,
because its relations carry none, so a policed relation is fail-closed at the
database too; `AcrossProjects` alone switches to the cross-project role, for a
query that spans projects on purpose and narrows by `project_id` itself.

The connection yields queries only against a `Grant`, so an unauthorized query
is as unobtainable as an unscoped one:

```rust
let grant = authz
    .require::<Project, DevelopProject>(&mut conn, &ctx, &project)
    .await?;
let conn = conn.get(grant);
let group: GroupDB = conn.fetch_one(&create.group).await?;
conn.insert(&collection).await?;
```

`Grant(())` has one constructor, so every mint is an auditable claim that
authorization was decided outside the type system — td-authz's own vocabulary,
plus the named paths with no requester to check (credential verification,
scheduler ticks, unauthenticated probes).

A write is lost unless it is committed — `GatedConnection` is `#[must_use]` and
only a write mode has `commit`:

```rust
let mut conn = db.begin().await?;
conn.get(grant).insert(&dao).await?;
conn.commit().await?;
```

When a service declares `&mut GatedConnection<RW>` as a parameter instead, the
wrapper owns the lifecycle and commits or rolls back around the body.

`GatedConnection<M, S, D>::enter_project(&project)`, for `S` `Global` or
`AcrossProjects`, is the one scope transition: it switches to the tenant role
with `app.project_id` set and *re-types* the connection to `InProject`,
consuming the unprojected view so the two cannot be held at once. Project
creation uses it once the `projects` row exists; a service handed a bare id
(an execution, a worker, a function version) resolves the owning project on an
`AcrossProjects` read, enters it, and enforces `require::<Project, R>` there.
Such a service owns its unit of work (`DbPool<AcrossProjects>` + `read()` /
`begin()`), since the pivot consumes the connection.

## Usage Examples

### Basic CRUD

`conn` below is the `&mut DomainConnection` a `GatedConnection` hands out
against a [`Grant`](#connections-and-transactions).

```rust
use td_queries::Queries;

conn.insert(&collection).await?;

let user: UserDB = conn.fetch_one(&user_id).await?;
let maybe: Option<UserDB> = conn.fetch_optional(&user_id).await?;
let roles: Vec<UserRoleDB> = conn.fetch_all(&user_id).await?;

conn.update(&update, &user_id).await?;
conn.delete::<UserDB>(&user_id).await?;
```

### Point-in-Time Reads

```rust
use td_queries::Queries;

// The open row: one per partition, flat predicate on the interval.
let function: FunctionDB = conn.latest().fetch_one(&function_id).await?;

// By name: the `unique_at` set on the view.
let function: FunctionDBWithNames = conn
    .latest()
    .fetch_one(and!(&collection_id, &function_name))
    .await?;

// Deletion is the closed interval with no successor: after this the entity
// is gone from `latest()` and from `as_of(&t)` for any `t >= at`.
conn.latest().close::<FunctionDB>(&function_id, &ctx.time).await?;

// Visible at a time, by condition — cost follows the condition.
let in_collection: Vec<FunctionDB> = conn.as_of(&at).fetch_all(&collection_id).await?;

// Every version of a partition, history on purpose.
let all: Vec<FunctionDB> = conn.history().fetch_all(&function_id).await?;

// A Versions selector over one partition.
let version: LogicalTableDataVersionDB = conn
    .data_version(&at, &table_id)
    .fetch_one(&versions)
    .await?;
```

The turbofish, when the result type does not pin the DAO, goes on the
terminal: `conn.latest().fetch_all::<FunctionDB>(&collection_id)`.

### Locks and Chunked Grains

```rust
use td_queries::Queries;

// Locked for the rest of the transaction.
let locked: CollectionDB = conn.for_update().fetch_one(&id).await?;

// Whatever no other claimer holds (SKIP LOCKED).
let mine: Vec<WorkerDB> = conn.for_update().claim_all(ready).await?;

// A slice too large for one statement's bind parameters.
conn.chunked().insert(&rows).await?;
let found: Vec<WorkerDB> = conn.chunked().fetch_all(&worker_ids, ()).await?;
```

### Custom Query with Sea-Query

```rust
use sea_query::Query;
use td_database::sea_query::builder::SeaQueryExt;

// Build a custom query and execute it
let count: i64 = Query::select()
    .expr(sea_query::Func::count(sea_query::Expr::col(MyDaoIden::Id)))
    .from(MyDao::TABLE)
    .and_where(sea_query::Expr::col(MyDaoIden::Status).eq("active"))
    .build_query_scalar()
    .fetch_one(conn)
    .await?;
```
