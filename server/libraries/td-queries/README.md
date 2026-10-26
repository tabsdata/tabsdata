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
picked. A word appears only where there is a choice: an interval table
(`scd2()`, and `scd2().as_of(&t)` for an instant other than now), the
version log (`timeline()`, and `timeline().at(&on, &partition)` for a
position in it), row locks (`for_update()`), batching (`chunked()`), or a per-DAO
handle (`logical_runs()`, `physical_runs()`, `table_data_versions()`,
`executions()`, `dependencies()`, `sessions()`, `metrics()`).

A grain word is a modifier and names no DAO; the terminal names the DAO and
the shape, exactly as the wordless grain does — `db.scd2().fetch_one::<D>(c)`,
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
   **`UnversionedDao`**, **`TableDao`**,
   **`ProjectScoped`**, **`ReachableFrom`** and **`InsertableFrom`** the
   attribute and the field marks call for — and the **capabilities**
   `ops(...)` declares: **`SelectDao`**, **`InsertDao`** (or
   **`Scd2OpenDao`** on an `scd2` relation), **`UpdateDao`**,
   **`UpsertDao`**, **`DeleteDao`**, **`Scd2CloseDao`**.
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
let f: FunctionDB = conn.scd2().fetch_one(and!(&function_id, &enabled)).await?;
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
  │    └── Partitioned — Partition/PARTITION: per-entity histories
  │         ├── Scd2Dao     — VALID_FROM/VALID_TO: the [valid_from, valid_to) chain
  │         └── TimelineDao — VersionOrder/VERSION_ORDER: a readable version log
  ├── UnversionedDao — marker: one row per entity, no version log
  └── TableDao       — marker: a base table, so writes are legal

SelectDao     (ops(select))         — marker: decodes a row; requires FromRow
Insertable    (ops(insert))         — insert_values(), over a TableDao; the payload, not a capability
├── InsertDao                       — the plain insert: db.insert(..)
└── Scd2OpenDao                     — a new version: db.scd2().open(.., at); Insertable + Scd2Dao
UpdateDao     (ops(update(RowDao))) — FilterBy, update_values(), over a TableDao
DeleteDao     (ops(delete))         — marker, over an unversioned TableDao
Scd2CloseDao  (ops(close))          — At: db.scd2().close(.., at), over an scd2 TableDao
UpsertDao     (ops(upsert(A, B)))   — key_columns(), value_columns(); an unversioned Insertable
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
  instant, not per row, and the partition column matches every version —
  and the read that takes it is `timeline().fetch_one` / `fetch_optional`:
  "this exact version", no time predicate.
- On `scd2()`, `scd2().as_of(&t)` and `scd2().last()`, they take an
  `AddressesOneVersion<D>`:
  the partition column qualifies, since intervals within a partition are
  disjoint and one is visible at any instant, and so does every declared
  `unique(..)` or `unique_at(..)` set. The pk does **not**: a version id has
  already picked the version, so ANDing an instant onto it could only turn a
  hit into a miss. `unique_at(..)` is unique among the
  versions visible at one instant only — one `(collection, name)` at a time,
  an invariant the write path keeps rather than an index — so it addresses
  one version but not one row of the history
  (`project_scoped(unique_at(CollectionIdName, FunctionIdName))`), and the
  name-addressed read is `and!(&collection, &name)`.

Both are the compile-time half of the claim. The runtime half is that a
second matching row is an error (`single_row`), never a silent pick.

Every wordless read (`fetch_one`, `fetch_optional`, `fetch_all`,
`fetch_first`, `fetch_last`, `list`, `fetch_nth_last`, `assert_exists` /
`assert_not_exists`) binds on `UnversionedDao`, which only an unversioned
relation implements. On a versioned one they do not exist: reach for
`scd2()` / `scd2().as_of(&t)` for the version visible at a time, or
`timeline()` to read the version log on purpose. The wordless `insert` is the
same: `InsertDao` is the plain insert, an `scd2` relation's `ops(insert)` DAO
is an `Scd2OpenDao` instead and opens a version through
`scd2().open(.., at)`, so the write that stamps `valid_from` and closes the
previous interval always says so, and says when, at the call site.

Materializations, whose close-out trigger keeps pending versions, also mint
through `table_data_versions().pending(.., at)`: an empty interval at `at`
that closes nothing and no read sees. `table_data_versions().settle(.., at)`
writes the outcome and puts it into the timeline at `at`, superseding the
open row then; a row already open is updated in place.

A `view = "..."` DAO gets no `TableDao`, so `insert`, `update` and `delete`
do not typecheck for it. `SELECT ... FOR UPDATE` stays available: locking
through a view locks the rows of every table it draws from.

A `#[dao(table = "...", ops(update))]` DAO is an update payload: `update` is
the only op it may declare, and it gets no `FromRow` — its `Option` fields
mean "leave the column alone", which a readable DAO would read as "nullable".
Its condition is typed over `U::FilterBy`, the payload DAO itself. A
partial-column payload names the relation's row DAO instead —
`#[dao(table = "executions", ops(update(ExecutionDB)), project_scoped)]` —
so it can be filtered on a column it does not carry. The named DAO must
address the same relation, asserted at compile time against
`Dao::RELATION`: a view's columns can be computed
(`collections_resolved.effective_base_path` is a `COALESCE`), and must not supply the filter
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
| `ops(close)` | `scd2` only: the row DAO `close(.., at)` ends intervals through |
| `scd2(at = T)` | On an `insert` or `close` DAO: the instant type `open` / `close` take (default `LockInstant`) |
| `timeline` | `TimelineDao` only — a positional history, no interval chain; same write bans |
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
connection), and no DAO declaring `insert` may carry a `Scd2ValidTo` one (the
close-out trigger owns the interval end). Both look through `Option<..>` and
wrapper newtypes, both run over the flattened field list too, and both are
compile errors naming the reason. That is what splits a written relation into
an insert DAO and a row DAO that flattens it and projects what the database
fills.

Which single column plays which role is a **field mark**:

| Mark | Role |
|------|------|
| `#[dao(flatten)]` | Embed a base DAO's columns (base first); on an `scd2` row DAO also the `Scd2Interval` projection |
| `#[dao(immutable)]` | Set-once column in generated UPDATEs |
| `#[dao(order_by)]` / `#[dao(order_by(asc|desc))]` | Default result ordering |
| `#[dao(pk)]` | The primary key, the row's addressing column |
| `#[dao(partition_by)]` | The partition of any versioned relation (`Partitioned::PARTITION`) |
| `#[dao(timeline(version_by))]` | Timeline version order (non-SCD2) |

A mark that only means something under one struct-level grain (`timeline`)
nests under that grain's word, and the struct must declare the
grain: `#[dao(timeline(version_by))]` on an `scd2` DAO is an error at the
mark. `scd2` owns no field mark: its interval columns are always `valid_from`
and `valid_to`, written by the doors, never by a payload. `partition_by`
belongs to every versioned relation, so it stays flat, as do the grain-free
`pk`, `order_by`, `immutable` and `flatten`. One attribute may carry several:
`#[dao(pk, partition_by)]`.

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
#[dao(view = "collections_resolved", project_scoped)]
pub struct ResolvedCollectionDB {
    #[dao(pk)]
    pub id: CollectionId,
    pub project_id: ProjectId,
    pub name: CollectionName,
    pub description: Description,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub group_id: GroupId,
    pub base_path: Option<BasePath>,
    pub deleted: CollectionWhenDeleted,
}
```

This generates `ResolvedCollectionDBIden` with variants `Table`, `Id`,
`ProjectId`, `Name`, … and implements `Dao` with `columns()` / `order_by()`,
`PkDao`, `ProjectScoped`, `AddressesOneRow` for `&CollectionId` (the name is
not unique here: a deleted collection keeps its last name), plus `SelectDao`
— a view is readable by construction and declares no `ops`.

### SCD2 DAO Example

```rust
#[ta_objects::Dao]
#[dao(table = "dependencies", ops(insert), scd2, project_scoped)]
pub struct DependencyInsertDB {
    #[dao(pk)]
    pub id: DependencyVersionId,
    #[dao(partition_by)]
    pub dependency_id: DependencyId,
    pub function_id: FunctionId,
    pub table_function_id: OfTable<FunctionId>,
    #[dao(order_by)]
    pub dep_pos: DependencyPos,
    ...
}
```

This implements `TableDao` / `Insertable` (it is a table and declares
`insert`) and `Scd2Dao`. Being `scd2`, it does **not**
implement `UnversionedDao`, and its insert capability is `Scd2OpenDao`, not
`InsertDao`: `db.insert(&dep)` does not compile, `db.scd2().open(&dep, at)`
does, `at` being the unit of work's `LockInstant`. The payload carries no
`valid_from` / `valid_to`: a field of either name or type is an error, since
`open` binds the start and the close-out trigger owns the end.

The row DAO is its flatten plus the interval the doors wrote, and it is the
one that is read; it alone implements `TimelineDao`:

```rust
#[ta_objects::Dao]
#[dao(table = "dependencies", ops(select), scd2, project_scoped)]
pub struct DependencyDB {
    #[dao(flatten)]
    pub base: DependencyInsertDB,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}
```

`SelectDao` is what enables `scd2()` / `scd2().as_of()` / `timeline()` /
`timeline().at()` / `timeline().heads_at()` — the kind traits are structure, and say
only how the relation is versioned.

## Query Surfaces

Every read is one verb over one **grain**. The grain says which rows a read
sees: a condition AND-ed onto the caller's, plus any statement suffix (a row
lock). The verb says the shape: `LIMIT 2` for a single row, ordered
`LIMIT 1` for the first or last, ordered for all, keyset-paged for a list.
The verbs are written once on `Reader<E, G>`; the doors pick the grain.

| Door | Grain | Rows | DAO bound |
|------|-------|------|-----------|
| `db.<verb>(..)` | `Unversioned` | the relation's rows | `UnversionedDao` |
| `db.for_update()` | `Locked` | the same, `FOR UPDATE` until the transaction ends | `UnversionedDao` + base table |
| `db.scd2()` | `Current` | the open intervals (`valid_to = 'infinity'`, served by the partial indexes) | `Scd2Dao` |
| `db.scd2().as_of(&t)` | `VisibleAt` | the versions visible at `t` | `Scd2Dao` + `TimelineDao` |
| `db.scd2().last()` | `LastVersion` | every matching timeline's last version, open or closed | `Scd2Dao` + pk |
| `db.timeline()` | `AllVersions` | every version row, no time predicate | `TimelineDao` |
| `db.timeline().until(&t)` | `Until` | every version at or before `t` | `TimelineDao` |

The bound is the grain's `Grain<D>` impl, so a read of the wrong kind of
relation does not compile: the wordless grain refuses a versioned relation,
`scd2()` an unversioned one, `last()` a DAO without a pk.

| Verb | Description |
|------|-------------|
| `fetch_one(condition)` | One row, error if absent; the condition must address one row (below) |
| `fetch_optional(condition)` | Same, `None` if absent |
| `fetch_first(condition)` | The first row in the DAO's order, of however many match |
| `fetch_last(condition)` | The last row in that order |
| `fetch_nth_last(condition, offset)` | The row `offset` places from the end (`0` = last) |
| `fetch_all(condition)` | Every matching row, in the DAO's order |
| `assert_exists` / `assert_not_exists` | Existence checks with a clean error |
| `list(condition, params)` | Paginated/filtered SELECT, returns DTOs |
| `chunked().fetch_all(values, extra)` | `fetch_all` over a slice too large for one statement's bind parameters: each chunk is one statement, the slice's conditions OR-joined and `extra` AND-ed onto every chunk |

Every read bounds the executor with `Serves<D>`: its domain, and an RLS scope
that can see `D`'s rows. A cross-project `list` narrows by the caller's
viewable projects in the condition and runs on `AcrossProjects`; a `Global`
handle cannot list a project-partitioned relation.

Writes live on the grain that has them:

- The wordless grain: `insert(dao)`, `upsert(dao)` / `insert_or_ignore(dao)`
  (for an `UpsertDao`), `update(dao, condition)` (typed on `U::FilterBy`,
  an `UnversionedDao`), `delete(condition)` (a `DeleteDao`), and
  `chunked()` with `insert(&[D])` and `update(dao, values)`.
- `scd2()`: `open(&dao, at)` (the trigger closes the partition's previous
  open row), `update(&dao, condition)` (an in-place payload change of the
  matching **open** rows; closed versions are history), `close::<D>(condition,
  at)` (the SCD2 deletion; `at` is the instant written into `valid_to`), and
  `chunked()` with `open(&[D], at)`, `update(&dao, values)` and
  `close(values, extra, at)`. An update payload over a versioned relation
  must leave the partition, version-order and interval-end columns alone; the
  macro does not check this.
- `for_update()`: `claim_all(condition)`, the `SKIP LOCKED` claim. A write
  unit's `LockInstant`, which `scd2().open` and `close` stamp, comes from the
  unit itself: `trx.locked_at()`.

`timeline()` mirrors the trait as `scd2()` mirrors `Scd2Dao`: on a `timeline`
relation it is the only door; on an `scd2` one (`Scd2Dao: TimelineDao`) it is
the deliberate read of the log rather than the interval, and the wordless
surface refuses both precisely so the choice shows at the call site. Its
positional reads follow.

### `DataVersion<'a, E, V, P>` — `db.timeline().at(&on, &partition)`

The version log read by position: a `Versions` selector (`Single`, `List` or
`Range` of `Head~k` / `Initial+k` / `Fixed` positions) over one partition at
`on` — `fetch_one`, `fetch_optional`, `fetch_all`, `fetch_relative_offset`. Positions resolve as top-k scans and
key probes on the `(partition, version_order, pk)` index, no window
functions.

### `Heads<'a, E, V, P>` — `db.timeline().heads_at(&on, &partitions)`

The newest version of each of many partitions at `on`, the `Head~0` of
`DataVersion` for all of them: `fetch_all`, at most one row per partition.
One top-1 `LATERAL` probe per partition on the same index, planned once per
chunk of partitions.

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
db.table_data_versions().committed_data_at(&table_id, &at).await?;
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
let function: FunctionDB = conn.scd2().fetch_one(&function_id).await?;

// By name: the `unique_at` set on the view.
let function: FunctionDBWithNames = conn
    .scd2()
    .fetch_one(and!(&collection_id, &function_name))
    .await?;

// The write unit's instant, taken before the grant unlocks the connection.
let at = trx.locked_at();

// Topology relations take the instant of the project's new topology version.
let topology_at = conn.topology().bump(&project_id, at).await?;

// A new version open from `topology_at`: the trigger closes the previous open
// row of the partition at that same instant.
conn.scd2().open(&function_version, topology_at).await?;

// Deletion is the closed interval with no successor: after this the entity
// is gone from `scd2()` and from `scd2().as_of(&t)` for any `t >= topology_at`.
conn.scd2().close::<FunctionDB>(&function_id, topology_at).await?;

// Visible at a time, by condition — cost follows the condition.
let in_collection: Vec<FunctionDB> = conn.scd2().as_of(&at).fetch_all(&collection_id).await?;

// Every version of a partition: the log, read on purpose.
let all: Vec<FunctionDB> = conn.timeline().fetch_all(&function_id).await?;

// A Versions selector over one partition: the log read by position.
let version: LogicalTableDataVersionDB = conn
    .timeline()
    .at(&at, &table_id)
    .fetch_one(&versions)
    .await?;
```

The turbofish, when the result type does not pin the DAO, goes on the
terminal: `conn.scd2().fetch_all::<FunctionDB>(&collection_id)`.

### Locks and Chunked Grains

```rust
use td_queries::Queries;

// Locked for the rest of the transaction.
let locked: WorkerDB = conn.for_update().fetch_one(&worker_id).await?;

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
