<!--
Copyright 2026 Tabsdata Inc.
-->

# td-queries

Type-safe, composable query abstractions for the Tabsdata database layer.

`td-queries` sits between application logic and `td-database`, providing trait-based
query patterns (CRUD, versioned, current-view, recursive, session) that work across
SQLite, PostgreSQL, and MySQL. Queries are built with **sea-query** and executed
via **sqlx** through a multi-database dispatch bridge.

## Table of Contents

- [The Sea-Query / SQLx Bridge](#the-sea-query--sqlx-bridge)
- [The Executor](#the-executor)
- [DAO/DTO System — From/Into Rows](#daodto-system--frominto-rows)
- [Query Traits](#query-traits)
- [Transaction Handling](#transaction-handling)
- [Tower Integration](#tower-integration)
- [Usage Examples](#usage-examples)

## The Sea-Query / SQLx Bridge

The bridge converts a sea-query AST into an sqlx-executable statement in three layers.

### Layer 1 — Building SQL + Values

The `SeaQuerySqlxBuild` trait (defined in `td-database`) converts any sea-query
statement into a `(String, SqlxValues)` tuple:

```rust
pub trait SeaQuerySqlxBuild {
    fn build_sea_query(self, db: impl DatabaseKind) -> (String, SqlxValues);
}
```

It is implemented for `&mut T where T: QueryStatementWriter` (all sea-query
builders: `SelectStatement`, `InsertStatement`, `UpdateStatement`,
`DeleteStatement`, etc.) and for `RawSeaQuery`. The implementation dispatches on
the database backend to choose the correct SQL dialect builder:

```rust
// Simplified — actual code in td-database/src/sea_query/builder.rs
let (sql, values) = match db.kind() {
    DatabaseBackend::Sqlite  => self.build(SqliteQueryBuilder),
    DatabaseBackend::Postgres => self.build(PostgresQueryBuilder),
    DatabaseBackend::MySql   => self.build(MysqlQueryBuilder),
};
(sql, SqlxValues(values))
```

`SqlxValues` (from the `sea-query-sqlx` crate) wraps the parameter vector and
implements sqlx's `Arguments` trait for every supported database.

### Layer 2 — Fluent Builder API

The `SeaQueryExt` trait adds fluent methods to any `SeaQuerySqlxBuild` type:

```rust
pub trait SeaQueryExt: SeaQuerySqlxBuild + Sized {
    fn build_query(self)           -> SeaQueryBuilder<Self>;       // raw DynamicRow
    fn build_query_as<O>(self)     -> SeaQueryAsBuilder<Self, O>;  // typed O: DynamicFromRow
    fn build_query_scalar<O>(self) -> SeaQueryScalarBuilder<Self, O>; // scalar O: DynamicDecode
}
```

Each builder type exposes execution methods (`execute`, `fetch_one`, `fetch_all`,
`fetch_optional`, `fetch`) that accept a `SeaExecutor`.

| Builder                  | Output type        | sqlx function used   |
|--------------------------|--------------------|----------------------|
| `SeaQueryBuilder`        | `DynamicRow`       | `query_with`         |
| `SeaQueryAsBuilder<_,O>` | `O: DynamicFromRow`| `query_as_with`      |
| `SeaQueryScalarBuilder`  | `O: DynamicDecode` | `query_scalar_with`  |

### Layer 3 — Multi-Database Execution

The `SeaExecutor` trait (in `td-database`) defines the execution interface:

```rust
pub trait SeaExecutor: DatabaseKind {
    fn execute(self, query: impl SeaQuerySqlxBuild)
        -> BoxFuture<'_, Result<DynamicQueryResult, Error>>;
    fn fetch_all_as<O: DynamicFromRow>(self, query: impl SeaQuerySqlxBuild)
        -> BoxFuture<'_, Result<Vec<O>, Error>>;
    fn fetch_one_scalar<O: DynamicDecode + DynamicType>(self, query: impl SeaQuerySqlxBuild)
        -> BoxFuture<'_, Result<O, Error>>;
    // ... fetch_one, fetch_optional, fetch_one_as, fetch_optional_as, etc.
}
```

Under the hood, a set of dispatch macros handles per-database branching at compile
time without runtime type erasure:

- **`resolve_executor!`** — pattern-matches the `DynamicPool`/`DynamicConnection`
  enum variant to extract the concrete sqlx executor.
- **`dispatch_future_raw!`** / **`dispatch_future_typed!`** — call
  `build_sea_query()`, then route to `sqlx::query_with` or
  `sqlx::query_as_with`/`query_scalar_with` through the resolved executor.
- **`impl_sea_executor!`** — expands all trait methods in a single macro call,
  parameterized by read/write context tokens.

`SeaExecutor` is implemented for three types:

| Implementor              | Read pool | Write pool | Notes                     |
|--------------------------|-----------|------------|---------------------------|
| `&TdPool`                | `ro`      | `rw`       | Main application pool     |
| `&DynamicPool`           | `ro`      | `rw`       | Inner pool enum           |
| `&mut DynamicConnection` | same conn | same conn  | Transactional operations  |

## The Executor

All query traits in this crate use **blanket implementations** on
`E: SeaExecutor + Send`. This means every executor type automatically gains
every query method:

```rust
impl<E> BasicQueries for E
where
    E: SeaExecutor + Send,
{ ... }
```

The same pattern applies to `BasicQueries`, `ChunkedQueries`, and
`RecursiveEntry`. Per-DAO surfaces (sessions, dependencies, executions,
LFRs, PFRs, LTDVs, and their pointer event logs) are reached through
their own entry traits — see [Per-DAO Surfaces](#per-dao-surfaces) below.

The point-in-time / current / historic surfaces are reached via the
`QueryEntry` trait constructors (`db.current()`, `db.versioned(&at)`,
`db.historic(&at)`) — each returns a wrapper type whose inherent
methods are the right operations for that read mode. See
[Query Traits](#query-traits) below.

In practice, this means you can call query methods directly on a pool reference,
a mutable connection, or a transaction-converted connection:

```rust
// On a pool
let user: UserDB = pool.fetch_one(&user_id).await?;

// On a connection (transactional)
let mut conn = tx.into_connection();
conn.insert(&new_user).await?;
conn.into_transaction().unwrap().commit().await?;
```

### Read/Write Routing

When using `TdPool` or `DynamicPool`, the executor automatically routes:
- **Read operations** (`fetch_*`, `fetch_*_as`, `fetch_*_scalar`) go through the
  `ro` (read-only) pool.
- **Write operations** (`execute`) go through the `rw` (read-write) pool.

This separation allows read replicas or separate connection limits for reads vs.
writes.

## DAO/DTO System — From/Into Rows

### DynamicFromRow

The `DynamicFromRow` trait (in `td-database/src/dynamic.rs`) composes the
per-database `FromRow` implementations into a single abstraction:

```rust
pub trait DynamicFromRow: FromSqliteRow + FromPgRow + FromMySqlRow {}
```

Each sub-trait is conditionally compiled based on the enabled database feature.
Thanks to blanket implementations, any struct that derives `sqlx::FromRow` for
the enabled backends automatically implements `DynamicFromRow`.

Similarly, `DynamicDecode` and `DynamicType` compose per-database `Decode` and
`Type` traits for scalar value extraction.

### The `#[Dao]` Macro

The `#[ta_objects::Dao]` attribute macro (defined in `tm-objects`) generates:

1. An **`Iden` enum** (e.g., `CollectionDBIden`) with variants for each field
   plus a `Table` variant. Field names are converted from `snake_case` to
   `UpperCamelCase` (e.g., `created_on` becomes `CreatedOn`).
2. **`sqlx::FromRow`** derive for all enabled database backends.
3. Implementations of **`DaoCore`**, **`Dao`**, and optionally **`DaoUpdate`**,
   **`VersionedDao`**, **`CurrentDao`**, **`Recursive`**, **`States<S>`**.
4. A **`derive_builder`** builder struct for constructing instances.

### DAO Trait Hierarchy

```
DaoCore           — table(), display_name(), column_display_values()
  ├── DaoUpdate   — update_values()  (for UPDATE SET)
  └── Dao         — columns(), insert_values(), order_by()  (requires DynamicFromRow)
       ├── VersionedDao  — pk_col(), partition_col(), version_order_col()
       │    ├── CurrentDao     — current_table_name()
       │    └── DataVersionDao — (VersionedDao + DaoUpdate)
       └── Recursive     — recurse_up(), recurse_down()
```

### `#[dao(...)]` Attribute Options

| Attribute | Purpose |
|-----------|---------|
| `sql_table = "..."` | Override the SQL table name |
| `update` | Generate only `DaoUpdate` (no `FromRow`/`Dao`) |
| `order_by(col = "...", dir = "...")` | Default result ordering |
| `versioned(pk, partition_by, version_by, current)` | Enable `VersionedDao` / `CurrentDao` |
| `recursive(up = "...", down = "...")` | Enable `Recursive` for CTE traversal |
| `data_version` | Enable `DataVersionDao` |
| `states(Name = Expr, ...)` | Compile-time state filters via `States<S>` |
| `unique(field1, field2, ...)` | Uniqueness constraints for error reporting |

### Example DAO Definition

```rust
#[ta_objects::Dao]
#[dao(sql_table = "collections_active")]
pub struct CollectionDB {
    pub id: CollectionId,
    pub name: CollectionName,
    pub description: Description,
    pub created_on: AtTime,
    pub created_by_id: UserId,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
}
```

This generates `CollectionDBIden` with variants `Table`, `Id`, `Name`,
`Description`, `CreatedOn`, `CreatedById`, `ModifiedOn`, `ModifiedById`, and
implements `Dao` with `columns()`, `insert_values()`, etc.

### Versioned DAO Example

```rust
#[ta_objects::Dao]
#[dao(
    sql_table = "dependencies",
    order_by(col = "dep_pos", dir = "desc"),
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "dependency_id",
        current = "dependencies_current"
    ),
    recursive(up = "table_function_id", down = "function_id"),
    states(
        Active = DependencyStatus::Active,
        User = and!(DependencyStatus::Active, System::FALSE),
    )
)]
pub struct DependencyDB { ... }
```

This additionally implements `VersionedDao` (enabling `VersionedQueries`),
`CurrentDao` (enabling `CurrentQueries`), `Recursive` (enabling
`RecursiveQueries`), and `States<1>` (`Active`) / `States<2>` (`User`) for
compile-time state filtering.

## Query Traits

### BasicQueries

Core CRUD operations for any `Dao`:

| Method | Description |
|--------|-------------|
| `insert(dao)` | INSERT a row |
| `fetch_one(condition)` | SELECT one row (error if not found) |
| `fetch_optional(condition)` | SELECT one row or None |
| `fetch_all(condition)` | SELECT all matching rows |
| `update::<U, D>(dao, condition)` | UPDATE matching rows |
| `delete::<D>(condition)` | DELETE matching rows |
| `list(condition, filter, params)` | Paginated/filtered SELECT, returns DTOs |
| `fetch_all_in::<S, D>(condition)` | SELECT with compile-time state filter |
| `fetch_latest_before::<S, D>(condition, offset)` | Nth most recent row |

### `Current<E>` — `db.current()`

Reads from a [`CurrentDao`]'s `_current` materialised view (maintained
by triggers, contains one row per partition — the latest version). No
CTE, no time travel. Reach via `db.current()`:

| Method | Description |
|--------|-------------|
| `fetch_one::<S, D>(condition)` | One current row |
| `fetch_optional::<S, D>(condition)` | Optional current row |
| `fetch_all::<S, D>(condition)` | All current rows |
| `fetch_all_chunked::<S, D, V>(values)` | All current rows for many values, OR'd & chunked |
| `list::<S, F, D>(condition, filter, params)` | Paginated current list |

### `Versioned<'a, T, E>` — `db.versioned(&at)`

Partition-scoped point-in-time read — the **safe** versioned surface.
Every method takes the partition and pushes it into the ranked CTE, so
the window only ranks rows for that partition. Cost is
O(rows-in-partition).

| Method | Description |
|--------|-------------|
| `fetch_one::<S, D>(partition)` | Latest version in `partition` at `at` |
| `fetch_optional::<S, D>(partition)` | Optional variant |
| `fetch_all_chunked::<S, D>(partitions)` | One row per partition at `at`, chunked |
| `data_version(partition).fetch_one::<S, D>(versions)` | `DataVersionDao` row resolution by [`Versions`] selector (Head/Initial/Fixed/Range/List) |
| `data_version(partition).fetch_optional::<S, D>(versions)` | Optional variant |
| `data_version(partition).fetch_all::<S, D>(versions)` | All matching rows |
| `data_version(partition).fetch_relative_offset::<S, D>(version)` | The `rn` of `version` in the partition timeline |

### `Historic<'a, T, E>` — `db.historic(&at)`

Cross-partition ranked-CTE scan — the **slow path**, opt-in by name.
Reach this surface only when the partition isn't known at the call
site (e.g. by-name lookups). Cost is O(table size). Per the safety
memo, the slow path is reachable *only* by saying `historic(at)`
explicitly.

| Method | Description |
|--------|-------------|
| `fetch_one::<S, D>(condition)` | Latest version matching `condition` at `at` |
| `fetch_optional::<S, D>(condition)` | Optional variant |
| `fetch_all::<S, D>(condition)` | All latest versions at `at` |
| `fetch_all_chunked::<S, D, V>(values)` | Same, chunked over `values` |
| `list_versions::<S, F, D>(condition, filter, params)` | Paginated latest versions |
| `list_full::<S, F, D>(condition, filter, params)` | All historical versions up to `at` |

The CTE strategy:
```sql
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY <partition_col>
        ORDER BY <version_order_col> DESC, <pk_col> DESC
    ) AS rn
    FROM <table>
    WHERE <version_order_col> <= ?
)
SELECT * FROM ranked WHERE rn = 1 AND <condition> AND <state>
```

### `Recursive<O, E>` — `db.recursive(opts)`

Graph traversal using `WITH RECURSIVE` CTEs. Used for upstream/downstream
dependency resolution. Dispatch is by the type of `opts`:

| `opts` type | Method | Description |
|-------------|--------|-------------|
| [`Options`] | `fetch_all::<D, R>()` | Non-versioned recursion |
| [`VersionedOptions<'a, D, R>`] | `fetch_all::<DS, RS>()` | Versioned recursion (D, R inferred from opts) |

### Per-DAO Surfaces

Each per-DAO surface is reached through an entry method on the executor
that yields a wrapper handle. The handle's inherent methods drop the
DAO/verb prefixes — the wrapper is the namespace.

```rust
db.sessions().purge().await?;
db.dependencies().active_for_function_at(&fid, &at).await?;
db.executions().list_by_table_data_version(&tdv, &params).await?;

// LFR identity + chain queries
db.lfr().previous_for_function_at(&fid, &before).await?;
db.lfr().to_commit(&execution_ids).await?;
db.lfr().to_execute(&at).await?;
db.lfr().for_dispatch(&fr_ids).await?;
db.lfr().blocked_ids(&execution_id, &at).await?;

// PFR identity queries (replay dispatch / in-flight checks)
db.pfr().to_dispatch().await?;
db.pfr().in_flight_for(&execution_id, &function_id).await?;

// LTDV queries + pointer-walking PTDV reads
db.ltdv().latest_bound_tdv_with_data_for_table(&table_id, &triggered_on, true).await?;

// Pointer event logs reached via the entity handle's `.pointers()`.
// The type parameter is bound by which entity you came from — no
// turbofish at the call site. At-time reads chain through `.at(&at)`
// mirroring the `versioned(&at).fetch_*` surface.
db.lfr().pointers().at(&at).concrete(&lfr_id).await?;
db.lfr().pointers().bind_with_known_state(&lfr_id, &fr_id, &latest, &set_on, &by).await?;
db.ltdv().pointers().at(&at).concrete(&ltdv_id).await?;
```

Each module owns its own entry trait (`SessionsEntry`, `DependenciesEntry`,
`LfrEntry`, `PfrEntry`, `LtdvEntry`, …). Adding a new per-DAO surface is
one `query_handle!` invocation plus the inherent `impl` for the wrapper.

## Transaction Handling

Transactions follow a three-step pattern:

```rust
// 1. Begin a transaction (uses the rw pool)
let tx: DynamicTransaction = pool.begin().await?;

// 2. Convert to a connection for query execution
let mut conn: DynamicConnection = tx.into_connection();

// Execute queries on the connection — all within the transaction
conn.insert(&dao).await?;
let result: MyDao = conn.fetch_one(&condition).await?;
conn.update::<MyUpdate, MyDao>(&update, &condition).await?;

// 3. Extract back to transaction and commit (or rollback)
conn.into_transaction()
    .expect("connection was created from a transaction")
    .commit()
    .await?;
```

`DynamicTransaction` also provides `as_mut_conn()` for borrowing the connection
without consuming the transaction:

```rust
let mut tx = pool.begin().await?;
{
    let mut conn = tx.as_mut_conn();
    conn.insert(&dao).await?;
}
tx.commit().await?;
```

## Tower Integration

The `tower::by` module provides ready-made async handler functions compatible
with the `td-tower` framework.

### Direct Insert Handlers

```rust
// Insert a single DAO
pub async fn insert<D: Dao>(
    conn: MutRef<DynamicConnection<'static>>,
    dao: Ref<D>,
) -> Result<(), TdError>;

// Insert a Vec of DAOs
pub async fn insert_vec<D: Dao>(
    conn: MutRef<DynamicConnection<'static>>,
    daos: Ref<Vec<D>>,
) -> Result<(), TdError>;
```

### `By<E>` — Generic CRUD Handlers

`By<E>` is a phantom-typed struct providing static async methods that combine
a condition extractor `E` with query execution:

```rust
// Fetch one by condition
By::<UserId>::fetch_one::<UserDB>(conn, by).await?;

// Update by condition
By::<UserId>::update::<UserUpdateDB, UserDB>(conn, update, by).await?;

// Delete by condition
By::<UserId>::delete::<UserDB>(conn, by).await?;

// Paginated list
By::<CollectionId>::list::<MyFilter, MyListDto>(conn, by, filter, list_params, query_params).await?;

// Versioned queries
By::<FunctionId>::fetch_one_version::<ACTIVE, FunctionDB>(conn, on, by).await?;
By::<CollectionId>::historic_list_versions::<MyFilter, ACTIVE, MyDto>(conn, by, filter, on, lp, qp).await?;

// Current queries
By::<CollectionId>::fetch_one_current::<ACTIVE, CollectionDB>(conn, by).await?;
```

## Usage Examples

### Basic CRUD

```rust
use td_queries::BasicQueries;

// Insert
pool.insert(&collection_dao).await?;

// Fetch by condition (SqlCondition can be a typed ID, tuple, etc.)
let user: UserDB = pool.fetch_one(&user_id).await?;

// Fetch optional
let maybe_user: Option<UserDB> = pool.fetch_optional(&user_id).await?;

// Fetch all matching a condition
let roles: Vec<RoleDB> = pool.fetch_all(&user_id).await?;

// Update
pool.update::<UserUpdateDB, UserDB>(&update_dao, &user_id).await?;

// Delete
pool.delete::<UserDB>(&user_id).await?;
```

### Versioned Point-in-Time Query

```rust
use td_queries::QueryEntry;

// Partition-scoped (safe): function_id is the partition column.
let function: FunctionDB = pool
    .versioned(&timestamp)
    .fetch_one::<{ FunctionDB::ACTIVE }, FunctionDB>(&function_id)
    .await?;

// Cross-partition (slow): name lookup, no partition known.
let by_name: Vec<FunctionDB> = pool
    .historic(&timestamp)
    .fetch_all::<{ FunctionDB::ACTIVE }, FunctionDB>(&function_name)
    .await?;

// List all latest versions at a point in time.
let functions: Vec<FunctionListDto> = pool
    .historic(&timestamp)
    .list_versions::<{ FunctionDB::ACTIVE }, MyFilter, FunctionListDto>(
        &collection_id, &filter, &query_params,
    )
    .await?;
```

### Transaction Flow

```rust
use td_queries::BasicQueries;

let tx = pool.begin().await?;
let mut conn = tx.into_connection();

conn.insert(&new_collection).await?;
conn.insert(&new_function).await?;

conn.into_transaction().unwrap().commit().await?;
```

### Custom Query with Sea-Query

```rust
use sea_query::Query;
use td_database::sea_query::builder::SeaQueryExt;

// Build a custom query and execute it
let count: i64 = Query::select()
    .expr(sea_query::Func::count(sea_query::Expr::col(MyDaoIden::Id)))
    .from(MyDao::table())
    .and_where(sea_query::Expr::col(MyDaoIden::Status).eq("active"))
    .build_query_scalar()
    .fetch_one(pool)
    .await?;
```
