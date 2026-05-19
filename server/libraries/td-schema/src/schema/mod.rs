//
// Copyright 2026 Tabs Data Inc.
//

pub mod archive;
pub mod backend;
pub mod current;
pub mod deltas;

use backend::Backend;

/// A complete schema snapshot.
///
/// Implemented by `current::Schema`. The [`build()`](SchemaVersion::build)
/// method must return the full set of SQL statements (tables, views, indices,
/// seed data) that create the schema from scratch for the given backend.
///
/// ## Major-version policy
///
/// As of Tabsdata 2.0, the schema is no longer upgraded in place across major
/// product versions. A 2.0 binary creates a fresh database and refuses to open
/// any database created by 1.x. The `tabsdata_version` row in
/// `tabsdata_system` is the gate that enforces this — see
/// [`td_schema::CURRENT_TABSDATA_VERSION`](crate::CURRENT_TABSDATA_VERSION).
///
/// `Schema::VERSION` is the schema counter *within* the current major; it
/// resets to 1 at each major bump.
///
/// ## Changing the schema (within a major)
///
/// Follow these steps **every time** you modify tables, views, indices, or seed
/// data in `current/`. The `test_schema_fingerprint` test will fail if you skip
/// any step.
///
/// 1. Copy `current/` to `archive/v{VERSION}/` and freeze the archive copy
///    (hardcode `VERSION`, remove the delegating `pub fn build()`).
/// 2. Create `deltas/v{VERSION}_to_v{NEW_VERSION}.rs` implementing
///    [`SchemaDelta`]. The last statement **must** bump `db_version`.
/// 3. Wire up the new archive and delta in their respective `mod.rs` files
///    (both `build()`/`get()` and `fingerprints()` match arms).
/// 4. Add `validate_schema_version` / `validate_delta` tests in each new module.
/// 5. Make your schema changes in `current/`.
/// 6. Bump [`VERSION`](SchemaVersion::VERSION) in `current::Schema`. Run
///    `cargo test -p td-schema` — copy the new fingerprints from the error
///    and update [`FINGERPRINTS`](SchemaVersion::FINGERPRINTS) in
///    `current::Schema` and in the new delta.
/// 7. Verify: `cargo test -p td-schema && cargo test -p td-database --features sqlite`.
///
/// See `README.md` for the full guide with code examples.
pub trait SchemaVersion {
    /// The database version this schema represents.
    const VERSION: usize;

    /// SHA-256 fingerprints of the SQL output, one per backend
    /// (indexed by [`Backend::index()`](backend::Backend::index):
    /// Sqlite=0, Postgres=1, MySql=2).
    ///
    /// The `test_schema_fingerprint` test recomputes these values and fails if
    /// any don't match, preventing silent edits to the baseline schema.
    const FINGERPRINTS: [&'static str; backend::BACKEND_COUNT];

    /// Build the complete schema as a list of SQL statements for the given backend.
    ///
    /// The resulting statements, when executed in order, must create all tables,
    /// views, indices, and seed data for a fresh installation at this version.
    fn build(backend: impl Into<Backend>) -> Vec<String>;
}

/// An incremental schema migration between two consecutive versions.
///
/// The last statement returned by [`up()`](SchemaDelta::up) **must** call
/// `backend.update_db_version(TO_VERSION)`.
pub trait SchemaDelta {
    /// The version being migrated from.
    const FROM_VERSION: usize;
    /// The version being migrated to.
    const TO_VERSION: usize;

    /// SHA-256 fingerprints of the SQL output, one per backend
    /// (indexed by [`Backend::index()`](backend::Backend::index):
    /// Sqlite=0, Postgres=1, MySql=2). Frozen once the delta lands.
    const FINGERPRINTS: [&'static str; backend::BACKEND_COUNT];

    /// SQL statements to upgrade from `FROM_VERSION` to `TO_VERSION`. The
    /// last statement **must** update `db_version` to
    /// [`TO_VERSION`](SchemaDelta::TO_VERSION).
    fn up(backend: Backend) -> Vec<String>;
}

/// A test fixture that creates tables and inserts test data.
///
/// Unlike [`SchemaVersion`], fixtures create lightweight, isolated test
/// schemas — not the full application schema. The SQL returned by
/// [`build()`](Fixture::build) is executed on a fresh, empty database.
pub trait Fixture {
    /// Build the fixture as a list of SQL statements (CREATE TABLE + INSERTs)
    /// for the given backend.
    fn build(backend: Backend) -> Vec<String>;
}

#[cfg(test)]
pub(crate) mod tests;
