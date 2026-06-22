//
// Copyright 2026 Tabs Data Inc.
//

//! The static authoring contract: [`SchemaVersion`] (a full schema snapshot) and
//! [`SchemaDelta`] (a migration between two consecutive versions).

use super::backend::{self, Backend};

/// A complete schema snapshot, implemented by `current::Schema`.
///
/// Tabsdata 2.0+ never upgrades across majors: a binary creates a fresh database
/// and refuses one from an older major (gated by the `tabsdata_version` row, see
/// [`CURRENT_TABSDATA_VERSION`](crate::CURRENT_TABSDATA_VERSION)). `VERSION` counts
/// schema revisions *within* the current major. To evolve the schema, follow the
/// runbook in `README.md`; `test_schema_fingerprint` enforces a fingerprint refresh.
pub trait SchemaVersion {
    /// The schema revision this snapshot represents.
    const VERSION: usize;
    /// Per-backend SHA-256 fingerprints of [`build`](Self::build)'s output (indexed
    /// by [`Backend::index`](backend::Backend::index)), guarded against silent edits.
    const FINGERPRINTS: [&'static str; backend::BACKEND_COUNT];
    /// All statements that create the schema from scratch for `backend`.
    fn build(backend: impl Into<Backend>) -> Vec<String>;
}

/// A migration between two consecutive schema versions. `up` returns only content
/// statements — the pool writes `db_version` after applying it.
pub trait SchemaDelta {
    /// Version migrated from.
    const FROM_VERSION: usize;
    /// Version migrated to.
    const TO_VERSION: usize;
    /// Per-backend SHA-256 fingerprints of [`up`](Self::up)'s output, frozen once
    /// the delta lands.
    const FINGERPRINTS: [&'static str; backend::BACKEND_COUNT];
    /// Statements upgrading from `FROM_VERSION` to `TO_VERSION`.
    fn up(backend: Backend) -> Vec<String>;
}
