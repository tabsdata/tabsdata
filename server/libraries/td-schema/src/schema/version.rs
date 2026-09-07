//
// Copyright 2026 Tabsdata Inc.
//

//! The static authoring contract: [`SchemaDelta`] (a migration between two
//! consecutive versions). A full schema snapshot is a module of part functions
//! (see `current/`) exposed through a [`SchemaSource`](super::SchemaSource).

/// A migration between two consecutive schema versions. `up` must end by
/// bumping `db_version` to `TO_VERSION`. The whole ladder runs as one
/// transaction (and the executor stamps the final version itself), so the
/// bump is a per-delta invariant the migration-guard tests enforce, not what
/// a resume relies on.
pub trait SchemaDelta {
    /// Version migrated from.
    const FROM_VERSION: usize;
    /// Version migrated to.
    const TO_VERSION: usize;
    /// Statements upgrading from `FROM_VERSION` to `TO_VERSION`.
    fn up() -> Vec<String>;
}
