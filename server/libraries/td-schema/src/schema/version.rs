//
// Copyright 2026 Tabsdata Inc.
//

//! The static authoring contract: [`SchemaDelta`] (a migration between two
//! consecutive versions). A full schema snapshot is a module of part functions
//! (see `current/`) exposed through a [`SchemaSource`](super::SchemaSource).

use super::backend::Backend;

/// A migration between two consecutive schema versions. `up` must end by bumping
/// `db_version` to `TO_VERSION`: each step commits separately, so that bump is
/// what lets a failed ladder resume instead of replaying.
pub trait SchemaDelta {
    /// Version migrated from.
    const FROM_VERSION: usize;
    /// Version migrated to.
    const TO_VERSION: usize;
    /// Statements upgrading from `FROM_VERSION` to `TO_VERSION`.
    fn up(backend: Backend) -> Vec<String>;
}
