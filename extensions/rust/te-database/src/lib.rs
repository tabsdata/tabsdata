//
// Copyright 2026 Tabs Data Inc.
//

//! Database edition extension.
//!
//! Re-exports `td-database`'s management API ([`td_database::components`]) and wires
//! *this edition's* schema components into the inventory registry. The built-in
//! apiserver component self-registers in `td-database`, so the open-source edition
//! adds nothing here; the enterprise edition swaps this crate (it is a `te-*`
//! extension crate) to register its own components — e.g. the agent schema —
//! without the open-source tree referencing them.
//!
//! Binaries pull the registrations into their final link with the
//! [`force_link!`] macro, which plants the `#[used]` anchor that defeats linker
//! dead-code elimination.

pub use td_database::components::*;

/// No-op symbol the [`force_link!`] anchor references to force this crate (and
/// thus its `inventory::submit!`s) into the final link. See the module docs.
pub fn link() {}

/// Plant the `#[used]` anchor that keeps this crate's schema-component
/// registrations in a binary's final link. Without it the linker may drop the
/// `inventory::submit!`s as unreferenced and the registry comes up empty at
/// runtime (caught at boot by
/// [`validate_components`](td_database::components::validate_components)). Invoke once
/// at the top level of every binary that opens the database:
///
/// ```ignore
/// te_database::force_link!();
/// ```
#[macro_export]
macro_rules! force_link {
    () => {
        #[used]
        static _TD_FORCE_DB_COMPONENTS: fn() = $crate::link;
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The built-in apiserver component (registered in `td-database`) reaches the
    /// registry of a binary that links it — the dead-code-elimination guarantee.
    #[test]
    fn registers_builtin_components() {
        let keys: Vec<&str> = registered().map(|c| c.key()).collect();
        assert!(
            keys.contains(&"apiserver"),
            "expected the apiserver component to be registered, got {keys:?}"
        );
    }
}
