//
// Copyright 2026 Tabs Data Inc.
//

//! Backend-feature propagation for downstream consumers of `td-database`.
//!
//! `td-database` (the source of truth) emits its enabled backend features as
//! build-script metadata. This helper reads that metadata in any direct
//! dependent's build script and re-emits matching `#[cfg(sqlite|postgres|mysql)]`
//! flags so the dependent's source can write `#[cfg(postgres)]` instead of
//! declaring (and forgetting to forward) its own `postgres` cargo feature.
//!
//! Consumer build.rs:
//!
//! ```no_run
//! fn main() {
//!     td_build::customizer::{Customization, Customizer};
//!     td_build::customizer::Customization::customize();
//!     td_build::backend_cfg::propagate();
//! }
//! ```
//!
//! Requires the consumer to depend on `td-database` directly — that's how
//! cargo populates the `DEP_TD_DATABASE_FEATURES_<KEY>` env vars this reads.

const FEATURES: &[&str] = &["sqlite", "postgres", "mysql"];

/// Read `DEP_TD_DATABASE_FEATURES_<KEY>` for each backend and re-emit as a
/// `cargo::rustc-cfg=<feature>`. Also registers each via `rustc-check-cfg` so
/// the compiler doesn't warn about the bare name being unrecognised, and
/// `rerun-if-env-changed` so the cfg state tracks td-database's feature state.
pub fn propagate() {
    for f in FEATURES {
        println!("cargo::rustc-check-cfg=cfg({f})");
        let env = format!("DEP_TD_DATABASE_FEATURES_{}", f.to_uppercase());
        println!("cargo::rerun-if-env-changed={env}");
        if std::env::var_os(&env).is_some() {
            println!("cargo::rustc-cfg={f}");
        }
    }
}
