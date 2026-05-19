//
// Copyright 2025 Tabs Data Inc.
//

use td_build::customizer::{Customization, Customizer};

fn main() {
    Customization::customize();

    // Propagate enabled backend features:
    //   - `cargo::rustc-cfg=<f>` makes the bare `#[cfg(sqlite|postgres|mysql)]`
    //     form available inside *this* crate's source — one naming convention
    //     workspace-wide instead of `feature = "..."` here and `<f>` elsewhere.
    //   - `cargo::metadata=<f>=1` exposes the same state to direct dependents
    //     as `DEP_TD_DATABASE_FEATURES_<KEY>` env vars (via the `links` setting),
    //     which they translate into matching `cargo::rustc-cfg=<f>` in their
    //     own build scripts. Single source of truth for backend selection.
    for f in ["sqlite", "postgres", "mysql"] {
        println!("cargo::rustc-check-cfg=cfg({f})");
        let env = format!("CARGO_FEATURE_{}", f.to_uppercase());
        println!("cargo::rerun-if-env-changed={env}");
        if std::env::var_os(&env).is_some() {
            println!("cargo::rustc-cfg={f}");
            println!("cargo::metadata={f}=1");
        }
    }
}
