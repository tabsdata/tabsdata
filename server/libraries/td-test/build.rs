//
// Copyright 2025 Tabsdata Inc.
//

use td_build::customizer::{Customization, Customizer};

fn main() {
    Customization::customize();
    // Propagates `cfg(sqlite|postgres|mysql)` from td-database's enabled
    // features. The db-provider impls (db::sqlite::SqliteProvider, etc.) gate
    // on these so a provider only exists when its backend is linked in.
    td_build::backend_cfg::propagate();
}
