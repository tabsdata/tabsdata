//
// Copyright 2026 Tabsdata Inc.
//

use td_build::customizer::{Customization, Customizer};

fn main() {
    Customization::customize();
    td_build::backend_cfg::propagate();
}
