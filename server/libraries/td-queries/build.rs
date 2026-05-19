//
// Copyright 2026 Tabs Data Inc.
//

use td_build::customizer::{Customization, Customizer};

fn main() {
    Customization::customize();
    td_build::backend_cfg::propagate();
}
