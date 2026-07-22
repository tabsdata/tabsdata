//
// Copyright 2025 Tabsdata Inc.
//

mod boot;

use crate::boot::{Boot, Loader};
use td_build::customizer::{Customization, Customizer};

fn main() {
    Customization::customize();
    Boot::load();
}
