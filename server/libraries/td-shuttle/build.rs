//
// Copyright 2025 Tabs Data Inc.
//

mod boot;

use crate::boot::{Boot, Loader};
use td_build::customizer::{Customization, Customizer};

fn main() {
    Customization::customize();
    Boot::load();
}
