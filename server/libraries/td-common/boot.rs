//
// Copyright 2025 Tabs Data Inc.
//

use td_build::customizer::{Customization, Customizer};
use td_build::stamper::{Stamper, Stamping};

pub fn boot() {
    Customization::customize();
    Stamping::stamp().unwrap();
}
