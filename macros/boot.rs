//
// Copyright 2024 Tabsdata Inc.
//

use td_build::customizer::{Customization, Customizer};
use td_build::verifier::{Verification, Verifier};

pub fn boot() {
    Customization::customize();
    Verification::verify();
}
