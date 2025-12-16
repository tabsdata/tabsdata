//
// Copyright 2025 Tabs Data Inc.
//

use td_build::customizer::{Customization, Customizer};
use td_build::verifier::{Verification, Verifier};

pub fn boot() {
    Customization::customize();
    Verification::verify();
}
