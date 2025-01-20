//
// Copyright 2024 Tabs Data Inc.
//

use crate::feature::check_cargo_features;

pub struct Verification;

pub trait Verifier {
    fn verify() {}
}

impl Verifier for Verification {
    fn verify() {
        check_cargo_features().unwrap();
    }
}
