//
// Copyright 2024 Tabs Data Inc.
//

use crate::feature::set_cargo_features;

pub struct Customization;

pub trait Customizer {
    fn customize() {}
}

impl Customizer for Customization {
    fn customize() {
        set_cargo_features().unwrap();
    }
}
