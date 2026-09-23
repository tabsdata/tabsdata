//
// Copyright 2025 Tabsdata Inc.
//

use td_build::customizer::{Customization, Customizer};
use td_build::stamper::{Stamper, Stamping};

pub fn boot() {
    Customization::customize();
    if let Err(error) = Stamping::stamp() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
