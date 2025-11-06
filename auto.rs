//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use te_system::edition::{Edition, TabsdataEdition};

fn main() {
    let edition = TabsdataEdition;
    println!("Name.......: {}", env!("CARGO_PKG_NAME"));
    println!("Version....: {}", env!("CARGO_PKG_VERSION"));
    println!("Edition....: {}", edition.name());
    println!("Description: {}", env!("CARGO_PKG_DESCRIPTION"));
    println!("Summary....: {}", edition.summary());
}
