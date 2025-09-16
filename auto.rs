//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use ta_tableframe::api::Extension;
use te_tableframe::engine::TableFrameExtension;

fn main() {
    println!("Name.......: {}", env!("CARGO_PKG_NAME"));
    println!("Version....: {}", env!("CARGO_PKG_VERSION"));
    println!("Edition....: {}", edition());
    println!("Description: {}", env!("CARGO_PKG_DESCRIPTION"));
    println!("Summary....: {}", summary());
}

#[cfg(not(feature = "enterprise"))]
fn edition() -> String {
    use ta_tableframe::api::OPEN_SOURCE;
    OPEN_SOURCE.to_string()
}

#[cfg(feature = "enterprise")]
fn edition() -> String {
    use ta_tableframe::api::ENTERPRISE;
    ENTERPRISE.to_string()
}

fn summary() -> String {
    TableFrameExtension.summary().unwrap()
}
