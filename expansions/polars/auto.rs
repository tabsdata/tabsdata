//
// Copyright 2025 Tabs Data Inc.
//

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
    "Standard".to_string()
}

#[cfg(feature = "enterprise")]
fn edition() -> String {
    "Enterprise".to_string()
}

fn summary() -> String {
    TableFrameExtension.summary().unwrap()
}
