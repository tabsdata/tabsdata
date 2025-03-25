//
// Copyright 2024 Tabs Data Inc.
//

use ta_tableframe::api::Extension;
use te_tableframe::engine::TableFrameExtension;

fn main() {
    println!("Running tableframe extension (Standard)...");
    println!("   - {}", TableFrameExtension.summary().unwrap());
    println!("Done running tableframe extension (Standard)...");
}
