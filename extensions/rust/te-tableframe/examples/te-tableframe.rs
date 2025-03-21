//
// Copyright 2024 Tabs Data Inc.
//

use ta_tableframe::api::Extension;
use te_tableframe::engine::TableframeExtension;

fn main() {
    println!("Running tableframe extension (Standard)...");
    println!("   - {}", TableframeExtension.summary().unwrap());
    println!("Done running tableframe extension (Standard)...");
}
