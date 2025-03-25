//
// Copyright 2024 Tabs Data Inc.
//

use ta_tableframe::api::Extension;
use te_tableframe::engine::TableFrameExtension;

fn main() {
    println!("Executing te-tableframe...");
    println!("   - {}", TableFrameExtension.summary().unwrap());
    println!("Done executing te-tableframe...");
}
