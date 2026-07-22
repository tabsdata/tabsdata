//
// Copyright 2025 Tabsdata Inc.
//

use std::time::Instant;

fn main() {
    let start = Instant::now();
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed);
}
