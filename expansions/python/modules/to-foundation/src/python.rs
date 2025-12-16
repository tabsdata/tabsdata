//
// Copyright 2025 Tabs Data Inc.
//

use std::time::Instant;

fn main() {
    let start = Instant::now();
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed);
}
