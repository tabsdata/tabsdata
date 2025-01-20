//
// Copyright 2024 Tabs Data Inc.
//

use td_interceptor::engine::Interceptor;
use td_interceptor_api::api::InterceptorPlugin;

fn main() {
    println!("Running interceptor plugin (Standard)...");
    println!("   - {}", Interceptor.summary().unwrap());
    println!("Done running interceptor plugin (Standard)...");
}
