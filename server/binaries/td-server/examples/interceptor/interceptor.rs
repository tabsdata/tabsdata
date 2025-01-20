//
// Copyright 2024 Tabs Data Inc.
//

use td_interceptor::engine::Interceptor;
use td_interceptor_api::api::InterceptorPlugin;

fn main() {
    println!("Executing td-interceptor...");
    println!("   - {}", Interceptor.summary().unwrap());
    println!("Done executing td-interceptor...");
}
