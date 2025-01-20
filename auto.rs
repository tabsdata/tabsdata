//
// Copyright 2024 Tabs Data Inc.
//

use td_interceptor::engine::Interceptor;
use td_interceptor_api::api::InterceptorPlugin;

fn main() {
    println!("Name.......: {}", env!("CARGO_PKG_NAME"));
    println!("Version....: {}", env!("CARGO_PKG_VERSION"));
    println!("edition....: {}", edition());
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
    Interceptor.summary().unwrap()
}
