//
// Copyright 2025 Tabs Data Inc.
//

use std::backtrace::Backtrace;
use std::panic;
use tracing_panic::panic_hook;

pub fn panic() {
    let system_panic_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        let _ = Backtrace::force_capture();
        panic_hook(panic_info);
        system_panic_hook(panic_info);
    }));
}
