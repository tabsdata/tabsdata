//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

#[test]
fn is_alive_returns_true_for_current_process() {
    assert!(is_alive(std::process::id()));
}

#[test]
fn is_alive_returns_false_for_unlikely_pid() {
    // u32::MAX is reserved on every platform we run on; no live process can
    // have it. This avoids racing with arbitrary PID reuse.
    assert!(!is_alive(u32::MAX));
}
