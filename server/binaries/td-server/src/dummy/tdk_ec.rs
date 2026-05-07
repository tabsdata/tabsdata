//
// Copyright 2026 Tabs Data Inc.
//

// Internal test worker for K Pop topology bring-up -- emulates an ephemeral
// worker (ticks every second, exits after 1 minute). Not a production binary.

use std::io::Write;
use std::thread::sleep;
use std::time::Duration;

const NAME: &str = "Dummy Ephemeral Worker";
const TICKS: u32 = 60;

//noinspection DuplicatedCode
fn main() {
    println!("[{NAME}] starting");
    std::io::stdout().flush().ok();
    for tick in 1..=TICKS {
        sleep(Duration::from_secs(1));
        println!("[{NAME}] tick {tick}/{TICKS}");
        std::io::stdout().flush().ok();
    }
    println!("[{NAME}] exiting");
    std::io::stdout().flush().ok();
}
