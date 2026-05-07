//
// Copyright 2026 Tabs Data Inc.
//

// Internal test worker for K Pop topology bring-up -- emulates an application
// worker (long-running, ticks every 30 seconds). Not a production binary.

use std::io::Write;
use std::thread::sleep;
use std::time::Duration;

const NAME: &str = "Dummy Application Worker";
const PERIOD: Duration = Duration::from_secs(30);

//noinspection DuplicatedCode
fn main() {
    println!("[{NAME}] starting");
    std::io::stdout().flush().ok();
    let mut tick: u64 = 0;
    loop {
        sleep(PERIOD);
        tick += 1;
        println!("[{NAME}] tick {tick}");
        std::io::stdout().flush().ok();
    }
}
