//
// Copyright 2024 Tabs Data Inc.
//

use td_common::attach::attach;
use td_process::launcher::hooks;
use td_shuttle::transporter::cli;

#[attach(signal = "transporter")]
fn main() {
    hooks::panic();

    cli::run()
}
