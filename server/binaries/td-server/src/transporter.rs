//
// Copyright 2024 Tabs Data Inc.
//

use td_common::attach::attach;
use td_shuttle::transporter::cli;

#[attach(signal = "transporter")]
fn main() {
    cli::run()
}
