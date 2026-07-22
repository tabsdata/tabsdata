//
// Copyright 2025 Tabsdata Inc.
//

use td_common::about;
use td_common::attach::attach;

#[attach(signal = "td_about")]
pub fn main() {
    about::show_build_metadata();
}
