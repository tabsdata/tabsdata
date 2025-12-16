//
// Copyright 2025 Tabs Data Inc.
//

use ta_system::edition::Edition;
use te_system::edition::TabsdataEdition;

fn main() {
    println!("Name.......: {}", env!("CARGO_PKG_NAME"));
    println!("Version....: {}", env!("CARGO_PKG_VERSION"));
    println!("Edition....: {}", edition());
    println!("Description: {}", env!("CARGO_PKG_DESCRIPTION"));
    println!("Summary....: {}", summary());
}

fn edition() -> String {
    TabsdataEdition.label().to_string()
}

fn summary() -> String {
    TabsdataEdition.summary().to_string()
}
