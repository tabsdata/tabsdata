//
//  Copyright 2024 Tabs Data Inc.
//

use std::io::Error;

pub const OPEN_SOURCE: &str = "Open Source";
pub const ENTERPRISE: &str = "Enterprise";

pub trait Extension {
    fn edition(&self) -> String;
    fn summary(&self) -> Result<String, Error>;
}
