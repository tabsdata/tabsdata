//
//  Copyright 2024 Tabs Data Inc.
//

use std::io::Error;

pub trait Extension {
    fn edition(&self) -> String;
    fn summary(&self) -> Result<String, Error>;
}
