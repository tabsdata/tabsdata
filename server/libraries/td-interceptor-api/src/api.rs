//
//  Copyright 2024 Tabs Data Inc.
//

use std::io::Error;

pub trait InterceptorPlugin {
    fn edition(&self) -> String;
    fn summary(&self) -> Result<String, Error>;
}
