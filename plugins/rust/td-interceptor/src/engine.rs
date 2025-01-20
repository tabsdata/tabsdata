//
//  Copyright 2024 Tabs Data Inc.
//

use std::io::Error;
use td_interceptor_api::api::InterceptorPlugin;

pub struct Interceptor;

impl InterceptorPlugin for Interceptor {
    fn edition(&self) -> String {
        "standard".to_string()
    }

    fn summary(&self) -> Result<String, Error> {
        Ok("td-interceptor-standard".to_string())
    }
}
